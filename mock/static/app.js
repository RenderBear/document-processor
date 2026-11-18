const fileInput = document.querySelector("#file-input");
const uploadButton = document.querySelector("#upload-button");
const clearButton = document.querySelector("#clear-button");
const clearCompletedButton = document.querySelector("#clear-completed");
const selectionSummary = document.querySelector("#selection-summary");
const selectionCount = document.querySelector("#selection-count");
const progressSummary = document.querySelector("#progress-summary");
const emptyState = document.querySelector("#empty-state");
const emptyCopy = document.querySelector("#empty-copy");
const fileList = document.querySelector("#file-list");
const queueList = document.querySelector("#queue-list");
const queueEmpty = document.querySelector("#queue-empty");
const producerInputs = Array.from(document.querySelectorAll('input[name="producer-kind"]'));
const contextField = document.querySelector("#context-field");
const contextInput = document.querySelector("#context-id");
const contextLabel = document.querySelector("#context-label");
const contextHelp = document.querySelector("#context-help");
const queueSearch = document.querySelector("#queue-search");

// Remove retired markup if a browser restored an older page shell from cache.
document.querySelector(".status-pane")?.remove();

const producerLabels = {
  chat: "Chat",
  rag_ingestion: "RAG",
  agent: "Agent",
  workflow: "Workflow",
};
const contextLabels = {
  rag_ingestion: ["Reference", "Identifies this bulk upload for indexing."],
  agent: ["Reference", "Identifies this bulk upload for the agent."],
  workflow: ["Reference", "Identifies this bulk upload for the workflow."],
};
const persistentProducerKinds = new Set(["rag_ingestion", "agent", "workflow"]);
const contextIdPattern = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,99}$/;
const activeKindStorageKey = "document-processing.active-kind";
const contextStoragePrefix = "document-processing.context.";

function readLocalStorage(key) {
  try {
    return window.localStorage.getItem(key);
  } catch {
    return null;
  }
}

function writeLocalStorage(key, value) {
  try {
    window.localStorage.setItem(key, value);
  } catch {
    // Storage can be unavailable in privacy-restricted browser contexts.
  }
}

function newSessionId() {
  if (window.crypto?.randomUUID) return `chat-${window.crypto.randomUUID()}`;
  return `chat-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

const savedProducerKind = readLocalStorage(activeKindStorageKey);
let activeProducerKind = producerLabels[savedProducerKind] ? savedProducerKind : "chat";
const chatContextId = newSessionId();
window.addEventListener("pagehide", () => {
  navigator.sendBeacon(`/artifact-sessions/${encodeURIComponent(chatContextId)}/close`);
});
let selectedItems = [];
let restoredItems = [];
let queueFilter = "";
let uploading = false;
let uploadingCount = 0;
let restoring = false;
let restoreError = null;
let restoreGeneration = 0;
// Monotonic ordering so the producer-agnostic queue can show newest jobs first.
// Restored activity is numbered oldest-to-newest at load; session uploads take
// higher numbers afterwards, so this session's work sorts above prior activity.
let sortSeq = 0;
const eventSources = new Map();
const uploadRequests = new Map();

function formatBytes(bytes) {
  if (bytes === null || bytes === undefined) return "Persisted activity";
  if (bytes === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  const unitIndex = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  const value = bytes / 1024 ** unitIndex;
  return `${value.toFixed(unitIndex === 0 || value >= 10 ? 0 : 1)} ${units[unitIndex]}`;
}

function normalizedContextInput() {
  return contextInput.value.trim() || "default";
}

function activeContextId() {
  return activeProducerKind === "chat" ? chatContextId : normalizedContextInput();
}

function itemContext(item) {
  return item.producerKind === "chat" ? "Current session" : item.contextId;
}

function matchesActiveCategory(item) {
  return item.producerKind === activeProducerKind && item.contextId === activeContextId();
}

function visibleSelectedItems() {
  return selectedItems.filter(matchesActiveCategory);
}

function allItems() {
  return [...restoredItems, ...selectedItems];
}

function recordHistory(item, key, state, message) {
  item.history ??= [];
  item.historyKeys ??= new Set();
  if (item.historyKeys.has(key)) return;
  item.historyKeys.add(key);
  item.history.push({
    state,
    message,
    time: new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }),
  });
}

function stateMessage(item, state) {
  if (state === "queued") return "The backend accepted the job and placed it in the durable queue.";
  if (state === "processing") return "A processor is working on the document.";
  if (state === "completed") return "Processing completed successfully.";
  if (state === "failed") return "Processing ended without a result.";
  return `The backend reported ${state} for job ${item.jobId}.`;
}

function responseDetail(responseText, fallback) {
  try {
    const parsed = JSON.parse(responseText);
    if (typeof parsed.detail === "string") return parsed.detail;
  } catch {
    // Use the supplied fallback for non-JSON responses.
  }
  return fallback;
}

function safeItemId(key) {
  return `job-${key.replace(/[^a-zA-Z0-9_-]/g, "-")}`;
}

function applyState(item, state) {
  item.state = state;
  item.terminal = ["completed", "failed"].includes(state);
  if (state === "queued") {
    Object.assign(item, { percent: 35, statusText: "Queued", statusName: "queued", progressText: "Waiting" });
  } else if (state === "processing") {
    Object.assign(item, { percent: 70, statusText: "Processing", statusName: "processing", progressText: "In progress" });
  } else if (state === "completed") {
    Object.assign(item, { percent: 100, statusText: "Completed", statusName: "completed", progressText: "Done" });
  } else if (state === "failed") {
    Object.assign(item, { percent: 100, statusText: "Failed", statusName: "error", progressText: "Processing failed" });
  }
}

function configureCategory() {
  producerInputs.forEach((input) => { input.checked = input.value === activeProducerKind; });
  contextLabel.textContent = "Reference";
  if (activeProducerKind === "chat") {
    // Chat has no user reference (it uses an automatic session), so the field
    // stays visible but greyed out and disabled.
    contextInput.disabled = true;
    contextInput.value = "";
    contextInput.placeholder = "Automatic for chat";
    contextHelp.textContent = "Chat uses an automatic session reference.";
  } else {
    contextInput.disabled = false;
    contextInput.placeholder = "";
    const savedContext = readLocalStorage(`${contextStoragePrefix}${activeProducerKind}`);
    contextInput.value = contextIdPattern.test(savedContext ?? "") ? savedContext : "default";
    [contextLabel.textContent, contextHelp.textContent] = contextLabels[activeProducerKind];
  }
  emptyCopy.textContent = "No jobs yet.";
}

function renderQueue() {
  // The compose list (files staged but not yet sent) belongs to the active
  // producer/context. The processing queue is producer-agnostic: it shows every
  // submitted job across producers and contexts, plus all restored activity,
  // newest first — mirroring the single shared scheduler queue.
  const queued = visibleSelectedItems().filter((item) => !item.submitted);
  const jobs = [
    ...restoredItems,
    ...selectedItems.filter((item) => item.submitted),
  ].sort((a, b) => (b.order ?? 0) - (a.order ?? 0));

  queueList.replaceChildren();
  queueEmpty.hidden = queued.length > 0;
  selectionSummary.hidden = queued.length === 0;
  selectionCount.textContent = `${queued.length} file${queued.length === 1 ? "" : "s"}`;
  uploadButton.disabled = queued.length === 0 || uploading;
  queued.forEach((item) => {
    const row = document.createElement("li");
    row.className = "queue-item";
    const glyph = document.createElement("span");
    glyph.className = "file-glyph";
    glyph.textContent = item.filename.split(".").pop()?.slice(0, 4) || "file";
    const copy = document.createElement("span");
    copy.className = "queue-file-copy";
    const name = document.createElement("span");
    name.className = "queue-file-name";
    name.textContent = item.filename;
    name.title = item.filename;
    const size = document.createElement("span");
    size.className = "queue-file-size";
    size.textContent = formatBytes(item.size);
    copy.append(name, size);
    const ready = document.createElement("span");
    ready.className = `queue-state ${item.statusName}`;
    ready.textContent = item.statusText;
    row.append(glyph, copy, ready);
    queueList.append(row);
  });

  fileList.replaceChildren();
  // Lexical filter over the processing queue: match the document name or the
  // reference that identifies the upload. Counts and summaries still reflect the full queue.
  const visibleJobs = queueFilter
    ? jobs.filter(
        (item) =>
          item.filename.toLowerCase().includes(queueFilter) ||
          (item.contextId ?? "").toLowerCase().includes(queueFilter),
      )
    : jobs;
  emptyState.hidden = visibleJobs.length > 0;
  if (jobs.length && !visibleJobs.length) {
    emptyCopy.textContent = `No jobs match “${queueSearch.value.trim()}”.`;
  } else if (!jobs.length) {
    emptyCopy.textContent = "No jobs yet.";
  }

  const active = jobs.filter((item) => ["queued", "processing"].includes(item.state)).length;
  clearCompletedButton.disabled = !jobs.some((item) => item.state === "completed");
  if (uploading) {
    progressSummary.textContent = `Sending ${uploadingCount} file${uploadingCount === 1 ? "" : "s"}`;
  } else if (restoring) {
    progressSummary.textContent = "Restoring saved activity";
  } else if (restoreError) {
    progressSummary.textContent = restoreError;
  } else if (active) {
    progressSummary.textContent = `${active} active job${active === 1 ? "" : "s"}`;
  } else if (jobs.length) {
    progressSummary.textContent = `${jobs.length} job${jobs.length === 1 ? "" : "s"}`;
  } else {
    progressSummary.textContent = "Waiting for files";
  }

  visibleJobs.forEach((item) => {
    const card = document.createElement("li");
    card.className = "file-card";
    card.id = safeItemId(item.key);
    card.dataset.status = item.statusName;

    const top = document.createElement("div");
    top.className = "file-card-top";
    const identity = document.createElement("span");
    identity.className = "file-identity";
    const name = document.createElement("span");
    name.className = "file-name";
    name.textContent = item.filename;
    name.title = item.filename;
    const jobId = document.createElement("code");
    jobId.className = "job-id";
    jobId.textContent = item.jobId ? `Job ${item.jobId}` : "Job ID unknown";
    jobId.title = item.jobId ?? "The browser did not receive a backend job ID";
    identity.append(name, jobId);
    const status = document.createElement("span");
    status.className = `file-status ${item.statusName}`;
    status.textContent = item.statusText;
    if (["uploading", "processing"].includes(item.state)) {
      const loader = document.createElement("span");
      loader.className = "status-loader";
      loader.setAttribute("aria-hidden", "true");
      status.prepend(loader);
    }
    if (item.monitoringStopped) status.title = "Last known backend state; live updates are stopped.";
    top.append(identity, status);

    const track = document.createElement("div");
    track.className = "progress-track";
    const fill = document.createElement("div");
    fill.className = "progress-fill";
    fill.style.width = `${item.percent}%`;
    fill.setAttribute("role", "progressbar");
    fill.setAttribute("aria-label", `${item.filename} progress`);
    fill.setAttribute("aria-valuemin", "0");
    fill.setAttribute("aria-valuemax", "100");
    fill.setAttribute("aria-valuenow", String(item.percent));
    track.append(fill);

    const meta = document.createElement("div");
    meta.className = "file-meta";
    const size = document.createElement("span");
    size.textContent = `${producerLabels[item.producerKind] ?? item.producerKind} · ${itemContext(item)} · ${formatBytes(item.size)}`;
    size.title = `Destination: ${item.contextId}`;
    const percentage = document.createElement("span");
    percentage.className = "percentage";
    percentage.textContent = item.monitoringStopped ? "Updates stopped" : item.progressText;
    meta.append(size, percentage);

    const controls = document.createElement("div");
    controls.className = "job-controls";
    const history = document.createElement("details");
    history.className = "job-history";
    const summary = document.createElement("summary");
    const historyCount = item.history?.length ?? 0;
    summary.textContent = `History (${historyCount})`;
    history.append(summary);
    const historyList = document.createElement("ol");
    historyList.className = "job-history-list";
    if (!historyCount) {
      const empty = document.createElement("li");
      empty.className = "job-history-empty";
      empty.textContent = "Waiting for backend history.";
      historyList.append(empty);
    } else {
      item.history.forEach((entry) => {
        const row = document.createElement("li");
        row.className = "job-history-row";
        const heading = document.createElement("span");
        heading.className = `history-state ${entry.state}`;
        heading.textContent = entry.state;
        const time = document.createElement("time");
        time.textContent = entry.time;
        const message = document.createElement("p");
        message.textContent = entry.message;
        row.append(heading, time, message);
        historyList.append(row);
      });
    }
    history.append(historyList);

    const actions = document.createElement("div");
    actions.className = "job-actions";
    if (item.file && item.state === "failed") {
      const retry = document.createElement("button");
      retry.type = "button";
      retry.className = "job-action retry";
      retry.textContent = "Retry";
      retry.disabled = uploading;
      retry.addEventListener("click", () => retryItem(item));
      actions.append(retry);
    } else if (!item.terminal && !item.monitoringStopped) {
      const stop = document.createElement("button");
      stop.type = "button";
      stop.className = "job-action stop";
      stop.textContent = "Stop";
      stop.title = item.jobId
        ? "Stop live updates in this browser. Backend processing continues."
        : "Stop this browser upload.";
      stop.addEventListener("click", () => stopItem(item));
      actions.append(stop);
    }
    controls.append(history, actions);

    card.append(top);
    if (!item.terminal) card.append(track);
    card.append(meta, controls);
    fileList.append(card);
  });
}

function chooseFiles(files) {
  if (uploading) return;
  const nonce = Date.now();
  const newItems = Array.from(files).map((file, index) => ({
    key: `upload-${nonce}-${index}`,
    file,
    filename: file.name,
    size: file.size,
    producerKind: activeProducerKind,
    contextId: activeContextId(),
    order: ++sortSeq,
    state: "ready",
    percent: 0,
    statusText: "Ready",
    statusName: "ready",
    progressText: "0%",
    submitted: false,
    terminal: false,
    history: [],
    historyKeys: new Set(),
  }));
  newItems.forEach((item) => recordHistory(item, "ready", "ready", "Ready to submit."));
  selectedItems = [...selectedItems, ...newItems];
  renderQueue();
}

function updateItem(key, updates) {
  const item = allItems().find((candidate) => candidate.key === key);
  if (!item) return;
  Object.assign(item, updates);
  renderQueue();
}

function watchJob(job, key) {
  eventSources.get(key)?.close();
  const eventSource = new EventSource(job.events_url);
  eventSources.set(key, eventSource);

  eventSource.addEventListener("state", (event) => {
    let update;
    try {
      update = JSON.parse(event.data);
    } catch {
      eventSource.close();
      eventSources.delete(key);
      const item = allItems().find((candidate) => candidate.key === key);
      if (!item) return;
      Object.assign(item, { percent: 100, statusText: "Failed", statusName: "error", progressText: "Invalid event", state: "failed", terminal: true });
      recordHistory(item, `invalid-event:${key}`, "error", "The backend sent an unreadable event.");
      renderQueue();
      return;
    }

    const item = allItems().find((candidate) => candidate.key === key);
    if (!item) return;
    applyState(item, update.state);
    recordHistory(
      item,
      `event:${update.sequence}`,
      update.state,
      stateMessage(item, update.state),
    );
    renderQueue();
    if (["completed", "failed"].includes(update.state)) {
      eventSource.close();
      eventSources.delete(key);
    }
  });

  eventSource.addEventListener("error", () => {
    if (eventSource.readyState !== EventSource.CLOSED) {
      updateItem(key, { statusText: "Reconnecting", statusName: "processing", progressText: "Event stream" });
      const item = allItems().find((candidate) => candidate.key === key);
      if (item) {
        recordHistory(
          item,
          `stream-interrupted:${item.jobId ?? key}`,
          "warning",
          "Live updates were interrupted. The browser is reconnecting automatically.",
        );
        renderQueue();
      }
    }
  });
}

function uploadItem(item) {
  return new Promise((resolve) => {
    const request = new XMLHttpRequest();
    const formData = new FormData();
    formData.append("file", item.file, item.filename);
    formData.append("producer_kind", item.producerKind);
    formData.append("context_id", item.contextId);

    request.open("POST", "/jobs");
    uploadRequests.set(item.key, request);
    Object.assign(item, {
      submitted: true,
      state: "uploading",
      terminal: false,
      monitoringStopped: false,
      acceptanceUnknown: false,
      percent: 0,
      statusText: "Uploading",
      statusName: "uploading",
      progressText: "Sending",
    });
    recordHistory(item, `uploading:${Date.now()}`, "uploading", "Sending the document to the backend.");
    renderQueue();

    request.upload.addEventListener("progress", (event) => {
      if (event.lengthComputable) {
        updateItem(item.key, {
          percent: Math.round((event.loaded / event.total) * 25),
          statusText: "Uploading",
          statusName: "uploading",
        });
      }
    });

    request.addEventListener("load", () => {
      uploadRequests.delete(item.key);
      if (request.status !== 202) {
        const detail = responseDetail(request.responseText, "Request rejected");
        const explanation = request.status === 503
          ? `${detail} The request was not accepted and no backend job was created. It is safe to retry.`
          : `${detail} No backend job was created.`;
        updateItem(item.key, {
          state: "failed",
          terminal: true,
          percent: 100,
          statusText: `HTTP ${request.status}`,
          statusName: "error",
          progressText: detail,
        });
        recordHistory(item, `submit-error:${item.key}:${request.status}:${Date.now()}`, "failed", explanation);
        renderQueue();
        resolve(false);
        return;
      }

      let job;
      try {
        job = JSON.parse(request.responseText);
      } catch {
        Object.assign(item, {
          state: "failed",
          terminal: true,
          acceptanceUnknown: true,
          percent: 100,
          statusText: "Unknown",
          statusName: "error",
          progressText: "Invalid backend response",
        });
        recordHistory(
          item,
          `invalid-response:${item.key}`,
          "error",
          "The backend returned an unreadable response. Job acceptance is unknown; check the backend queue before retrying.",
        );
        renderQueue();
        resolve(false);
        return;
      }
      Object.assign(item, { jobId: job.job_id, eventsUrl: job.events_url, acceptanceUnknown: false });
      applyState(item, job.state);
      recordHistory(
        item,
        `accepted:${item.jobId}`,
        "accepted",
        `Backend job ${item.jobId} created in ${producerLabels[item.producerKind]} · ${itemContext(item)}.`,
      );
      renderQueue();
      if (job.terminal) {
        resolve(job.state === "completed");
      } else {
        watchJob(job, item.key);
        resolve(true);
      }
    });

    request.addEventListener("error", () => {
      uploadRequests.delete(item.key);
      Object.assign(item, {
        state: "failed",
        terminal: true,
        acceptanceUnknown: true,
        percent: 100,
        statusText: "Unknown",
        statusName: "error",
        progressText: "Acceptance unknown",
      });
      recordHistory(
        item,
        `network-error:${item.key}:${Date.now()}`,
        "failed",
        "The browser did not receive a response. Job acceptance is unknown; check the backend queue before retrying.",
      );
      renderQueue();
      resolve(false);
    });

    request.addEventListener("abort", () => {
      uploadRequests.delete(item.key);
      Object.assign(item, {
        state: "failed",
        terminal: true,
        acceptanceUnknown: true,
        percent: 100,
        statusText: "Stopped",
        statusName: "error",
        progressText: "Acceptance unknown",
      });
      recordHistory(
        item,
        `upload-stopped:${Date.now()}`,
        "warning",
        "The browser stopped waiting for the upload response. Job acceptance is unknown; check the backend queue before retrying.",
      );
      renderQueue();
      resolve(false);
    });

    request.send(formData);
  });
}

function setUploadControls(disabled) {
  clearButton.disabled = disabled;
  producerInputs.forEach((input) => { input.disabled = disabled; });
  contextInput.disabled = disabled;
}

async function sendItems(items) {
  if (!items.length || uploading) return;
  uploading = true;
  uploadingCount = items.length;
  setUploadControls(true);
  renderQueue();
  await Promise.all(items.map(uploadItem));
  uploading = false;
  uploadingCount = 0;
  setUploadControls(false);
  configureCategory();
  renderQueue();
}

async function startUpload() {
  await sendItems(visibleSelectedItems().filter((item) => !item.submitted));
}

function stopItem(item) {
  const request = uploadRequests.get(item.key);
  if (request) {
    request.abort();
    return;
  }
  eventSources.get(item.key)?.close();
  eventSources.delete(item.key);
  item.monitoringStopped = true;
  recordHistory(
    item,
    `updates-stopped:${Date.now()}`,
    "stopped",
    "Live updates stopped in this browser. Backend processing continues.",
  );
  renderQueue();
}

async function retryItem(item) {
  if (uploading || !item.file) return;
  if (item.acceptanceUnknown && !window.confirm(
    "Job acceptance is unknown. Check the backend queue first. Retry this file now?",
  )) return;
  eventSources.get(item.key)?.close();
  eventSources.delete(item.key);
  recordHistory(
    item,
    `retry:${Date.now()}`,
    "retrying",
    item.jobId
      ? `Resubmitting the source file as a new job after ${item.jobId}.`
      : "Resubmitting the source file to the backend.",
  );
  Object.assign(item, {
    jobId: null,
    eventsUrl: null,
    monitoringStopped: false,
    acceptanceUnknown: false,
  });
  await sendItems([item]);
}

function closeRestoredStreams() {
  restoredItems.forEach((item) => {
    eventSources.get(item.key)?.close();
    eventSources.delete(item.key);
  });
}

async function restoreJobs() {
  const generation = ++restoreGeneration;
  closeRestoredStreams();
  restoredItems = [];
  restoreError = null;
  restoring = true;
  renderQueue();
  // Producer-agnostic restore: request every persisted projection, regardless of
  // producer or context. The backend still supports scoped filters; the page just
  // does not use them, because the queue reflects the one shared scheduler queue.
  try {
    const response = await fetch("/jobs", { headers: { Accept: "application/json" } });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const body = await response.json();
    if (generation !== restoreGeneration) return;
    const sessionJobIds = new Set(
      selectedItems.filter((item) => item.jobId).map((item) => item.jobId),
    );
    restoredItems = body.jobs.filter((job) => !sessionJobIds.has(job.job_id)).map((job) => {
      const item = {
        key: `restored-${job.job_id}`,
        filename: job.filename,
        size: null,
        producerKind: job.producer_kind,
        contextId: job.context_id,
        order: ++sortSeq,
        jobId: job.job_id,
        eventsUrl: job.events_url,
        terminal: job.terminal,
        submitted: true,
        history: [],
        historyKeys: new Set(),
      };
      applyState(item, job.state);
      return item;
    });
    restoredItems.forEach((item) => {
      watchJob({ events_url: item.eventsUrl }, item.key);
    });
  } catch {
    if (generation === restoreGeneration) {
      restoreError = "Saved activity is temporarily unavailable";
    }
  } finally {
    if (generation === restoreGeneration) {
      restoring = false;
      renderQueue();
    }
  }
}

fileInput.addEventListener("change", () => {
  chooseFiles(fileInput.files);
  fileInput.value = "";
});
uploadButton.addEventListener("click", startUpload);
clearButton.addEventListener("click", () => {
  const removed = selectedItems.filter(
    (item) => matchesActiveCategory(item) && !item.submitted,
  );
  removed.forEach((item) => {
    eventSources.get(item.key)?.close();
    eventSources.delete(item.key);
  });
  selectedItems = selectedItems.filter(
    (item) => !matchesActiveCategory(item) || item.submitted,
  );
  fileInput.value = "";
  renderQueue();
});

queueSearch.addEventListener("input", () => {
  queueFilter = queueSearch.value.trim().toLowerCase();
  renderQueue();
});

clearCompletedButton.addEventListener("click", () => {
  // Eject completed jobs from the producer-agnostic queue, closing any lingering
  // streams. Backend records are untouched; this only clears the browser view.
  const isCompleted = (item) => item.state === "completed";
  [...restoredItems, ...selectedItems].filter(isCompleted).forEach((item) => {
    eventSources.get(item.key)?.close();
    eventSources.delete(item.key);
  });
  restoredItems = restoredItems.filter((item) => !isCompleted(item));
  selectedItems = selectedItems.filter((item) => !isCompleted(item));
  renderQueue();
});

producerInputs.forEach((input) => {
  input.addEventListener("change", () => {
    if (persistentProducerKinds.has(activeProducerKind)) {
      writeLocalStorage(`${contextStoragePrefix}${activeProducerKind}`, normalizedContextInput());
    }
    activeProducerKind = input.value;
    writeLocalStorage(activeKindStorageKey, activeProducerKind);
    configureCategory();
    // Only the compose target changes; the producer-agnostic queue is untouched.
    renderQueue();
  });
});

contextInput.addEventListener("change", () => {
  if (!contextInput.checkValidity()) {
    contextInput.reportValidity();
    return;
  }
  contextInput.value = normalizedContextInput();
  writeLocalStorage(`${contextStoragePrefix}${activeProducerKind}`, contextInput.value);
  configureCategory();
  renderQueue();
});
configureCategory();
restoreJobs();
