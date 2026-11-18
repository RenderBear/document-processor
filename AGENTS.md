## Critical rules — never violate

- **Never push to the remote.** No `git push` under any circumstances. Committing locally is fine; publishing is the user's decision.
- **Never commit `.env` files** or any file containing secrets, credentials, API keys, or tokens. If a change would stage one, stop and tell the user.
- **Never run your own browser** - Request the user to verify UI changes.

