# COEN 366 — Campus Emergency Notification and Response System (CENRS)

**Course:** COEN 366 — Communication Networks and Protocols, Fall 2026  
**Language:** Python  
**Current stage:** Phase 1 — UDP registration and subscriptions

## Project overview

CENRS is a campus emergency notification system developed for COEN 366. It uses a central Emergency Server and multiple clients. Clients register as a `SUBSCRIBER`, `PUBLISHER`, or `BOTH`, and subscribers choose which campus locations and alert categories they want to follow.

The full project will use **UDP** for registration, subscriptions, emergency alerts, acknowledgements, updates, and status reports. It will use **TCP** for direct publisher-to-subscriber attachment downloads and for recovering the current active-alert list from the server.

We chose **JSON messages encoded as UTF-8 bytes** for the shared application-layer protocol.

## Current progress

**Implemented in the current Phase 1 starter:**
- Shared JSON message building, encoding, decoding, and validation in `common/protocol.py`.
- Thread-safe request-number generation and a CRC32 checksum helper.
- A UDP server that receives client messages and dispatches them to the appropriate handler.
- Registration and subscription handling, including invalid-request responses.
- Basic JSON-file storage for registered users and subscriptions.
- A small demo client that sends registration, subscription, unsubscribe, and deregistration requests.

**Verified so far:**
- The demo client received `REGISTERED` after registering Gianluca.
- The demo client received `SUBSCRIBED` for `HALL/FIRE`.
- The existing **15 protocol unit tests** passed.

**Still to verify:**
- Confirm from server logs or stored state that `UNSUBSCRIBE` and `DE-REGISTER` were processed. The demo client does not wait for success responses for these operations.
- Test persistence and re-registration after a server restart.
- Complete additional server/storage integration tests.

**Not implemented yet:** Emergency publishing, notification acknowledgements and retries, incident updates and closure, safety reports, TCP synchronization, and peer-to-peer file transfers.

## Project structure

```text
Coen366-project/
├── common/
│   ├── __init__.py
│   ├── config.py
│   ├── log.py
│   └── protocol.py         # JSON message format and helpers
├── server/
│   ├── __init__.py
│   ├── server.py           # UDP socket and request handlers
│   └── storage.py          # JSON-file persistence
├── client/
│   ├── __init__.py
│   └── demo_client.py      # Temporary Phase 1 test client
├── tests/
│   ├── __init__.py
│   └── test_protocol.py    # Protocol unit tests
├── data/                   # Created/used at runtime (not committed)
├── .gitignore
└── README.md
```

Other modules and tests will be added as later phases are implemented.

## Requirements

- Python 3 (development is currently being done with Python 3.13).
- No external Python packages are required for the current Phase 1 starter; it uses Python's standard library.
- IntelliJ IDEA with Python support, PyCharm, VS Code, or a terminal.

## Run the project locally

Run these commands **from the project root** (`Coen366-project`), not from inside `server/` or `client/`.

### Terminal 1 — Start the UDP server

```bash
python3 -m server.server
```

The default local test address is `127.0.0.1:5000`. Leave the server running.

### Terminal 2 — Run the demo client

```bash
python3 -m client.demo_client
```

The demo uses the username `Gianluca` and the local UDP port `5001`. It sends, in order:

1. `REGISTER` → expects `REGISTERED`.
2. `SUBSCRIBE` to `HALL/FIRE` → expects `SUBSCRIBED`.
3. `UNSUBSCRIBE` from `HALL/FIRE` → sends without waiting for a response.
4. `DE-REGISTER` → sends without waiting for a response.

Check **Terminal 1** for server-side logs of the last two operations.

### Terminal 3 — Run unit tests

```bash
python3 -m unittest discover -s tests -v
```

At the time of this README, the existing `test_protocol.py` contains **15 passing tests**. Additional integration tests may increase that count.

## JSON protocol

All client/server messages use the helpers in `common/protocol.py`:

- `build_message(...)` — create a message dictionary.
- `validate_message(...)` — check message type and required fields.
- `encode_message(...)` — convert a dictionary into UTF-8 JSON bytes for sending.
- `decode_message(...)` — convert received bytes back into a validated dictionary.
- `next_request_id()` / `RqCounter` — generate request numbers.
- `crc32_hex(...)` — calculate a CRC32 checksum for later file transfers.

Example registration message:

```json
{
  "type": "REGISTER",
  "rq": 1,
  "name": "Gianluca",
  "role": "SUBSCRIBER",
  "ip": "127.0.0.1",
  "udp_port": 5001,
  "tcp_port": 6001
}
```

Use **JSON**, not the old space-separated message format. Do not change protocol field names or add new response types without coordinating with the team, because the clients and server must agree on the same schemas.

## Persistence and logging

The Phase 1 server uses a local JSON file at `data/server_state.json` for user and subscription data. This is **runtime data**, so the directory is ignored by Git. The server recreates it as needed.

To check the current data while the server is running, inspect the JSON file. To test that information survives a restart, register and subscribe a user **without deregistering**, restart the server, and inspect the restored state. The default demo deregisters its user at the end, so it is not a persistence test by itself.

The server prints received messages, sent replies, and errors in its terminal. These logs are useful for the final demonstration.

## Next development phases

- **Phase 2:** Publish and distribute alerts; update and close incidents; implement `ALERT_ACK`, retransmissions for CRITICAL alerts and `ALL_CLEAR`, duplicate detection, and user safety reports.
- **Phase 3:** Add UDP attachment discovery, direct peer-to-peer TCP downloads with checksums, TCP active-alert synchronization, crash recovery, and integrated failure tests.
- **Final:** Complete the report, test cases, and full multi-client demonstration.

## Team workflow

1. Work on a separate Git branch for each feature.
2. Keep the shared JSON protocol consistent across the server and client.
3. Run `python3 -m unittest discover -s tests -v` before committing.
4. Push your feature branch and create a pull request for review.
5. Do not commit temporary runtime data, virtual environments, passwords, or editor-specific files.

**Note:** This repository is a work in progress, not the finished COEN 366 submission.
