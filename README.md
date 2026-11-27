# COEN 366 — Campus Emergency Notification and Response System (CENRS)

**Course:** COEN 366 — Communication Networks and Protocols, Fall 2026  
**Language:** Python  
**Current stage:** Phase 3 — attachment discovery and P2P TCP transfer done; `GET_ACTIVE_ALERTS` and recovery still to do

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
│   ├── client.py           # Client class: UDP requests, replies, alerts, ACKs
│   ├── cli.py              # Command shell (main thread): parses typed commands, calls Client
│   ├── transfer.py         # Phase 3 P2P attachment transfer over TCP
│   └── demo_client.py      # Temporary Phase 1 test client
├── tests/
│   ├── __init__.py
│   └── test_protocol.py    # Protocol unit tests
├── data/                   # Created/used at runtime (not committed)
├── .gitignore
└── README.md
```

Other modules and tests will be added as later phases are implemented.

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

## Phase 2 — Emergency notification and reliability (server side)

Phase 2 adds alert publishing, distribution to matching subscribers, ACK timers with
retransmission, incident update and close, and user status reports. Nothing in Phase 1
was changed: `common/protocol.py` and `client/demo_client.py` are untouched, and
`server/server.py` and `server/storage.py` only gained new entries and methods.

### New and extended files

| File | Role |
|---|---|
| `common/config.py` | Added `LOSS_RATE` (simulated loss of fan-out packets, default 0.0). `ACK_TIMEOUT` and `MAX_ATTEMPTS` were already there. |
| `server/storage.py` | Added incidents, a persisted incident counter (`INC001`, `INC002`, ...), ACK delivery states and status reports, all behind the existing lock and atomic save. |
| `server/reliability.py` | `TimerManager`: one `threading.Timer` per pending delivery, retransmission, duplicate ACK detection. |
| `server/handlers/incidents.py` | `PUBLISH_ALERT`, `UPDATE_ALERT`, `CLOSE_ALERT`, subscription matching with the `ALL` wildcard, fan-out. |
| `server/handlers/acks.py` | `ALERT_ACK`. |
| `server/handlers/status.py` | `STATUS_REPORT`. |
| `server/server.py` | Registers the new handlers in the existing dispatch dictionary, creates the `TimerManager`, adds the Phase 2 denial replies. |
| `client/phase2_test_client.py` | Small UDP peer helper used by the demo. |
| `tests/test_reliability.py` | 14 in-process tests (no sockets). |
| `tests/run_phase2_demo.py` | Scripted multi-client demonstration. |

### Behaviour

- Only a registered `PUBLISHER` or `BOTH` can publish. The reply `ALERT_ACCEPTED` is sent first, then an `ALERT` with a fresh server RQ# goes to every registered `SUBSCRIBER`/`BOTH` whose subscription matches (`ALL` matches any location or category).
- `CRITICAL` alerts, `CRITICAL` updates and every `ALL_CLEAR` require an `ALERT_ACK` echoing the RQ# of the message received. Without an ACK within `ACK_TIMEOUT` the identical packet is resent, up to `MAX_ATTEMPTS` total transmissions, then the delivery is recorded as `UNCONFIRMED`. A duplicate or stale ACK is logged and ignored.
- Only the publisher that created an incident may update or close it, and only while it is `ACTIVE`. Each update or close increments the version.
- `STATUS_REPORT` is accepted from a registered client for an `ACTIVE` incident; only the latest status per (incident, client) is kept.
- Denial reasons: `Not_Registered`, `Unauthorized_Role`, `Unknown_Incident`, `Incident_Closed`, `Not_Owner`, `Name_Mismatch`. Malformed messages are handled by the Phase 1 code.
- Direct replies (`ALERT_ACCEPTED`, `ALERT_DENIED`, `ALERT_UPDATED`, `ALERT_CLOSED`, `STATUS_RECORDED`, `STATUS_REJECTED`) are never subject to `LOSS_RATE`; only fan-out messages and retransmissions are.

### Assumptions

- Timeout 3 s and 3 total attempts (`common/config.py`).
- A client is identified by its UDP address, as in Phase 1. After a server restart the online table is empty, so a client must re-register before publishing or reporting; otherwise it receives `Not_Registered`.
- Fan-out targets every registered user on disk whose role and subscription match.
- `ALL_CLEAR` carries the message `Incident closed: <reason>` and also reaches every client that received the original `ALERT`.
- Timers do not survive a restart. On load, deliveries still `PENDING` are rewritten as `UNCONFIRMED_RESTART` and the count is logged.

## Interactive client (`client/client.py` + `client/cli.py`)

`client.py` holds the `Client` class (sockets, threads, protocol operations). `cli.py` is the
command shell: it parses what the user types, calls `Client` methods and prints the results.
`python -m client.client` still works and starts the shell (same as `python -m client.cli`).

One process = one user. Start the server, then one client per terminal, each on its own UDP port
(on Windows use `py -3` instead of `python`):

```bash
python -m server.server
python -m client.client --name CampusSecurity --role PUBLISHER --port 6001
python -m client.client --name Alice --role SUBSCRIBER --port 6002
python -m client.client --name Bob --role BOTH --port 6003 --log bob.log
```

Options: `--tcp-port` (default UDP port + 1000), `--ip`, `--server-ip`, `--server-port`,
`--log FILE`, `--auto-register`. Type `help` at the prompt:

```text
register | deregister
sub HALL FIRE | unsub HALL FIRE
publish FIRE HALL CRITICAL "Fire alarm" "Evacuate the building" evacuation_map.pdf
update INC001 WARNING "Fire contained" "Stay outside"
close INC001 "Resolved"
status INC001 SAFE
attach INC001 evacuation_map.pdf   download an attachment from its publisher (Phase 3)
alerts            incidents received so far
ack off | ack on  stop/resume ACKs, to demo server retransmission and UNCONFIRMED
raw <json>        send any text as-is (malformed-message tests)
quit              exit without de-registering
```

Each client is multi-threaded: the **main thread** reads commands and waits for the reply with
the matching RQ#, while a **`udp-recv` thread** receives everything at any time. For `ALERT`,
`ALERT_UPDATE` and `ALL_CLEAR` it checks `(Incident_ID, Version)` for duplicates (a duplicate is
logged, not displayed again) and sends `ALERT_ACK` with the same RQ# for `CRITICAL` messages and
every `ALL_CLEAR`, including for duplicates. The log shows the thread name on each line.

Windows note: the server and client sockets disable `SIO_UDP_CONNRESET`
(`common/net.py`). Without it, sending an alert to a client that had quit made the
server's next `recvfrom` raise WinError 10054 and the server stopped.

### Commands

```bash
python -m unittest discover -s tests -v     # 58 tests
python -m tests.run_phase2_demo             # server + publisher + two subscribers, in one process
python -m server.server                     # manual run; drive it with client/client.py
```

## Phase 3 — Attachments: discovery (UDP) and direct P2P transfer (TCP)

The server never stores or relays files. It only tells a subscriber where the publisher is;
the file then goes **directly from the publisher to the subscriber over TCP**.

### Try it

```bash
python -m server.server
python -m client.client --name CampusSecurity --role PUBLISHER --port 6001
python -m client.client --name Alice --role SUBSCRIBER --port 6002
```

1. Put a file in `data/user/CampusSecurity/shared/` (e.g. `evacuation_map.pdf`).
2. Publisher: `register`, then `publish FIRE HALL CRITICAL "Fire" "Evacuate" evacuation_map.pdf`
3. Alice: `register`, then `attach INC001 evacuation_map.pdf`
4. The file appears in `data/user/Alice/downloads/`.

### Each client's folder (`data/user/<Name>/`, git-ignored)

```text
data/user/CampusSecurity/
  shared/           files this client can publish as attachments
  downloads/        verified downloads (a download is written as <file>.part and renamed only when the CRC matches)
  published.json    the publisher's notes: {"INC001": {"file_name": "evacuation_map.pdf", "status": "ACTIVE"}}
```

`published.json` is updated on `ALERT_ACCEPTED` and `ALERT_CLOSED`, and reloaded at startup, so a
restarted publisher keeps serving its ACTIVE incidents. `publish` is refused locally if the
attachment is not a plain file name present in `shared/`.

### Message flow

```
Alice ── ATTACH_REQ (UDP) ──► Server      checks: Not_Registered, Unknown_Incident, Incident_Closed,
Alice ◄── ATTACH_INFO / ATTACH_DENIED ──  File_Not_For_Incident, Publisher_Unavailable (not registered this run)

Alice ── TCP connect ──► CampusSecurity (its registered TCP port)
      ── GET_ATTACHMENT ──►               checks: Unknown_Incident, Incident_Closed,
      ◄── ATTACH_DATA header + bytes ──   File_Not_For_Incident, File_Not_Available (else ATTACH_ERROR)
      ── ATTACH_OK / ATTACH_ERROR ──►     then the connection is closed (one request per connection)
```

### TCP framing

Every TCP message is one JSON object followed by `\n` (JSON never contains a raw newline).
`ATTACH_DATA` is followed immediately by exactly `file_size` raw bytes:

```
{"type": "ATTACH_DATA", "rq": 4, "incident_id": "INC001", "file_name": "evacuation_map.pdf", "file_size": 300000, "checksum": "B4879384"}\n
<300000 raw bytes>
```

### Integrity and retries

- Checksum: CRC32 (8 hex chars) computed chunk by chunk with `FileChecksum` in `common/protocol.py`,
  the only place that knows the algorithm.
- Transfer errors are retried, up to 3 attempts, each on a new connection: connection
  refused/timeout, `Incomplete_Transfer`, `Checksum_Mismatch`.
- Refusals are reported and not retried: `ATTACH_DENIED` from the server, `ATTACH_ERROR` from the publisher.
- Timeouts: 5 s to connect, 10 s without data (`common/config.py`).

### Concurrency at the publisher

A PUBLISHER/BOTH client runs a `tcp-listen` thread (`accept()`) and hands each connection to a
pool of 8 `tcp-worker` threads (`TCP_WORKERS`), the same model as the server's UDP workers.
Transfers never block the client's `udp-recv` thread or its command prompt.

### Files

| File | Change |
|---|---|
| `server/handlers/attachments.py` | New: `ATTACH_REQ` handler |
| `server/server.py` | Registers `ATTACH_REQ` |
| `client/client.py` | User folder, `published.json`, publish check; `start()` and `attach` call into `transfer.py` |
| `client/transfer.py` | New: subscriber side (`attach`, `download` with retries) and publisher side (`start_listener`, `accept_loop`, `serve`, `check_request`). Each function takes the `Client` as its first argument. |
| `common/protocol.py` | `FileChecksum`; `ATTACH_DATA` no longer has a `data` field |
| `common/config.py` | `TCP_WORKERS`, `TCP_CONNECT_TIMEOUT`, `TCP_IO_TIMEOUT`, `TRANSFER_ATTEMPTS`, `USER_DATA_DIR` |
| `tests/test_attachments.py` | New: 15 tests (server checks, publisher serving, end-to-end, concurrent downloads, bad transfers) |
