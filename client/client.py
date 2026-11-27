"""CENRS client (peer): one process = one user, identified by its UDP port.

Run one instance per user, each in its own terminal, from the project root:

    py -3 -m client.client --name Alice --role SUBSCRIBER --port 6002
    py -3 -m client.client --name CampusSecurity --role PUBLISHER --port 6001

Threads inside ONE client process (spec section 4: the client must be multi-threaded):

    main thread       reads commands typed by the user and sends requests
    udp-recv thread   receives every UDP datagram at any time:
                        - ALERT / ALERT_UPDATE / ALL_CLEAR -> duplicate check, display,
                          automatic ALERT_ACK when required (CRITICAL or ALL_CLEAR)
                        - replies (REGISTERED, ALERT_ACCEPTED, ...) -> handed to the
                          request that is waiting for that RQ#

Because the receiver runs on its own thread, an alert that arrives while the user
is typing, or while another request is waiting for its reply, is ACKed immediately.
(Phase 3 adds a TCP listener thread for publishers serving attachments.)

Type `help` at the prompt for the command list.
"""

import argparse
import json
import queue
import shlex
import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from common import config
from common.log import get_logger
from common.net import disable_udp_connreset
from common.protocol import (
    FileChecksum,
    MalformedMessage,
    RqCounter,
    build_message,
    decode_message,
    encode_message,
)

NOTIFICATION_TYPES = {"ALERT", "ALERT_UPDATE", "ALL_CLEAR"}
REPLY_TIMEOUT = 3.0          # seconds to wait for the server's reply to a request
NO_REPLY_WAIT = 1.0          # UNSUBSCRIBE / DE-REGISTER: the server only answers on failure
CHUNK_SIZE = 64 * 1024       # files are read and written in 64 KiB pieces


class Client:

    def __init__(self, name, role, ip="127.0.0.1", udp_port=6001, tcp_port=None,
                 server=(config.SERVER_IP, config.SERVER_UDP_PORT), logfile=None,
                 data_dir=config.USER_DATA_DIR):

        self.name = name
        self.role = role.upper()
        self.server = tuple(server)

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        disable_udp_connreset(self.sock)
        self.sock.bind((ip, udp_port))

        # Port 0 lets the OS choose (used by tests); register with the real port.
        self.address = self.sock.getsockname()
        self.ip, self.udp_port = self.address
        self.tcp_port = tcp_port if tcp_port is not None else self.udp_port + 1000

        # Phase 3: this client's own folder (like its own disk): data/user/<Name>/
        self.home = Path(data_dir) / name
        self.shared = self.home / "shared"          # files this client publishes
        self.downloads = self.home / "downloads"    # verified downloads
        self.shared.mkdir(parents=True, exist_ok=True)
        self.downloads.mkdir(parents=True, exist_ok=True)

        # Notes about what this client published: {"INC001": {"file_name", "status"}}.
        # Saved so a restarted publisher still serves its ACTIVE incidents.
        self.published_file = self.home / "published.json"
        self.published = (json.loads(self.published_file.read_text(encoding="utf-8"))
                          if self.published_file.exists() else {})

        self.log = get_logger(f"CENRS.client.{name}.{self.udp_port}", logfile)
        self.log.propagate = False          # don't print twice if the server's root logger exists

        self.rq = RqCounter()               # this client's own request numbers
        self.lock = threading.Lock()
        self.waiting = {}                   # rq -> Queue for the request awaiting that reply

        self.seen = set()                   # (incident_id, version) already processed
        self.incidents = {}                 # incident_id -> latest known state
        self.displayed = []                 # notifications shown to the user (non-duplicates)
        self.duplicates = 0
        self.auto_ack = True                # `ack off` simulates a client that never ACKs

        self.tcp = None                     # TCP listening socket (publishers only)
        self.running = False
        self.receiver = threading.Thread(target=self._receive_loop, name="udp-recv", daemon=True)

    # =========================================================
    # Lifecycle
    # =========================================================
    def start(self):
        self.running = True
        self.receiver.start()

        # Phase 3: publishers serve attachments over TCP. One thread accepts connections,
        # a pool of TCP_WORKERS threads serves them (same model as the server's UDP workers).
        if self.role in ("PUBLISHER", "BOTH"):
            # create_server = bind + listen, with SO_REUSEADDR on Linux/macOS so a restarted
            # publisher can reuse its port immediately (old connections may be in TIME_WAIT).
            self.tcp = socket.create_server((self.ip, self.tcp_port))
            self.tcp_port = self.tcp.getsockname()[1]       # real port if 0 was given (tests)
            self.tcp_workers = ThreadPoolExecutor(config.TCP_WORKERS,
                                                  thread_name_prefix="tcp-worker")
            threading.Thread(target=self._accept_loop, name="tcp-listen", daemon=True).start()

        self.log.info("Client %s (%s) on UDP %s:%d, TCP port %d, server %s:%d",
                      self.name, self.role, self.ip, self.udp_port, self.tcp_port, *self.server)

    def stop(self):
        self.running = False
        self.sock.close()
        if self.tcp:
            self.tcp.close()
            self.tcp_workers.shutdown(wait=False)

    # =========================================================
    # Sending
    # =========================================================
    def _send(self, message):
        self.sock.sendto(encode_message(message), self.server)
        self.log.info("SEND %s", json.dumps(message, ensure_ascii=False))

    def request(self, msg_type, wait=REPLY_TIMEOUT, **fields):
        """Send a request with a fresh RQ# and wait for the reply carrying the same RQ#.

        Returns the reply message, or None if nothing arrived within `wait` seconds.
        Raises MalformedMessage if the fields are invalid (nothing is sent then).
        """

        message = build_message(msg_type, rq=self.rq.next(), **fields)
        replies = queue.Queue()

        with self.lock:
            self.waiting[message["rq"]] = replies

        try:
            self._send(message)
            return replies.get(timeout=wait)
        except queue.Empty:
            return None
        finally:
            with self.lock:
                self.waiting.pop(message["rq"], None)

    def send_raw(self, text):
        """Send text exactly as typed (to test how the server handles malformed input)."""

        self.sock.sendto(text.encode("utf-8"), self.server)
        self.log.info("SEND RAW %s", text)

    # =========================================================
    # Protocol operations (one per request type)
    # =========================================================
    def register(self):
        return self.request("REGISTER", name=self.name, role=self.role, ip=self.ip,
                            udp_port=self.udp_port, tcp_port=self.tcp_port)

    def deregister(self, wait=NO_REPLY_WAIT):
        return self.request("DE-REGISTER", wait=wait, name=self.name)

    def subscribe(self, location, category):
        return self.request("SUBSCRIBE", location=location, category=category)

    def unsubscribe(self, location, category, wait=NO_REPLY_WAIT):
        return self.request("UNSUBSCRIBE", wait=wait, location=location, category=category)

    def publish(self, category, location, severity, title, description, attachment="NONE"):

        # The attachment must be a plain file name present in our shared/ folder.
        if attachment != "NONE" and (Path(attachment).name != attachment
                                     or not (self.shared / attachment).is_file()):
            raise ValueError(f"{attachment} not found in {self.shared}")

        reply = self.request("PUBLISH_ALERT", category=category, location=location,
                             severity=severity, title=title, description=description,
                             attachment=attachment)

        if reply and reply["type"] == "ALERT_ACCEPTED" and attachment != "NONE":
            self._set_published(reply["incident_id"], file_name=attachment, status="ACTIVE")

        return reply

    def update(self, incident_id, severity, title, description):
        return self.request("UPDATE_ALERT", incident_id=incident_id, severity=severity,
                            title=title, description=description)

    def close(self, incident_id, reason):
        reply = self.request("CLOSE_ALERT", incident_id=incident_id, reason=reason)

        if reply and reply["type"] == "ALERT_CLOSED" and incident_id in self.published:
            self._set_published(incident_id, status="CLOSED")

        return reply

    def _set_published(self, incident_id, **fields):
        """Update our notes about one published incident and save them to published.json."""

        with self.lock:
            self.published.setdefault(incident_id, {}).update(fields)
            self.published_file.write_text(json.dumps(self.published, indent=2), encoding="utf-8")

    def status(self, incident_id, status):
        return self.request("STATUS_REPORT", incident_id=incident_id, name=self.name,
                            status=status)

    # =========================================================
    # Subscriber side of P2P transfer: the `attach` command
    # =========================================================
    def attach(self, incident_id, file_name):
        """ATTACH_REQ to the server (UDP), then download from the publisher (TCP).

        Only transfer errors are retried (up to TRANSFER_ATTEMPTS); refusals are reported as-is.
        Returns one line describing the result.
        """

        if Path(file_name).name != file_name:
            return f"invalid file name {file_name}"

        info = self.request("ATTACH_REQ", incident_id=incident_id, file_name=file_name)

        if info is None:
            return "no reply from server (timeout)"
        if info["type"] != "ATTACH_INFO":
            return f"{info['type']} {info['reason']}"          # ATTACH_DENIED: not retried

        attempts = config.TRANSFER_ATTEMPTS
        for attempt in range(1, attempts + 1):
            status, detail = self._download(info)
            if status != "retry":
                return detail
            self.log.info("Attempt %d/%d failed: %s", attempt, attempts, detail)

        return f"failed after {attempts} attempts: {detail}"

    def _download(self, info):
        """One TCP attempt. Returns ("ok" | "refused" | "retry", detail)."""

        incident_id, file_name = info["incident_id"], info["file_name"]
        part = self.downloads / (file_name + ".part")     # renamed only once the CRC matches

        try:
            with socket.create_connection((info["ip"], info["tcp_port"]),
                                          timeout=config.TCP_CONNECT_TIMEOUT) as conn:
                conn.settimeout(config.TCP_IO_TIMEOUT)
                rq = self.rq.next()
                self._send_line(conn, build_message("GET_ATTACHMENT", rq=rq, subscriber_name=self.name,
                                                    incident_id=incident_id, file_name=file_name))

                stream = conn.makefile("rb")
                header = decode_message(stream.readline())
                self.log.info("TCP RECV %s", json.dumps(header, ensure_ascii=False))

                if header["type"] == "ATTACH_ERROR":
                    return "refused", f"ATTACH_ERROR {header['reason']}"   # publisher said no
                if header["type"] != "ATTACH_DATA":
                    return "retry", f"unexpected reply {header['type']}"

                # Read exactly file_size bytes, computing the CRC as we go.
                checksum = FileChecksum()
                remaining = header["file_size"]
                with open(part, "wb") as f:
                    while remaining:
                        chunk = stream.read(min(CHUNK_SIZE, remaining))
                        if not chunk:
                            break                                    # publisher closed early
                        f.write(chunk)
                        checksum.update(chunk)
                        remaining -= len(chunk)

                if remaining:
                    error = "Incomplete_Transfer"
                elif checksum.hexdigest() != header["checksum"]:
                    error = "Checksum_Mismatch"
                else:
                    error = None

                if error:
                    self._send_line(conn, build_message("ATTACH_ERROR", rq=rq, incident_id=incident_id,
                                                        file_name=file_name, reason=error))
                    return "retry", error

                self._send_line(conn, build_message("ATTACH_OK", rq=rq, incident_id=incident_id,
                                                    file_name=file_name))
                path = self.downloads / file_name
                part.replace(path)
                return "ok", f"saved {path} (CRC {checksum.hexdigest()})"

        except (OSError, MalformedMessage) as error:
            return "retry", f"{type(error).__name__}: {error}"     # connect failure, timeout, ...

        finally:
            part.unlink(missing_ok=True)                            # never leave a partial file

    # =========================================================
    # Publisher side of P2P transfer (tcp-listen + tcp-worker threads)
    # =========================================================
    def _accept_loop(self):

        while self.running:
            try:
                conn, _ = self.tcp.accept()
            except OSError:
                break                                       # socket closed by stop()
            self.tcp_workers.submit(self._serve, conn)

    def _send_line(self, conn, message):
        """TCP framing: one JSON message per line."""

        conn.sendall(encode_message(message) + b"\n")
        self.log.info("TCP SEND %s", json.dumps(message, ensure_ascii=False))

    def _serve(self, conn):
        """Answer one GET_ATTACHMENT: ATTACH_DATA line + exactly file_size bytes, or ATTACH_ERROR."""

        with conn:
            try:
                conn.settimeout(config.TCP_IO_TIMEOUT)
                stream = conn.makefile("rb")

                request = decode_message(stream.readline())
                if request["type"] != "GET_ATTACHMENT":
                    raise MalformedMessage("Expected_GET_ATTACHMENT")
                self.log.info("TCP RECV %s", json.dumps(request, ensure_ascii=False))

                rq, incident_id, file_name = request["rq"], request["incident_id"], request["file_name"]
                reason = self._check_request(incident_id, file_name)

                if reason:
                    self._send_line(conn, build_message("ATTACH_ERROR", rq=rq, incident_id=incident_id,
                                                        file_name=file_name, reason=reason))
                    return

                path = self.shared / file_name
                checksum = FileChecksum()
                with open(path, "rb") as f:
                    for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
                        checksum.update(chunk)

                self.log.info("Serving %s to %s (%s)", file_name, request["subscriber_name"], incident_id)
                self._send_line(conn, build_message(
                    "ATTACH_DATA", rq=rq, incident_id=incident_id, file_name=file_name,
                    file_size=path.stat().st_size, checksum=checksum.hexdigest()))

                with open(path, "rb") as f:
                    conn.sendfile(f)                        # the raw bytes right after the header

                answer = stream.readline()                  # ATTACH_OK or ATTACH_ERROR
                self.log.info("TCP RECV %s", answer.decode("utf-8", "replace").strip() or "(closed)")

            except (MalformedMessage, OSError) as error:
                self.log.warning("TCP transfer aborted: %s", error)

    def _check_request(self, incident_id, file_name):
        """Publisher-side checks before sending a file. Returns the ATTACH_ERROR reason or None."""

        with self.lock:
            entry = self.published.get(incident_id)

        if entry is None:
            return "Unknown_Incident"
        if entry["status"] != "ACTIVE":
            return "Incident_Closed"
        if entry["file_name"] != file_name:
            return "File_Not_For_Incident"      # also blocks names like ../x: only our own names match
        if not (self.shared / file_name).is_file():
            return "File_Not_Available"
        return None

    # =========================================================
    # Receiving (udp-recv thread)
    # =========================================================
    def _receive_loop(self):

        while self.running:
            try:
                data, sender = self.sock.recvfrom(65535)
            except ConnectionResetError:
                continue
            except OSError:
                break                      # socket closed by stop()

            try:
                message = decode_message(data)
            except MalformedMessage as error:
                self.log.warning("BAD MESSAGE from %s: %s", sender, error)
                continue

            self.log.info("RECV %s", json.dumps(message, ensure_ascii=False))

            try:
                if message["type"] in NOTIFICATION_TYPES:
                    self._on_notification(message)
                else:
                    self._on_reply(message)
            except Exception:
                self.log.exception("Error while processing %s", message["type"])

    def _on_reply(self, message):

        with self.lock:
            replies = self.waiting.get(message["rq"])

        if replies is None:
            self.log.info("(no request waiting for rq=%d; reply arrived late or was unsolicited)",
                          message["rq"])
            return

        replies.put(message)

    def _on_notification(self, message):
        """Duplicate check on (Incident_ID, Version), display once, ACK every copy if required."""

        key = (message["incident_id"], message["version"])
        needs_ack = message["type"] == "ALL_CLEAR" or message.get("severity") == "CRITICAL"

        with self.lock:
            duplicate = key in self.seen
            self.seen.add(key)
            if duplicate:
                self.duplicates += 1
            else:
                self._apply(message)
                self.displayed.append(message)

        if duplicate:
            self.log.info("DUPLICATE %s %s v%d detected, not displayed again%s",
                          message["type"], *key, " (re-sending ACK)" if needs_ack else "")
        else:
            self._display(message)

        if needs_ack:
            self._ack(message)

    def _ack(self, message):

        if not self.auto_ack:
            self.log.info("ACK suppressed for %s %s v%d (ack is off)",
                          message["type"], message["incident_id"], message["version"])
            return

        self._send(build_message(
            "ALERT_ACK", rq=message["rq"],                  # same RQ# as the notification
            incident_id=message["incident_id"], version=message["version"], name=self.name,
        ))

    def _apply(self, message):
        """Update the local copy of the incident (caller holds self.lock)."""

        incident_id = message["incident_id"]
        state = self.incidents.setdefault(incident_id, {"incident_id": incident_id, "version": 0})

        if message["version"] < state["version"]:
            return                                          # older than what we have (reordered)

        state["version"] = message["version"]

        if message["type"] == "ALERT":
            for field in ("category", "location", "severity", "title", "description", "attachment"):
                state[field] = message[field]
            state["status"] = "ACTIVE"

        elif message["type"] == "ALERT_UPDATE":
            for field in ("severity", "title", "description"):
                state[field] = message[field]
            state.setdefault("status", "ACTIVE")

        elif message["type"] == "ALL_CLEAR":
            state["status"] = "CLOSED"
            state["message"] = message["message"]

    def _display(self, message):

        kind = message["type"]
        incident_id = message["incident_id"]

        if kind == "ALERT":
            lines = [
                f"*** {message['severity']} ALERT {incident_id} v{message['version']} ***",
                f"    {message['category']} at {message['location']}: {message['title']}",
                f"    {message['description']}",
            ]
            if message["attachment"] != "NONE":
                lines.append(f"    Attachment: {message['attachment']}")

        elif kind == "ALERT_UPDATE":
            lines = [
                f"*** UPDATE {incident_id} v{message['version']} now {message['severity']} ***",
                f"    {message['title']}: {message['description']}",
            ]

        else:
            lines = [
                f"*** ALL CLEAR {incident_id} v{message['version']} ***",
                f"    {message['message']}",
            ]

        self.log.info("\n" + "\n".join(lines))


# =============================================================
# Interactive shell (main thread)
# =============================================================
HELP = """Commands (use quotes for text with spaces):
  register
  deregister
  sub   <LOCATION> <CATEGORY>                 e.g. sub HALL FIRE   (ALL is a wildcard)
  unsub <LOCATION> <CATEGORY>
  publish <CATEGORY> <LOCATION> <SEVERITY> "<title>" "<description>" [attachment]
  update  <INCIDENT_ID> <SEVERITY> "<title>" "<description>"
  close   <INCIDENT_ID> "<reason>"
  status  <INCIDENT_ID> SAFE|NEED_HELP|LEFT_AREA
  attach  <INCIDENT_ID> <file_name>       download an attachment directly from its publisher
  alerts                      incidents this client has received
  ack on|off                   off = never ACK (demo of server retransmission)
  raw <json>                   send any text as-is, e.g. raw {"type":"SUBSCRIBE","rq":99}
  help
  quit                         exit WITHOUT de-registering (keeps state on the server)
Locations:  HALL EV JMSB LOYOLA SGW ALL
Categories: FIRE SECURITY WEATHER POWER BUILDING GENERAL ALL
Severities: INFO WARNING CRITICAL"""


def _expect(args, low, high=None, usage=""):
    high = low if high is None else high
    if not low <= len(args) <= high:
        raise ValueError("usage: " + usage)


def run_command(client, line):
    """Execute one shell line. Returns False when the user wants to quit."""

    try:
        parts = shlex.split(line)
    except ValueError as error:
        print("Parse error:", error)
        return True

    if not parts:
        return True

    command, args = parts[0].lower(), parts[1:]
    upper = [a.upper() for a in args]
    reply = None
    no_reply_is_success = False

    try:
        if command in ("quit", "exit"):
            return False

        elif command == "help":
            print(HELP)
            return True

        elif command == "register":
            reply = client.register()

        elif command == "deregister":
            reply = client.deregister()
            no_reply_is_success = True

        elif command in ("sub", "subscribe"):
            _expect(args, 2, usage="sub <LOCATION> <CATEGORY>")
            reply = client.subscribe(upper[0], upper[1])

        elif command in ("unsub", "unsubscribe"):
            _expect(args, 2, usage="unsub <LOCATION> <CATEGORY>")
            reply = client.unsubscribe(upper[0], upper[1])
            no_reply_is_success = True

        elif command == "publish":
            _expect(args, 5, 6, 'publish <CATEGORY> <LOCATION> <SEVERITY> "<title>" "<description>" [attachment]')
            attachment = args[5] if len(args) == 6 else "NONE"
            reply = client.publish(upper[0], upper[1], upper[2], args[3], args[4], attachment)

        elif command == "update":
            _expect(args, 4, usage='update <INCIDENT_ID> <SEVERITY> "<title>" "<description>"')
            reply = client.update(upper[0], upper[1], args[2], args[3])

        elif command == "close":
            _expect(args, 2, usage='close <INCIDENT_ID> "<reason>"')
            reply = client.close(upper[0], args[1])

        elif command == "status":
            _expect(args, 2, usage="status <INCIDENT_ID> SAFE|NEED_HELP|LEFT_AREA")
            reply = client.status(upper[0], upper[1])

        elif command == "attach":
            _expect(args, 2, usage="attach <INCIDENT_ID> <file_name>")
            print("->", client.attach(upper[0], args[1]))
            return True

        elif command == "alerts":
            with client.lock:
                incidents = [dict(i) for i in client.incidents.values()]
            if not incidents:
                print("No incidents received yet.")
            for i in incidents:
                print(f"  {i['incident_id']} v{i['version']} {i.get('status', '?')} "
                      f"{i.get('severity', '?')} {i.get('category', '?')}@{i.get('location', '?')} "
                      f"\"{i.get('title', '')}\" attachment={i.get('attachment', 'NONE')}")
            return True

        elif command == "ack":
            _expect(args, 1, usage="ack on|off")
            client.auto_ack = args[0].lower() == "on"
            print("Automatic ALERT_ACK is", "ON" if client.auto_ack else "OFF")
            return True

        elif command == "raw":
            text = line.strip()[3:].strip()
            if not text:
                raise ValueError("usage: raw <json>")
            client.send_raw(text)
            return True

        else:
            print(f"Unknown command '{command}'. Type 'help'.")
            return True

    except (ValueError, MalformedMessage) as error:
        # MalformedMessage: the client refused to send an invalid message
        print("Not sent:", error)
        return True

    if reply is not None:
        print("->", reply["type"], {k: v for k, v in reply.items() if k not in ("type", "rq")})
    elif no_reply_is_success:
        print("-> no reply (the server only answers this request when it fails)")
    else:
        print("-> no reply from server (timeout)")

    return True


def main():

    parser = argparse.ArgumentParser(description="CENRS client (one user per process)")
    parser.add_argument("--name", required=True)
    parser.add_argument("--role", required=True, choices=["SUBSCRIBER", "PUBLISHER", "BOTH"],
                        type=str.upper)
    parser.add_argument("--port", type=int, required=True, help="this client's UDP port")
    parser.add_argument("--tcp-port", type=int, default=None,
                        help="this client's TCP port (default: UDP port + 1000)")
    parser.add_argument("--ip", default="127.0.0.1")
    parser.add_argument("--server-ip", default=config.SERVER_IP)
    parser.add_argument("--server-port", type=int, default=config.SERVER_UDP_PORT)
    parser.add_argument("--log", default=None, help="also write the log to this file")
    parser.add_argument("--auto-register", action="store_true", help="send REGISTER at startup")
    args = parser.parse_args()

    client = Client(args.name, args.role, ip=args.ip, udp_port=args.port,
                    tcp_port=args.tcp_port, server=(args.server_ip, args.server_port),
                    logfile=args.log)
    client.start()
    print("Type 'help' for commands.")

    if args.auto_register:
        run_command(client, "register")

    try:
        while True:
            try:
                line = input(f"{client.name}> ")
            except EOFError:
                break
            if not run_command(client, line):
                break
    except KeyboardInterrupt:
        print()
    finally:
        client.stop()


if __name__ == "__main__":
    main()
