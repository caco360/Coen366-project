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
Phase 3 adds a tcp-listen thread + tcp-worker pool for publishers serving attachments;
that TCP code lives in client/transfer.py.

The main thread's command shell (`help`, `sub`, `publish`, ...) lives in client/cli.py.
"""

import json
import queue
import socket
import threading
from pathlib import Path

from client import transfer
from common import config
from common.log import get_logger
from common.net import disable_udp_connreset
from common.protocol import (
    MalformedMessage,
    RqCounter,
    build_message,
    decode_message,
    encode_message,
)

NOTIFICATION_TYPES = {"ALERT", "ALERT_UPDATE", "ALL_CLEAR"}
REPLY_TIMEOUT = 3.0          # seconds to wait for the server's reply to a request
NO_REPLY_WAIT = 1.0          # UNSUBSCRIBE / DE-REGISTER: the server only answers on failure


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

        # Phase 3: publishers serve attachments over TCP (see client/transfer.py).
        if self.role in ("PUBLISHER", "BOTH"):
            transfer.start_listener(self)

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

    def get_incidents(self):
        """Copy of the incidents received so far (for the `alerts` command)."""

        with self.lock:
            return [dict(i) for i in self.incidents.values()]

    def _set_published(self, incident_id, **fields):
        """Update our notes about one published incident and save them to published.json."""

        with self.lock:
            self.published.setdefault(incident_id, {}).update(fields)
            self.published_file.write_text(json.dumps(self.published, indent=2), encoding="utf-8")

    def status(self, incident_id, status):
        return self.request("STATUS_REPORT", incident_id=incident_id, name=self.name,
                            status=status)

    # =========================================================
    # Phase 3: attachment transfer over TCP (code in client/transfer.py)
    # =========================================================
    def attach(self, incident_id, file_name):
        return transfer.attach(self, incident_id, file_name)

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


# `py -m client.client` still starts the command shell (imported here, not at the top,
# because client/cli.py imports this file).
if __name__ == "__main__":
    from client.cli import main
    main()
