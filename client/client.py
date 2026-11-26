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
                 server=(config.SERVER_IP, config.SERVER_UDP_PORT), logfile=None):

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

        self.running = False
        self.receiver = threading.Thread(target=self._receive_loop, name="udp-recv", daemon=True)

    # =========================================================
    # Lifecycle
    # =========================================================
    def start(self):
        self.running = True
        self.receiver.start()
        self.log.info("Client %s (%s) on UDP %s:%d, TCP port %d, server %s:%d",
                      self.name, self.role, self.ip, self.udp_port, self.tcp_port, *self.server)

    def stop(self):
        self.running = False
        self.sock.close()

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
        return self.request("PUBLISH_ALERT", category=category, location=location,
                            severity=severity, title=title, description=description,
                            attachment=attachment)

    def update(self, incident_id, severity, title, description):
        return self.request("UPDATE_ALERT", incident_id=incident_id, severity=severity,
                            title=title, description=description)

    def close(self, incident_id, reason):
        return self.request("CLOSE_ALERT", incident_id=incident_id, reason=reason)

    def status(self, incident_id, status):
        return self.request("STATUS_REPORT", incident_id=incident_id, name=self.name,
                            status=status)

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
  alerts                       incidents this client has received
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
