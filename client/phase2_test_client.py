"""Minimal UDP peer used by the Phase 2 demo script (demo_client.py is left untouched).

A Peer owns one UDP socket bound to a fixed port, builds every message through
common/protocol.py, and offers blocking receive with a timeout.
"""

import socket

from common.protocol import build_message, decode_message, encode_message


class Peer:

    def __init__(self, name, port, server=("127.0.0.1", 5000), host="127.0.0.1"):
        self.name = name
        self.address = (host, port)
        self.server = server

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(self.address)
        self.socket.settimeout(2.0)

    def send(self, msg_type, rq=None, **fields):
        """Build, validate and send one message to the server. Returns the message."""

        message = build_message(msg_type, rq=rq, **fields)
        self.socket.sendto(encode_message(message), self.server)
        return message

    def recv(self, timeout=2.0):
        """Return the next decoded message, or None on timeout."""

        self.socket.settimeout(timeout)
        try:
            data, _ = self.socket.recvfrom(65535)
        except socket.timeout:
            return None
        return decode_message(data)

    def drain(self, timeout=0.3):
        """Collect every message that arrives until the socket stays quiet for `timeout`."""

        received = []
        while True:
            message = self.recv(timeout)
            if message is None:
                return received
            received.append(message)

    # ---- convenience wrappers ----
    def register(self, role):
        self.send(
            "REGISTER", name=self.name, role=role,
            ip=self.address[0], udp_port=self.address[1], tcp_port=self.address[1] + 1000,
        )
        return self.recv()

    def subscribe(self, location, category):
        self.send("SUBSCRIBE", location=location, category=category)
        return self.recv()

    def ack(self, alert):
        """ALERT_ACK echoing the rq of the ALERT / ALERT_UPDATE / ALL_CLEAR it answers."""

        return self.send(
            "ALERT_ACK", rq=alert["rq"], incident_id=alert["incident_id"],
            version=alert["version"], name=self.name,
        )

    def close(self):
        self.socket.close()
