"""Client tests: request/reply matching, auto-ACK, duplicate detection, end-to-end with the server.

Everything runs on 127.0.0.1 with OS-assigned ports (port 0), so tests never
collide with a server or client you have running by hand.
"""

import socket
import tempfile
import threading
import time
import unittest
from pathlib import Path

from client.client import Client
from common import config
from common.protocol import build_message, decode_message, encode_message
from server.server import EmergencyServer


class FakeServer:
    """A bare UDP socket standing in for the server, to inject exact packets."""

    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("127.0.0.1", 0))
        self.sock.settimeout(0.5)
        self.address = self.sock.getsockname()

    def send(self, message, address):
        self.sock.sendto(encode_message(message), address)

    def recv_all(self, quiet=0.3):
        received = []
        self.sock.settimeout(quiet)
        while True:
            try:
                data, _ = self.sock.recvfrom(65535)
            except socket.timeout:
                return received
            received.append(decode_message(data))

    def close(self):
        self.sock.close()


class ClientNotificationTests(unittest.TestCase):
    """The client against a fake server: ACK rules and duplicate detection (spec 2.5, 2.7)."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.fake = FakeServer()
        self.client = Client("Alice", "SUBSCRIBER", udp_port=0, server=self.fake.address,
                             data_dir=self.tmp.name)
        self.client.start()

    def tearDown(self):
        self.client.stop()
        self.fake.close()
        self.tmp.cleanup()

    def alert(self, severity, rq=83, version=1):
        return build_message(
            "ALERT", rq=rq, incident_id="INC102", version=version, category="FIRE",
            location="HALL", severity=severity, title="Fire alarm",
            description="Evacuate building", attachment="NONE",
        )

    def test_critical_alert_is_acked_with_same_rq(self):
        self.fake.send(self.alert("CRITICAL", rq=83), self.client.address)
        acks = self.fake.recv_all()

        self.assertEqual(len(acks), 1)
        self.assertEqual(acks[0]["type"], "ALERT_ACK")
        self.assertEqual(acks[0]["rq"], 83)
        self.assertEqual(acks[0]["incident_id"], "INC102")
        self.assertEqual(acks[0]["version"], 1)
        self.assertEqual(acks[0]["name"], "Alice")
        self.assertEqual(len(self.client.displayed), 1)

    def test_info_alert_is_not_acked(self):
        self.fake.send(self.alert("INFO"), self.client.address)
        self.assertEqual(self.fake.recv_all(), [])
        self.assertEqual(len(self.client.displayed), 1)

    def test_duplicate_alert_is_reacked_but_not_displayed_again(self):
        packet = self.alert("CRITICAL", rq=83)
        self.fake.send(packet, self.client.address)
        self.fake.send(packet, self.client.address)
        self.fake.send(packet, self.client.address)

        acks = self.fake.recv_all()
        self.assertEqual(len(acks), 3)                   # every copy is ACKed again
        self.assertEqual(len(self.client.displayed), 1)  # but shown only once
        self.assertEqual(self.client.duplicates, 2)

    def test_critical_update_needs_ack_and_new_version_is_not_a_duplicate(self):
        self.fake.send(self.alert("INFO", version=1), self.client.address)
        update = build_message(
            "ALERT_UPDATE", rq=118, incident_id="INC102", version=2, severity="CRITICAL",
            title="Fire spreading", description="Leave now",
        )
        self.fake.send(update, self.client.address)

        acks = self.fake.recv_all()
        self.assertEqual([(a["rq"], a["version"]) for a in acks], [(118, 2)])
        self.assertEqual(len(self.client.displayed), 2)

        state = self.client.incidents["INC102"]
        self.assertEqual(state["version"], 2)
        self.assertEqual(state["severity"], "CRITICAL")
        self.assertEqual(state["title"], "Fire spreading")
        self.assertEqual(state["location"], "HALL")      # kept from the original ALERT

    def test_all_clear_always_acked_and_marks_closed(self):
        self.fake.send(self.alert("INFO", version=1), self.client.address)
        all_clear = build_message(
            "ALL_CLEAR", rq=125, incident_id="INC102", version=3, message="Building_Reopened",
        )
        self.fake.send(all_clear, self.client.address)
        self.fake.send(all_clear, self.client.address)   # retransmission

        acks = self.fake.recv_all()
        self.assertEqual([a["rq"] for a in acks], [125, 125])
        self.assertEqual(self.client.incidents["INC102"]["status"], "CLOSED")
        self.assertEqual(len(self.client.displayed), 2)

    def test_ack_off_suppresses_acks(self):
        self.client.auto_ack = False
        self.fake.send(self.alert("CRITICAL"), self.client.address)
        self.assertEqual(self.fake.recv_all(), [])
        self.assertEqual(len(self.client.displayed), 1)

    def test_request_times_out_when_no_reply(self):
        reply = self.client.request("SUBSCRIBE", wait=0.2, location="HALL", category="FIRE")
        self.assertIsNone(reply)

    def test_malformed_packet_does_not_kill_receiver(self):
        self.fake.sock.sendto(b"not json at all", self.client.address)
        self.fake.send(self.alert("CRITICAL"), self.client.address)
        self.assertEqual(len(self.fake.recv_all()), 1)
        self.assertTrue(self.client.receiver.is_alive())


class ClientEndToEndTests(unittest.TestCase):
    """Real server + real clients over UDP on loopback."""

    def setUp(self):
        self.saved = (config.ACK_TIMEOUT, config.LOSS_RATE)
        config.ACK_TIMEOUT = 0.3
        config.LOSS_RATE = 0.0

        self.tmp = tempfile.TemporaryDirectory()
        self.server = EmergencyServer(
            host="127.0.0.1", port=0, data_file=Path(self.tmp.name) / "state.json",
        )
        self.server_address = self.server.socket.getsockname()
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

        self.clients = []

    def tearDown(self):
        for c in self.clients:
            c.stop()
        self.server.timers.shutdown()
        self.server.workers.shutdown(wait=True)   # let in-flight handlers finish writing state
        # The server thread is a daemon blocked in recvfrom; it ends with the test process.
        config.ACK_TIMEOUT, config.LOSS_RATE = self.saved
        self.tmp.cleanup()

    def make(self, name, role):
        # Explicit tcp_port: with udp_port=0 the default (udp_port + 1000) can exceed 65535.
        # Publishers listen on TCP, so they get 0 (a free port); subscribers never listen.
        tcp_port = 7999 if role == "SUBSCRIBER" else 0
        c = Client(name, role, udp_port=0, tcp_port=tcp_port, server=self.server_address,
                   data_dir=self.tmp.name)
        c.start()
        self.clients.append(c)
        return c

    def test_full_flow_publish_ack_update_status_close(self):
        pub = self.make("CampusSecurity", "PUBLISHER")
        alice = self.make("Alice", "SUBSCRIBER")

        self.assertEqual(pub.register()["type"], "REGISTERED")
        self.assertEqual(alice.register()["type"], "REGISTERED")
        self.assertEqual(alice.subscribe("HALL", "FIRE")["type"], "SUBSCRIBED")

        denied = alice.publish("FIRE", "HALL", "INFO", "x", "y")
        self.assertEqual(denied["type"], "ALERT_DENIED")
        self.assertEqual(denied["reason"], "Unauthorized_Role")

        accepted = pub.publish("FIRE", "HALL", "CRITICAL", "Fire alarm", "Evacuate Hall now")
        self.assertEqual(accepted["type"], "ALERT_ACCEPTED")
        incident_id = accepted["incident_id"]

        time.sleep(0.3)
        self.assertEqual(alice.incidents[incident_id]["title"], "Fire alarm")
        self.assertEqual(self.server.storage.get_ack(incident_id, 1, "Alice"), "CONFIRMED")

        updated = pub.update(incident_id, "WARNING", "Fire contained", "Stay outside")
        self.assertEqual(updated["version"], 2)

        recorded = alice.status(incident_id, "SAFE")
        self.assertEqual(recorded["type"], "STATUS_RECORDED")

        closed = pub.close(incident_id, "Resolved")
        self.assertEqual(closed["type"], "ALERT_CLOSED")

        time.sleep(0.3)
        self.assertEqual(alice.incidents[incident_id]["status"], "CLOSED")
        self.assertEqual(self.server.storage.get_ack(incident_id, 3, "Alice"), "CONFIRMED")

    def test_silent_client_gets_retransmissions_then_unconfirmed(self):
        pub = self.make("Pub", "PUBLISHER")
        bob = self.make("Bob", "SUBSCRIBER")
        pub.register()
        bob.register()
        bob.subscribe("ALL", "ALL")
        bob.auto_ack = False

        incident_id = pub.publish("POWER", "EV", "CRITICAL", "Outage", "Power cut")["incident_id"]
        time.sleep(config.ACK_TIMEOUT * config.MAX_ATTEMPTS + 0.5)

        self.assertEqual(len(bob.displayed), 1)
        self.assertEqual(bob.duplicates, config.MAX_ATTEMPTS - 1)
        self.assertEqual(self.server.storage.get_ack(incident_id, 1, "Bob"), "UNCONFIRMED")

    def test_server_survives_a_client_that_quit(self):
        pub = self.make("Pub", "PUBLISHER")
        gone = self.make("Gone", "SUBSCRIBER")
        pub.register()
        gone.register()
        gone.subscribe("ALL", "ALL")
        gone.stop()                                     # client process "exits"

        pub.publish("FIRE", "HALL", "INFO", "one", "first")
        time.sleep(0.2)
        second = pub.publish("FIRE", "HALL", "INFO", "two", "second")
        self.assertEqual(second["type"], "ALERT_ACCEPTED")

    def test_unsubscribe_and_deregister_have_no_success_reply(self):
        alice = self.make("Alice", "SUBSCRIBER")
        alice.register()
        alice.subscribe("HALL", "FIRE")

        self.assertIsNone(alice.unsubscribe("HALL", "FIRE", wait=0.3))
        denied = alice.unsubscribe("HALL", "FIRE", wait=0.5)
        self.assertEqual(denied["reason"], "Subscription_Not_Found")

        self.assertIsNone(alice.deregister(wait=0.3))
        self.assertIsNone(self.server.storage.get_user("Alice"))


if __name__ == "__main__":
    unittest.main()
