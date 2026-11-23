"""Phase 2 tests: fan-out matching, ACK timers, retransmission, denials, persistence.

No sockets are opened. The TimerManager and the handlers receive recording
functions instead of the real socket, so every test runs in-process.
"""

import tempfile
import time
import unittest
from pathlib import Path

from common import config
from common.protocol import build_message, decode_message
from server.handlers.acks import AckHandler
from server.handlers.incidents import IncidentHandlers, matches
from server.handlers.status import StatusHandler
from server.reliability import TimerManager
from server.storage import Storage

PUB = ("127.0.0.1", 6001)
ALICE = ("127.0.0.1", 6002)
BOB = ("127.0.0.1", 6003)
CAROL = ("127.0.0.1", 6004)
OTHER_PUB = ("127.0.0.1", 6005)
STRANGER = ("127.0.0.1", 6999)

FAST_TIMEOUT = 0.05          # seconds, so three attempts finish in well under a second
SETTLE = 0.5                 # long enough for 3 attempts + UNCONFIRMED at FAST_TIMEOUT


def register(storage, name, role, address):
    message = build_message(
        "REGISTER", rq=1, name=name, role=role,
        ip=address[0], udp_port=address[1], tcp_port=address[1] + 1000,
    )
    storage.register(message, address)


class Phase2TestCase(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "state.json"
        self.storage = Storage(self.path)

        self.sent = []       # fan-out traffic: (decoded message, address)
        self.replies = []    # direct replies: (address, message)

        self.timers = TimerManager(
            self.storage,
            lambda packet, address: self.sent.append((decode_message(packet), address)),
        )
        self.incidents = IncidentHandlers(
            self.storage, self.timers,
            lambda address, message: self.replies.append((address, message)),
        )
        self.acks = AckHandler(self.timers)
        self.status = StatusHandler(self.storage)

        self.saved_config = (config.ACK_TIMEOUT, config.MAX_ATTEMPTS, config.LOSS_RATE)
        config.ACK_TIMEOUT = FAST_TIMEOUT
        config.LOSS_RATE = 0.0

        register(self.storage, "CampusSecurity", "PUBLISHER", PUB)
        register(self.storage, "OtherPublisher", "PUBLISHER", OTHER_PUB)
        register(self.storage, "Alice", "SUBSCRIBER", ALICE)
        register(self.storage, "Bob", "SUBSCRIBER", BOB)
        register(self.storage, "Carol", "SUBSCRIBER", CAROL)
        self.storage.subscribe(ALICE, "HALL", "FIRE")
        self.storage.subscribe(BOB, "ALL", "ALL")
        self.storage.subscribe(CAROL, "EV", "FIRE")

    def tearDown(self):
        self.timers.shutdown()
        config.ACK_TIMEOUT, config.MAX_ATTEMPTS, config.LOSS_RATE = self.saved_config
        self.tmp.cleanup()

    # ---- helpers ----
    def publish(self, severity="CRITICAL", rq=10):
        message = build_message(
            "PUBLISH_ALERT", rq=rq, category="FIRE", location="HALL", severity=severity,
            title="Fire alarm", description="Evacuate Hall", attachment="NONE",
        )
        self.incidents.handle_publish(message, PUB)
        return self.replies[-1][1]["incident_id"]

    def sent_to(self, address, msg_type=None):
        return [
            m for m, a in self.sent
            if a == address and (msg_type is None or m["type"] == msg_type)
        ]

    def ack(self, name, address, alert):
        message = build_message(
            "ALERT_ACK", rq=alert["rq"], incident_id=alert["incident_id"],
            version=alert["version"], name=name,
        )
        self.acks.handle_ack(message, address)

    # ---- matching ----
    def test_wildcard_matching(self):
        incident = {"location": "HALL", "category": "FIRE"}
        self.assertTrue(matches("HALL", "FIRE", incident))
        self.assertTrue(matches("ALL", "FIRE", incident))
        self.assertTrue(matches("HALL", "ALL", incident))
        self.assertTrue(matches("ALL", "ALL", incident))
        self.assertFalse(matches("EV", "FIRE", incident))
        self.assertFalse(matches("HALL", "POWER", incident))

    # ---- publish and fan-out ----
    def test_publish_replies_then_fans_out_to_matching_subscribers(self):
        incident_id = self.publish(rq=10)

        address, reply = self.replies[0]
        self.assertEqual(address, PUB)
        self.assertEqual(reply["type"], "ALERT_ACCEPTED")
        self.assertEqual(reply["rq"], 10)
        self.assertEqual(incident_id, "INC001")

        self.assertEqual(len(self.sent_to(ALICE, "ALERT")), 1)
        self.assertEqual(len(self.sent_to(BOB, "ALERT")), 1)
        self.assertEqual(self.sent_to(CAROL), [])

        alert = self.sent_to(ALICE, "ALERT")[0]
        self.assertEqual(alert["incident_id"], incident_id)
        self.assertEqual(alert["version"], 1)
        self.assertEqual(alert["severity"], "CRITICAL")

        self.assertTrue(self.timers.is_pending("Alice", incident_id, 1))
        self.assertTrue(self.timers.is_pending("Bob", incident_id, 1))
        self.assertEqual(self.storage.get_ack(incident_id, 1, "Alice"), "PENDING")

    def test_info_alert_needs_no_ack(self):
        incident_id = self.publish(severity="INFO")
        self.assertEqual(len(self.sent_to(ALICE, "ALERT")), 1)
        self.assertFalse(self.timers.is_pending("Alice", incident_id, 1))
        self.assertIsNone(self.storage.get_ack(incident_id, 1, "Alice"))

    def test_publish_denied_for_unregistered_and_for_subscriber(self):
        message = build_message(
            "PUBLISH_ALERT", rq=5, category="FIRE", location="HALL", severity="INFO",
            title="x", description="y", attachment="NONE",
        )
        with self.assertRaises(ValueError) as denied:
            self.incidents.handle_publish(message, STRANGER)
        self.assertEqual(str(denied.exception), "Not_Registered")

        with self.assertRaises(ValueError) as denied:
            self.incidents.handle_publish(message, ALICE)
        self.assertEqual(str(denied.exception), "Unauthorized_Role")

        self.assertEqual(self.sent, [])

    def test_direct_replies_are_never_dropped_by_loss_rate(self):
        config.LOSS_RATE = 1.0
        incident_id = self.publish()

        self.assertEqual(self.replies[0][1]["type"], "ALERT_ACCEPTED")
        self.assertEqual(self.sent, [])                         # every fan-out packet dropped
        self.assertTrue(self.timers.is_pending("Alice", incident_id, 1))   # timer still armed

    # ---- timers ----
    def test_timer_retransmits_three_times_then_unconfirmed(self):
        incident_id = self.publish()
        time.sleep(SETTLE)

        alerts = self.sent_to(ALICE, "ALERT")
        self.assertEqual(len(alerts), config.MAX_ATTEMPTS)
        self.assertEqual(len({a["rq"] for a in alerts}), 1)   # identical packet each time
        self.assertEqual(self.storage.get_ack(incident_id, 1, "Alice"), "UNCONFIRMED")
        self.assertFalse(self.timers.is_pending("Alice", incident_id, 1))

    def test_ack_with_alert_rq_confirms_and_cancels_timer(self):
        incident_id = self.publish()
        alert = self.sent_to(ALICE, "ALERT")[0]

        self.ack("Alice", ALICE, alert)                        # same rq as the ALERT

        self.assertEqual(self.storage.get_ack(incident_id, 1, "Alice"), "CONFIRMED")
        self.assertFalse(self.timers.is_pending("Alice", incident_id, 1))

        time.sleep(SETTLE)
        self.assertEqual(len(self.sent_to(ALICE, "ALERT")), 1)   # no retransmission
        self.assertEqual(self.storage.get_ack(incident_id, 1, "Bob"), "UNCONFIRMED")

    def test_duplicate_ack_is_ignored(self):
        incident_id = self.publish()
        alert = self.sent_to(ALICE, "ALERT")[0]

        self.assertTrue(self.timers.ack("Alice", incident_id, 1, rq=alert["rq"]))
        self.assertFalse(self.timers.ack("Alice", incident_id, 1, rq=alert["rq"]))
        self.ack("Alice", ALICE, alert)                        # handler path, must not raise
        self.assertEqual(self.storage.get_ack(incident_id, 1, "Alice"), "CONFIRMED")

    # ---- update ----
    def test_update_to_critical_starts_timers_for_new_version(self):
        incident_id = self.publish(severity="INFO")
        self.assertFalse(self.timers.is_pending("Alice", incident_id, 1))

        message = build_message(
            "UPDATE_ALERT", rq=20, incident_id=incident_id, severity="CRITICAL",
            title="Fire spreading", description="Leave now",
        )
        self.incidents.handle_update(message, PUB)

        reply = self.replies[-1][1]
        self.assertEqual(reply["type"], "ALERT_UPDATED")
        self.assertEqual(reply["version"], 2)

        update = self.sent_to(ALICE, "ALERT_UPDATE")[0]
        self.assertEqual(update["version"], 2)
        self.assertEqual(update["severity"], "CRITICAL")
        self.assertTrue(self.timers.is_pending("Alice", incident_id, 2))
        self.assertTrue(self.timers.is_pending("Bob", incident_id, 2))
        self.assertEqual(self.sent_to(CAROL), [])

    def test_update_to_warning_needs_no_ack(self):
        incident_id = self.publish()
        message = build_message(
            "UPDATE_ALERT", rq=21, incident_id=incident_id, severity="WARNING",
            title="Contained", description="Stay away",
        )
        self.incidents.handle_update(message, PUB)
        self.assertEqual(len(self.sent_to(ALICE, "ALERT_UPDATE")), 1)
        self.assertFalse(self.timers.is_pending("Alice", incident_id, 2))

    def test_update_denied_for_closed_unknown_and_non_owner(self):
        incident_id = self.publish(severity="WARNING")
        update = build_message(
            "UPDATE_ALERT", rq=30, incident_id=incident_id, severity="INFO",
            title="t", description="d",
        )

        with self.assertRaises(ValueError) as denied:
            self.incidents.handle_update(update, OTHER_PUB)
        self.assertEqual(str(denied.exception), "Not_Owner")

        unknown = build_message(
            "UPDATE_ALERT", rq=31, incident_id="INC999", severity="INFO",
            title="t", description="d",
        )
        with self.assertRaises(ValueError) as denied:
            self.incidents.handle_update(unknown, PUB)
        self.assertEqual(str(denied.exception), "Unknown_Incident")

        close = build_message("CLOSE_ALERT", rq=32, incident_id=incident_id, reason="Resolved")
        self.incidents.handle_close(close, PUB)

        with self.assertRaises(ValueError) as denied:
            self.incidents.handle_update(update, PUB)
        self.assertEqual(str(denied.exception), "Incident_Closed")

    # ---- close ----
    def test_close_sends_all_clear_with_ack_timers(self):
        incident_id = self.publish(severity="WARNING")
        close = build_message("CLOSE_ALERT", rq=40, incident_id=incident_id, reason="Fire out")
        self.incidents.handle_close(close, PUB)

        reply = self.replies[-1][1]
        self.assertEqual(reply["type"], "ALERT_CLOSED")
        self.assertEqual(reply["rq"], 40)
        self.assertEqual(self.storage.get_incident(incident_id)["status"], "CLOSED")

        all_clear = self.sent_to(ALICE, "ALL_CLEAR")[0]
        self.assertEqual(all_clear["version"], 2)
        self.assertEqual(all_clear["message"], "Incident closed: Fire out")
        self.assertTrue(self.timers.is_pending("Alice", incident_id, 2))
        self.assertTrue(self.timers.is_pending("Bob", incident_id, 2))

        self.ack("Alice", ALICE, all_clear)
        self.assertEqual(self.storage.get_ack(incident_id, 2, "Alice"), "CONFIRMED")

    # ---- status ----
    def test_status_report_recorded_latest_wins_and_denials(self):
        incident_id = self.publish(severity="INFO")

        report = build_message(
            "STATUS_REPORT", rq=50, incident_id=incident_id, name="Alice", status="NEED_HELP",
        )
        reply = self.status.handle_status(report, ALICE)
        self.assertEqual(reply["type"], "STATUS_RECORDED")
        self.assertEqual(reply["rq"], 50)

        report = build_message(
            "STATUS_REPORT", rq=51, incident_id=incident_id, name="Alice", status="SAFE",
        )
        self.status.handle_status(report, ALICE)
        self.assertEqual(self.storage.get_status(incident_id, "Alice"), "SAFE")

        with self.assertRaises(ValueError) as denied:
            self.status.handle_status(report, STRANGER)
        self.assertEqual(str(denied.exception), "Not_Registered")

        with self.assertRaises(ValueError) as denied:
            self.status.handle_status(report, BOB)            # Bob claiming to be Alice
        self.assertEqual(str(denied.exception), "Name_Mismatch")

        unknown = build_message(
            "STATUS_REPORT", rq=52, incident_id="INC999", name="Alice", status="SAFE",
        )
        with self.assertRaises(ValueError) as denied:
            self.status.handle_status(unknown, ALICE)
        self.assertEqual(str(denied.exception), "Unknown_Incident")

        close = build_message("CLOSE_ALERT", rq=53, incident_id=incident_id, reason="Done")
        self.incidents.handle_close(close, PUB)
        with self.assertRaises(ValueError) as denied:
            self.status.handle_status(report, ALICE)
        self.assertEqual(str(denied.exception), "Incident_Closed")

    # ---- persistence ----
    def test_storage_round_trip_and_restart_recovery(self):
        incident_id = self.publish()                            # leaves Alice and Bob PENDING
        self.storage.set_status(incident_id, "Alice", "SAFE")
        self.timers.shutdown()                                  # simulate a crash: no more timers

        reloaded = Storage(self.path)

        incident = reloaded.get_incident(incident_id)
        self.assertEqual(incident["version"], 1)
        self.assertEqual(incident["publisher"], "CampusSecurity")
        self.assertEqual(sorted(incident["recipients"]), ["Alice", "Bob"])
        self.assertEqual(reloaded.get_status(incident_id, "Alice"), "SAFE")
        self.assertEqual(reloaded.get_ack(incident_id, 1, "Alice"), "UNCONFIRMED_RESTART")
        self.assertEqual(reloaded.get_ack(incident_id, 1, "Bob"), "UNCONFIRMED_RESTART")
        self.assertEqual(reloaded.new_incident_id(), "INC002")  # counter continues

        self.assertIsNone(reloaded.online_name(PUB))            # must re-register after restart


if __name__ == "__main__":
    unittest.main()
