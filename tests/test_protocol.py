import unittest
import threading

from common.protocol import (
    MalformedMessage,
    build_message,
    encode_message,
    decode_message,
    validate_message,
    next_request_id,
    crc32_hex,
    RqCounter,
)


class TestProtocol(unittest.TestCase):

    def test_register_round_trip(self):
        msg = build_message(
            "REGISTER",
            rq=1,
            name="Gianluca",
            role="SUBSCRIBER",
            ip="127.0.0.1",
            udp_port=5001,
            tcp_port=6001,
        )
        data = encode_message(msg)
        decoded = decode_message(data)

        self.assertEqual(decoded["type"], "REGISTER")
        self.assertEqual(decoded["name"], "Gianluca")
        self.assertEqual(decoded["role"], "SUBSCRIBER")
        self.assertEqual(decoded["udp_port"], 5001)
        self.assertEqual(decoded["tcp_port"], 6001)

    def test_text_with_spaces(self):
        msg = build_message(
            "PUBLISH_ALERT",
            rq=2,
            category="FIRE",
            location="HALL",
            severity="CRITICAL",
            title="Fire Alarm",
            description="Fire alarm at Hall Building",
            attachment="evacuation_map.pdf",
        )
        data = encode_message(msg)
        decoded = decode_message(data)

        self.assertEqual(decoded["title"], "Fire Alarm")
        self.assertEqual(decoded["description"], "Fire alarm at Hall Building")

    def test_missing_required_field(self):
        msg = {
            "type": "REGISTER",
            "rq": 1,
            "name": "Gianluca",
            # missing role
            "ip": "127.0.0.1",
            "udp_port": 5001,
            "tcp_port": 6001,
        }
        with self.assertRaises(MalformedMessage):
            validate_message(msg)

    def test_unknown_message_type(self):
        msg = {
            "type": "HELLO_WORLD",
            "rq": 1
        }
        with self.assertRaises(MalformedMessage):
            validate_message(msg)

    def test_invalid_json(self):
        bad = b'{"type": "REGISTER", "rq": 1,,}'
        with self.assertRaises(MalformedMessage):
            decode_message(bad)

    def test_not_utf8(self):
        bad = b"\xff\xfe\xfa"
        with self.assertRaises(MalformedMessage):
            decode_message(bad)

    def test_empty_message(self):
        with self.assertRaises(MalformedMessage):
            decode_message(b"   ")

    def test_invalid_role(self):
        with self.assertRaises(MalformedMessage):
            build_message(
                "REGISTER",
                rq=1,
                name="Gianluca",
                role="ADMIN",
                ip="127.0.0.1",
                udp_port=5001,
                tcp_port=6001,
            )

    def test_invalid_port(self):
        with self.assertRaises(MalformedMessage):
            build_message(
                "REGISTER",
                rq=1,
                name="Gianluca",
                role="SUBSCRIBER",
                ip="127.0.0.1",
                udp_port=70000,
                tcp_port=6001,
            )

    def test_invalid_severity(self):
        with self.assertRaises(MalformedMessage):
            build_message(
                "PUBLISH_ALERT",
                rq=2,
                category="FIRE",
                location="HALL",
                severity="URGENT",
                title="Fire Alarm",
                description="Evacuate immediately",
                attachment="NONE",
            )

    def test_crc32(self):
        data = b"hello world"
        checksum = crc32_hex(data)
        self.assertEqual(len(checksum), 8)
        self.assertTrue(isinstance(checksum, str))

    def test_next_request_id(self):
        a = next_request_id()
        b = next_request_id()
        self.assertEqual(b, a + 1)

    def test_thread_safe_counter(self):
        counter = RqCounter()
        results = []
        lock = threading.Lock()

        def worker():
            value = counter.next()
            with lock:
                results.append(value)

        threads = [threading.Thread(target=worker) for _ in range(50)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(results), 50)
        self.assertEqual(len(set(results)), 50)
        self.assertEqual(min(results), 1)
        self.assertEqual(max(results), 50)

    def test_alert_ack_valid(self):
        msg = build_message(
            "ALERT_ACK",
            rq=83,
            incident_id="INC102",
            version=1,
            name="Gianluca",
        )
        data = encode_message(msg)
        decoded = decode_message(data)
        self.assertEqual(decoded["type"], "ALERT_ACK")
        self.assertEqual(decoded["incident_id"], "INC102")

    def test_status_report_valid(self):
        msg = build_message(
            "STATUS_REPORT",
            rq=91,
            incident_id="INC102",
            name="Gianluca",
            status="SAFE",
        )
        data = encode_message(msg)
        decoded = decode_message(data)
        self.assertEqual(decoded["status"], "SAFE")


if __name__ == "__main__":
    unittest.main()