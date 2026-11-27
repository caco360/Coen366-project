"""Phase 3 tests: ATTACH_REQ on the server, publisher records, P2P TCP transfer."""

import json
import os
import socket
import tempfile
import threading
import unittest
from pathlib import Path

from client.client import Client
from common.protocol import build_message, crc32_hex, decode_message, encode_message
from server.handlers.attachments import AttachmentHandler
from server.server import EmergencyServer
from server.storage import Storage
from tests.test_client import FakeServer

PUB = ("127.0.0.1", 6001)
ALICE = ("127.0.0.1", 6002)
STRANGER = ("127.0.0.1", 6999)


def register(storage, name, role, address):
    storage.register(build_message(
        "REGISTER", rq=1, name=name, role=role,
        ip=address[0], udp_port=address[1], tcp_port=address[1] + 1000,
    ), address)


class ServerAttachReqTests(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.storage = Storage(Path(self.tmp.name) / "state.json")
        self.handler = AttachmentHandler(self.storage)
        register(self.storage, "CampusSecurity", "PUBLISHER", PUB)
        register(self.storage, "Alice", "SUBSCRIBER", ALICE)
        self.incident = self.storage.create_incident(
            "CampusSecurity", "FIRE", "HALL", "CRITICAL", "Fire", "Evacuate", "map.pdf")

    def tearDown(self):
        self.tmp.cleanup()

    def ask(self, incident_id=None, file_name="map.pdf", address=ALICE):
        message = build_message("ATTACH_REQ", rq=91, incident_id=incident_id or self.incident,
                                file_name=file_name)
        return self.handler.handle_attach_req(message, address)

    def test_attach_info_returns_publisher_endpoint(self):
        reply = self.ask()
        self.assertEqual(reply["type"], "ATTACH_INFO")
        self.assertEqual(reply["rq"], 91)
        self.assertEqual(reply["incident_id"], self.incident)
        self.assertEqual(reply["publisher_name"], "CampusSecurity")
        self.assertEqual(reply["ip"], "127.0.0.1")
        self.assertEqual(reply["tcp_port"], 7001)
        self.assertEqual(reply["file_name"], "map.pdf")

    def assertDenied(self, reply, reason, incident_id=None):
        self.assertEqual(reply["type"], "ATTACH_DENIED")
        self.assertEqual(reply["rq"], 91)
        self.assertEqual(reply["incident_id"], incident_id or self.incident)
        self.assertEqual(reply["reason"], reason)

    def test_attach_denied_reasons(self):
        self.assertDenied(self.ask(address=STRANGER), "Not_Registered")
        self.assertDenied(self.ask(incident_id="INC999"), "Unknown_Incident", "INC999")
        self.assertDenied(self.ask(file_name="other.pdf"), "File_Not_For_Incident")

        no_file = self.storage.create_incident(
            "CampusSecurity", "FIRE", "HALL", "INFO", "t", "d", "NONE")
        self.assertDenied(self.ask(incident_id=no_file, file_name="NONE"),
                          "File_Not_For_Incident", no_file)

        del self.storage.online[PUB]                       # publisher not registered this run
        self.assertDenied(self.ask(), "Publisher_Unavailable")

        self.storage.update_incident(self.incident, status="CLOSED")
        self.assertDenied(self.ask(), "Incident_Closed")


def reply_once(fake, make_reply):
    """Fake server answers the next request it receives with make_reply(request)."""

    def run():
        fake.sock.settimeout(3)
        data, address = fake.sock.recvfrom(65535)
        fake.send(make_reply(decode_message(data)), address)

    thread = threading.Thread(target=run)
    thread.start()
    return thread


class ClientPublishRecordTests(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.fake = FakeServer()
        self.pub = self.make()
        (self.pub.shared / "map.pdf").write_bytes(b"map")

    def tearDown(self):
        self.pub.stop()
        self.fake.close()
        self.tmp.cleanup()

    def make(self):
        c = Client("CampusSecurity", "PUBLISHER", udp_port=0, tcp_port=0,
                   server=self.fake.address, data_dir=self.tmp.name)
        c.start()
        return c

    def test_publish_refused_when_file_missing_or_path(self):
        for name in ("missing.pdf", "../map.pdf"):
            with self.assertRaises(ValueError):
                self.pub.publish("FIRE", "HALL", "CRITICAL", "t", "d", name)
        self.assertEqual(self.fake.recv_all(), [])          # nothing was sent

    def test_published_json_written_on_accept_and_close(self):
        t = reply_once(self.fake, lambda r: build_message(
            "ALERT_ACCEPTED", rq=r["rq"], incident_id="INC001"))
        self.pub.publish("FIRE", "HALL", "CRITICAL", "t", "d", "map.pdf")
        t.join()

        saved = json.loads((self.pub.home / "published.json").read_text())
        self.assertEqual(saved, {"INC001": {"file_name": "map.pdf", "status": "ACTIVE"}})

        t = reply_once(self.fake, lambda r: build_message(
            "ALERT_CLOSED", rq=r["rq"], incident_id="INC001"))
        self.pub.close("INC001", "Resolved")
        t.join()

        saved = json.loads((self.pub.home / "published.json").read_text())
        self.assertEqual(saved["INC001"]["status"], "CLOSED")

    def test_published_json_reloaded_on_restart(self):
        t = reply_once(self.fake, lambda r: build_message(
            "ALERT_ACCEPTED", rq=r["rq"], incident_id="INC001"))
        self.pub.publish("FIRE", "HALL", "CRITICAL", "t", "d", "map.pdf")
        t.join()
        self.pub.stop()

        self.pub = self.make()                               # same name and data_dir
        self.assertEqual(self.pub.published["INC001"]["file_name"], "map.pdf")


class PublisherServeTests(unittest.TestCase):
    """The test plays the subscriber with a raw TCP socket against a real publisher Client."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.fake = FakeServer()
        self.pub = Client("CampusSecurity", "PUBLISHER", udp_port=0, tcp_port=0,
                          server=self.fake.address, data_dir=self.tmp.name)
        self.pub.start()
        self.content = os.urandom(200_000)                   # more than one 64 KiB chunk
        (self.pub.shared / "map.pdf").write_bytes(self.content)
        self.pub._set_published("INC001", file_name="map.pdf", status="ACTIVE")

    def tearDown(self):
        self.pub.stop()
        self.fake.close()
        self.tmp.cleanup()

    def get(self, incident_id="INC001", file_name="map.pdf"):
        """Connect, send GET_ATTACHMENT, return (socket, stream, decoded header line)."""

        sock = socket.create_connection(("127.0.0.1", self.pub.tcp_port), timeout=5)
        sock.sendall(encode_message(build_message(
            "GET_ATTACHMENT", rq=94, subscriber_name="Alice",
            incident_id=incident_id, file_name=file_name)) + b"\n")
        stream = sock.makefile("rb")
        return sock, stream, decode_message(stream.readline())

    def test_serves_file_with_header_and_exact_bytes(self):
        sock, stream, header = self.get()

        self.assertEqual(header["type"], "ATTACH_DATA")
        self.assertEqual(header["rq"], 94)
        self.assertEqual(header["file_size"], len(self.content))
        self.assertEqual(header["checksum"], crc32_hex(self.content))
        self.assertEqual(stream.read(header["file_size"]), self.content)

        sock.sendall(encode_message(build_message(
            "ATTACH_OK", rq=94, incident_id="INC001", file_name="map.pdf")) + b"\n")
        self.assertEqual(stream.read(), b"")                 # publisher closed the connection
        sock.close()

    def assertRefused(self, reason, **request):
        sock, stream, header = self.get(**request)
        self.assertEqual(header["type"], "ATTACH_ERROR")
        self.assertEqual(header["reason"], reason)
        self.assertEqual(stream.read(), b"")                 # no file bytes, then closed
        sock.close()

    def test_refusals(self):
        self.assertRefused("Unknown_Incident", incident_id="INC999")
        self.assertRefused("File_Not_For_Incident", file_name="other.pdf")
        self.assertRefused("File_Not_For_Incident", file_name="../published.json")

        (self.pub.shared / "map.pdf").unlink()
        self.assertRefused("File_Not_Available")

        self.pub._set_published("INC001", status="CLOSED")
        self.assertRefused("Incident_Closed")

    def test_restarted_publisher_rebinds_same_tcp_port_after_a_transfer(self):
        sock, stream, header = self.get()
        stream.read(header["file_size"])
        sock.close()
        port = self.pub.tcp_port
        self.pub.stop()

        self.pub = Client("CampusSecurity", "PUBLISHER", udp_port=0, tcp_port=port,
                          server=self.fake.address, data_dir=self.tmp.name)
        self.pub.start()                                     # must not fail: port reused at once
        self.assertEqual(self.get()[2]["type"], "ATTACH_DATA")   # still serves from published.json

    def test_two_subscribers_at_once(self):
        first = self.get()                                   # both requests sent before
        second = self.get()                                  # either file is read
        for sock, stream, header in (first, second):
            self.assertEqual(stream.read(header["file_size"]), self.content)
            sock.close()


class AttachEndToEndTests(unittest.TestCase):
    """Real server + real clients: `attach` = ATTACH_REQ (UDP) + GET_ATTACHMENT (TCP)."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.server = EmergencyServer(host="127.0.0.1", port=0,
                                      data_file=Path(self.tmp.name) / "state.json")
        self.server_address = self.server.socket.getsockname()
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.clients = []
        self.content = os.urandom(200_000)

    def tearDown(self):
        for c in self.clients:
            c.stop()
        self.server.timers.shutdown()
        self.server.workers.shutdown(wait=True)
        self.tmp.cleanup()

    def make(self, name, role):
        c = Client(name, role, udp_port=0, tcp_port=0 if role != "SUBSCRIBER" else 7999,
                   server=self.server_address, data_dir=self.tmp.name)
        c.start()
        c.register()
        self.clients.append(c)
        return c

    def publish(self, content=None):
        """A publisher with shared/map.pdf publishes it; returns (publisher, incident_id)."""

        pub = self.make("CampusSecurity", "PUBLISHER")
        (pub.shared / "map.pdf").write_bytes(self.content if content is None else content)
        reply = pub.publish("FIRE", "HALL", "CRITICAL", "Fire", "Evacuate", "map.pdf")
        return pub, reply["incident_id"]

    def assertSaved(self, client, result, content=None):
        self.assertTrue(result.startswith("saved"), result)
        self.assertEqual((client.downloads / "map.pdf").read_bytes(),
                         self.content if content is None else content)
        self.assertEqual(list(client.downloads.glob("*.part")), [])

    def test_attach_downloads_verified_file(self):
        _, incident_id = self.publish()
        alice = self.make("Alice", "SUBSCRIBER")
        self.assertSaved(alice, alice.attach(incident_id, "map.pdf"))

    def test_attach_empty_file(self):
        _, incident_id = self.publish(content=b"")
        alice = self.make("Alice", "SUBSCRIBER")
        self.assertSaved(alice, alice.attach(incident_id, "map.pdf"), content=b"")

    def test_attach_denied_not_retried(self):
        _, incident_id = self.publish()
        alice = self.make("Alice", "SUBSCRIBER")
        self.assertEqual(alice.attach(incident_id, "other.pdf"),
                         "ATTACH_DENIED File_Not_For_Incident")

    def test_publisher_gone_after_info_retries_then_fails(self):
        pub, incident_id = self.publish()
        pub.stop()                                   # still "online" on the server
        alice = self.make("Alice", "SUBSCRIBER")
        result = alice.attach(incident_id, "map.pdf")
        self.assertTrue(result.startswith("failed after 3 attempts"), result)
        self.assertEqual(list(alice.downloads.iterdir()), [])

    def test_two_subscribers_download_concurrently(self):
        _, incident_id = self.publish()
        subs = [self.make("Alice", "SUBSCRIBER"), self.make("Bob", "SUBSCRIBER")]
        results = {}
        threads = [threading.Thread(target=lambda c=c: results.update(
            {c.name: c.attach(incident_id, "map.pdf")})) for c in subs]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        for c in subs:
            self.assertSaved(c, results[c.name])

    # ---- bad transfers, served by a fake publisher socket in the test ----
    def fake_publisher(self, bad_first_answer):
        """Register a fake PUBLISHER whose TCP port is a socket in this test; it publishes map.pdf.

        Connection 1 is answered by bad_first_answer(conn, rq); connection 2 correctly.
        Returns (incident_id, list that receives the subscriber's reply line on connection 1).
        """

        listener = socket.create_server(("127.0.0.1", 0))
        udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp.bind(("127.0.0.1", 0))
        udp.settimeout(3)
        self.addCleanup(listener.close)
        self.addCleanup(udp.close)

        def send(msg_type, **fields):
            udp.sendto(encode_message(build_message(msg_type, rq=1, **fields)), self.server_address)
            return decode_message(udp.recvfrom(65535)[0])

        send("REGISTER", name="FakePub", role="PUBLISHER", ip="127.0.0.1",
             udp_port=udp.getsockname()[1], tcp_port=listener.getsockname()[1])
        incident_id = send("PUBLISH_ALERT", category="FIRE", location="HALL", severity="INFO",
                           title="t", description="d", attachment="map.pdf")["incident_id"]
        replies = []

        def serve():
            for attempt in (1, 2):
                conn, _ = listener.accept()
                with conn:
                    stream = conn.makefile("rb")
                    rq = decode_message(stream.readline())["rq"]
                    if attempt == 1:
                        bad_first_answer(conn, rq)
                        replies.append(decode_message(stream.readline()))
                    else:
                        conn.sendall(self.header(rq, len(self.content), crc32_hex(self.content)))
                        conn.sendall(self.content)
                        stream.readline()

        threading.Thread(target=serve, daemon=True).start()
        return incident_id, replies

    def header(self, rq, size, checksum):
        return encode_message(build_message(
            "ATTACH_DATA", rq=rq, incident_id="X", file_name="map.pdf",
            file_size=size, checksum=checksum)) + b"\n"

    def test_bad_checksum_then_good_is_retried(self):
        def bad(conn, rq):
            conn.sendall(self.header(rq, len(self.content), "00000000") + self.content)

        incident_id, replies = self.fake_publisher(bad)
        alice = self.make("Alice", "SUBSCRIBER")
        self.assertSaved(alice, alice.attach(incident_id, "map.pdf"))
        self.assertEqual(replies[0]["type"], "ATTACH_ERROR")
        self.assertEqual(replies[0]["reason"], "Checksum_Mismatch")

    def test_incomplete_then_good_is_retried(self):
        def bad(conn, rq):
            conn.sendall(self.header(rq, len(self.content), crc32_hex(self.content))
                         + self.content[:40])
            conn.shutdown(socket.SHUT_WR)            # stop sending, still read the reply

        incident_id, replies = self.fake_publisher(bad)
        alice = self.make("Alice", "SUBSCRIBER")
        self.assertSaved(alice, alice.attach(incident_id, "map.pdf"))
        self.assertEqual(replies[0]["reason"], "Incomplete_Transfer")


if __name__ == "__main__":
    unittest.main()
