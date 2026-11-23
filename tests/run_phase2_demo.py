"""Scripted Phase 2 demonstration: publish, fan-out, ACK timers, update, status, close.

Run from the project root:   python -m tests.run_phase2_demo

The server runs in a background thread on 127.0.0.1:5000 with a temporary data
file, so the real data/server_state.json is not touched. Server log lines are
prefixed with a timestamp; client-side output is prefixed with [CLIENT].
"""

import logging
import sys
import tempfile
import threading
import time
from pathlib import Path

from client.phase2_test_client import Peer
from common import config
from server.server import EmergencyServer

SERVER = ("127.0.0.1", 5000)


def banner(text):
    print("\n" + "=" * 70)
    print(text)
    print("=" * 70)


def show(peer, message):
    print(f"[CLIENT {peer.name}] got {message['type']} {message}")


def main():

    # Demo only: send the server log to stdout so it interleaves with the client output.
    for handler in logging.getLogger().handlers:
        handler.setStream(sys.stdout)

    # A short timeout keeps the demo quick while still showing three attempts.
    config.ACK_TIMEOUT = 1.0
    config.LOSS_RATE = 0.0

    tmp = tempfile.TemporaryDirectory()
    server = EmergencyServer(
        host=SERVER[0], port=SERVER[1],
        data_file=Path(tmp.name) / "server_state.json",
    )
    threading.Thread(target=server.serve_forever, daemon=True, name="server").start()
    time.sleep(0.2)

    publisher = Peer("CampusSecurity", 6001, SERVER)
    alice = Peer("Alice", 6002, SERVER)
    bob = Peer("Bob", 6003, SERVER)

    try:
        banner("1. Registration and subscriptions (Phase 1 messages)")
        show(publisher, publisher.register("PUBLISHER"))
        show(alice, alice.register("SUBSCRIBER"))
        show(bob, bob.register("SUBSCRIBER"))
        show(alice, alice.subscribe("HALL", "FIRE"))
        show(bob, bob.subscribe("ALL", "ALL"))

        banner("2. A SUBSCRIBER tries to publish -> ALERT_DENIED Unauthorized_Role")
        alice.send(
            "PUBLISH_ALERT", category="FIRE", location="HALL", severity="INFO",
            title="Not allowed", description="Alice is only a subscriber", attachment="NONE",
        )
        show(alice, alice.recv())

        banner("3. CRITICAL alert: Alice ACKs, Bob stays silent -> retransmissions, UNCONFIRMED")
        publisher.send(
            "PUBLISH_ALERT", category="FIRE", location="HALL", severity="CRITICAL",
            title="Fire alarm", description="Fire alarm at Hall Building, evacuate",
            attachment="NONE",
        )
        accepted = publisher.recv()
        show(publisher, accepted)
        incident_id = accepted["incident_id"]

        alert = alice.recv()
        show(alice, alert)
        alice.ack(alert)
        print(f"[CLIENT Alice] sent ALERT_ACK rq={alert['rq']} (same rq as the ALERT)")

        print("[CLIENT Bob] received the ALERT but will NOT acknowledge ...")
        time.sleep(config.ACK_TIMEOUT * config.MAX_ATTEMPTS + 0.5)

        copies = bob.drain()
        print(f"[CLIENT Bob] received {len(copies)} copies of the ALERT, "
              f"rq values = {[m['rq'] for m in copies]}")
        print(f"[SERVER STATE] Alice v1 = {server.storage.get_ack(incident_id, 1, 'Alice')}, "
              f"Bob v1 = {server.storage.get_ack(incident_id, 1, 'Bob')}")

        banner("4. Duplicate ACK from Alice -> logged and ignored")
        alice.ack(alert)
        time.sleep(0.3)

        banner("5. UPDATE_ALERT to WARNING -> ALERT_UPDATE v2, no ACK needed")
        publisher.send(
            "UPDATE_ALERT", incident_id=incident_id, severity="WARNING",
            title="Fire contained", description="Stay away from Hall Building",
        )
        show(publisher, publisher.recv())
        show(alice, alice.recv())
        show(bob, bob.recv())

        banner("6. STATUS_REPORT from Alice -> STATUS_RECORDED")
        alice.send("STATUS_REPORT", incident_id=incident_id, name="Alice", status="SAFE")
        show(alice, alice.recv())
        print(f"[SERVER STATE] status Alice/{incident_id} = "
              f"{server.storage.get_status(incident_id, 'Alice')}")

        banner("7. CLOSE_ALERT -> ALL_CLEAR v3 to both, both ACK -> CONFIRMED")
        publisher.send("CLOSE_ALERT", incident_id=incident_id, reason="Fire extinguished")
        show(publisher, publisher.recv())

        for peer in (alice, bob):
            all_clear = peer.recv()
            show(peer, all_clear)
            peer.ack(all_clear)
        time.sleep(0.3)

        print(f"[SERVER STATE] Alice v3 = {server.storage.get_ack(incident_id, 3, 'Alice')}, "
              f"Bob v3 = {server.storage.get_ack(incident_id, 3, 'Bob')}")

        banner("8. UPDATE on a CLOSED incident -> ALERT_DENIED Incident_Closed")
        publisher.send(
            "UPDATE_ALERT", incident_id=incident_id, severity="INFO",
            title="Too late", description="Incident is closed",
        )
        show(publisher, publisher.recv())

        banner("Demo finished")
        print("Incident record:", server.storage.get_incident(incident_id))

    finally:
        for peer in (publisher, alice, bob):
            peer.close()
        server.timers.shutdown()
        tmp.cleanup()
        # The server thread is a daemon blocked in recvfrom; it ends with the process.


if __name__ == "__main__":
    main()
