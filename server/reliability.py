"""ACK timers and retransmission for CRITICAL alerts and ALL_CLEAR (Phase 2).

One TimerManager lives in the server. Each outgoing message that needs an ACK
gets its own threading.Timer, so many deliveries can be waiting at the same
time without blocking the UDP receive loop or each other.
"""

import logging
import random
import threading

from common import config

log = logging.getLogger("CENRS")


class TimerManager:

    def __init__(self, storage, sendto):
        """sendto(packet_bytes, address) is injected so tests can record instead of sending."""

        self.storage = storage
        self.sendto = sendto
        self.lock = threading.Lock()

        # (name, incident_id, version) -> {"packet", "address", "attempts", "label", "timer"}
        self.pending = {}

    # -----------------------------------
    # Sending with simulated loss
    # -----------------------------------
    def transmit(self, packet, address):
        """Send a fan-out packet unless simulated loss drops it. Returns True if sent."""

        if random.random() < config.LOSS_RATE:
            return False

        self.sendto(packet, address)
        return True

    # -----------------------------------
    # Start waiting for an ACK
    # -----------------------------------
    def start(self, name, incident_id, version, packet, address, label):
        """Record the first transmission (already sent by the caller) and arm its timer."""

        key = (name, incident_id, version)

        with self.lock:

            old = self.pending.get(key)
            if old is not None:
                old["timer"].cancel()

            self.storage.set_ack(incident_id, version, name, "PENDING")

            entry = {
                "packet": packet,
                "address": address,
                "attempts": 1,
                "label": label,
                "timer": None,
            }

            self.pending[key] = entry
            self._arm(key, entry)

    def _arm(self, key, entry):
        """Create and start the timer for one pending delivery."""

        timer = threading.Timer(config.ACK_TIMEOUT, self._on_timeout, [key])
        timer.daemon = True
        entry["timer"] = timer
        timer.start()

    # -----------------------------------
    # Timeout: retransmit or give up
    # -----------------------------------
    def _on_timeout(self, key):

        name, incident_id, version = key

        with self.lock:

            entry = self.pending.get(key)

            if entry is None:
                return  # the ACK arrived just before the timer woke up

            if entry["attempts"] >= config.MAX_ATTEMPTS:
                del self.pending[key]
                self.storage.set_ack(incident_id, version, name, "UNCONFIRMED")
                log.info("[TIMER] UNCONFIRMED %s %s v%d", name, incident_id, version)
                return

            entry["attempts"] += 1
            sent = self.transmit(entry["packet"], entry["address"])

            log.info(
                "[TIMER] retransmit attempt %d/%d %s to %s %s v%d%s",
                entry["attempts"], config.MAX_ATTEMPTS, entry["label"],
                name, incident_id, version,
                "" if sent else " (dropped by LOSS_RATE)",
            )

            self._arm(key, entry)

    # -----------------------------------
    # ACK received
    # -----------------------------------
    def ack(self, name, incident_id, version, rq=None):
        """Confirm a delivery. Returns False for a duplicate or stale ACK."""

        key = (name, incident_id, version)

        with self.lock:

            entry = self.pending.pop(key, None)

            if entry is None:
                log.info(
                    "[ACK] duplicate or stale ACK ignored %s %s v%d (rq=%s)",
                    name, incident_id, version, rq,
                )
                return False

            entry["timer"].cancel()
            self.storage.set_ack(incident_id, version, name, "CONFIRMED")

            log.info("[ACK] CONFIRMED %s %s v%d (rq=%s)", name, incident_id, version, rq)
            return True

    # -----------------------------------
    # Helpers
    # -----------------------------------
    def is_pending(self, name, incident_id, version):

        with self.lock:
            return (name, incident_id, version) in self.pending

    def shutdown(self):
        """Cancel every timer (server shutdown)."""

        with self.lock:
            for entry in self.pending.values():
                entry["timer"].cancel()
            self.pending.clear()
