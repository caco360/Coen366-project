"""Phase 2 handler: ALERT_ACK from subscribers."""

import logging

log = logging.getLogger("CENRS")


class AckHandler:

    def __init__(self, timers):
        self.timers = timers

    def handle_ack(self, message, address):
        """Confirm the pending delivery; a duplicate or stale ACK is logged and ignored."""

        log.info(
            "[RECV] ALERT_ACK rq=%d from %s %s %s v%d",
            message["rq"], message["name"], address,
            message["incident_id"], message["version"],
        )

        self.timers.ack(
            message["name"], message["incident_id"], message["version"], rq=message["rq"],
        )
        return None
