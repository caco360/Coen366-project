"""Phase 2 handler: STATUS_REPORT from registered clients."""

import logging

from common.protocol import build_message

log = logging.getLogger("CENRS")


class StatusHandler:

    def __init__(self, storage):
        self.storage = storage

    def handle_status(self, message, address):
        """Record SAFE / NEED_HELP / LEFT_AREA for an ACTIVE incident, keeping only the latest."""

        name = self.storage.online_name(address)

        log.info(
            "[RECV] STATUS_REPORT rq=%d from %s %s %s %s",
            message["rq"], name or "unregistered", address,
            message["incident_id"], message["status"],
        )

        if name is None:
            raise ValueError("Not_Registered")

        if message["name"] != name:
            raise ValueError("Name_Mismatch")

        incident = self.storage.get_incident(message["incident_id"])

        if incident is None:
            raise ValueError("Unknown_Incident")

        if incident["status"] != "ACTIVE":
            raise ValueError("Incident_Closed")

        self.storage.set_status(message["incident_id"], name, message["status"])

        log.info("[STATUS] %s reported %s for %s", name, message["status"], message["incident_id"])

        return build_message("STATUS_RECORDED", rq=message["rq"])
