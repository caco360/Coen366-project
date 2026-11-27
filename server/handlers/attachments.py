"""Phase 3 handler: ATTACH_REQ (where can a subscriber download an incident's attachment?).

The server only gives out the publisher's TCP endpoint; it never stores or relays the file.
"""

import logging

from common.protocol import build_message

log = logging.getLogger("CENRS")


class AttachmentHandler:

    def __init__(self, storage):
        self.storage = storage

    def handle_attach_req(self, message, address):
        """Reply ATTACH_INFO, or ATTACH_DENIED with the first failed check.

        The denial is returned (not raised) because ATTACH_DENIED also needs the incident_id.
        """

        incident_id = message["incident_id"]
        log.info("[RECV] ATTACH_REQ rq=%d from %s %s %s %s", message["rq"],
                 self.storage.online_name(address) or "unregistered", address,
                 incident_id, message["file_name"])

        reason = self._check(message, address)

        if reason:
            return build_message("ATTACH_DENIED", rq=message["rq"],
                                 incident_id=incident_id, reason=reason)

        publisher = self.storage.get_incident(incident_id)["publisher"]
        user = self.storage.get_user(publisher)

        return build_message("ATTACH_INFO", rq=message["rq"], incident_id=incident_id,
                             publisher_name=publisher, ip=user["ip"],
                             tcp_port=user["tcp_port"], file_name=message["file_name"])

    def _check(self, message, address):
        """Return the denial reason, or None if the request is valid."""

        if self.storage.online_name(address) is None:
            return "Not_Registered"

        incident = self.storage.get_incident(message["incident_id"])

        if incident is None:
            return "Unknown_Incident"
        if incident["status"] != "ACTIVE":
            return "Incident_Closed"
        if incident["attachment"] in ("NONE", "") or incident["attachment"] != message["file_name"]:
            return "File_Not_For_Incident"
        if incident["publisher"] not in self.storage.online.values():
            return "Publisher_Unavailable"   # must be registered in the current server run

        return None
