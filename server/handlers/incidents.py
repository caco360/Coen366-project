"""Phase 2 handlers: publish, update and close incidents, and fan-out to subscribers."""

import logging

from common.protocol import build_message, encode_message

log = logging.getLogger("CENRS")

PUBLISHER_ROLES = ("PUBLISHER", "BOTH")
SUBSCRIBER_ROLES = ("SUBSCRIBER", "BOTH")


def matches(sub_location, sub_category, incident):
    """A subscription matches when each side equals the incident value or is ALL."""

    location_ok = sub_location in ("ALL", incident["location"])
    category_ok = sub_category in ("ALL", incident["category"])
    return location_ok and category_ok


class IncidentHandlers:

    def __init__(self, storage, timers, send):
        """send(address, message_dict) is the server's direct reply path (never dropped)."""

        self.storage = storage
        self.timers = timers
        self.send = send

    # -----------------------------------
    # Validation helpers
    # -----------------------------------
    def _publisher(self, address):
        """Name of the registered publisher at this address, or a denial reason."""

        name = self.storage.online_name(address)

        if name is None:
            raise ValueError("Not_Registered")

        user = self.storage.get_user(name)

        if user is None or user["role"] not in PUBLISHER_ROLES:
            raise ValueError("Unauthorized_Role")

        return name

    def _owned_active_incident(self, message, address):
        """Publisher name and incident for UPDATE_ALERT / CLOSE_ALERT, after all checks."""

        name = self._publisher(address)
        incident = self.storage.get_incident(message["incident_id"])

        if incident is None:
            raise ValueError("Unknown_Incident")

        if incident["status"] != "ACTIVE":
            raise ValueError("Incident_Closed")

        if incident["publisher"] != name:
            raise ValueError("Not_Owner")

        return name, incident

    def _who(self, address):
        return self.storage.online_name(address) or "unregistered"

    # -----------------------------------
    # PUBLISH_ALERT
    # -----------------------------------
    def handle_publish(self, message, address):

        log.info(
            '[RECV] PUBLISH_ALERT rq=%d from %s %s %s %s %s "%s"',
            message["rq"], self._who(address), address,
            message["category"], message["location"], message["severity"], message["title"],
        )

        name = self._publisher(address)

        incident_id = self.storage.create_incident(
            publisher=name,
            category=message["category"],
            location=message["location"],
            severity=message["severity"],
            title=message["title"],
            description=message["description"],
            attachment=message["attachment"],
        )

        # Reply first, then distribute. Direct replies never go through LOSS_RATE.
        self.send(address, build_message("ALERT_ACCEPTED", rq=message["rq"], incident_id=incident_id))

        self.fan_out(self.storage.get_incident(incident_id), "ALERT")
        return None

    # -----------------------------------
    # UPDATE_ALERT
    # -----------------------------------
    def handle_update(self, message, address):

        log.info(
            '[RECV] UPDATE_ALERT rq=%d from %s %s %s %s "%s"',
            message["rq"], self._who(address), address,
            message["incident_id"], message["severity"], message["title"],
        )

        name, incident = self._owned_active_incident(message, address)

        version = self.storage.update_incident(
            incident["incident_id"],
            severity=message["severity"],
            title=message["title"],
            description=message["description"],
        )

        self.send(address, build_message(
            "ALERT_UPDATED", rq=message["rq"],
            incident_id=incident["incident_id"], version=version,
        ))

        self.fan_out(self.storage.get_incident(incident["incident_id"]), "ALERT_UPDATE")
        return None

    # -----------------------------------
    # CLOSE_ALERT
    # -----------------------------------
    def handle_close(self, message, address):

        log.info(
            '[RECV] CLOSE_ALERT rq=%d from %s %s %s "%s"',
            message["rq"], self._who(address), address,
            message["incident_id"], message["reason"],
        )

        name, incident = self._owned_active_incident(message, address)

        self.storage.update_incident(incident["incident_id"], status="CLOSED")

        self.send(address, build_message(
            "ALERT_CLOSED", rq=message["rq"], incident_id=incident["incident_id"],
        ))

        self.fan_out(
            self.storage.get_incident(incident["incident_id"]),
            "ALL_CLEAR",
            message="Incident closed: " + message["reason"],
        )
        return None

    # -----------------------------------
    # Distribution
    # -----------------------------------
    def _targets(self, incident, include_recipients):
        """Registered subscribers whose subscription matches (plus past recipients for ALL_CLEAR)."""

        users = self.storage.list_users()
        targets = {}

        for name, user in users.items():

            if user["role"] not in SUBSCRIBER_ROLES:
                continue

            subscribed = any(
                matches(location, category, incident)
                for location, category in user["subscriptions"]
            )

            if subscribed or (include_recipients and name in incident["recipients"]):
                targets[name] = user

        return targets

    @staticmethod
    def _outgoing(incident, msg_type, extra):
        """Build the fan-out message with a fresh server RQ#."""

        if msg_type == "ALERT":
            return build_message(
                "ALERT",
                incident_id=incident["incident_id"], version=incident["version"],
                category=incident["category"], location=incident["location"],
                severity=incident["severity"], title=incident["title"],
                description=incident["description"], attachment=incident["attachment"],
            )

        if msg_type == "ALERT_UPDATE":
            return build_message(
                "ALERT_UPDATE",
                incident_id=incident["incident_id"], version=incident["version"],
                severity=incident["severity"], title=incident["title"],
                description=incident["description"],
            )

        if msg_type == "ALL_CLEAR":
            return build_message(
                "ALL_CLEAR",
                incident_id=incident["incident_id"], version=incident["version"],
                message=extra["message"],
            )

        raise ValueError("Unknown_Fan_Out_Type")

    def fan_out(self, incident, msg_type, **extra):
        """Send msg_type to every matching subscriber; start ACK timers where required."""

        is_all_clear = msg_type == "ALL_CLEAR"
        needs_ack = is_all_clear or incident["severity"] == "CRITICAL"

        targets = self._targets(incident, include_recipients=is_all_clear)
        sent_to = []

        for name, user in targets.items():

            address = (user["ip"], user["udp_port"])
            message = self._outgoing(incident, msg_type, extra)
            packet = encode_message(message)

            delivered = self.timers.transmit(packet, address)

            log.info(
                "[SEND] %s rq=%d to %s %s %s v%d %s%s",
                msg_type, message["rq"], name, address,
                incident["incident_id"], incident["version"], incident["severity"],
                "" if delivered else " (dropped by LOSS_RATE)",
            )

            if needs_ack:
                self.timers.start(
                    name, incident["incident_id"], incident["version"],
                    packet, address, "%s rq=%d" % (msg_type, message["rq"]),
                )

            sent_to.append(name)

        if not is_all_clear:
            self.storage.add_recipients(incident["incident_id"], sent_to)

        return sent_to
