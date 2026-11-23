
"""Thread-safe JSON storage for CENRS Phase 1."""

import copy
import json
import logging
import os
import threading
from pathlib import Path

log = logging.getLogger("CENRS")


class Storage:

    def __init__(self, path):

        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

        self.lock = threading.RLock()

        # Basic database structure
        self.data = {
            "users": {},
            "incidents": {},
            "status_reports": {},
            "deliveries": {},
            "incident_counter": 0      # Phase 2: last INC number handed out
        }

        # Recover previous information
        if self.path.exists():
            saved_data = json.loads(
                self.path.read_text(encoding="utf-8")
            )
            self.data.update(saved_data)
            self._recover_after_restart()   # Phase 2

        # Tracks clients registered during the current run
        self.online = {}

    # -----------------------------------
    # Save the database
    # -----------------------------------
    def _save(self):

        temp = self.path.with_name(self.path.name + ".tmp")

        temp.write_text(
            json.dumps(self.data, indent=2),
            encoding="utf-8"
        )

        os.replace(temp, self.path)

    # -----------------------------------
    # Register user
    # -----------------------------------
    def register(self, message, address):

        with self.lock:

            name = message["name"]

            if address != (message["ip"], message["udp_port"]):
                raise ValueError("Endpoint_Mismatch")

            if self.online.get(address) not in (None, name):
                raise ValueError("Endpoint_Already_Registered")

            if name in self.online.values() and self.online.get(address) != name:
                raise ValueError("Name_Already_Exists")

            previous = self.data["users"].get(name, {})

            if previous and previous["role"] != message["role"]:
                raise ValueError("Role_Change_Not_Allowed")

            self.data["users"][name] = {
                "role": message["role"],
                "ip": message["ip"],
                "udp_port": message["udp_port"],
                "tcp_port": message["tcp_port"],
                "subscriptions": previous.get("subscriptions", [])
            }

            self.online[address] = name
            self._save()

    # -----------------------------------
    # Subscribe user
    # -----------------------------------
    def subscribe(self, address, location, category):

        with self.lock:

            user = self._subscriber(address)
            subscription = [location, category]

            if subscription not in user["subscriptions"]:
                user["subscriptions"].append(subscription)
                self._save()

    # -----------------------------------
    # Unsubscribe user
    # -----------------------------------
    def unsubscribe(self, address, location, category):

        with self.lock:

            user = self._subscriber(address)
            subscription = [location, category]

            if subscription not in user["subscriptions"]:
                raise ValueError("Subscription_Not_Found")

            user["subscriptions"].remove(subscription)
            self._save()

    # -----------------------------------
    # Deregister user
    # -----------------------------------
    def deregister(self, address, name):

        with self.lock:

            if self.online.get(address) != name:
                return

            del self.online[address]
            del self.data["users"][name]

            self._save()

    # -----------------------------------
    # Verify subscriber
    # -----------------------------------
    def _subscriber(self, address):

        name = self.online.get(address)

        if name is None:
            raise ValueError("Not_Registered")

        user = self.data["users"][name]

        if user["role"] not in ("SUBSCRIBER", "BOTH"):
            raise ValueError("Unauthorized_Role")

        return user

    # -----------------------------------
    # Retrieve a user's information
    # -----------------------------------
    def get_user(self, name):

        with self.lock:
            return copy.deepcopy(
                self.data["users"].get(name)
            )

    # =========================================================
    # Phase 2 additions: incidents, ACK deliveries, status reports
    # =========================================================

    @staticmethod
    def _ack_key(incident_id, version, name):
        """Composite key for a delivery, kept as a plain string so JSON can store it."""
        return f"{incident_id}|{version}|{name}"

    @staticmethod
    def _status_key(incident_id, name):
        """Composite key for a status report, kept as a plain string."""
        return f"{incident_id}|{name}"

    def _recover_after_restart(self):
        """Timers do not survive a restart, so a PENDING delivery can no longer be waiting."""

        count = 0

        for key, state in self.data["deliveries"].items():
            if state == "PENDING":
                self.data["deliveries"][key] = "UNCONFIRMED_RESTART"
                count += 1

        if count:
            log.info("[RESTART] %d pending deliveries marked UNCONFIRMED_RESTART", count)
            self._save()

    # -----------------------------------
    # Identity helpers
    # -----------------------------------
    def online_name(self, address):
        """Name registered from this UDP address in the current run, or None."""

        with self.lock:
            return self.online.get(address)

    def list_users(self):
        """Copy of every registered user (used by fan-out)."""

        with self.lock:
            return copy.deepcopy(self.data["users"])

    # -----------------------------------
    # Incidents
    # -----------------------------------
    def new_incident_id(self):
        """Next unique ID such as INC001. The counter is persisted so IDs never repeat."""

        with self.lock:
            self.data["incident_counter"] += 1
            self._save()
            return "INC%03d" % self.data["incident_counter"]

    def create_incident(self, publisher, category, location, severity,
                        title, description, attachment):
        """Store a new ACTIVE incident at version 1 and return its ID."""

        with self.lock:
            incident_id = self.new_incident_id()

            self.data["incidents"][incident_id] = {
                "incident_id": incident_id,
                "version": 1,
                "status": "ACTIVE",
                "publisher": publisher,
                "category": category,
                "location": location,
                "severity": severity,
                "title": title,
                "description": description,
                "attachment": attachment,
                "recipients": []          # names that were sent the ALERT
            }

            self._save()
            return incident_id

    def get_incident(self, incident_id):

        with self.lock:
            return copy.deepcopy(self.data["incidents"].get(incident_id))

    def update_incident(self, incident_id, **changes):
        """Apply field changes, bump the version, save. Returns the new version."""

        with self.lock:
            incident = self.data["incidents"][incident_id]
            incident.update(changes)
            incident["version"] += 1
            self._save()
            return incident["version"]

    def add_recipients(self, incident_id, names):
        """Remember who received an ALERT so ALL_CLEAR can reach them later."""

        with self.lock:
            recipients = self.data["incidents"][incident_id]["recipients"]

            for name in names:
                if name not in recipients:
                    recipients.append(name)

            self._save()

    def list_incidents(self):

        with self.lock:
            return copy.deepcopy(self.data["incidents"])

    # -----------------------------------
    # ACK deliveries
    # -----------------------------------
    def set_ack(self, incident_id, version, name, state):
        """state is PENDING, CONFIRMED, UNCONFIRMED or UNCONFIRMED_RESTART."""

        with self.lock:
            key = self._ack_key(incident_id, version, name)
            self.data["deliveries"][key] = state
            self._save()

    def get_ack(self, incident_id, version, name):

        with self.lock:
            return self.data["deliveries"].get(
                self._ack_key(incident_id, version, name)
            )

    # -----------------------------------
    # Status reports
    # -----------------------------------
    def set_status(self, incident_id, name, status):
        """Keep only the most recent status per (incident, client)."""

        with self.lock:
            key = self._status_key(incident_id, name)
            self.data["status_reports"][key] = status
            self._save()

    def get_status(self, incident_id, name):

        with self.lock:
            return self.data["status_reports"].get(
                self._status_key(incident_id, name)
            )
