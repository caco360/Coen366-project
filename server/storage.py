
"""Thread-safe JSON storage for CENRS Phase 1."""

import copy
import json
import os
import threading
from pathlib import Path


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
            "deliveries": {}
        }

        # Recover previous information
        if self.path.exists():
            saved_data = json.loads(
                self.path.read_text(encoding="utf-8")
            )
            self.data.update(saved_data)

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
