
"""CENRS Phase 1 UDP server."""

import json
import logging
import socket
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from common.protocol import (
    VALID_CATEGORIES,
    VALID_LOCATIONS,
    MalformedMessage,
    build_message,
    decode_message,
    encode_message,
)
from server.storage import Storage


# Server configuration
HOST = "127.0.0.1"
PORT = 5000
DATA_FILE = Path(__file__).resolve().parent.parent / "data" / "server_state.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(message)s"
)
log = logging.getLogger("CENRS")


# These error responses already exist in protocol.py
DENIAL_TYPES = {
    "REGISTER": "REGISTER-DENIED",
    "SUBSCRIBE": "SUBSCRIPTION-DENIED",
    "UNSUBSCRIBE": "SUBSCRIPTION-DENIED",
}


class EmergencyServer:

    def __init__(self, host=HOST, port=PORT, data_file=DATA_FILE):

        self.storage = Storage(data_file)

        # Create the UDP socket
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind((host, port))

        # Allow multiple requests to be processed simultaneously
        self.workers = ThreadPoolExecutor(max_workers=8)

        # Connect message types to their functions
        self.handlers = {
            "REGISTER": self.register,
            "SUBSCRIBE": self.subscribe,
            "UNSUBSCRIBE": self.unsubscribe,
            "DE-REGISTER": self.deregister,
        }

    # -----------------------------------
    # Send a response to a client
    # -----------------------------------
    def send(self, address, message):

        data = encode_message(message)
        self.socket.sendto(data, address)

        log.info("SEND %s -> %s", message["type"], address)

    # -----------------------------------
    # Process received messages
    # -----------------------------------
    def handle(self, data, address):

        try:
            message = decode_message(data)

        except MalformedMessage as error:
            log.warning("BAD MESSAGE %s: %s", address, error)
            self.reject_malformed(data, address, str(error))
            return

        kind = message["type"]
        log.info("RECV %s <- %s", kind, address)

        # Find the function responsible for this message
        handler = self.handlers.get(kind)

        if handler is None:
            log.info("%s belongs to a later phase", kind)
            return

        try:
            response = handler(message, address)

        except ValueError as error:
            self.reject(kind, message["rq"], address, str(error))
            return

        except Exception:
            log.exception("Unexpected error while handling %s", kind)
            return

        # Only send a response when one is defined
        if response is not None:
            self.send(address, response)

    # -----------------------------------
    # Reject an invalid request
    # -----------------------------------
    def reject(self, kind, rq, address, reason):

        reply_type = DENIAL_TYPES.get(kind)

        if reply_type is None:
            log.warning("REJECT %s: %s", kind, reason)
            return

        response = build_message(
            reply_type,
            rq=rq,
            reason=reason
        )

        self.send(address, response)

    # -----------------------------------
    # Handle malformed JSON messages
    # -----------------------------------
    def reject_malformed(self, data, address, reason):

        try:
            raw = json.loads(data)

            if isinstance(raw, dict) and type(raw.get("rq")) is int:
                self.reject(
                    raw.get("type"),
                    raw["rq"],
                    address,
                    reason
                )

        except (ValueError, TypeError, UnicodeError):
            pass

    # -----------------------------------
    # Registration
    # -----------------------------------
    def register(self, message, address):

        self.storage.register(message, address)

        return build_message(
            "REGISTERED",
            rq=message["rq"],
            locations=sorted(VALID_LOCATIONS),
            categories=sorted(VALID_CATEGORIES),
        )

    # -----------------------------------
    # Subscription
    # -----------------------------------
    def subscribe(self, message, address):

        self.storage.subscribe(
            address,
            message["location"],
            message["category"]
        )

        return build_message(
            "SUBSCRIBED",
            rq=message["rq"]
        )

    # -----------------------------------
    # Unsubscribe
    # -----------------------------------
    def unsubscribe(self, message, address):

        self.storage.unsubscribe(
            address,
            message["location"],
            message["category"]
        )

        log.info("Removed subscription for %s", address)

    # -----------------------------------
    # Deregistration
    # -----------------------------------
    def deregister(self, message, address):

        self.storage.deregister(
            address,
            message["name"]
        )

        log.info("Processed DE-REGISTER for %s", message["name"])

    # -----------------------------------
    # Start the server
    # -----------------------------------
    def serve_forever(self):

        log.info("UDP server listening at %s", self.socket.getsockname())

        try:
            while True:
                data, address = self.socket.recvfrom(65535)
                self.workers.submit(self.handle, data, address)

        except KeyboardInterrupt:
            log.info("Server shutting down")

        finally:
            self.workers.shutdown(wait=True)
            self.socket.close()


if __name__ == "__main__":
    EmergencyServer().serve_forever()
