
"""Simple UDP client for testing Phase 1."""

import socket
import time

from common.protocol import (
    build_message,
    decode_message,
    encode_message
)

SERVER = ("127.0.0.1", 5000)
CLIENT = ("127.0.0.1", 5001)


def send(client, kind, rq, expect_reply=True, **fields):

    message = build_message(kind, rq=rq, **fields)

    client.sendto(encode_message(message), SERVER)

    print("SENT:", message)

    if not expect_reply:
        return

    try:
        data, _ = client.recvfrom(65535)
        print("RECEIVED:", decode_message(data))

    except socket.timeout:
        print("No response from server (timeout)")


def main():

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as client:

        client.bind(CLIENT)
        client.settimeout(3)

        # Register the user
        send(
            client,
            "REGISTER",
            1,
            name="Gianluca",
            role="SUBSCRIBER",
            ip="127.0.0.1",
            udp_port=5001,
            tcp_port=6001
        )

        # Subscribe to fire alerts at HALL
        send(
            client,
            "SUBSCRIBE",
            2,
            location="HALL",
            category="FIRE"
        )

        # Remove the subscription
        send(
            client,
            "UNSUBSCRIBE",
            3,
            expect_reply=False,
            location="HALL",
            category="FIRE"
        )

        time.sleep(0.2)

        # Deregister the user
        send(
            client,
            "DE-REGISTER",
            4,
            expect_reply=False,
            name="Gianluca"
        )


if __name__ == "__main__":
    main()
