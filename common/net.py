"""Small socket helpers shared by the server and the client."""

import socket


def disable_udp_connreset(sock):
    """Stop Windows from failing recvfrom() after sending to a closed UDP port.

    On Windows, when a datagram is sent to a port with no listener (for example a
    client that has quit), the ICMP "port unreachable" reply makes the *next*
    recvfrom() on that socket raise ConnectionResetError (WinError 10054), which
    would kill a receive loop. UDP is connectionless, so we turn that off.
    No effect on Linux/macOS.
    """

    if hasattr(socket, "SIO_UDP_CONNRESET"):
        try:
            sock.ioctl(socket.SIO_UDP_CONNRESET, False)
        except (OSError, ValueError):
            pass
