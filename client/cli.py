"""CENRS client command shell (main thread): reads what the user types and calls Client methods.

Run one instance per user, each in its own terminal, from the project root:

    py -3 -m client.client --name Alice --role SUBSCRIBER --port 6002
    py -3 -m client.client --name CampusSecurity --role PUBLISHER --port 6001

(`py -3 -m client.cli ...` does the same.) Type `help` at the prompt for the command list.
"""

import argparse
import shlex

from client.client import Client
from common import config
from common.protocol import MalformedMessage

HELP = """Commands (use quotes for text with spaces):
  register
  deregister
  sub   <LOCATION> <CATEGORY>                 e.g. sub HALL FIRE   (ALL is a wildcard)
  unsub <LOCATION> <CATEGORY>
  publish <CATEGORY> <LOCATION> <SEVERITY> "<title>" "<description>" [attachment]
  update  <INCIDENT_ID> <SEVERITY> "<title>" "<description>"
  close   <INCIDENT_ID> "<reason>"
  status  <INCIDENT_ID> SAFE|NEED_HELP|LEFT_AREA
  attach  <INCIDENT_ID> <file_name>       download an attachment directly from its publisher
  alerts                      incidents this client has received
  ack on|off                   off = never ACK (demo of server retransmission)
  raw <json>                   send any text as-is, e.g. raw {"type":"SUBSCRIBE","rq":99}
  help
  quit                         exit WITHOUT de-registering (keeps state on the server)
Locations:  HALL EV JMSB LOYOLA SGW ALL
Categories: FIRE SECURITY WEATHER POWER BUILDING GENERAL ALL
Severities: INFO WARNING CRITICAL"""


def _expect(args, low, high=None, usage=""):
    high = low if high is None else high
    if not low <= len(args) <= high:
        raise ValueError("usage: " + usage)


def run_command(client, line):
    """Execute one shell line. Returns False when the user wants to quit."""

    try:
        parts = shlex.split(line)
    except ValueError as error:
        print("Parse error:", error)
        return True

    if not parts:
        return True

    command, args = parts[0].lower(), parts[1:]
    upper = [a.upper() for a in args]
    reply = None
    no_reply_is_success = False

    try:
        if command in ("quit", "exit"):
            return False

        elif command == "help":
            print(HELP)
            return True

        elif command == "register":
            reply = client.register()

        elif command == "deregister":
            reply = client.deregister()
            no_reply_is_success = True

        elif command in ("sub", "subscribe"):
            _expect(args, 2, usage="sub <LOCATION> <CATEGORY>")
            reply = client.subscribe(upper[0], upper[1])

        elif command in ("unsub", "unsubscribe"):
            _expect(args, 2, usage="unsub <LOCATION> <CATEGORY>")
            reply = client.unsubscribe(upper[0], upper[1])
            no_reply_is_success = True

        elif command == "publish":
            _expect(args, 5, 6, 'publish <CATEGORY> <LOCATION> <SEVERITY> "<title>" "<description>" [attachment]')
            attachment = args[5] if len(args) == 6 else "NONE"
            reply = client.publish(upper[0], upper[1], upper[2], args[3], args[4], attachment)

        elif command == "update":
            _expect(args, 4, usage='update <INCIDENT_ID> <SEVERITY> "<title>" "<description>"')
            reply = client.update(upper[0], upper[1], args[2], args[3])

        elif command == "close":
            _expect(args, 2, usage='close <INCIDENT_ID> "<reason>"')
            reply = client.close(upper[0], args[1])

        elif command == "status":
            _expect(args, 2, usage="status <INCIDENT_ID> SAFE|NEED_HELP|LEFT_AREA")
            reply = client.status(upper[0], upper[1])

        elif command == "attach":
            _expect(args, 2, usage="attach <INCIDENT_ID> <file_name>")
            print("->", client.attach(upper[0], args[1]))
            return True

        elif command == "alerts":
            incidents = client.get_incidents()
            if not incidents:
                print("No incidents received yet.")
            for i in incidents:
                print(f"  {i['incident_id']} v{i['version']} {i.get('status', '?')} "
                      f"{i.get('severity', '?')} {i.get('category', '?')}@{i.get('location', '?')} "
                      f"\"{i.get('title', '')}\" attachment={i.get('attachment', 'NONE')}")
            return True

        elif command == "ack":
            _expect(args, 1, usage="ack on|off")
            client.auto_ack = args[0].lower() == "on"
            print("Automatic ALERT_ACK is", "ON" if client.auto_ack else "OFF")
            return True

        elif command == "raw":
            text = line.strip()[3:].strip()
            if not text:
                raise ValueError("usage: raw <json>")
            client.send_raw(text)
            return True

        else:
            print(f"Unknown command '{command}'. Type 'help'.")
            return True

    except (ValueError, MalformedMessage) as error:
        # MalformedMessage: the client refused to send an invalid message
        print("Not sent:", error)
        return True

    if reply is not None:
        print("->", reply["type"], {k: v for k, v in reply.items() if k not in ("type", "rq")})
    elif no_reply_is_success:
        print("-> no reply (the server only answers this request when it fails)")
    else:
        print("-> no reply from server (timeout)")

    return True


def main():

    parser = argparse.ArgumentParser(description="CENRS client (one user per process)")
    parser.add_argument("--name", required=True)
    parser.add_argument("--role", required=True, choices=["SUBSCRIBER", "PUBLISHER", "BOTH"],
                        type=str.upper)
    parser.add_argument("--port", type=int, required=True, help="this client's UDP port")
    parser.add_argument("--tcp-port", type=int, default=None,
                        help="this client's TCP port (default: UDP port + 1000)")
    parser.add_argument("--ip", default="127.0.0.1")
    parser.add_argument("--server-ip", default=config.SERVER_IP)
    parser.add_argument("--server-port", type=int, default=config.SERVER_UDP_PORT)
    parser.add_argument("--log", default=None, help="also write the log to this file")
    parser.add_argument("--auto-register", action="store_true", help="send REGISTER at startup")
    args = parser.parse_args()

    client = Client(args.name, args.role, ip=args.ip, udp_port=args.port,
                    tcp_port=args.tcp_port, server=(args.server_ip, args.server_port),
                    logfile=args.log)
    client.start()
    print("Type 'help' for commands.")

    if args.auto_register:
        run_command(client, "register")

    try:
        while True:
            try:
                line = input(f"{client.name}> ")
            except EOFError:
                break
            if not run_command(client, line):
                break
    except KeyboardInterrupt:
        print()
    finally:
        client.stop()


if __name__ == "__main__":
    main()
