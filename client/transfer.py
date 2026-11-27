"""Phase 3: peer-to-peer attachment transfer over TCP (spec section 2.9).

Every function takes the Client as its first argument and uses its folders
(client.shared, client.downloads), its publisher notes (client.published), its lock,
its log and its RQ counter.

    Subscriber side:  attach() -> ATTACH_REQ to the server (UDP), then download() from
                      the publisher (TCP), retrying only transfer errors.
    Publisher side:   start_listener() starts the tcp-listen thread (accept_loop), which
                      hands each connection to a pool of TCP_WORKERS threads (serve).

TCP framing: one newline-terminated JSON header, then exactly file_size raw bytes.
"""

import json
import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from common import config
from common.protocol import FileChecksum, MalformedMessage, build_message, decode_message, encode_message

CHUNK_SIZE = 64 * 1024       # files are read and written in 64 KiB pieces


def send_line(client, conn, message):
    """TCP framing: one JSON message per line."""

    conn.sendall(encode_message(message) + b"\n")
    client.log.info("TCP SEND %s", json.dumps(message, ensure_ascii=False))


# =========================================================
# Subscriber side: the `attach` command
# =========================================================
def attach(client, incident_id, file_name):
    """ATTACH_REQ to the server (UDP), then download from the publisher (TCP).

    Only transfer errors are retried (up to TRANSFER_ATTEMPTS); refusals are reported as-is.
    Returns one line describing the result.
    """

    if Path(file_name).name != file_name:
        return f"invalid file name {file_name}"

    info = client.request("ATTACH_REQ", incident_id=incident_id, file_name=file_name)

    if info is None:
        return "no reply from server (timeout)"
    if info["type"] != "ATTACH_INFO":
        return f"{info['type']} {info['reason']}"          # ATTACH_DENIED: not retried

    attempts = config.TRANSFER_ATTEMPTS
    for attempt in range(1, attempts + 1):
        status, detail = download(client, info)
        if status != "retry":
            return detail
        client.log.info("Attempt %d/%d failed: %s", attempt, attempts, detail)

    return f"failed after {attempts} attempts: {detail}"


def download(client, info):
    """One TCP attempt. Returns ("ok" | "refused" | "retry", detail)."""

    incident_id, file_name = info["incident_id"], info["file_name"]
    part = client.downloads / (file_name + ".part")     # renamed only once the CRC matches

    try:
        with socket.create_connection((info["ip"], info["tcp_port"]),
                                      timeout=config.TCP_CONNECT_TIMEOUT) as conn:
            conn.settimeout(config.TCP_IO_TIMEOUT)
            rq = client.rq.next()
            send_line(client, conn, build_message("GET_ATTACHMENT", rq=rq, subscriber_name=client.name,
                                                  incident_id=incident_id, file_name=file_name))

            stream = conn.makefile("rb")
            header = decode_message(stream.readline())
            client.log.info("TCP RECV %s", json.dumps(header, ensure_ascii=False))

            if header["type"] == "ATTACH_ERROR":
                return "refused", f"ATTACH_ERROR {header['reason']}"   # publisher said no
            if header["type"] != "ATTACH_DATA":
                return "retry", f"unexpected reply {header['type']}"

            # Read exactly file_size bytes, computing the CRC as we go.
            checksum = FileChecksum()
            remaining = header["file_size"]
            with open(part, "wb") as f:
                while remaining:
                    chunk = stream.read(min(CHUNK_SIZE, remaining))
                    if not chunk:
                        break                                    # publisher closed early
                    f.write(chunk)
                    checksum.update(chunk)
                    remaining -= len(chunk)

            if remaining:
                error = "Incomplete_Transfer"
            elif checksum.hexdigest() != header["checksum"]:
                error = "Checksum_Mismatch"
            else:
                error = None

            if error:
                send_line(client, conn, build_message("ATTACH_ERROR", rq=rq, incident_id=incident_id,
                                                      file_name=file_name, reason=error))
                return "retry", error

            send_line(client, conn, build_message("ATTACH_OK", rq=rq, incident_id=incident_id,
                                                  file_name=file_name))
            path = client.downloads / file_name
            part.replace(path)
            return "ok", f"saved {path} (CRC {checksum.hexdigest()})"

    except (OSError, MalformedMessage) as error:
        return "retry", f"{type(error).__name__}: {error}"     # connect failure, timeout, ...

    finally:
        part.unlink(missing_ok=True)                            # never leave a partial file


# =========================================================
# Publisher side (tcp-listen + tcp-worker threads)
# =========================================================
def start_listener(client):
    """Open the TCP listening socket and start the tcp-listen thread.

    One thread accepts connections, a pool of TCP_WORKERS threads serves them
    (same model as the server's UDP workers).
    """

    # create_server = bind + listen, with SO_REUSEADDR on Linux/macOS so a restarted
    # publisher can reuse its port immediately (old connections may be in TIME_WAIT).
    client.tcp = socket.create_server((client.ip, client.tcp_port))
    client.tcp_port = client.tcp.getsockname()[1]       # real port if 0 was given (tests)
    client.tcp_workers = ThreadPoolExecutor(config.TCP_WORKERS, thread_name_prefix="tcp-worker")
    threading.Thread(target=accept_loop, args=(client,), name="tcp-listen", daemon=True).start()


def accept_loop(client):

    while client.running:
        try:
            conn, _ = client.tcp.accept()
        except OSError:
            break                                       # socket closed by client.stop()
        client.tcp_workers.submit(serve, client, conn)


def serve(client, conn):
    """Answer one GET_ATTACHMENT: ATTACH_DATA line + exactly file_size bytes, or ATTACH_ERROR."""

    with conn:
        try:
            conn.settimeout(config.TCP_IO_TIMEOUT)
            stream = conn.makefile("rb")

            request = decode_message(stream.readline())
            if request["type"] != "GET_ATTACHMENT":
                raise MalformedMessage("Expected_GET_ATTACHMENT")
            client.log.info("TCP RECV %s", json.dumps(request, ensure_ascii=False))

            rq, incident_id, file_name = request["rq"], request["incident_id"], request["file_name"]
            reason = check_request(client, incident_id, file_name)

            if reason:
                send_line(client, conn, build_message("ATTACH_ERROR", rq=rq, incident_id=incident_id,
                                                      file_name=file_name, reason=reason))
                return

            path = client.shared / file_name
            checksum = FileChecksum()
            with open(path, "rb") as f:
                for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
                    checksum.update(chunk)

            client.log.info("Serving %s to %s (%s)", file_name, request["subscriber_name"], incident_id)
            send_line(client, conn, build_message(
                "ATTACH_DATA", rq=rq, incident_id=incident_id, file_name=file_name,
                file_size=path.stat().st_size, checksum=checksum.hexdigest()))

            with open(path, "rb") as f:
                conn.sendfile(f)                        # the raw bytes right after the header

            answer = stream.readline()                  # ATTACH_OK or ATTACH_ERROR
            client.log.info("TCP RECV %s", answer.decode("utf-8", "replace").strip() or "(closed)")

        except (MalformedMessage, OSError) as error:
            client.log.warning("TCP transfer aborted: %s", error)


def check_request(client, incident_id, file_name):
    """Publisher-side checks before sending a file. Returns the ATTACH_ERROR reason or None."""

    with client.lock:
        entry = client.published.get(incident_id)

    if entry is None:
        return "Unknown_Incident"
    if entry["status"] != "ACTIVE":
        return "Incident_Closed"
    if entry["file_name"] != file_name:
        return "File_Not_For_Incident"      # also blocks names like ../x: only our own names match
    if not (client.shared / file_name).is_file():
        return "File_Not_Available"
    return None
