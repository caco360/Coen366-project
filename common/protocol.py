import json
import threading
import zlib


class MalformedMessage(Exception):
    pass


# =========================
# Allowed protocol values
# =========================
VALID_ROLES = {"SUBSCRIBER", "PUBLISHER", "BOTH"}
VALID_SEVERITIES = {"INFO", "WARNING", "CRITICAL"}
VALID_INCIDENT_STATUSES = {"ACTIVE", "CLOSED"}
VALID_USER_STATUSES = {"SAFE", "NEED_HELP", "LEFT_AREA"}

VALID_LOCATIONS = {"HALL", "EV", "JMSB", "LOYOLA", "SGW", "ALL"}
VALID_CATEGORIES = {"FIRE", "SECURITY", "WEATHER", "POWER", "BUILDING", "GENERAL", "ALL"}


# =========================
# Message schema
# required = must exist
# optional = allowed if present
# =========================
MESSAGE_SCHEMAS = {
    # Phase 1
    "REGISTER": {
        "required": {"rq", "name", "role", "ip", "udp_port", "tcp_port"},
        "optional": set(),
    },
    "REGISTERED": {
        "required": {"rq", "locations", "categories"},
        "optional": set(),
    },
    "REGISTER-DENIED": {
        "required": {"rq", "reason"},
        "optional": set(),
    },
    "DE-REGISTER": {
        "required": {"rq", "name"},
        "optional": set(),
    },
    "SUBSCRIBE": {
        "required": {"rq", "location", "category"},
        "optional": set(),
    },
    "SUBSCRIBED": {
        "required": {"rq"},
        "optional": set(),
    },
    "SUBSCRIPTION-DENIED": {
        "required": {"rq", "reason"},
        "optional": set(),
    },
    "UNSUBSCRIBE": {
        "required": {"rq", "location", "category"},
        "optional": set(),
    },

    # Phase 2
    "PUBLISH_ALERT": {
        "required": {"rq", "category", "location", "severity", "title", "description", "attachment"},
        "optional": set(),
    },
    "ALERT_ACCEPTED": {
        "required": {"rq", "incident_id"},
        "optional": set(),
    },
    "ALERT_DENIED": {
        "required": {"rq", "reason"},
        "optional": set(),
    },
    "ALERT": {
        "required": {
            "rq", "incident_id", "version", "category", "location",
            "severity", "title", "description", "attachment"
        },
        "optional": set(),
    },
    "ALERT_ACK": {
        "required": {"rq", "incident_id", "version", "name"},
        "optional": set(),
    },
    "UPDATE_ALERT": {
        "required": {"rq", "incident_id", "severity", "title", "description"},
        "optional": set(),
    },
    "ALERT_UPDATED": {
        "required": {"rq", "incident_id", "version"},
        "optional": set(),
    },
    "ALERT_UPDATE": {
        "required": {"rq", "incident_id", "version", "severity", "title", "description"},
        "optional": set(),
    },
    "CLOSE_ALERT": {
        "required": {"rq", "incident_id", "reason"},
        "optional": set(),
    },
    "ALERT_CLOSED": {
        "required": {"rq", "incident_id"},
        "optional": set(),
    },
    "ALL_CLEAR": {
        "required": {"rq", "incident_id", "version", "message"},
        "optional": set(),
    },
    "STATUS_REPORT": {
        "required": {"rq", "incident_id", "name", "status"},
        "optional": set(),
    },
    "STATUS_RECORDED": {
        "required": {"rq"},
        "optional": set(),
    },
    "STATUS_REJECTED": {
        "required": {"rq", "reason"},
        "optional": set(),
    },

    # Phase 3
    "ATTACH_REQ": {
        "required": {"rq", "incident_id", "file_name"},
        "optional": set(),
    },
    "ATTACH_INFO": {
        "required": {"rq", "incident_id", "publisher_name", "ip", "tcp_port", "file_name"},
        "optional": set(),
    },
    "ATTACH_DENIED": {
        "required": {"rq", "incident_id", "reason"},
        "optional": set(),
    },
    "GET_ATTACHMENT": {
        "required": {"rq", "subscriber_name", "incident_id", "file_name"},
        "optional": set(),
    },
    "ATTACH_DATA": {
        "required": {"rq", "incident_id", "file_name", "file_size", "checksum"},
        "optional": {"data"},
    },
    "ATTACH_OK": {
        "required": {"rq", "incident_id", "file_name"},
        "optional": set(),
    },
    "ATTACH_ERROR": {
        "required": {"rq", "incident_id", "file_name", "reason"},
        "optional": set(),
    },
    "GET_ACTIVE_ALERTS": {
        "required": {"rq", "name"},
        "optional": set(),
    },
    "ACTIVE_ALERTS": {
        "required": {"rq", "number_alerts"},
        "optional": {"records"},
    },
}


# =========================
# Thread-safe RQ counter
# =========================
class RqCounter:
    def __init__(self, start=1):
        self._n = start - 1
        self._lock = threading.Lock()

    def next(self):
        with self._lock:
            self._n += 1
            return self._n


_global_counter = RqCounter()


def next_request_id():
    return _global_counter.next()


# =========================
# Checksum helper
# =========================
def crc32_hex(data, running=0):
    return "%08X" % (zlib.crc32(data, running) & 0xFFFFFFFF)


# =========================
# Validation helpers
# =========================
def _require_string(value, field_name):
    if not isinstance(value, str) or value == "":
        raise MalformedMessage(f"Invalid_{field_name}")


def _require_int(value, field_name):
    if not isinstance(value, int):
        raise MalformedMessage(f"Invalid_{field_name}")


def _require_port(value, field_name):
    _require_int(value, field_name)
    if not (1 <= value <= 65535):
        raise MalformedMessage(f"Invalid_{field_name}")


def _require_list(value, field_name):
    if not isinstance(value, list):
        raise MalformedMessage(f"Invalid_{field_name}")


def _validate_known_fields(msg):
    msg_type = msg["type"]

    if "rq" in msg:
        _require_int(msg["rq"], "rq")

    if "name" in msg:
        _require_string(msg["name"], "name")

    if "publisher_name" in msg:
        _require_string(msg["publisher_name"], "publisher_name")

    if "subscriber_name" in msg:
        _require_string(msg["subscriber_name"], "subscriber_name")

    if "role" in msg and msg["role"] not in VALID_ROLES:
        raise MalformedMessage("Invalid_role")

    if "ip" in msg:
        _require_string(msg["ip"], "ip")

    if "udp_port" in msg:
        _require_port(msg["udp_port"], "udp_port")

    if "tcp_port" in msg:
        _require_port(msg["tcp_port"], "tcp_port")

    if "location" in msg and msg["location"] not in VALID_LOCATIONS:
        raise MalformedMessage("Invalid_location")

    if "category" in msg and msg["category"] not in VALID_CATEGORIES:
        raise MalformedMessage("Invalid_category")

    if "severity" in msg and msg["severity"] not in VALID_SEVERITIES:
        raise MalformedMessage("Invalid_severity")

    if "status" in msg:
        # STATUS_REPORT uses SAFE / NEED_HELP / LEFT_AREA
        if msg_type == "STATUS_REPORT":
            if msg["status"] not in VALID_USER_STATUSES:
                raise MalformedMessage("Invalid_status")
        else:
            if msg["status"] not in VALID_INCIDENT_STATUSES:
                raise MalformedMessage("Invalid_status")

    if "incident_id" in msg:
        _require_string(msg["incident_id"], "incident_id")

    if "version" in msg:
        _require_int(msg["version"], "version")
        if msg["version"] < 1:
            raise MalformedMessage("Invalid_version")

    if "title" in msg:
        _require_string(msg["title"], "title")

    if "description" in msg:
        _require_string(msg["description"], "description")

    if "attachment" in msg:
        _require_string(msg["attachment"], "attachment")

    if "file_name" in msg:
        _require_string(msg["file_name"], "file_name")

    if "file_size" in msg:
        _require_int(msg["file_size"], "file_size")
        if msg["file_size"] < 0:
            raise MalformedMessage("Invalid_file_size")

    if "checksum" in msg:
        _require_string(msg["checksum"], "checksum")

    if "reason" in msg:
        _require_string(msg["reason"], "reason")

    if "message" in msg:
        _require_string(msg["message"], "message")

    if "locations" in msg:
        _require_list(msg["locations"], "locations")

    if "categories" in msg:
        _require_list(msg["categories"], "categories")

    if "number_alerts" in msg:
        _require_int(msg["number_alerts"], "number_alerts")
        if msg["number_alerts"] < 0:
            raise MalformedMessage("Invalid_number_alerts")

    if "records" in msg:
        _require_list(msg["records"], "records")


# =========================
# Core protocol functions
# =========================
def validate_message(message):
    if not isinstance(message, dict):
        raise MalformedMessage("Message_Not_Object")

    if "type" not in message:
        raise MalformedMessage("Missing_type")

    msg_type = message["type"]

    if not isinstance(msg_type, str) or msg_type == "":
        raise MalformedMessage("Invalid_type")

    if msg_type not in MESSAGE_SCHEMAS:
        raise MalformedMessage("Unknown_Message_Type")

    schema = MESSAGE_SCHEMAS[msg_type]
    required = schema["required"]
    optional = schema["optional"]
    allowed = required | optional | {"type"}

    missing = required - message.keys()
    if missing:
        missing_list = ",".join(sorted(missing))
        raise MalformedMessage(f"Missing_Fields:{missing_list}")

    extra = set(message.keys()) - allowed
    if extra:
        extra_list = ",".join(sorted(extra))
        raise MalformedMessage(f"Unexpected_Fields:{extra_list}")

    _validate_known_fields(message)
    return True


def build_message(msg_type, rq=None, **fields):
    if rq is None:
        rq = next_request_id()

    message = {"type": msg_type, "rq": rq}
    message.update(fields)

    validate_message(message)
    return message


def encode_message(message):
    validate_message(message)
    try:
        return json.dumps(message, ensure_ascii=False).encode("utf-8")
    except (TypeError, ValueError):
        raise MalformedMessage("Encode_Error")


def decode_message(data):
    if not isinstance(data, (bytes, bytearray)):
        raise MalformedMessage("Not_Bytes")

    try:
        text = data.decode("utf-8").strip()
    except UnicodeDecodeError:
        raise MalformedMessage("Not_UTF8")

    if not text:
        raise MalformedMessage("Empty_Message")

    try:
        message = json.loads(text)
    except json.JSONDecodeError:
        raise MalformedMessage("Invalid_JSON")

    validate_message(message)
    return message