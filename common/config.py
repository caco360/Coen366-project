SERVER_IP = "127.0.0.1"
SERVER_UDP_PORT = 5000
SERVER_TCP_PORT = 6000

ACK_TIMEOUT = 3        # seconds
MAX_ATTEMPTS = 3

LOCATIONS = ["HALL", "EV", "JMSB", "LOYOLA", "SGW", "ALL"]
CATEGORIES = ["FIRE", "SECURITY", "WEATHER", "POWER", "BUILDING", "GENERAL", "ALL"]
SEVERITIES = ["INFO", "WARNING", "CRITICAL"]
ROLES = ["SUBSCRIBER", "PUBLISHER", "BOTH"]
USER_STATUSES = ["SAFE", "NEED_HELP", "LEFT_AREA"]

DB_PATH = "cenrs.db"

# Phase 2: probability of dropping an outgoing ALERT / ALERT_UPDATE / ALL_CLEAR
# (fan-out and retransmissions only, never direct replies). Used to demo retransmission.
LOSS_RATE = 0.0

# Phase 3: peer-to-peer attachments over TCP
TCP_WORKERS = 8             # publisher thread pool size for GET_ATTACHMENT connections
TCP_CONNECT_TIMEOUT = 5     # seconds
TCP_IO_TIMEOUT = 10         # seconds without data before a transfer is abandoned
TRANSFER_ATTEMPTS = 3       # tries for transfer errors (connect, incomplete, checksum)
USER_DATA_DIR = "data/user" # each client uses data/user/<Name>/{shared,downloads,published.json}
