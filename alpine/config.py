from dotenv import load_dotenv
import os
from pathlib import Path

load_dotenv()
# Base data directory for storing artifacts
DATA_DIR = Path(os.getenv("DATA_DIR", "/workspaces/mountain-awareness-ledger/data")).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)

# HTTP client settings
USER_AGENT = os.getenv(
    "USER_AGENT",
    "AlpineLedgerBot/0.1 (+https://github.com/your-org/mountain-awareness-ledger)"
)

# Network timeout (seconds)
try:
    TIMEOUT_S = int(os.getenv("TIMEOUT_S", "20"))
except ValueError:
    TIMEOUT_S = 20
