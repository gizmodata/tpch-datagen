import os
import math
from pathlib import Path


# Constants
DATA_DIR = Path("data")
WORK_DIR = Path("/tmp")
DEFAULT_NUM_CHUNKS = 10
DEFAULT_NUM_PROCESSES = os.cpu_count()
