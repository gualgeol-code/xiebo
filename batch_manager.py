#!/usr/bin/env python3
"""
Batch Manager untuk executable xiebo binary - FIXED RESUME LOGIC
"""

import subprocess
import json
import os
import sys
import time
import signal
from datetime import datetime
import argparse
import threading
import math

# ==================== KONFIGURASI ====================
XIEBO_BINARY = "./xiebo"
LOG_FILE = "batch_progress.json"
BATCH_SIZE = 100000000  # 100 juta keys per batch

# ==================== BATCH MANAGER ====================
class XieboBatchManager:
    def __init__(self, xiebo_binary, log_file=LOG_FILE):
        self.xiebo_binary = xiebo_binary
        self.log_file = log_file
        self.batches = []
        self.running
