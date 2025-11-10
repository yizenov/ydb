#!/usr/bin/env python3
"""
Script to get analytics branches from muted_tests_config.json.
Outputs JSON array of branch names.
"""

import sys
import json
from pathlib import Path

# Add mute_utils directory to path
mute_utils_dir = Path(__file__).parent
sys.path.insert(0, str(mute_utils_dir))

from read_muted_tests_config import read_config_from_file

if __name__ == "__main__":
    config = read_config_from_file()
    branches = config['analytics']['branches']
    print(json.dumps(branches))

