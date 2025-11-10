#!/usr/bin/env python3
"""
Script to check muted_tests_config.json for issues and alerts.
Outputs 'true' or 'false' for each check.
"""

import sys
from pathlib import Path

# Add mute_utils directory to path
mute_utils_dir = Path(__file__).parent
sys.path.insert(0, str(mute_utils_dir))

from read_muted_tests_config import read_config_from_file, should_create_issues, should_send_alerts

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: check_config.py <branch> <build_type> <check_type>")
        print("  check_type: 'issues' or 'alerts'")
        sys.exit(1)
    
    branch = sys.argv[1]
    build_type = sys.argv[2]
    check_type = sys.argv[3]
    
    config = read_config_from_file()
    
    if check_type == 'issues':
        result = should_create_issues(config, branch, build_type)
    elif check_type == 'alerts':
        result = should_send_alerts(config, branch, build_type)
    else:
        print(f"Error: Unknown check_type '{check_type}'. Use 'issues' or 'alerts'")
        sys.exit(1)
    
    print('true' if result else 'false')

