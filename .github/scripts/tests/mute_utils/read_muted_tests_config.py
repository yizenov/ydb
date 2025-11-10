#!/usr/bin/env python3
"""
Utility to read and validate muted_tests_config.json configuration file.
"""

import os
import json
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any


def get_config_path() -> Path:
    """
    Get the path to muted_tests_config.json.
    
    Returns:
        Path: Path to the config file
    """
    # From mute_utils: go up to .github, then to config
    script_dir = Path(__file__).parent.parent.parent.parent
    config_path = script_dir / "config" / "muted_tests_config.json"
    return config_path


def read_config_from_file(config_path: Optional[Path] = None) -> Dict[str, Any]:
    """
    Read muted_tests_config.json from file system.
    
    Args:
        config_path: Optional path to config file. If None, uses default location.
        
    Returns:
        dict: Configuration dictionary
        
    Raises:
        FileNotFoundError: If config file doesn't exist
        json.JSONDecodeError: If config file is invalid JSON
        ValueError: If config structure is invalid
    """
    if config_path is None:
        config_path = get_config_path()
    
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    validate_config(config)
    return config


def read_config_from_main_branch() -> Dict[str, Any]:
    """
    Read muted_tests_config.json from main branch using git.
    
    Returns:
        dict: Configuration dictionary
        
    Raises:
        subprocess.CalledProcessError: If git command fails
        json.JSONDecodeError: If config file is invalid JSON
        ValueError: If config structure is invalid
    """
    import subprocess
    
    config_path = get_config_path()
    relative_path = config_path.relative_to(Path.cwd())
    
    try:
        # Get config from main branch
        result = subprocess.run(
            ['git', 'show', f'main:{relative_path}'],
            capture_output=True,
            text=True,
            check=True,
            cwd=Path.cwd()
        )
        config = json.loads(result.stdout)
        validate_config(config)
        return config
    except subprocess.CalledProcessError as e:
        raise FileNotFoundError(f"Failed to read config from main branch: {e.stderr}")
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(f"Invalid JSON in config from main branch: {e.msg}", e.doc, e.pos)


def validate_config(config: Dict[str, Any]) -> None:
    """
    Validate configuration structure.
    
    Args:
        config: Configuration dictionary to validate
        
    Raises:
        ValueError: If config structure is invalid
    """
    if not isinstance(config, dict):
        raise ValueError("Config must be a dictionary")
    
    # Check version
    if 'version' not in config:
        raise ValueError("Config must have 'version' field")
    
    # Check analytics section
    if 'analytics' not in config:
        raise ValueError("Config must have 'analytics' section")
    analytics = config['analytics']
    if not isinstance(analytics, dict):
        raise ValueError("'analytics' must be a dictionary")
    if 'branches' not in analytics or not isinstance(analytics['branches'], list):
        raise ValueError("'analytics.branches' must be a list")
    if 'build_types' not in analytics or not isinstance(analytics['build_types'], dict):
        raise ValueError("'analytics.build_types' must be a dictionary")
    if 'default' not in analytics['build_types']:
        raise ValueError("'analytics.build_types' must have 'default' key")
    if not isinstance(analytics['build_types']['default'], list):
        raise ValueError("'analytics.build_types.default' must be a list")
    
    # Check issues section
    if 'issues' not in config:
        raise ValueError("Config must have 'issues' section")
    issues = config['issues']
    if not isinstance(issues, dict):
        raise ValueError("'issues' must be a dictionary")
    if 'branches' not in issues or not isinstance(issues['branches'], list):
        raise ValueError("'issues.branches' must be a list")
    if 'build_types' not in issues or not isinstance(issues['build_types'], list):
        raise ValueError("'issues.build_types' must be a list")
    
    # Check alerts section
    if 'alerts' not in config:
        raise ValueError("Config must have 'alerts' section")
    alerts = config['alerts']
    if not isinstance(alerts, dict):
        raise ValueError("'alerts' must be a dictionary")
    if 'branches' not in alerts or not isinstance(alerts['branches'], list):
        raise ValueError("'alerts.branches' must be a list")
    if 'default_build_types' not in alerts or not isinstance(alerts['default_build_types'], list):
        raise ValueError("'alerts.default_build_types' must be a list")


def get_build_types_for_branch(config: Dict[str, Any], branch: str) -> List[str]:
    """
    Get build types for a specific branch from analytics config.
    
    Args:
        config: Configuration dictionary
        branch: Branch name
        
    Returns:
        list: List of build types for the branch
    """
    analytics = config.get('analytics', {})
    build_types = analytics.get('build_types', {})
    
    # Check if branch has specific build types
    if branch in build_types:
        return build_types[branch]
    
    # Fall back to default
    return build_types.get('default', ['relwithdebinfo'])


def is_branch_monitored(config: Dict[str, Any], branch: str, section: str = 'analytics') -> bool:
    """
    Check if a branch is monitored in a specific section.
    
    Args:
        config: Configuration dictionary
        branch: Branch name
        section: Section to check ('analytics', 'issues', or 'alerts')
        
    Returns:
        bool: True if branch is monitored
    """
    if section not in config:
        return False
    
    branches = config[section].get('branches', [])
    return branch in branches


def should_create_issues(config: Dict[str, Any], branch: str, build_type: str) -> bool:
    """
    Check if issues should be created for a branch and build type.
    
    Args:
        config: Configuration dictionary
        branch: Branch name
        build_type: Build type
        
    Returns:
        bool: True if issues should be created
    """
    issues = config.get('issues', {})
    branches = issues.get('branches', [])
    build_types = issues.get('build_types', [])
    
    return branch in branches and build_type in build_types


def should_send_alerts(config: Dict[str, Any], branch: str, build_type: str) -> bool:
    """
    Check if alerts should be sent for a branch and build type.
    
    Args:
        config: Configuration dictionary
        branch: Branch name
        build_type: Build type
        
    Returns:
        bool: True if alerts should be sent
    """
    alerts = config.get('alerts', {})
    branches = alerts.get('branches', [])
    default_build_types = alerts.get('default_build_types', [])
    
    return branch in branches and build_type in default_build_types


def main():
    """CLI interface for reading config."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Read and validate muted_tests_config.json")
    parser.add_argument('--from-main', action='store_true', 
                       help='Read config from main branch instead of local file')
    parser.add_argument('--validate-only', action='store_true',
                       help='Only validate config, do not print')
    parser.add_argument('--get-build-types', metavar='BRANCH',
                       help='Get build types for a branch')
    parser.add_argument('--check-branch', metavar='BRANCH',
                       help='Check if branch is monitored (use with --section)')
    parser.add_argument('--section', choices=['analytics', 'issues', 'alerts'],
                       default='analytics', help='Section to check (for --check-branch)')
    
    args = parser.parse_args()
    
    try:
        if args.from_main:
            config = read_config_from_main_branch()
        else:
            config = read_config_from_file()
        
        if args.validate_only:
            print("✓ Config is valid")
            sys.exit(0)
        
        if args.get_build_types:
            build_types = get_build_types_for_branch(config, args.get_build_types)
            print(' '.join(build_types))
            sys.exit(0)
        
        if args.check_branch:
            is_monitored = is_branch_monitored(config, args.check_branch, args.section)
            print('true' if is_monitored else 'false')
            sys.exit(0)
        
        # Default: print config as JSON
        print(json.dumps(config, indent=2))
        
    except (FileNotFoundError, json.JSONDecodeError, ValueError) as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

