#!/usr/bin/env python3
"""
Storage for pending Telegram notifications that need to be aggregated.
"""

import os
import sys
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pathlib import Path

# Add analytics directory to path for ydb_wrapper import
dir_path = os.path.dirname(__file__)
sys.path.insert(0, f"{dir_path}/../analytics")

try:
    import ydb
    from ydb_wrapper import YDBWrapper
    YDB_AVAILABLE = True
except ImportError:
    YDB_AVAILABLE = False
    print("⚠️ YDB client not available. Install with: pip install ydb")


TABLE_NAME = "notification_pending"
TABLE_PATH = "test_results/analytics/notification_pending"


def create_table(ydb_wrapper: YDBWrapper) -> None:
    """
    Create notification_pending table if it doesn't exist.
    
    Args:
        ydb_wrapper: YDBWrapper instance
    """
    table_path = ydb_wrapper.get_table_path(TABLE_NAME, base_path=TABLE_PATH)
    print(f"📋 Creating table if not exists: {table_path}")
    
    create_sql = f"""
        CREATE table IF NOT EXISTS `{table_path}` (
            `team_name` Utf8 NOT NULL,
            `branch` Utf8 NOT NULL,
            `build_type` Utf8 NOT NULL,
            `created_at` Timestamp NOT NULL,
            `notification_data` JsonDocument NOT NULL,
            PRIMARY KEY (`team_name`, `branch`, `build_type`, `created_at`)
        )
            PARTITION BY HASH(team_name, branch)
            WITH (STORE = COLUMN)
    """
    
    ydb_wrapper.create_table(table_path, create_sql)
    print(f"✅ Table created/verified: {table_path}")


def store_notification(
    team_name: str,
    branch: str,
    build_type: str,
    notification_data: Dict[str, Any],
    ydb_wrapper: Optional[YDBWrapper] = None
) -> bool:
    """
    Store a notification in the queue for later aggregation.
    
    Args:
        team_name: Team name
        branch: Branch name
        build_type: Build type
        notification_data: Dictionary containing notification data (issues, stats, etc.)
        ydb_wrapper: Optional YDBWrapper instance (will create if not provided)
        
    Returns:
        bool: True if successful, False otherwise
    """
    if not YDB_AVAILABLE:
        print("❌ YDB not available, cannot store notification")
        return False
    
    try:
        if ydb_wrapper is None:
            ydb_wrapper = YDBWrapper(silent=True)
        
        if not ydb_wrapper.check_credentials():
            print("❌ YDB credentials not available")
            return False
        
        table_path = ydb_wrapper.get_table_path(TABLE_NAME, base_path=TABLE_PATH)
        
        # Create table if needed
        create_table(ydb_wrapper)
        
        # Prepare data
        now = datetime.utcnow()
        record = {
            'team_name': team_name,
            'branch': branch,
            'build_type': build_type,
            'created_at': now,
            'notification_data': json.dumps(notification_data)
        }
        
        # Prepare column types
        column_types = (
            ydb.BulkUpsertColumns()
            .add_column("team_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
            .add_column("branch", ydb.OptionalType(ydb.PrimitiveType.Utf8))
            .add_column("build_type", ydb.OptionalType(ydb.PrimitiveType.Utf8))
            .add_column("created_at", ydb.OptionalType(ydb.PrimitiveType.Timestamp))
            .add_column("notification_data", ydb.OptionalType(ydb.PrimitiveType.JsonDocument))
        )
        
        # Insert record
        ydb_wrapper.bulk_upsert_batches(table_path, [record], column_types, batch_size=1)
        
        print(f"✅ Stored notification for team '{team_name}' (branch: {branch}, build_type: {build_type})")
        return True
        
    except Exception as e:
        print(f"❌ Error storing notification: {e}")
        import traceback
        print(traceback.format_exc())
        return False


def get_pending_notifications(
    team_name: Optional[str] = None,
    branch: Optional[str] = None,
    build_type: Optional[str] = None,
    since: Optional[datetime] = None,
    ydb_wrapper: Optional[YDBWrapper] = None
) -> List[Dict[str, Any]]:
    """
    Get pending notifications from storage.
    
    Args:
        team_name: Optional team name filter
        branch: Optional branch filter
        build_type: Optional build_type filter
        since: Optional datetime to filter notifications created after this time
        ydb_wrapper: Optional YDBWrapper instance
        
    Returns:
        list: List of notification records
    """
    if not YDB_AVAILABLE:
        print("❌ YDB not available, cannot get notifications")
        return []
    
    try:
        if ydb_wrapper is None:
            ydb_wrapper = YDBWrapper(silent=True)
        
        if not ydb_wrapper.check_credentials():
            print("❌ YDB credentials not available")
            return []
        
        table_path = ydb_wrapper.get_table_path(TABLE_NAME, base_path=TABLE_PATH)
        
        # Build query
        conditions = []
        if team_name:
            conditions.append(f"team_name = '{team_name}'")
        if branch:
            conditions.append(f"branch = '{branch}'")
        if build_type:
            conditions.append(f"build_type = '{build_type}'")
        if since:
            since_str = since.strftime('%Y-%m-%dT%H:%M:%SZ')
            conditions.append(f"created_at >= Timestamp('{since_str}')")
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        query = f"""
            SELECT 
                team_name,
                branch,
                build_type,
                created_at,
                notification_data
            FROM `{table_path}`
            WHERE {where_clause}
            ORDER BY team_name, branch, build_type, created_at
        """
        
        results = ydb_wrapper.execute_query(query)
        
        notifications = []
        for row in results:
            notification = {
                'team_name': row.team_name,
                'branch': row.branch,
                'build_type': row.build_type,
                'created_at': row.created_at,
                'notification_data': json.loads(row.notification_data) if isinstance(row.notification_data, str) else row.notification_data
            }
            notifications.append(notification)
        
        print(f"📊 Retrieved {len(notifications)} pending notifications")
        return notifications
        
    except Exception as e:
        print(f"❌ Error getting notifications: {e}")
        import traceback
        print(traceback.format_exc())
        return []


def delete_notifications(
    team_name: Optional[str] = None,
    branch: Optional[str] = None,
    build_type: Optional[str] = None,
    before: Optional[datetime] = None,
    ydb_wrapper: Optional[YDBWrapper] = None
) -> bool:
    """
    Delete notifications from storage.
    
    Args:
        team_name: Optional team name filter
        branch: Optional branch filter
        build_type: Optional build_type filter
        before: Optional datetime to delete notifications created before this time
        ydb_wrapper: Optional YDBWrapper instance
        
    Returns:
        bool: True if successful, False otherwise
    """
    if not YDB_AVAILABLE:
        print("❌ YDB not available, cannot delete notifications")
        return False
    
    try:
        if ydb_wrapper is None:
            ydb_wrapper = YDBWrapper(silent=True)
        
        if not ydb_wrapper.check_credentials():
            print("❌ YDB credentials not available")
            return False
        
        table_path = ydb_wrapper.get_table_path(TABLE_NAME, base_path=TABLE_PATH)
        
        # Build query
        conditions = []
        if team_name:
            conditions.append(f"team_name = '{team_name}'")
        if branch:
            conditions.append(f"branch = '{branch}'")
        if build_type:
            conditions.append(f"build_type = '{build_type}'")
        if before:
            before_str = before.strftime('%Y-%m-%dT%H:%M:%SZ')
            conditions.append(f"created_at < Timestamp('{before_str}')")
        
        if not conditions:
            print("⚠️ No conditions specified, refusing to delete all notifications")
            return False
        
        where_clause = " AND ".join(conditions)
        
        query = f"""
            DELETE FROM `{table_path}`
            WHERE {where_clause}
        """
        
        ydb_wrapper.execute_query(query)
        
        print(f"✅ Deleted notifications matching conditions")
        return True
        
    except Exception as e:
        print(f"❌ Error deleting notifications: {e}")
        import traceback
        print(traceback.format_exc())
        return False


def main():
    """CLI interface for notification storage."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Manage notification storage")
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Store command
    store_parser = subparsers.add_parser('store', help='Store a notification')
    store_parser.add_argument('--team-name', required=True, help='Team name')
    store_parser.add_argument('--branch', required=True, help='Branch name')
    store_parser.add_argument('--build-type', required=True, help='Build type')
    store_parser.add_argument('--data-file', required=True, help='Path to JSON file with notification data')
    
    # Get command
    get_parser = subparsers.add_parser('get', help='Get pending notifications')
    get_parser.add_argument('--team-name', help='Filter by team name')
    get_parser.add_argument('--branch', help='Filter by branch')
    get_parser.add_argument('--build-type', help='Filter by build type')
    get_parser.add_argument('--since', help='Get notifications since this datetime (ISO format)')
    
    # Delete command
    delete_parser = subparsers.add_parser('delete', help='Delete notifications')
    delete_parser.add_argument('--team-name', help='Filter by team name')
    delete_parser.add_argument('--branch', help='Filter by branch')
    delete_parser.add_argument('--build-type', help='Filter by build type')
    delete_parser.add_argument('--before', help='Delete notifications before this datetime (ISO format)')
    
    # Create table command
    create_parser = subparsers.add_parser('create-table', help='Create notification table')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    if args.command == 'store':
        with open(args.data_file, 'r') as f:
            notification_data = json.load(f)
        
        success = store_notification(
            team_name=args.team_name,
            branch=args.branch,
            build_type=args.build_type,
            notification_data=notification_data
        )
        sys.exit(0 if success else 1)
    
    elif args.command == 'get':
        since = None
        if args.since:
            since = datetime.fromisoformat(args.since.replace('Z', '+00:00'))
        
        notifications = get_pending_notifications(
            team_name=args.team_name,
            branch=args.branch,
            build_type=args.build_type,
            since=since
        )
        
        print(json.dumps(notifications, indent=2, default=str))
        sys.exit(0)
    
    elif args.command == 'delete':
        before = None
        if args.before:
            before = datetime.fromisoformat(args.before.replace('Z', '+00:00'))
        
        success = delete_notifications(
            team_name=args.team_name,
            branch=args.branch,
            build_type=args.build_type,
            before=before
        )
        sys.exit(0 if success else 1)
    
    elif args.command == 'create-table':
        with YDBWrapper(silent=False) as ydb_wrapper:
            if ydb_wrapper.check_credentials():
                create_table(ydb_wrapper)
                sys.exit(0)
            else:
                print("❌ YDB credentials not available")
                sys.exit(1)


if __name__ == "__main__":
    main()

