#!/usr/bin/env python3
"""
Data Migration Executor
Migrates data from old DB to new DB based on schema analysis.
"""

import psycopg2
import json
import sys
from typing import Dict, List, Optional, Any
from datetime import datetime


class DataMigrator:
    """Handles actual data migration between databases"""

    def __init__(self, old_db_config: Dict, new_db_config: Dict,
                 changes: List[Dict], resolutions: Optional[Dict] = None,
                 batch_size: int = 1000, table_order: Optional[List[str]] = None):
        self.old_db_config = old_db_config
        self.new_db_config = new_db_config
        self.changes = changes
        self.resolutions = resolutions or {'resolutions': {}}
        self.batch_size = batch_size
        self.table_order = table_order  # For foreign key constraint ordering
        self.stats = {
            'tables_processed': 0,
            'rows_migrated': 0,
            'rows_failed': 0,
            'errors': []
        }

    def get_resolutions_for_table(self, table: str) -> List[Dict]:
        """Get manual resolutions for a specific table"""
        return self.resolutions.get('resolutions', {}).get(table, [])

    def build_column_mapping(self, table: str) -> Dict[str, str]:
        """Build mapping of old columns to new columns for a table"""
        mapping = {}

        # Get manual resolutions first (higher priority)
        resolutions = self.get_resolutions_for_table(table)
        for res in resolutions:
            if res.get('action') == 'rename':
                old_col = res.get('old_column')
                new_col = res.get('new_column')
                if old_col and new_col:
                    mapping[old_col] = new_col
            elif res.get('action') == 'ignore':
                # Mark column to be ignored
                old_col = res.get('old_column')
                if old_col:
                    mapping[old_col] = None  # None means ignore

        # Add automatic mappings from schema analysis
        for change in self.changes:
            if change['table'] == table:
                if change['old_column'] and change['new_column']:
                    # Only add if not already resolved manually
                    if change['old_column'] not in mapping:
                        mapping[change['old_column']] = change['new_column']

        return mapping

    def get_transform_rules(self, table: str) -> Dict[str, Dict]:
        """Get data transformation rules for specific columns"""
        transforms = {}
        resolutions = self.get_resolutions_for_table(table)

        for res in resolutions:
            if res.get('action') == 'transform':
                old_col = res.get('old_column')
                transform_sql = res.get('transform_sql')
                value_mapping = res.get('value_mapping')  # Direct mapping
                lookup_table = res.get('lookup_table')    # Database lookup
                default_value = res.get('default_value')  # Fallback value

                if old_col:
                    transforms[old_col] = {
                        'sql': transform_sql,
                        'mapping': value_mapping,
                        'lookup': lookup_table,
                        'default_value': default_value
                    }

        return transforms

    def get_default_values(self, table: str) -> Dict[str, Any]:
        """Get default values for new columns"""
        defaults = {}
        resolutions = self.get_resolutions_for_table(table)

        for res in resolutions:
            if res.get('action') == 'default':
                new_col = res.get('new_column')
                default_val = res.get('default_value')
                # Allow default_value to be set even if it's falsy (e.g., 0, empty string)
                if new_col is not None and default_val is not None:
                    # Skip USE_SCHEMA_DEFAULT - let DB handle it
                    if default_val != "USE_SCHEMA_DEFAULT":
                        defaults[new_col] = default_val

        return defaults

    def get_lookup_cache(self, lookup_config: Dict) -> Dict:
        """Load lookup table into memory for fast lookups"""
        cache = {}

        try:
            new_conn = psycopg2.connect(
                host=self.new_db_config['host'],
                port=self.new_db_config['port'],
                database=self.new_db_config['database'],
                user=self.new_db_config['username'],
                password=self.new_db_config['password'],
                sslmode=self.new_db_config.get('sslmode', 'prefer')
            )
            cursor = new_conn.cursor()

            table = lookup_config.get('table')
            key_col = lookup_config.get('key_column')
            value_col = lookup_config.get('value_column')
            schema = lookup_config.get('schema', self.new_db_config['schema'])

            query = f"SELECT {key_col}, {value_col} FROM {schema}.{table}"
            cursor.execute(query)

            for row in cursor.fetchall():
                cache[row[0]] = row[1]

            cursor.close()
            new_conn.close()

            print(f"    Loaded {len(cache)} entries from lookup table {table}")

        except Exception as e:
            print(f"    ⚠ Error loading lookup table: {e}")

        return cache

    def apply_transform(self, value: Any, old_col: str, new_col: str,
                        transform_rule: Dict, lookup_caches: Dict) -> Any:
        """Apply transformation to a value"""
        # Priority 1: Direct value mapping (for varchar -> int conversions)
        if transform_rule.get('mapping'):
            mapping = transform_rule['mapping']
            if value in mapping:
                return mapping[value]
            elif transform_rule.get('default_value') is not None:
                return transform_rule['default_value']
            else:
                raise ValueError(f"Value '{value}' not found in mapping for column '{old_col}' -> '{new_col}'")

        # Priority 2: Database lookup (for reference table lookups)
        if transform_rule.get('lookup'):
            lookup_config = transform_rule['lookup']
            cache_key = f"{lookup_config['table']}_{lookup_config['key_column']}"

            # Load cache if not already loaded
            if cache_key not in lookup_caches:
                lookup_caches[cache_key] = self.get_lookup_cache(lookup_config)

            cache = lookup_caches[cache_key]
            if value in cache:
                return cache[value]
            elif transform_rule.get('default_value') is not None:
                return transform_rule['default_value']
            else:
                raise ValueError(f"Value '{value}' not found in lookup table for column '{old_col}' -> '{new_col}'")

        # Priority 3: SQL expression or simple passthrough
        # For complex SQL transforms, this would need SQL execution
        # For now, return the original value (can be extended for SQL transforms)
        if transform_rule.get('sql'):
            # TODO: Implement SQL expression execution if needed
            # For now, just return the original value
            print(f"    ⚠ SQL transform specified for '{old_col}' -> '{new_col}' but not executed, using original value")

        return value

    def get_tables_to_migrate(self) -> List[str]:
        """Get list of tables that need migration, preserving order from config"""
        tables_list = []

        # Check resolutions for tables to drop
        dropped_tables = set()
        for table, resolutions in self.resolutions.get('resolutions', {}).items():
            for res in resolutions:
                if res.get('action') == 'drop_table':
                    dropped_tables.add(table)

        # Collect tables from changes
        tables_from_changes = set()
        for change in self.changes:
            table = change['table']
            if table in dropped_tables:
                continue
            # Skip tables that don't exist in new DB
            if not (change.get('description', '').startswith("Table '") and
                   'not in new DB' in change.get('description', '')):
                tables_from_changes.add(table)

        # If tables were specified in config, use that order
        # This is critical for foreign key constraints
        if hasattr(self, 'table_order') and self.table_order:
            print("  Using table order from config (important for foreign keys)")
            for table in self.table_order:
                if table in tables_from_changes and table not in dropped_tables:
                    tables_list.append(table)
            # Add any remaining tables not in config order
            for table in sorted(tables_from_changes):
                if table not in tables_list:
                    tables_list.append(table)
        # If no changes detected (identical schemas), get all tables from old DB
        elif len(tables_from_changes) == 0:
            print("  No schema changes detected - will migrate all tables")
            try:
                old_conn = psycopg2.connect(
                    host=self.old_db_config['host'],
                    port=self.old_db_config['port'],
                    database=self.old_db_config['database'],
                    user=self.old_db_config['username'],
                    password=self.old_db_config['password'],
                    sslmode=self.old_db_config.get('sslmode', 'prefer')
                )
                cursor = old_conn.cursor()

                query = """
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = %s 
                    AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                """
                cursor.execute(query, (self.old_db_config['schema'],))
                all_tables = [row[0] for row in cursor.fetchall()]

                cursor.close()
                old_conn.close()

                # Respect config order if provided
                if hasattr(self, 'table_order') and self.table_order:
                    print("  Using table order from config (important for foreign keys)")
                    for table in self.table_order:
                        if table in all_tables:
                            tables_list.append(table)
                    # Add remaining tables
                    for table in all_tables:
                        if table not in tables_list:
                            tables_list.append(table)
                else:
                    tables_list = all_tables

            except Exception as e:
                print(f"  ⚠ Error fetching tables: {e}")
        else:
            tables_list = sorted(list(tables_from_changes))

        return tables_list

    def fetch_old_schema(self, table: str) -> List[str]:
        """Get column names from old database table"""
        conn = psycopg2.connect(
            host=self.old_db_config['host'],
            port=self.old_db_config['port'],
            database=self.old_db_config['database'],
            user=self.old_db_config['username'],
            password=self.old_db_config['password'],
            sslmode=self.old_db_config.get('sslmode', 'prefer')
        )
        cursor = conn.cursor()

        query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """
        cursor.execute(query, (self.old_db_config['schema'], table))
        columns = [row[0] for row in cursor.fetchall()]

        cursor.close()
        conn.close()
        return columns

    def fetch_new_schema(self, table: str) -> Dict[str, Any]:
        """Get column info from new database table"""
        conn = psycopg2.connect(
            host=self.new_db_config['host'],
            port=self.new_db_config['port'],
            database=self.new_db_config['database'],
            user=self.new_db_config['username'],
            password=self.new_db_config['password'],
            sslmode=self.new_db_config.get('sslmode', 'prefer')
        )
        cursor = conn.cursor()

        query = """
            SELECT column_name, column_default, is_nullable, data_type
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """
        cursor.execute(query, (self.new_db_config['schema'], table))

        columns = {}
        for row in cursor.fetchall():
            columns[row[0]] = {
                'default': row[1],
                'nullable': row[2] == 'YES',
                'type': row[3]
            }

        cursor.close()
        conn.close()
        return columns

    def transform_row(self, old_row: Dict[str, Any],
                      column_mapping: Dict[str, str],
                      new_schema: Dict[str, Any],
                      table: str,
                      lookup_caches: Dict) -> Dict[str, Any]:
        """Transform a row from old schema to new schema"""
        new_row = {}
        transform_rules = self.get_transform_rules(table)
        default_values = self.get_default_values(table)

        # Map existing columns
        for old_col, value in old_row.items():
            new_col = column_mapping.get(old_col, old_col)

            # Skip if marked to ignore
            if new_col is None:
                continue

            if new_col in new_schema:
                # Apply transformation if specified
                if old_col in transform_rules:
                    transform = transform_rules[old_col]
                    value = self.apply_transform(
                        value, old_col, new_col, transform, lookup_caches
                    )

                # Handle JSON/JSONB types - convert dict/list to JSON string
                col_type = new_schema[new_col]['type'].upper()
                if col_type in ('JSON', 'JSONB'):
                    if isinstance(value, (dict, list)):
                        new_row[new_col] = json.dumps(value)
                    elif isinstance(value, str):
                        # Already a JSON string, use as-is
                        new_row[new_col] = value
                    elif value is None:
                        new_row[new_col] = None
                    else:
                        # Try to serialize other types
                        new_row[new_col] = json.dumps(value)
                elif isinstance(value, (dict, list)):
                    # Non-JSON column but got dict/list - serialize to string
                    new_row[new_col] = json.dumps(value)
                else:
                    new_row[new_col] = value

        # Add default values for new columns not in old DB
        for new_col, col_info in new_schema.items():
            if new_col not in new_row:
                # Check manual defaults first
                if new_col in default_values:
                    new_row[new_col] = default_values[new_col]
                elif col_info['nullable']:
                    new_row[new_col] = None
                elif col_info['default']:
                    # Column has a default in DB - don't include in INSERT
                    # so database applies its default value
                    pass  # Don't add to new_row, let DB use its default
                # If not nullable and no default - this will cause an error
                # which is caught and reported during migration

        return new_row

    def migrate_table(self, table: str, dry_run: bool = True) -> Dict:
        """Migrate data for a single table"""
        print(f"\n{'[DRY RUN] ' if dry_run else ''}Migrating table: {table}")

        result = {
            'table': table,
            'rows_migrated': 0,
            'rows_failed': 0,
            'errors': []
        }

        # Lookup cache for this table (shared across all rows)
        lookup_caches = {}

        try:
            # Get schemas
            old_columns = self.fetch_old_schema(table)
            new_schema = self.fetch_new_schema(table)
            column_mapping = self.build_column_mapping(table)

            print(f"  Old columns: {len(old_columns)}, New columns: {len(new_schema)}")

            # Connect to both databases
            old_conn = psycopg2.connect(
                host=self.old_db_config['host'],
                port=self.old_db_config['port'],
                database=self.old_db_config['database'],
                user=self.old_db_config['username'],
                password=self.old_db_config['password'],
                sslmode=self.old_db_config.get('sslmode', 'prefer')
            )
            new_conn = psycopg2.connect(
                host=self.new_db_config['host'],
                port=self.new_db_config['port'],
                database=self.new_db_config['database'],
                user=self.new_db_config['username'],
                password=self.new_db_config['password'],
                sslmode=self.new_db_config.get('sslmode', 'prefer')
            )

            old_cursor = old_conn.cursor()
            new_cursor = new_conn.cursor()

            # Count total rows
            schema_prefix = f"{self.old_db_config['schema']}." if self.old_db_config['schema'] else ""
            count_query = f"SELECT COUNT(*) FROM {schema_prefix}{table}"
            old_cursor.execute(count_query)
            total_rows = old_cursor.fetchone()[0]
            print(f"  Total rows to migrate: {total_rows}")

            # Fetch data in batches
            select_query = f"SELECT * FROM {schema_prefix}{table}"
            old_cursor.execute(select_query)

            rows_processed = 0

            while True:
                rows = old_cursor.fetchmany(self.batch_size)
                if not rows:
                    break

                for row in rows:
                    try:
                        old_row = dict(zip(old_columns, row))
                        new_row = self.transform_row(old_row, column_mapping, new_schema, table, lookup_caches)

                        if not dry_run:
                            # Insert into new database
                            cols = list(new_row.keys())
                            values = [new_row[col] for col in cols]

                            placeholders = ', '.join(['%s'] * len(cols))
                            col_names = ', '.join(cols)
                            insert_query = f"""
                                INSERT INTO {self.new_db_config['schema']}.{table} 
                                ({col_names}) VALUES ({placeholders})
                            """
                            new_cursor.execute(insert_query, values)

                        result['rows_migrated'] += 1

                    except Exception as e:
                        result['rows_failed'] += 1
                        result['errors'].append(f"Row {rows_processed}: {str(e)}")
                        if len(result['errors']) <= 10:
                            print(f"    ⚠ Error on row {rows_processed}: {e}")

                    rows_processed += 1

                if not dry_run and rows_processed % self.batch_size == 0:
                    new_conn.commit()
                    print(f"    Committed {rows_processed}/{total_rows} rows...")

            if not dry_run:
                new_conn.commit()

            print(f"  ✓ Processed: {rows_processed}, Success: {result['rows_migrated']}, Failed: {result['rows_failed']}")

            old_cursor.close()
            new_cursor.close()
            old_conn.close()
            new_conn.close()

        except Exception as e:
            result['errors'].append(f"Table migration failed: {str(e)}")
            print(f"  ✗ Error: {e}")

        return result

    def migrate_all(self, dry_run: bool = True) -> Dict:
        """Migrate all tables"""
        print("=" * 80)
        print(f"{'DRY RUN - ' if dry_run else ''}DATA MIGRATION")
        print("=" * 80)

        if dry_run:
            print("⚠ Running in DRY RUN mode - no data will be written")

        tables = self.get_tables_to_migrate()
        print(f"\nTables to migrate: {len(tables)}")
        print(f"Batch size: {self.batch_size}")

        start_time = datetime.now()
        results = []

        for table in tables:
            result = self.migrate_table(table, dry_run)
            results.append(result)
            self.stats['tables_processed'] += 1
            self.stats['rows_migrated'] += result['rows_migrated']
            self.stats['rows_failed'] += result['rows_failed']
            self.stats['errors'].extend(result['errors'])

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        # Summary
        print("\n" + "=" * 80)
        print("MIGRATION SUMMARY")
        print("=" * 80)
        print(f"Tables processed: {self.stats['tables_processed']}")
        print(f"Total rows migrated: {self.stats['rows_migrated']}")
        print(f"Total rows failed: {self.stats['rows_failed']}")
        print(f"Duration: {duration:.2f} seconds")

        if self.stats['errors']:
            print(f"\nErrors encountered: {len(self.stats['errors'])}")
            print("First 10 errors:")
            for error in self.stats['errors'][:10]:
                print(f"  - {error}")

        print("=" * 80)

        return {
            'stats': self.stats,
            'results': results,
            'duration': duration
        }


def load_resolutions(resolutions_file: str) -> Optional[Dict]:
    """Load manual resolutions from JSON file"""
    try:
        with open(resolutions_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"❌ Resolutions file '{resolutions_file}' not found")
        sys.exit(1)


def load_changes(changes_file: str) -> List[Dict]:
    """Load changes from JSON file"""
    try:
        with open(changes_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"❌ Changes file '{changes_file}' not found")
        print("Run schema analysis first to generate migration_changes.json")
        sys.exit(1)


def load_config(config_file: str) -> dict:
    """Load configuration from JSON file"""
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"❌ Configuration file '{config_file}' not found")
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/data_migrator.py <config_file> [OPTIONS]")
        print("\nOptions:")
        print("  --live                   Execute actual migration (default is dry run)")
        print("  --batch-size N           Process N rows per batch (default: 1000)")
        print("  --changes-file FILE      Specify changes JSON file")
        print("  --resolutions FILE       Specify manual resolutions file (optional)")
        print("  --prefix PREFIX          Use prefix for result files")
        print("\nExamples:")
        print("  python scripts/data_migrator.py config/migration-config.json --prefix prod")
        print("  python scripts/data_migrator.py config/migration-config.json --changes-file results/prod_migration_changes.json --resolutions results/prod_manual_resolutions.json --live")
        sys.exit(1)

    config_file = sys.argv[1]
    dry_run = '--live' not in sys.argv

    # Parse options
    batch_size = 1000
    changes_file = None
    resolutions_file = None
    prefix = ""

    i = 2
    while i < len(sys.argv):
        if sys.argv[i] == '--batch-size' and i + 1 < len(sys.argv):
            batch_size = int(sys.argv[i + 1])
            i += 2
        elif sys.argv[i] == '--changes-file' and i + 1 < len(sys.argv):
            changes_file = sys.argv[i + 1]
            i += 2
        elif sys.argv[i] == '--resolutions' and i + 1 < len(sys.argv):
            resolutions_file = sys.argv[i + 1]
            i += 2
        elif sys.argv[i] == '--prefix' and i + 1 < len(sys.argv):
            prefix = sys.argv[i + 1] + "_"
            i += 2
        elif sys.argv[i] == '--live':
            i += 1
        else:
            i += 1

    # Auto-detect changes file if not specified
    if not changes_file:
        changes_file = f"results/{prefix}migration_changes.json"

    print("📋 Loading configuration...")
    config = load_config(config_file)

    print(f"📋 Loading schema changes from {changes_file}...")
    changes = load_changes(changes_file)

    # Load manual resolutions if provided
    resolutions = None
    if resolutions_file:
        print(f"📋 Loading manual resolutions from {resolutions_file}...")
        resolutions = load_resolutions(resolutions_file)

    # Check for breaking changes
    breaking = [c for c in changes if c.get('requires_manual', False)]
    if breaking and not resolutions:
        print(f"\n❌ Found {len(breaking)} breaking changes that require manual intervention:")
        for change in breaking[:5]:
            print(f"  - [{change['table']}] {change['description']}")
        print(f"\nGenerate resolution template with:")
        print(f"  python scripts/manual_resolutions.py generate {changes_file} --prefix {prefix.rstrip('_')}")
        sys.exit(1)

    # Initialize migrator
    migrator = DataMigrator(
        old_db_config=config['old_database'],
        new_db_config=config['new_database'],
        changes=changes,
        resolutions=resolutions,
        batch_size=batch_size,
        table_order=config.get('tables')  # Preserve table order for foreign keys
    )

    # Execute migration
    result = migrator.migrate_all(dry_run=dry_run)

    # Save results to results folder
    result_file = f"results/{prefix}migration_result_{'dryrun' if dry_run else 'live'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(result_file, 'w') as f:
        json.dump(result, f, indent=2, default=str)

    print(f"\n📄 Results saved to: {result_file}")

    if dry_run:
        print("\n💡 To execute actual migration, run with --live flag")

