#!/usr/bin/env python3
"""
Automated Database Migration Tool for PostgreSQL on Azure
Handles schema comparison with read-only access.
Requires: pip install psycopg2-binary
"""

import psycopg2
import json
import sys
import os
from typing import Dict, List, Tuple, Any, Optional
from dataclasses import dataclass
from enum import Enum
from difflib import SequenceMatcher


class ChangeType(Enum):
    """Types of schema changes"""
    SAFE = "safe"
    TRANSFORMABLE = "transformable"
    BREAKING = "breaking"


@dataclass
class Column:
    """Database column representation"""
    name: str
    type: str
    nullable: bool
    default: Any = None
    primary_key: bool = False
    character_maximum_length: Optional[int] = None


@dataclass
class SchemaChange:
    """Represents a schema change"""
    change_type: ChangeType
    table: str
    description: str
    old_column: Optional[str] = None
    new_column: Optional[str] = None
    auto_action: Optional[str] = None
    requires_manual: bool = False


class PostgreSQLSchemaExtractor:
    """Extracts schema information from PostgreSQL databases"""

    def __init__(self, host: str, port: int, database: str, username: str, password: str, schema: str,
                 sslmode: str = 'prefer'):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.schema = schema
        self.sslmode = sslmode

    def get_all_tables(self) -> List[str]:
        """Get all table names in the schema"""
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.username,
                password=self.password,
                sslmode=self.sslmode
            )
            cursor = conn.cursor()

            query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = %s 
                AND table_type = 'BASE TABLE'
                ORDER BY table_name;
            """

            cursor.execute(query, (self.schema,))
            tables = [row[0] for row in cursor.fetchall()]

            cursor.close()
            conn.close()

            return tables

        except psycopg2.Error as e:
            print(f"❌ Database connection error: {e}")
            sys.exit(1)

    def extract_schema(self, tables: Optional[List[str]] = None) -> Dict[str, List[Column]]:
        """Extract schema for specified tables from PostgreSQL database"""
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.username,
                password=self.password,
                sslmode=self.sslmode
            )
            cursor = conn.cursor()

            if tables is None:
                query = """
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = %s 
                    AND table_type = 'BASE TABLE'
                    ORDER BY table_name;
                """
                cursor.execute(query, (self.schema,))
                tables = [row[0] for row in cursor.fetchall()]
                print(f"  Found {len(tables)} tables in schema '{self.schema}'")

            schema_dict = {}

            for table_name in tables:
                query = """
                    SELECT 
                        c.column_name,
                        c.data_type,
                        c.is_nullable,
                        c.column_default,
                        c.character_maximum_length,
                        CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_primary_key
                    FROM 
                        information_schema.columns c
                    LEFT JOIN (
                        SELECT ku.column_name
                        FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage ku
                            ON tc.constraint_name = ku.constraint_name
                            AND tc.table_schema = ku.table_schema
                        WHERE tc.constraint_type = 'PRIMARY KEY'
                            AND tc.table_schema = %s
                            AND tc.table_name = %s
                    ) pk ON c.column_name = pk.column_name
                    WHERE 
                        c.table_schema = %s
                        AND c.table_name = %s
                    ORDER BY 
                        c.ordinal_position;
                """

                cursor.execute(query, (self.schema, table_name, self.schema, table_name))
                columns_info = cursor.fetchall()

                if not columns_info:
                    print(f"⚠ Warning: Table '{table_name}' not found in schema '{self.schema}'")
                    continue

                columns = []
                for col in columns_info:
                    columns.append(Column(
                        name=col[0],
                        type=col[1],
                        nullable=(col[2] == 'YES'),
                        default=col[3],
                        character_maximum_length=col[4],
                        primary_key=col[5]
                    ))

                schema_dict[table_name] = columns

            cursor.close()
            conn.close()

            return schema_dict

        except psycopg2.Error as e:
            print(f"❌ Database connection error: {e}")
            sys.exit(1)
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            sys.exit(1)


class SchemaAnalyzer:
    """Analyzes differences between schemas"""

    @staticmethod
    def calculate_similarity(str1: str, str2: str) -> float:
        """Calculate similarity between two strings"""
        return SequenceMatcher(None, str1.lower(), str2.lower()).ratio()

    @staticmethod
    def find_renamed_column(old_col: str, new_columns: List[str], threshold: float = 0.7) -> Optional[str]:
        """Find potential renamed column using similarity matching"""
        best_match = None
        best_score = threshold

        for new_col in new_columns:
            score = SchemaAnalyzer.calculate_similarity(old_col, new_col)
            if score > best_score:
                best_score = score
                best_match = new_col

        return best_match

    @staticmethod
    def is_type_compatible(old_type: str, new_type: str, old_col: Column, new_col: Column) -> bool:
        """Check if type conversion is safe"""
        old_type = old_type.upper()
        new_type = new_type.upper()

        if old_type == new_type:
            if 'CHARACTER' in old_type or 'VARCHAR' in old_type:
                if old_col.character_maximum_length and new_col.character_maximum_length:
                    if old_col.character_maximum_length > new_col.character_maximum_length:
                        return False
            return True

        safe_conversions = {
            'SMALLINT': ['INTEGER', 'BIGINT', 'NUMERIC', 'REAL', 'DOUBLE PRECISION', 'TEXT'],
            'INTEGER': ['BIGINT', 'NUMERIC', 'REAL', 'DOUBLE PRECISION', 'TEXT'],
            'BIGINT': ['NUMERIC', 'TEXT'],
            'NUMERIC': ['TEXT'],
            'REAL': ['DOUBLE PRECISION', 'NUMERIC', 'TEXT'],
            'DOUBLE PRECISION': ['NUMERIC', 'TEXT'],
            'CHARACTER VARYING': ['TEXT'],
            'CHARACTER': ['CHARACTER VARYING', 'TEXT'],
            'DATE': ['TIMESTAMP WITHOUT TIME ZONE', 'TIMESTAMP WITH TIME ZONE', 'TEXT'],
            'TIMESTAMP WITHOUT TIME ZONE': ['TIMESTAMP WITH TIME ZONE', 'TEXT'],
            'BOOLEAN': ['TEXT'],
        }

        for old_base, compatible in safe_conversions.items():
            if old_base in old_type:
                for new_base in compatible:
                    if new_base in new_type:
                        return True

        return False

    def compare_schemas(self, current_schema: Dict[str, List[Column]],
                        new_schema: Dict[str, List[Column]]) -> List[SchemaChange]:
        """Compare schemas and identify gaps"""
        changes = []

        current_tables = set(current_schema.keys())
        new_tables = set(new_schema.keys())
        tables_to_analyze = current_tables

        for table in tables_to_analyze:
            if table not in new_tables:
                changes.append(SchemaChange(
                    change_type=ChangeType.BREAKING,
                    table=table,
                    description=f"Table '{table}' exists in current DB but not in new DB",
                    requires_manual=True
                ))
                continue

            current_cols = {col.name: col for col in current_schema[table]}
            new_cols = {col.name: col for col in new_schema[table]}

            current_col_names = set(current_cols.keys())
            new_col_names = set(new_cols.keys())

            for col_name in current_col_names:
                if col_name in new_col_names:
                    curr_col = current_cols[col_name]
                    new_col = new_cols[col_name]

                    if curr_col.type != new_col.type:
                        is_safe = self.is_type_compatible(curr_col.type, new_col.type, curr_col, new_col)
                        changes.append(SchemaChange(
                            change_type=ChangeType.TRANSFORMABLE if is_safe else ChangeType.BREAKING,
                            table=table,
                            description=f"Column '{col_name}' type mismatch: current({curr_col.type}) vs new({new_col.type})",
                            old_column=col_name,
                            new_column=col_name,
                            auto_action=f"CAST to {new_col.type}" if is_safe else None,
                            requires_manual=not is_safe
                        ))
                else:
                    potential_match = self.find_renamed_column(col_name, list(new_col_names - current_col_names))
                    if potential_match:
                        changes.append(SchemaChange(
                            change_type=ChangeType.TRANSFORMABLE,
                            table=table,
                            description=f"Column '{col_name}' likely renamed to '{potential_match}' in new DB",
                            old_column=col_name,
                            new_column=potential_match,
                            auto_action="Rename column"
                        ))
                    else:
                        changes.append(SchemaChange(
                            change_type=ChangeType.BREAKING,
                            table=table,
                            description=f"Column '{col_name}' exists in current DB but not in new DB",
                            old_column=col_name,
                            requires_manual=True
                        ))

            for col_name in new_col_names - current_col_names:
                new_col = new_cols[col_name]

                # Check if column existed in old DB via potential rename
                is_potential_rename = any(
                    c.new_column == col_name and c.old_column is not None
                    for c in changes if hasattr(c, 'new_column')
                )

                if is_potential_rename:
                    # Already handled as a rename, skip
                    continue

                # New column that doesn't exist in old DB
                if new_col.nullable:
                    # Nullable column - safe, will be NULL
                    changes.append(SchemaChange(
                        change_type=ChangeType.SAFE,
                        table=table,
                        description=f"Column '{col_name}' only exists in new DB (nullable)",
                        new_column=col_name,
                        auto_action="Will be NULL during migration"
                    ))
                elif new_col.default is not None:
                    # NOT NULL but has a default - needs attention, user can override
                    changes.append(SchemaChange(
                        change_type=ChangeType.BREAKING,
                        table=table,
                        description=f"Column '{col_name}' is NOT NULL in new DB, not in old DB, with default: {new_col.default}",
                        new_column=col_name,
                        auto_action=f"Schema default: {new_col.default}",
                        requires_manual=True
                    ))
                else:
                    # NOT NULL without default - BREAKING, must provide value
                    changes.append(SchemaChange(
                        change_type=ChangeType.BREAKING,
                        table=table,
                        description=f"Column '{col_name}' is NOT NULL in new DB without default, not in old DB",
                        new_column=col_name,
                        requires_manual=True
                    ))

        return changes


class MigrationOrchestrator:
    """Main orchestrator for migration analysis"""

    def __init__(self, old_db_config: Dict, new_db_config: Dict, tables: Optional[List[str]] = None,
                 table_mapping: Optional[Dict[str, str]] = None):
        self.old_db_config = old_db_config
        self.new_db_config = new_db_config
        self.tables = tables
        self.table_mapping = table_mapping or {}  # Map old table names to new table names
        self.analyzer = SchemaAnalyzer()

    def get_new_table_name(self, old_table: str) -> str:
        """Get the new table name (handles table renames)"""
        return self.table_mapping.get(old_table, old_table)

    def analyze(self) -> Tuple[List[SchemaChange], bool]:
        """Analyze schema differences"""
        print("🔌 Connecting to OLD database...")
        old_extractor = PostgreSQLSchemaExtractor(**self.old_db_config)

        tables_to_extract_old = self.tables
        if tables_to_extract_old is None:
            print("  No specific tables provided, scanning all tables in schema...")

        old_schema = old_extractor.extract_schema(tables_to_extract_old)
        print(f"✓ Extracted schema for {len(old_schema)} tables from OLD database")

        print("\n🔌 Connecting to NEW database...")
        new_extractor = PostgreSQLSchemaExtractor(**self.new_db_config)

        # For new database, extract using mapped table names
        if self.table_mapping and tables_to_extract_old:
            tables_to_extract_new = [self.get_new_table_name(t) for t in tables_to_extract_old]
        else:
            tables_to_extract_new = tables_to_extract_old

        new_schema = new_extractor.extract_schema(tables_to_extract_new)
        print(f"✓ Extracted schema for {len(new_schema)} tables from NEW database")

        # If table mapping exists, remap new_schema keys to match old table names for comparison
        if self.table_mapping:
            # Create reverse mapping: new_name -> old_name
            reverse_mapping = {v: k for k, v in self.table_mapping.items()}
            remapped_new_schema = {}
            for table_name, columns in new_schema.items():
                # Use old table name as key if this table was renamed
                old_name = reverse_mapping.get(table_name, table_name)
                remapped_new_schema[old_name] = columns
            new_schema = remapped_new_schema

        print("\n🔍 Analyzing differences...")
        changes = self.analyzer.compare_schemas(old_schema, new_schema)

        can_auto_migrate = not any(change.requires_manual for change in changes)

        return changes, can_auto_migrate

    def generate_report(self, changes: List[SchemaChange]) -> str:
        """Generate migration report"""
        report = ["=" * 80]
        report.append("DATABASE MIGRATION ANALYSIS REPORT")
        report.append("=" * 80)
        report.append(
            f"Old Database: {self.old_db_config['host']}/{self.old_db_config['database']}.{self.old_db_config['schema']}")
        report.append(
            f"New Database: {self.new_db_config['host']}/{self.new_db_config['database']}.{self.new_db_config['schema']}")

        if self.tables:
            report.append(f"Tables Analyzed: {', '.join(self.tables)}")
        else:
            report.append("Tables Analyzed: ALL tables in schema")

        if self.table_mapping:
            report.append(f"Table Mappings: {len(self.table_mapping)} table(s) renamed")
            for old_name, new_name in self.table_mapping.items():
                report.append(f"  - {old_name} -> {new_name}")

        report.append("")

        safe = [c for c in changes if c.change_type == ChangeType.SAFE]
        transformable = [c for c in changes if c.change_type == ChangeType.TRANSFORMABLE]
        breaking = [c for c in changes if c.change_type == ChangeType.BREAKING]

        report.append(f"Total Changes: {len(changes)}")
        report.append(f"  - Safe (auto-handled): {len(safe)}")
        report.append(f"  - Transformable (auto-converted): {len(transformable)}")
        report.append(f"  - Breaking (manual required): {len(breaking)}")
        report.append("")

        if safe:
            report.append("SAFE CHANGES (Can be handled automatically):")
            report.append("-" * 80)
            for change in safe:
                report.append(f"  [{change.table}] {change.description}")
                if change.auto_action:
                    report.append(f"    → Action: {change.auto_action}")
            report.append("")

        if transformable:
            report.append("TRANSFORMABLE CHANGES (Can be converted automatically):")
            report.append("-" * 80)
            for change in transformable:
                report.append(f"  [{change.table}] {change.description}")
                if change.auto_action:
                    report.append(f"    → Action: {change.auto_action}")
            report.append("")

        if breaking:
            report.append("BREAKING CHANGES (MANUAL INTERVENTION REQUIRED):")
            report.append("-" * 80)
            for change in breaking:
                report.append(f"  [{change.table}] {change.description}")
                report.append(f"    ⚠ Manual action required!")
            report.append("")

        report.append("=" * 80)

        if breaking:
            report.append("❌ MIGRATION CANNOT PROCEED AUTOMATICALLY")
            report.append("Please resolve the breaking changes above.")
        else:
            report.append("✅ MIGRATION CAN PROCEED AUTOMATICALLY")

        report.append("=" * 80)

        return "\n".join(report)


def load_config(config_file: str) -> dict:
    """Load configuration from JSON file"""
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"❌ Configuration file '{config_file}' not found")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON in configuration file: {e}")
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/db_migration_tool.py <config_file> [--prefix PREFIX]")
        print("\nExample: python scripts/db_migration_tool.py config/migration-config.json --prefix prod")
        print("Output: results/prod_migration_changes.json, results/prod_migration_report.txt")
        sys.exit(1)

    config_file = sys.argv[1]

    # Parse prefix option
    prefix = ""
    if '--prefix' in sys.argv:
        idx = sys.argv.index('--prefix')
        if idx + 1 < len(sys.argv):
            prefix = sys.argv[idx + 1] + "_"

    print("📋 Loading configuration...")
    config = load_config(config_file)

    required_keys = ['old_database', 'new_database']
    for key in required_keys:
        if key not in config:
            print(f"❌ Missing required key in config: '{key}'")
            sys.exit(1)

    tables = config.get('tables', None)

    if tables:
        print(f"📊 Will analyze {len(tables)} specified tables")
    else:
        print("📊 Will analyze ALL tables in the specified schemas")

    orchestrator = MigrationOrchestrator(
        old_db_config=config['old_database'],
        new_db_config=config['new_database'],
        tables=tables,
        table_mapping=config.get('table_mapping')
    )

    print("\n" + "=" * 80)
    changes, can_auto_migrate = orchestrator.analyze()

    print("\n" + "=" * 80)
    report = orchestrator.generate_report(changes)
    print(report)

    # Save files with prefix to results folder
    report_file = f"results/{prefix}migration_report.txt"
    os.makedirs(os.path.dirname(report_file), exist_ok=True)
    with open(report_file, "w") as f:
        f.write(report)
    print(f"\n📄 Report saved to: {report_file}")

    changes_json = [
        {
            'type': change.change_type.value,
            'table': change.table,
            'description': change.description,
            'old_column': change.old_column,
            'new_column': change.new_column,
            'auto_action': change.auto_action,
            'requires_manual': change.requires_manual
        }
        for change in changes
    ]

    json_file = f"results/{prefix}migration_changes.json"
    with open(json_file, "w") as f:
        json.dump(changes_json, f, indent=2)
    print(f"📄 Detailed changes saved to: {json_file}")

