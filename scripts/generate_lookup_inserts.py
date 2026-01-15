#!/usr/bin/env python3
"""
Generate SQL INSERT statements for missing lookup table values.
Parses migration result files and extracts missing values from lookup table errors.
"""

import json
import re
import sys
from datetime import datetime
from typing import Dict, List, Set


def parse_lookup_errors(errors: List[str]) -> Dict[str, Set[str]]:
    """
    Parse error messages and extract missing values grouped by target table.

    Error format: "Row XXXX: Value 'xxx' not found in lookup table for column 'yyy' -> 'zzz'"

    Returns: Dict mapping table_name -> set of missing values
    """
    # Pattern to match: Value 'xxx' not found in lookup table for column 'yyy' -> 'zzz'
    pattern = r"Value '(.+?)' not found in lookup table for column '(.+?)' -> '(.+?)'"

    missing_values: Dict[str, Set[str]] = {}

    for error in errors:
        match = re.search(pattern, error)
        if match:
            value = match.group(1)
            source_column = match.group(2)
            target_table = match.group(3)

            if target_table not in missing_values:
                missing_values[target_table] = set()

            missing_values[target_table].add(value)

    return missing_values


def generate_sql_inserts(missing_values: Dict[str, Set[str]],
                         column_name: str = None) -> str:
    """
    Generate SQL INSERT statements for missing lookup values.

    Args:
        missing_values: Dict mapping table_name -> set of missing values
        column_name: If provided, use this as the column name;
                     otherwise assume column name = table name

    Returns: SQL string with INSERT statements
    """
    sql_lines = []
    sql_lines.append("-- Auto-generated SQL to insert missing lookup table values")
    sql_lines.append(f"-- Generated at: {datetime.now().isoformat()}")
    sql_lines.append("")

    for table_name, values in sorted(missing_values.items()):
        col_name = column_name if column_name else table_name

        sql_lines.append(f"-- Missing values for table: {table_name}")
        sql_lines.append(f"-- Total missing: {len(values)}")
        sql_lines.append("")

        # Sort values for consistent output
        sorted_values = sorted(values)

        for value in sorted_values:
            # Escape single quotes in value
            escaped_value = value.replace("'", "''")
            sql_lines.append(f"INSERT INTO {table_name} ({col_name}) VALUES ('{escaped_value}');")

        sql_lines.append("")

    return "\n".join(sql_lines)


def generate_sql_inserts_batch(missing_values: Dict[str, Set[str]],
                               column_name: str = None,
                               batch_size: int = 100) -> str:
    """
    Generate batched SQL INSERT statements for better performance.

    Args:
        missing_values: Dict mapping table_name -> set of missing values
        column_name: If provided, use this as the column name
        batch_size: Number of values per INSERT statement

    Returns: SQL string with batched INSERT statements
    """
    sql_lines = []
    sql_lines.append("-- Auto-generated SQL to insert missing lookup table values (batched)")
    sql_lines.append(f"-- Generated at: {datetime.now().isoformat()}")
    sql_lines.append("")

    for table_name, values in sorted(missing_values.items()):
        col_name = column_name if column_name else table_name

        sql_lines.append(f"-- Missing values for table: {table_name}")
        sql_lines.append(f"-- Total missing: {len(values)}")
        sql_lines.append("")

        sorted_values = sorted(values)

        # Process in batches
        for i in range(0, len(sorted_values), batch_size):
            batch = sorted_values[i:i + batch_size]

            # Escape and format values
            value_list = ", ".join(
                f"('{v.replace(chr(39), chr(39)+chr(39))}')"
                for v in batch
            )

            sql_lines.append(f"INSERT INTO {table_name} ({col_name}) VALUES")
            sql_lines.append(f"  {value_list};")
            sql_lines.append("")

    return "\n".join(sql_lines)


def process_migration_result(result_file: str, output_file: str = None,
                             batched: bool = False, batch_size: int = 100) -> None:
    """
    Process a migration result file and generate SQL for missing lookup values.

    Args:
        result_file: Path to the migration result JSON file
        output_file: Path for output SQL file (optional, will print to stdout if not provided)
        batched: Whether to generate batched INSERT statements
        batch_size: Batch size for batched inserts
    """
    # Load migration result
    try:
        with open(result_file, 'r') as f:
            result = json.load(f)
    except FileNotFoundError:
        print(f"❌ Result file '{result_file}' not found", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON in '{result_file}': {e}", file=sys.stderr)
        sys.exit(1)

    # Get errors from stats section (contains all errors)
    errors = result.get('stats', {}).get('errors', [])

    if not errors:
        print("✅ No errors found in migration result - no SQL needed!")
        return

    # Parse lookup errors
    missing_values = parse_lookup_errors(errors)

    if not missing_values:
        print("✅ No lookup table errors found - no SQL needed!")
        return

    # Generate summary
    total_missing = sum(len(v) for v in missing_values.values())
    print(f"📊 Found {total_missing} unique missing values across {len(missing_values)} lookup table(s):")
    for table, values in sorted(missing_values.items()):
        print(f"   - {table}: {len(values)} missing values")
    print()

    # Generate SQL
    if batched:
        sql = generate_sql_inserts_batch(missing_values, batch_size=batch_size)
    else:
        sql = generate_sql_inserts(missing_values)

    # Output
    if output_file:
        with open(output_file, 'w') as f:
            f.write(sql)
        print(f"✅ SQL written to: {output_file}")
    else:
        print("Generated SQL:")
        print("-" * 60)
        print(sql)


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate SQL INSERT statements for missing lookup table values"
    )
    parser.add_argument(
        "result_file",
        help="Path to migration result JSON file"
    )
    parser.add_argument(
        "-o", "--output",
        help="Output SQL file path (prints to stdout if not specified)"
    )
    parser.add_argument(
        "-b", "--batched",
        action="store_true",
        help="Generate batched INSERT statements for better performance"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Batch size for batched inserts (default: 100)"
    )

    args = parser.parse_args()

    process_migration_result(
        result_file=args.result_file,
        output_file=args.output,
        batched=args.batched,
        batch_size=args.batch_size
    )


if __name__ == "__main__":
    main()

