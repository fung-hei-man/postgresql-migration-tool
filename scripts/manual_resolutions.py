#!/usr/bin/env python3
"""
Manual Resolutions Config Generator
Helps resolve breaking changes by creating a resolution config file.
"""

import json
import sys


def generate_resolution_template(changes_file: str, output_file: str):
    """Generate template for manual resolutions based on breaking changes"""

    try:
        with open(changes_file, 'r') as f:
            changes = json.load(f)
    except FileNotFoundError:
        print(f"❌ Changes file '{changes_file}' not found")
        sys.exit(1)

    breaking = [c for c in changes if c.get('requires_manual', False)]

    if not breaking:
        print("✅ No breaking changes found - no manual resolutions needed!")
        return

    print(f"Found {len(breaking)} breaking changes that need resolution:\n")

    resolutions = {
        "_instructions": {
            "description": "Specify how to resolve each breaking change",
            "actions": {
                "rename": "Map old column to new column name",
                "transform": "Apply custom transformation to data",
                "default": "Use a default value for new required columns",
                "ignore": "Skip this column (data will be lost)",
                "drop_table": "Confirm table should not be migrated"
            },
            "transform_options": {
                "value_mapping": "Direct value mapping (e.g., 'ABC' -> 1)",
                "lookup_table": "Lookup from reference table in new DB",
                "default_value": "Fallback value if mapping/lookup fails"
            },
            "examples": {
                "value_mapping": {
                    "old_column": "status_code",
                    "new_column": "status_id",
                    "action": "transform",
                    "value_mapping": {"ACTIVE": 1, "INACTIVE": 2, "PENDING": 3},
                    "default_value": 0
                },
                "lookup_table": {
                    "old_column": "category_name",
                    "new_column": "category_id",
                    "action": "transform",
                    "lookup_table": {
                        "table": "categories",
                        "key_column": "name",
                        "value_column": "id",
                        "schema": "public"
                    },
                    "default_value": -1
                }
            }
        },
        "resolutions": {}
    }

    for i, change in enumerate(breaking, 1):
        table = change['table']
        desc = change['description']

        print(f"{i}. [{table}] {desc}")

        if table not in resolutions['resolutions']:
            resolutions['resolutions'][table] = []

        resolution = {
            "description": desc,
            "action": "SPECIFY_ACTION",
        }

        if change.get('old_column'):
            resolution["old_column"] = change['old_column']

        if change.get('new_column'):
            resolution["new_column"] = change['new_column']
        elif "renamed to" in desc:
            resolution["new_column"] = "SPECIFY_NEW_COLUMN_NAME"

        if "renamed" in desc.lower() or "likely renamed" in desc.lower():
            resolution["action"] = "rename"
            resolution["_suggestion"] = "Specify correct new_column name"
        elif "type mismatch" in desc.lower() or "incompatible type" in desc.lower():
            resolution["action"] = "transform"
            resolution["transform_sql"] = "CAST({old_column} AS new_type)"
            resolution["_suggestion"] = "Provide SQL expression to convert data"
        elif "is not null in new db" in desc.lower() and "with default:" in desc.lower():
            # New NOT NULL column with schema default
            resolution["action"] = "default"
            # Extract default value from description
            default_part = desc.split("with default:")[-1].strip()
            resolution["default_value"] = "USE_SCHEMA_DEFAULT"
            resolution["schema_default"] = default_part
            resolution["_suggestion"] = f"Schema has default: {default_part}. Set 'default_value' to use a custom value, or remove this resolution to let DB apply its default"
        elif "is not null in new db" in desc.lower() and "without default" in desc.lower():
            # New NOT NULL column without default - MUST provide value
            resolution["action"] = "default"
            resolution["default_value"] = "SPECIFY_DEFAULT_VALUE"
            resolution["_suggestion"] = "REQUIRED: Provide default value for NOT NULL column without schema default"
        elif "exists in current db but not in new db" in desc.lower():
            # Column removed in new schema
            if "table" in desc.lower():
                resolution["action"] = "drop_table"
                resolution["_suggestion"] = "Confirm table should not be migrated"
            else:
                resolution["action"] = "ignore"
                resolution["_suggestion"] = "Confirm data loss is acceptable"
        elif "table" in desc.lower() and "not in new db" in desc.lower():
            resolution["action"] = "drop_table"
            resolution["_suggestion"] = "Confirm table should not be migrated"

        resolutions['resolutions'][table].append(resolution)

    with open(output_file, 'w') as f:
        json.dump(resolutions, f, indent=2)

    print(f"\n📄 Resolution template saved to: {output_file}")
    print(f"\nEdit this file to specify how to resolve each breaking change, then run:")
    print(f"python scripts/data_migrator.py <config> --changes-file {changes_file} --resolutions {output_file} --live")


def validate_resolutions(resolutions_file: str, changes_file: str) -> bool:
    """Validate that all breaking changes have resolutions"""

    try:
        with open(resolutions_file, 'r') as f:
            resolutions = json.load(f)
        with open(changes_file, 'r') as f:
            changes = json.load(f)
    except FileNotFoundError as e:
        print(f"❌ File not found: {e}")
        return False

    breaking = [c for c in changes if c.get('requires_manual', False)]
    resolved_count = sum(len(r) for r in resolutions.get('resolutions', {}).values())

    print(f"Breaking changes: {len(breaking)}")
    print(f"Resolutions provided: {resolved_count}")

    unresolved = []
    for table, table_resolutions in resolutions.get('resolutions', {}).items():
        for res in table_resolutions:
            if res.get('action') == 'SPECIFY_ACTION':
                unresolved.append(f"{table}: {res.get('description', 'Unknown')}")
            elif res.get('action') == 'rename' and res.get('new_column') == 'SPECIFY_NEW_COLUMN_NAME':
                unresolved.append(f"{table}: Need to specify new_column for rename")
            elif res.get('action') == 'default' and res.get('default_value') == 'SPECIFY_DEFAULT_VALUE':
                unresolved.append(f"{table}: Need to specify default_value")

    if unresolved:
        print(f"\n❌ Found {len(unresolved)} unresolved items:")
        for item in unresolved:
            print(f"  - {item}")
        return False

    print("✅ All breaking changes have resolutions specified")
    return True


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage:")
        print("  python scripts/manual_resolutions.py generate <changes.json> [--prefix PREFIX]")
        print("  python scripts/manual_resolutions.py validate <resolutions.json> <changes.json>")
        print("\nExamples:")
        print("  python scripts/manual_resolutions.py generate results/prod_migration_changes.json --prefix prod")
        print(
            "  python scripts/manual_resolutions.py validate results/prod_manual_resolutions.json results/prod_migration_changes.json")
        sys.exit(1)

    command = sys.argv[1]

    if command == "generate":
        changes_file = sys.argv[2]

        # Parse prefix
        prefix = ""
        if '--prefix' in sys.argv:
            idx = sys.argv.index('--prefix')
            if idx + 1 < len(sys.argv):
                prefix = sys.argv[idx + 1] + "_"

        output_file = f"results/{prefix}manual_resolutions.json"
        generate_resolution_template(changes_file, output_file)

    elif command == "validate":
        if len(sys.argv) < 4:
            print("Usage: python scripts/manual_resolutions.py validate <resolutions.json> <changes.json>")
            sys.exit(1)

        resolutions_file = sys.argv[2]
        changes_file = sys.argv[3]
        validate_resolutions(resolutions_file, changes_file)

    else:
        print(f"Unknown command: {command}")
        print("Available commands: generate, validate")
        sys.exit(1)