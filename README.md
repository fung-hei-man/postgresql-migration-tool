# Database Migration Tool

An automated PostgreSQL database migration tool that intelligently compares schemas, detects changes, and migrates data with minimal manual intervention.

## Features

- ✅ **Read-only schema analysis** - Safely analyze differences without modifying databases
- 🔄 **Automatic change detection** - Identifies renamed columns, type conversions, and structural changes
- 🤖 **Smart automation** - Handles safe migrations automatically
- 🛠️ **Manual resolution support** - Provides templates for resolving breaking changes
- 🔒 **Safe execution** - Dry run mode by default with batch processing and rollback support
- 📊 **Detailed reporting** - Comprehensive reports on changes and migration progress

## Prerequisites

- Python 3.7+
- PostgreSQL database access (read-only for analysis, write access for migration)
- Required Python package: `psycopg2-binary`

```bash
pip install psycopg2-binary
```

## Project Structure

```
/scripts
  ├── db_migration_tool.py       # Schema analysis tool
  ├── manual_resolutions.py      # Breaking change resolution generator
  └── data_migrator.py            # Data migration executor

/config
  └── migration-config.json       # Database connection configuration

/results
  ├── *_migration_report.txt      # Human-readable analysis reports
  ├── *_migration_changes.json    # Machine-readable change details
  ├── *_manual_resolutions.json   # Manual resolution configurations
  └── *_migration_result_*.json   # Migration execution results
```

## Quick Start

### 1. Configure Database Connections

Create `config/migration-config.json`:

```json
{
  "old_database": {
    "host": "old-server.postgres.database.azure.com",
    "port": 5432,
    "database": "mydb",
    "username": "readonly_user",
    "password": "your_password",
    "schema": "public",
    "sslmode": "prefer"
  },
  "new_database": {
    "host": "new-server.postgres.database.azure.com",
    "port": 5432,
    "database": "mydb",
    "username": "readwrite_user",
    "password": "your_password",
    "schema": "public",
    "sslmode": "prefer"
  },
  "tables": ["parent_table", "child_table", "orders"]
}
```

**Configuration Options:**

- **tables**: (Optional) Specific tables to analyze/migrate. **Order matters for foreign key constraints** - list parent tables before child tables. Omit to scan all tables in alphabetical order.
- **sslmode**: `disable`, `allow`, `prefer` (default), `require`
- **schema**: PostgreSQL schema name (e.g., `public`, `dbo`)

### 2. Analyze Schema Differences

```bash
python scripts/db_migration_tool.py config/migration-config.json --prefix prod
```

**Output:**
- `results/prod_migration_report.txt` - Human-readable analysis
- `results/prod_migration_changes.json` - Detailed change catalog

**Example Report:**
```
================================================================================
DATABASE MIGRATION ANALYSIS REPORT
================================================================================
Total Changes: 8
  - Safe (auto-handled): 2
  - Transformable (auto-converted): 3
  - Breaking (manual required): 3

SAFE CHANGES (Can be handled automatically):
  [users] Column 'created_at' only exists in new DB
    → Action: Default in new DB: CURRENT_TIMESTAMP

TRANSFORMABLE CHANGES (Can be converted automatically):
  [users] Column 'user_name' likely renamed to 'username' in new DB
    → Action: Rename column

BREAKING CHANGES (MANUAL INTERVENTION REQUIRED):
  [users] Column 'email' type mismatch: current(varchar) vs new(text)
    ⚠ Manual action required!
```

### 3. Handle Breaking Changes (If Any)

If breaking changes are detected:

#### Generate Resolution Template

```bash
python scripts/manual_resolutions.py generate results/prod_migration_changes.json --prefix prod
```

**Output:** `results/prod_manual_resolutions.json`

#### Edit Resolutions

Edit the generated file to specify how to handle each breaking change:

```json
{
  "resolutions": {
    "users": [
      {
        "description": "Column 'user_name' likely renamed to 'username'",
        "old_column": "user_name",
        "new_column": "username",
        "action": "rename"
      },
      {
        "description": "Column 'status' type changed varchar to int",
        "old_column": "status",
        "new_column": "status_id",
        "action": "transform",
        "value_mapping": {
          "ACTIVE": 1,
          "INACTIVE": 2,
          "PENDING": 3
        },
        "default_value": 0
      },
      {
        "description": "Column 'category' needs lookup from reference table",
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
      },
      {
        "description": "New NOT NULL column 'created_at' without default",
        "new_column": "created_at",
        "action": "default",
        "default_value": "2024-01-01"
      },
      {
        "description": "Column 'legacy_field' removed",
        "old_column": "legacy_field",
        "action": "ignore"
      }
    ]
  }
}
```

**Available Actions:**
- `rename` - Map old column to new column
- `transform` - Apply value transformation:
  - **value_mapping**: Direct key-value mapping (e.g., "ABC" → 1)
  - **lookup_table**: Lookup from reference table in new database
  - **default_value**: Fallback if mapping/lookup fails
- `default` - Provide default value for new required columns
- `ignore` - Skip column (data will be lost)
- `drop_table` - Exclude table from migration

#### Validate Resolutions

```bash
python scripts/manual_resolutions.py validate results/prod_manual_resolutions.json results/prod_migration_changes.json
```

### 4. Execute Data Migration

#### Dry Run (Recommended First)

```bash
python scripts/data_migrator.py config/migration-config.json --prefix prod
```

With resolutions (if breaking changes exist):
```bash
python scripts/data_migrator.py config/migration-config.json --prefix prod --resolutions results/prod_manual_resolutions.json
```

**Dry Run Output:**
```
================================================================================
DRY RUN - DATA MIGRATION
================================================================================
⚠ Running in DRY RUN mode - no data will be written

Tables to migrate: 3
Batch size: 1000

[DRY RUN] Migrating table: users
  Old columns: 5, New columns: 6
  Total rows to migrate: 15234
  ✓ Processed: 15234, Success: 15234, Failed: 0

================================================================================
MIGRATION SUMMARY
================================================================================
Tables processed: 3
Total rows migrated: 50000
Total rows failed: 0
Duration: 1.23 seconds
```

#### Live Migration

After verifying dry run results:

```bash
python scripts/data_migrator.py config/migration-config.json --prefix prod --resolutions results/prod_manual_resolutions.json --live
```

**Additional Options:**
```bash
# Custom batch size (default: 1000)
python scripts/data_migrator.py config/migration-config.json --live --batch-size 5000

# Specify custom changes file
python scripts/data_migrator.py config/migration-config.json --changes-file results/custom_changes.json --live
```

## Workflow Examples

### Scenario 1: Identical Schemas (Simple Data Copy)

```bash
# 1. Analyze (will show no changes)
python scripts/db_migration_tool.py config/migration-config.json --prefix prod

# 2. Dry run
python scripts/data_migrator.py config/migration-config.json --prefix prod

# 3. Live migration
python scripts/data_migrator.py config/migration-config.json --prefix prod --live
```

### Scenario 2: Schema with Breaking Changes

```bash
# 1. Analyze
python scripts/db_migration_tool.py config/migration-config.json --prefix prod

# 2. Generate resolution template
python scripts/manual_resolutions.py generate results/prod_migration_changes.json --prefix prod

# 3. Edit results/prod_manual_resolutions.json to specify fixes

# 4. Validate resolutions
python scripts/manual_resolutions.py validate results/prod_manual_resolutions.json results/prod_migration_changes.json

# 5. Dry run with resolutions
python scripts/data_migrator.py config/migration-config.json --prefix prod --resolutions results/prod_manual_resolutions.json

# 6. Live migration
python scripts/data_migrator.py config/migration-config.json --prefix prod --resolutions results/prod_manual_resolutions.json --live
```

### Scenario 3: Multiple Environments

```bash
# Development environment
python scripts/db_migration_tool.py config/dev-config.json --prefix dev
python scripts/data_migrator.py config/dev-config.json --prefix dev --live

# Production environment
python scripts/db_migration_tool.py config/prod-config.json --prefix prod
python scripts/data_migrator.py config/prod-config.json --prefix prod --live
```

## Advanced Features

### Foreign Key Constraint Handling

**Problem:** Child tables with foreign keys fail if parent table isn't migrated first.

**Solution:** Specify table order in config:

```json
{
  "tables": [
    "users",           // Parent table first
    "orders",          // Child table references users
    "order_items"      // Child table references orders
  ]
}
```

The tool respects this order during migration to maintain referential integrity.

### Value Mapping Transformations

**Problem:** Column type changed from `varchar` to `int` with categorical mappings.

**Solution:** Use `value_mapping` in resolutions:

```json
{
  "old_column": "status",
  "new_column": "status_id",
  "action": "transform",
  "value_mapping": {
    "ACTIVE": 1,
    "INACTIVE": 2,
    "PENDING": 3,
    "CANCELLED": 4
  },
  "default_value": 0
}
```

- Maps old string values to new integer IDs
- `default_value` used if value not in mapping
- Fails migration if no default and unmapped value found

### Database Lookup Transformations

**Problem:** Old column is `varchar`, new column is `int` referencing a lookup table.

**Example:** `products.category_name` (varchar) → `products.category_id` (int) where `categories` table has `(name, id)` mapping.

**Solution:** Use `lookup_table` in resolutions:

```json
{
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
```

**How it works:**
1. Tool loads entire lookup table into memory once
2. For each row, looks up old value to get new value
3. Uses `default_value` if lookup fails
4. Efficient for large datasets (cached lookups)

**Requirements:**
- Lookup table must exist in NEW database before migration
- Lookup table must be populated with all expected values

**Alternative for complex cases:**
If lookup logic is too complex, use placeholder approach:
```json
{
  "new_column": "category_id",
  "action": "default",
  "default_value": -1
}
```
Then run a SQL script after migration:
```sql
UPDATE products p
SET category_id = c.id
FROM categories c
WHERE p.category_name = c.name;
```

### Safe Changes (Auto-handled)
- New nullable columns → Inserts NULL
- New columns with defaults → Uses database default
- New tables → Creates empty tables

### Transformable Changes (Auto-converted)
- Column renames (detected via similarity) → Maps data automatically
- Safe type conversions (e.g., INT→BIGINT) → Automatic casting
- VARCHAR expansions → Direct copy

### Breaking Changes (Manual intervention required)
- Incompatible type changes → Need custom transformation
- New NOT NULL columns without defaults → Need default value
- Removed columns → Need confirmation to ignore
- VARCHAR→VARCHAR with size reduction → Potential data loss

## Automatic Type Conversions

The tool automatically handles these PostgreSQL type conversions:

| From | To |
|------|-----|
| SMALLINT | INTEGER, BIGINT, NUMERIC, REAL, DOUBLE PRECISION, TEXT |
| INTEGER | BIGINT, NUMERIC, REAL, DOUBLE PRECISION, TEXT |
| BIGINT | NUMERIC, TEXT |
| REAL | DOUBLE PRECISION, NUMERIC, TEXT |
| VARCHAR | TEXT |
| DATE | TIMESTAMP, TEXT |
| BOOLEAN | TEXT |

## Best Practices

### Before Migration

1. ✅ **Backup both databases** - Always have a rollback plan
2. ✅ **Test on non-production first** - Validate with dev/staging environment
3. ✅ **Review the analysis report** - Understand all detected changes
4. ✅ **Run dry run multiple times** - Verify expected behavior
5. ✅ **Check database constraints** - Ensure foreign keys and triggers are compatible

### During Migration

1. ✅ **Monitor progress** - Watch for errors in real-time
2. ✅ **Use appropriate batch size** - Balance between performance and rollback granularity
3. ✅ **Check first table carefully** - Verify data accuracy before continuing
4. ✅ **Keep old database online** - Don't delete until migration is verified

### After Migration

1. ✅ **Verify row counts** - Compare totals between old and new databases
2. ✅ **Spot check data** - Sample records for accuracy
3. ✅ **Test application** - Ensure apps work with new database
4. ✅ **Update indexes and constraints** - Recreate as needed
5. ✅ **Keep migration logs** - Save all result files for reference

## Troubleshooting

### Connection Issues

**Error:** `could not translate host name`
- Verify hostname is correct
- Check DNS resolution: `nslookup your-server.postgres.database.azure.com`
- Ensure you're on correct network/VPN if using private endpoint

**Error:** `server does not support SSL`
- Set `"sslmode": "disable"` in config for non-SSL servers

**Error:** `authentication failed`
- Verify username format (Azure uses `username@servername`)
- Check password is correct
- Ensure user has required permissions

### Migration Issues

**Error:** `Table not found in schema`
- Verify schema name is correct (case-sensitive)
- Check user has SELECT permission on the schema

**Error:** `Breaking changes detected`
- Generate and fill out manual resolutions file
- Use `--resolutions` flag when running migrator

**No tables to migrate**
- For identical schemas, this is now fixed - tool will migrate all tables
- If issue persists, check that tables exist in old database

### Performance Issues

**Migration is slow**
- Increase batch size: `--batch-size 5000`
- Check network latency between databases
- Consider creating indexes after migration instead of before

## Output Files Reference

| File | Purpose | When Created |
|------|---------|--------------|
| `*_migration_report.txt` | Human-readable analysis summary | After schema analysis |
| `*_migration_changes.json` | Detailed change catalog | After schema analysis |
| `*_manual_resolutions.json` | Breaking change resolutions | When breaking changes exist |
| `*_migration_result_dryrun_*.json` | Dry run execution results | After dry run |
| `*_migration_result_live_*.json` | Live migration results | After live migration |

## Security Considerations

1. **Never commit passwords** - Use environment variables or secure vaults
2. **Use read-only accounts** for schema analysis
3. **Restrict write access** to migration account
4. **Review generated SQL** in resolutions before execution
5. **Audit migration logs** for compliance

## Limitations

- Currently supports PostgreSQL only
- Text/JSON data only (no binary file uploads)
- Does not handle:
  - Stored procedures
  - Triggers
  - Views (only BASE TABLEs)
  - Sequences (except auto-increment columns)
  - Partitioned tables (migrates as regular tables)

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Review migration logs in `/results`
3. Verify database permissions and connectivity
4. Test with minimal tables first to isolate issues

## License

[Add your license information here]

---

**Version:** 1.0  
**Last Updated:** January 2025