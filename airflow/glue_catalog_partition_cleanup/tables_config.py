# Configuration for tables to be processed
# Each table should specify:
# - database: Glue database name (required)
# - table: Glue table name (required)
# - retention_days: Manual retention in days (optional)

TABLES_CONFIG = [
    # Example with manual retention 
    # {
    #     'database': 'stg',
    #     'table': 'events',
    #     'retention_days': 30  # (optional)
    # },
]

