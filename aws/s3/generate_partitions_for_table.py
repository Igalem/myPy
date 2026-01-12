from datetime import date, timedelta

table = "stg.test_population"
hour_value = "00"          # change if you use a different hour for DAILY
location2_root = "s3://staging/population"

start = date(2024, 11, 1)  # 2025-11-15 - 6 months
end   = date(2025, 11, 24)
special = date(2025, 11, 25)

# 1) Partitions from location1 (no LOCATION clause)
parts_loc1 = []
d = start
while d <= end:
    y, m, day = d.strftime("%Y"), d.strftime("%m"), d.strftime("%d")
    parts_loc1.append(
        f"PARTITION (tile_granularity='DAILY', year='{y}', month='{m}', day='{day}', hour='{hour_value}')"
    )
    d += timedelta(days=1)

sql_loc1 = f"ALTER TABLE {table} ADD IF NOT EXISTS\n" + "\n".join(parts_loc1) + ";\n"

# 2) Special partition for 2025-11-16 from location2
y, m, day = special.strftime("%Y"), special.strftime("%m"), special.strftime("%d")
path = f"{location2_root}/year={y}/month={m}/day={day}/hour={hour_value}/"

sql_loc2 = f"""ALTER TABLE {table} ADD IF NOT EXISTS
PARTITION (tile_granularity='DAILY', year='{y}', month='{m}', day='{day}', hour='{hour_value}')
  LOCATION '{path}';"""

print(sql_loc1)
print()
print(sql_loc2)
