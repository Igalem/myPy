#!/usr/bin/python
#
# ---- Counting total rows in tables under Snowflake schema ----
# 
# Count rows in table name under Snowflake schema names
#
# Version : 1.0v
# 
#
import snowflake.connector
import sys
 
# Gets the version
sqlSnowflake = snowflake.connector.connect(
user=os.environ.get('P_SNOWFLAKE_USERNAME'),
password=os.environ.get('P_SNOWFLAKE_PWD'),
account=os.environ.get('P_SNOWFLAKE_ACCOUNT'),
region=os.environ.get('P_SNOWFLAKE_REGION'),
database=os.environ.get('P_SNOWFLAKE_DB'),
warehouse=os.environ.get('P_SNOWFLAKE_WH'),
schema=os.environ.get('P_SNOWFLAKE_SCHEMA')
)

print('\033c')
print('\n        ***** Counting total rows in tables under Snowflake schema ***** \n')

table_schema = input('Enter schema name: ')

SQLselect = "SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = '" + table_schema + "' And table_type != 'VIEW'" # LIMIT 10"

try:
	sqlSnowflake = sqlSnowflake.cursor()
except:
	print('Unable to connect into Snowflake account')
	sqlSnowflake.close()
	exit()

print('\n- Searching all tables under schema name: %s ' % (table_schema))
sqlSnowflake.execute(SQLselect)
data = sqlSnowflake.fetchall()

print('- Total tables found: ' + str(len(data)) + '\n')

if len(data) == 0:
	print(str(len(data)) + ' tables found under scehma name: ' + table_schema + '\n')
	sqlSnowflake.close()
	exit()

SQLstmt = []
num = 1

for row in data:
	table_name=row[0]
	
	fetchSQLstmt="Select count(*) from " + table_schema + '.' + str(table_name)
	count_exe = sqlSnowflake.execute(fetchSQLstmt).fetchall()[0][0]
	
	if num == 1:
		print('Table names result: \n')
		fieldMaxLen = (max([len(n[0]) for n in data])) + 1
		print('| id | Table name' + ' ' * (fieldMaxLen - 10) + '| Total rows|')
		print('-' * (fieldMaxLen + len('| Total rows | ') + len('| id ')) ) 
		print('| ' + str(num) + ' | ' + table_name + ' ' * (fieldMaxLen - len(table_name)) + 
			' | ' + str(count_exe) + ' ' * (len('Total rows') - len(str(count_exe))) + '|')
		num+=1
	else:
		print('| ' + str(num) + ' | ' + table_name + ' ' * (fieldMaxLen - len(table_name)) + 
			' | ' + str(count_exe) + ' ' * (len('Total rows') - len(str(count_exe))) + '|')
		num+=1
		
	
sqlSnowflake.close()