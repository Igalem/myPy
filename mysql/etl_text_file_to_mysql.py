#!/usr/bin/python
#
#
#   ---- Automation Extract&Load Text file types (csv / txt / TAB del) into MySQL Database ----
#
# Include & Support:
#
#   1) Source file dialog with user for definition extract
#   2) Target table creation (New table / Generated table)
#   3) Row count limitation for ETL data load
#   4) Database Create / Truncate / Insert table (DDL)
#   5) Summary Report
#
# Version : 1.0v

import os
import mysql.connector

## =================================================================> MySQL DB connection details:

mysqlDS = {
						'host' : os.environ.get('P_MYSQL_HOST'),
						'user' : os.environ.get('P_MYSQL_USERNAME'),
						'password' : os.environ.get('P_MYSQL_PWD'),
						'database' : os.environ.get('P_MYSQL_DB')
						}

## =================================================================> User Dialog and inputs:

print('\033c')
print('        ***** Automation ETL process to load Flat file into MySQL ***** \n')

try:
	mydb = mysql.connector.connect(
  host = mysqlDS['host'],
  user = mysqlDS['user'],
  password = mysqlDS['password'],
  database = mysqlDS['database']
	)	
except mysql.connector.Error as e:
  print("      >> Error code:", e.errno)        # error number
  print("      >> SQLSTATE value:", e.sqlstate) # SQLSTATE value
  print("      >> Error message:", e.msg)       # error message
  print("      >> Error:", e)                   # errno, sqlstate, msg values
  s = str(e)
  print("      >> Error:", s)
  exit()                   # errno, sqlstate, msg values

schema = mysqlDS['database']
mycursor = mydb.cursor()
data = []  

print('   | 1) Flat file definition: ')
print('   | ----------------------------------------------------------------------------------- |')

filePath = input('   |- Please insert source file path > ') 
fileName = input('   |- Insert source filename > ') 

if len(filePath) > 0:
	filePath
else:
	filePath = os.path.abspath(os.getcwd()) + '/'

try:
	listDirectory = os.listdir(filePath)
except:
	print('\n- Error: Datasource fileName or path not exists (!) ')
	print('Migration aboarted.  \n')
	exit()
else:
	pass

if fileName not in listDirectory:
	print('\n- Error: fileName or path not exists (!) ')
	print('ETL process aboarted.  \n')
	exit()

## ----------->>> Open source file for reading data:

fileNamePath = filePath + '/' + fileName
openFile = open(fileNamePath, 'r', encoding='utf-8',errors='ignore').read().splitlines()

fileTotalRecords = str(len(openFile))

print('\n   | 2) File definition: ' + fileName + ' (total rows counted:' + fileTotalRecords + ')')
print('   | ----------------------------------------------------------------------------------- |')

header =input('   |- Set first row as header (Y/N) > ').upper() 
if header != 'Y':
	header='N'

fieldDelimiter = input('   |- Insert fields delimiter character ([,] - defualt) > ')
if fieldDelimiter is None or len(fieldDelimiter) == 0:
	fieldDelimiter = ','

fieldTermmination = input('   |- Insert field values delimiter character ([None] - defualt) > ')
if fieldTermmination is None or len(fieldTermmination) == 0:
	fieldTermmination = ''

limit = input('   |- Inser rows limit (0-no limit) > ')
try:
	int(limit)
except:
	limit = 0

limit = int(limit)

if limit>len(openFile) or limit==0:
	if limit!=0:
		print('   |- Warnning: Source file total counted rows (' +str(len(openFile)) + ') is lower then inserted limit value (' + str(limit) + ')')
		print('   |            Setting value to limit=' + str(len(openFile)) + '\n')
	
	limit=len(openFile)

else:
	if header.upper() == 'Y':
		limit+=1	

print('\n   | 3) Target table definition: ' + fileName + ' (total rows counted:' + str(len(openFile)) + ')')
print('   | ----------------------------------------------------------------------------------- |')

createTargetTable = str(input('   |- Create target table ? (Y/N) > '))
if createTargetTable.upper() == 'Y':
	tableName =   str(input('   |-                 Enter table name > ').upper()).\
									  replace('.', '_').replace(' ', '_').replace('(','_').\
									  replace(')','_').replace('-','_').replace('\ufeff', '')

else:
	tableName = fileName.replace('.', '_').replace(' ', '_').replace('(','_').replace(')','_').replace('-','_')

print(' --------------------------------->>>> ' + tableName)

truncateTargetTable = input('   |- Truncate target table (Y/N) > ').upper() 
if truncateTargetTable.upper() != 'Y':
	truncateTargetTable='N'

print('\n   |- Log output details:')

###########################################################

## ----------->>> Generate Header Columns

if header.upper()=='Y':
	startRow=1
	tableHeader = [str(field) + ' varchar(255)' for field in list(openFile[0].\
		replace('(','_').replace(')','_').replace(' ', '_').\
		replace('"', '').replace('.','_').replace("'", '_').\
		replace('%', '_').replace('\ufeff', '').split(','))]

	tableHeaderInsert = [field for field in list(openFile[0].\
		replace('(','_').replace(')','_').replace(' ', '_').\
		replace('"', '').replace('.','_').replace("'", '_').\
		replace('%', '_').replace('\ufeff', '').split(','))]

else:
	startRow=0
	tableHeader = ['field' + str(i+1) + ' varchar(255)' for i in range(len(list(openFile[0].split(','))))]	
	tableHeaderInsert = ['field' + str(i+1) for i in range(len(list(openFile[0].split(','))))]	

## ----------->>> Generate Data by fields

cnt=0
data=[]
val=''
delimier = fieldDelimiter 
rowData = []

for line in openFile[startRow:limit]:
	for l in line:
		if l == '"':
			if cnt==1:
				cnt=0
			else:
				cnt+=1	
		if l != delimier:
			val+=l
		else:
			if cnt ==0:
				if fieldTermmination is not None or len(fieldTermmination) > 0:
					val = val.replace(fieldTermmination, '')						
				rowData.append(val)
				val=''
	rowData.append(val)
	
	data.append(rowData)
	rowData = []

## ----------->>> Complete missing columns by Header structure

headerColCounted = len(tableHeader)

for i in range(len(data)):
	colCounted = len(data[i])
	if colCounted == headerColCounted:
		data[i]
	else:	
		colDelta = headerColCounted - colCounted
		for ii in range(colDelta):
			data[i].insert(len(data[i]), '')
			
dataTotalRecords = str(len(data))	

## =================================================================> SQL Create statment (DLL)

createSQLState = 'Create Table If Not Exists ' + schema + '.' + tableName + ' (\n' + ',\n'.join(tableHeader) + ');' 

###print('\n' + createSQLState )

try:
	mycursor.execute(createSQLState)
except:
	print('      >> Error: failed to create table name:' + schema + '.' + tableName)	
	print('\n', createSQLState)
	print('      >> Process aborted.')
	exit(0)
else:
	print('      >> Target table: ' + schema + '.' + tableName + ' replaced or created.')
	

## =================================================================> Truncate Target table SQL statment (DDL)

if truncateTargetTable.upper() == 'Y':
	truncateSQLState = 'Truncate Table ' + schema + '.' + tableName +';'
	print('      >> ' + truncateSQLState )
	mycursor = mydb.cursor()
	mycursor.execute(truncateSQLState)	

## =================================================================> INSERT INTO sql query statement

values = ['%s' for field in list(openFile[0].split(','))]

insertSQLState='Insert into ' + schema + '.' + tableName + '(' + ',\n'.join(tableHeaderInsert) + ') Values(' + ','.join(values) + ');'

print('      >> Inserting source data file into target table...')

mycursor = mydb.cursor()

try:
	mycursor.executemany(insertSQLState,data)
	
except:
	print('      >> Error: failed to insert data into table name: ' + schema + '.' + tableName)	
	print('\n', insertSQLState)	

	print('>>>>>>>>> ' + str(len(data)))

	for row in data[:10]:
			print(row)
	
	print('      >> Process aborted.')
	exit(0)
else:	
	mydb.commit()

	print('      >> ' + dataTotalRecords + '/' + fileTotalRecords + ' rows loaded successfuly into target table : ' + schema + '.' + tableName)
	print('      >> ' + dataTotalRecords + ' rows commited. ')
	print('\n')
	print('               |                              Summary                                               |')
	print('               | ---------------------------------------------------------------------------------- |')
	print('               |   Source file location: ' + filePath + fileName )
	print('               |   Total record in file: ' + fileTotalRecords)
	print('               |   Header row: ' + header)
	print('               |   ')
	print('               |   Target table: ' + schema + '.' + tableName)
	print('               |   Total Inserted rows: ' + dataTotalRecords)
	print('               | ---------------------------------------------------------------------------------- |')

