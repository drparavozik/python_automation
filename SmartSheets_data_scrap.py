# -*- coding: utf-8 -*-

import smartsheet 
import pandas as pd
import numpy as np
import pyodbc 

def get_db_connection():
    conn = pyodbc.connect(
        #pyodbc connection
        )
    
    
    return conn

token = #token
ss_client = smartsheet.Smartsheet(token)
sheets_response = ss_client.Sheets.list_sheets(include="attachments,source", include_all=True)
sheets = sheets_response.data

sheet_id = 0

for single_sheet in sheets:
    if single_sheet.name == 'Анкетні Дані':
        sheet_id = single_sheet.id
    
columns_response = ss_client.Sheets.get_columns(sheet_id, include_all=True)
columns = columns_response.data

column_dict = {}

for column in columns:
    column_dict[str(column.id)] = column.title
    
get_sheet_response = ss_client.Sheets.get_sheet(sheet_id)    

client_db = []
for row in get_sheet_response.rows:
    partition = {}
    partition['_ROWID_'] = row.id
    for cell in row.cells:
        partition[cell.column_id] = cell.value
    client_db.append(partition)
    
df = pd.DataFrame(client_db)
df = df.fillna('NULL')

columns = list(df.columns)
df = df[columns]
#columns.sort()
columns_string = ''
last = len(columns)
current = 1
for i in columns:
    if current == 1: 
        columns_string += '(' + '"' + str(i) + '"' + ','
        current += 1
    elif current != last:
        columns_string += '"' + str(i) + '"' + ','
        current += 1
    else:
        columns_string += '"' + str(i) + '"' + ')'
    
query_header = ' '.join(['INSERT INTO sfe.SmartsheetProfiles ' + columns_string + ' VALUES'])
query_values = ''
recordsCount = 50
float_columns = [7239395364366212, 1328420853442436, 8365295271208836, 3861695643838340, 1046945876731780, 5550545504102276, 2172845783574404, 7802345317787524]
cur = get_db_connection().cursor()
cur.execute('TRUNCATE TABLE sfe.SmartsheetProfiles')
cur.commit()

for index, row in df.iterrows():
    values = []
    for col in columns:
        if col in float_columns:
            if row[col] == 'NULL': 
                values.append('NULL')
            else:
                values.append(row[col])
        else:
            if row[col] == 'NULL': 
                values.append('NULL')
            else:
                values.append('N' + '\'' + str(row[col]).replace('\'','').replace('\n',' ') + '\'')
    print(values)
    

    if recordsCount > 0:
        if len(query_values) == 0:
            query_values = "(" + ", ".join(str(x) for x in values) + ")"
        else:
            query_values += "," + "(" + ", ".join(str(x) for x in values) + ")"
    
    recordsCount -= 1
    
    if recordsCount == 0:
        cur.execute('{header} {values}'.format(header = query_header,  values = query_values))
        cur.commit()
        query_values = ""
        recordsCount = 50
    


