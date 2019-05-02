# -*- coding: utf-8 -*-

import requests
from simple_salesforce import Salesforce
import numpy as np
import pandas as pd
import os
import datetime
import time
#import simple_salesforce + sf instance + response with access token (could be func)


def get_parent_name(df, id_value):
    try:
        item = df.loc[id_value]
    except KeyError:
        print('Can\'t find {0}'.format(id_value))
        return None
    try:
        if item['ParentId'] == None:
            return item['Name']
        else:
            return get_parent_name(df, item['ParentId'])
    except:
        return ''
    

def create_report(start_date, end_date):
    date_format = '%Y-%m-%d'
    
    print('Running create report procedure...')
    
    if not (isinstance(start_date, datetime.date) and isinstance(end_date, datetime.date)):
        raise TypeError('Interval arguments must be of datetime.date type')
    if end_date < start_date:
        raise ValueError('End date is less than start date.')
    
    sf = teva_salesforce.sf_instance()[0]
    q = sf.query_all
    accounts = q('SELECT Id, Name, Account_Identifier_vod__c, ParentId, External_ID_vod__c, xR1_Account_Type__c FROM Account WHERE xR1_Account_Type__c IN (\'Pharmacy\', \'Distributor\', \'Pharmacy chain\') AND xR1_Account_Status__c=\'Active\'')
    accounts = accounts['records']
    acc = pd.DataFrame(accounts).set_index('Id')
    print('Number of fetched active pharmacy accounts: {0}'.format(len(accounts)))
    
    addresses = q('SELECT Account_vod__c, Address_line_2_vod__c, City_vod__c FROM Address_vod__c WHERE Primary_vod__c=true')
    addresses = addresses['records']
    addr = pd.DataFrame(addresses).set_index('Account_vod__c')
    addr['Address_line_2_vod__c'] = addr['Address_line_2_vod__c'].str.partition(',')[[0]]
    print('Number of fetched adresses: {0}'.format(len(addresses)))
    
    acc = acc.merge(addr, how='left', left_index=True, right_index=True)
    acc = acc[['Name', 'Account_Identifier_vod__c', 'ParentId', 'External_ID_vod__c', 'Address_line_2_vod__c', 'City_vod__c', 'xR1_Account_Type__c']]
    
    # Add parent name column, keep corporation name existing
    acc['MainParentName'] = [get_parent_name(acc, x) for x in acc.index.values]
    print('Getting parent name...', end='')
    acc['ParentName'] = acc.merge(acc, how='left', left_on='ParentId', right_index=True)['Name_y'].fillna('')
    print('done')
    acc = acc.loc[acc['xR1_Account_Type__c']=='Pharmacy', ('Name', 'Account_Identifier_vod__c', 'External_ID_vod__c', 'Address_line_2_vod__c', 'City_vod__c', 'MainParentName', 'ParentName')]
    inventory_monitorings = q('SELECT Account_vod__c, Call2_vod__c, CreatedDate, Id FROM Inventory_Monitoring_vod__c WHERE CreatedDate >= ' + start_date.strftime(date_format) + 'T00:00:00Z AND CreatedDate < ' + (end_date + datetime.timedelta(days=1)).strftime(date_format) + 'T00:00:00Z')
    inventory_monitorings = inventory_monitorings['records']
    
    im = pd.DataFrame(inventory_monitorings).set_index('Id')[['Account_vod__c', 'CreatedDate']].drop_duplicates()
    im['NumOfPhotos'] = 0
    print('Number of fetched inventory monitorings: {0}'.format(im.shape[0]))
    

    length = im.shape[0]
    im_tmp = '\'' + im.index.values + '\''
    
    im['NumOfPhotos'] = 0
    print('Getting IM Ids...')
    for i in range(1, im.shape[0] // 100 + 2):
        start = 100*(i-1)
        end = 100 * i
        if end > im.shape[0]:
            end = im.shape[0]
        records = q('SELECT ParentId, COUNT(Id) NumOfPhotos FROM Attachment WHERE ContentType=\'image/jpeg\' AND ParentId IN (' + ','.join(im_tmp[start:end]) + ') GROUP BY ParentId')
        records = pd.DataFrame(records['records'])
        
        if records.shape[0] > 0:
            records = records.set_index('ParentId')['NumOfPhotos']

            im.loc[im.index.isin(records.index), ('NumOfPhotos',)] = records
        print('Fetched {0} of {1} inventory monitoring attachement(s) data.'.format(end, length))

    im = im.groupby('Account_vod__c').sum()
    acc = acc.merge(im, how='left', left_index=True, right_index=True)
    acc['NumOfPhotos'].fillna(0, inplace=True)
    acc['HasPhotos'] = ['Yes' if x > 0 else 'No' for x in acc['NumOfPhotos']]
    return acc

if __name__ == '__main__':
    df = create_report(datetime.date(2019, 4, 1), datetime.date(2019, 4, 25))
    df.to_excel('photos_report.xlsx')
