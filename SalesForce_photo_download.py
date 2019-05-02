# -*- coding: utf-8 -*-

import requests
import numpy as np
import math
import pandas as pd
import os
import zipfile
import datetime
import time
#import simple_salesforce + sf instance + response with access token (could be func)
import shutil

def get_photo(sf, body_url, name, folder, account_id):
    region = get_region(sf, account_id).replace(' ', '_')
    if region == None:
        region = 'Undefined'
    if folder == None:
        folder = 'Undefined'
    directory = 'photos2/' + folder + '/' + region
    fname = directory + '/' + name + '.jpg'
    if os.path.exists(fname):
        print('{0} file has been already downloaded'.format(name))
        return
    # ToDo: reference to global object reposponse. Name appropriately or pass as argument
    req = None
    while req == None:
        try:
            req = requests.get(response['instance_url']+body_url, headers = {'Authorization': 'Bearer ' + response['access_token']})
        except:
            print('Connection refused. Waiting for 5 seconds.')
            time.sleep(5)
    if not os.path.isdir(directory):
        os.makedirs(directory)
    s = 'Сергиенко Василия _ Героев 93-й бригады'
    if s in name:
        name_n = name.replace(s, 'Сергиенко Василия_Героев')
        f = open(directory + '/' + name_n + '.jpg', 'wb')        
    else:
        f = open(directory + '/' + name + '.jpg', 'wb')
    f.write(req.content)
    f.close()

def get_region(sf, account_id):
    records = sf.query("SELECT Address_line_2_vod__c FROM Address_vod__c WHERE Account_vod__c = '" + account_id + "'")
    return records['records'][0]['Address_line_2_vod__c'].partition(',')[0]

def get_parent_name(df, id_value):
    try:
        item = df.loc[id_value]
    except KeyError:
        return None
    if item['ParentId'] == None:
        return item['Name']
    else:
        return get_parent_name(df, item['ParentId'])

def get_title(sf, account_id):
    records = sf.query("SELECT Name, Account_Identifier_vod__c, ParentId, External_ID_vod__c FROM Account WHERE Id = '" + account_id + "'")
    records = records['records'][0]
    if records['Name'] == None:
        acc_name = ''
    else:
        acc_name = records['Name'][:50]
    if records['Account_Identifier_vod__c'] == None:
        acc_address = ''
    else:
        acc_address = records['Account_Identifier_vod__c']
    if records['External_ID_vod__c'] == None:
        acc_name = ''
    else:
        external_id = records['External_ID_vod__c']
    
    acc_parent = records['ParentId']
    acc_parent_title = None
    while acc_parent != None:
        records = sf.query("SELECT Name, Account_Identifier_vod__c, ParentId FROM Account WHERE Id = '" + acc_parent + "'")
        records = records['records'][0]
        acc_parent = records['ParentId']
    if acc_parent_title == None:
        acc_parent_title = records['Name']

    print('Got parent name for {0}'.format(acc_name))
    return (external_id + '_' + acc_name + '_' + acc_address, acc_parent_title)


sf = teva_salesforce.sf_instance()

response = sf[1]

sf = sf[0]
q = sf.query_all

period_start = datetime.date(2019, 4, 19)
period_end = datetime.date(2019, 4, 25)
days = (period_end - period_start).days + 1

date_format = '%Y-%m-%d'



print('Getting account list...', end='')
accounts = q('SELECT Id, Name, Account_Identifier_vod__c, ParentId, External_ID_vod__c, xR1_Account_Type__c FROM Account WHERE xR1_Account_Type__c IN (\'Pharmacy\', \'Distributor\', \'Pharmacy chain\') AND xR1_Account_Status__c=\'Active\'')
print('done')
accounts = accounts['records']
acc = pd.DataFrame(accounts).set_index('Id')
# Add parent name column, keep corporation name existing
print('Getting main parent name...', end='')
acc['MainParentName'] = [get_parent_name(acc, x) for x in acc.index.values]
print('done')
acc.loc[:,['Name','Account_Identifier_vod__c','External_ID_vod__c']].fillna(value='', inplace=True)
acc['ApplicableName'] = acc['External_ID_vod__c'] + '_' + acc['Name'].str[:50] + '_' + acc['Account_Identifier_vod__c']

print('{0} days to fetch images.'.format(days))

for d in range(days):
    start_date = period_start + datetime.timedelta(days=d)
    end_date = start_date + datetime.timedelta(days=1)
    print('\nProcessing {0}'.format(start_date.strftime(date_format)))
    # Gets all inventory monitoring for chosen date
    records = q("SELECT Account_vod__c, Call2_vod__c, CreatedDate,Id FROM Inventory_Monitoring_vod__c WHERE CreatedDate >= " + start_date.strftime(date_format) + "T00:00:00Z AND CreatedDate < " + end_date.strftime(date_format) + "T00:00:00Z")
    records = records['records']
    
    if len(records) == 0:
        print('Nothing to download.')
        continue

    im = pd.DataFrame(records).set_index('Id')[['Account_vod__c', 'CreatedDate']].drop_duplicates()

    if im.shape[0] == 0:
        print('No inventory monitorings to download in chosen period')
        continue
    print('{0} IM recond(s) fetched.'.format(im.shape[0]))
    
    
    
    im = im.merge(acc, how='left', left_on='Account_vod__c', right_index=True)
    imgs = pd.DataFrame()
    im_tmp = '\'' + im.index.values + '\''
    
    print('Getting IM Ids...')
    for i in range(1, im.shape[0] // 100 + 2):
        start = 100*(i-1)
        end = 100 * i
        if end > im.shape[0]:
            end = im.shape[0]
        if start == end:
            break
        records = q('SELECT Id, Body, Name, ParentId FROM Attachment where ContentType=\'image/jpeg\' AND ParentId IN (' + ','.join(im_tmp[start:end]) + ')')
        records = records['records']
    
        if len(records) > 0:
            imgs = imgs.append(records)
        print('Fetched {0} of {1} inventory monitoring attachement(s) data.'.format(end, im.shape[0]))
    
    
    if imgs.shape[0] == 0:
        print('No images to download in chosen period')
        continue
    
    imgs = imgs[['Id', 'Name', 'ParentId', 'Body']].set_index('Id').drop_duplicates()
    
    print('{0} image recond(s) fetched.'.format(imgs.shape[0]))
    
    
    
    counter = 0
    total = imgs.shape[0]
    for i, r in im.iterrows():
        for ii, ir in imgs[imgs['ParentId'] == i].iterrows():
            counter += 1
            if not isinstance(r['ApplicableName'], str):
                r['ApplicableName'], r['MainParentName'] = get_title(sf, r['Account_vod__c'])
                print('Got non-active pharmancy name.')
            get_photo(sf, ir['Body'], (r['ApplicableName'] + '_' + ir['Name'][0:19]).replace(':','-').replace('/', '_'), r['MainParentName'], r['Account_vod__c'])
            print('{1} of {2}: Got {0} image'.format(ir['Name'], counter, total))

    print('Zipping...', end='')                
    zipf = zipfile.ZipFile('inventory_monitoring_{0}.zip'.format(start_date.strftime('%Y%m%d')), 'w', zipfile.ZIP_STORED)
    path = 'photos2/'
    for root, dirs, files in os.walk(path):
        for file in files:
            zipf.write(os.path.join(root, file))
    zipf.close()
    print('done')
    
    print('Removing photos directory...', end='')
    shutil.rmtree(path)
    print('done')
    

print('Done!')
