#!/usr/bin/python2.7
import json
import os
import sys
import hashlib
import logging

from datetime import datetime
from datetime import timedelta

import boto
from boto.s3.key import Key

def read_config():
    try:
        with open('/etc/s3backuptest_conf.json', 'rb') as conf_data:
            conf = json.load(conf_data)

        #Verifying configuration loaded from file
        for jkeys in ['retention_period', 'retention_policy', 'location', 'backup_bucket']:
            conf[jkeys]
        int(conf['retention_period'])
        if not conf['retention_policy'] in ['delete_all', 'delete_remote', 'delete_local', 'skip']:
            raise BaseException('Unknown retention policy')
        #TODO: Check if local folder path is valid and has a slash at the end
    except:
        logging.warning('Configuration file not found, corrupted or contains invalid values')
        logging.info('Using default configuration')
        conf = {
                'retention_period'  : '7',
                'retention_policy'  : 'delete_all',
                'location'          : '/tmp/s3backuptest/',
                'backup_bucket'     : 'junetestbkp',
        }
    return conf

def s3_connect(bucket):
    try:
        conn = boto.connect_s3()
        bucket = conn.get_bucket(bucket)
    except:
        logging.critical('Connection to AWS failed, exiting...')
        sys.exit()
    return bucket 

def sync_remote(retention_policy, retention_period, location):
#This mode presumes that wanted state is the one in the local directory
    oldest = datetime.today() + timedelta(days=-int(retention_period))
    for f in os.listdir(location):
        full_path = location + f
        creation_time = datetime.fromtimestamp(os.path.getctime(full_path))
        remote_key = bucket.get_key(f)
        if (creation_time < oldest):
            local_retention_policy[retention_policy](full_path)
            if remote_key:
                remote_retention_policy[retention_policy](f)
            logging.info(full_path + ' is older than ' + retention_period + ' days, handling accordind to configured retention policy.')
        else:
            if remote_key:
                remote_md5 = remote_key.etag.strip('"')
                local_md5 = hashlib.md5(open(full_path, 'rb').read()).hexdigest()
                if remote_md5 == local_md5:
                    logging.info(full_path + ' backup is up to date')
                else:
                    remote_key.set_contents_from_filename(full_path)
                    logging.info(full_path + ' backup updated')
            else:
                k = Key(bucket)
                k.key = f
                k.set_contents_from_filename(full_path)
                logging.info(full_path + ' backup created')

def sync_local(retention_policy, retention_period, location):
#This mode presumes that wanted state is the one in the remote directory
    oldest = datetime.today() + timedelta(days=-int(retention_period))
    if not os.path.isdir(location):
        os.mkdir(location, 775)
    for key in bucket.list():
        f = bucket.get_key(key.name)
        creation_time = datetime.strptime(str(f.date), '%a, %d %b %Y %H:%M:%S %Z')
        full_path = location + key.name
        local_key = os.path.exists(full_path)
        if (creation_time < oldest):
            remote_retention_policy[retention_policy](key.name)
            if local_key:
                local_retention_policy[retention_policy](full_path)
            logging.info(key.name + ' is older than ' + retention_period + ' days, handling accordind to configured retention policy.')
        else:
            if local_key:
                remote_md5 = f.etag.strip('"')
                local_md5 = hashlib.md5(open(full_path, 'rb').read()).hexdigest()
                if remote_md5 == local_md5:
                    logging.info(full_path + ' file is up to date')
                else:
                    location = open(full_path, "wb")
                    f.get_file(location)
                    location.close()
                    logging.info(full_path + ' file updated')
            else:
                location = open(full_path, "wb")
                f.get_file(location)
                location.close()
                logging.info(full_path + ' file created')  

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO, filename='/var/log/s3backuptest.log')
logging.info('Script started.')

conf = read_config()
bucket = s3_connect(conf['backup_bucket'])

local_retention_policy = {
                    'delete_all'    :   os.unlink,
                    'delete_local'  :   os.unlink,
                    'delete_remote' :   lambda *args: None,
                    'skip'          :   lambda *args: None,
}
remote_retention_policy = {
                    'delete_all'    :   bucket.delete_key,
                    'delete_local'  :   lambda *args: None,
                    'delete_remote' :   bucket.delete_key,
                    'skip'          :   lambda *args: None,
}

if len(sys.argv) < 2:
    print('Argument missing. Run "./s3backup.py sync_local" to sync AWS S3 bucket with local directory or "./s3backup.py sync_remote" to sync local directory with AWS S3 bucket.')
elif (sys.argv[1] == 'sync_local'):
    sync_local(conf['retention_policy'], conf['retention_period'], conf['location'])
elif (sys.argv[1] == 'sync_remote'):
    sync_remote(conf['retention_policy'], conf['retention_period'], conf['location'])
else:
    print('Invalid argument. Run "./s3backup.py sync_local" to sync AWS S3 bucket with local directory or "./s3backup.py sync_remote" to sync local directory with AWS S3 bucket.')

logging.info('Script finished')
