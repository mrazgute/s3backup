#!/usr/bin/python2.7
import json
import os
import sys
import hashlib
import logging
import errno

from datetime import datetime
from datetime import timedelta

import boto
from boto.s3.key import Key

def read_config():

    try:
        logging.info('Reading conf file')
        with open('s3backuptest_conf.json', 'rb') as conf_data:
            conf = json.load(conf_data)

        #Verifying configuration loaded from file
        logging.info('Validating conf file')

        #Checking if all necessary parameters are available        
        for jkeys in ['retention_period', 'retention_policy', 'location', 'backup_bucket']:
            conf[jkeys]

        #Checking if retention_period is valid
        int(conf['retention_period'])

        #Checking if retention policy is valid
        if not conf['retention_policy'] in ['delete_all', 'delete_remote', 'delete_local', 'skip']:
            raise ValueError('Unknown retention policy')

        #Checking if local directory path is valid and directory is accessable
        if not os.path.isdir(conf['location']):
            os.mkdir(location, 775)
        else:
            testfile_path = os.path.join(conf['location'], '.s3synctestfile.test')
            with open(testfile_path, 'w') as testfile:
                testfile.write('folder testing')
            with open(testfile_path, 'r') as testfile:
                testfile.read()
            os.unlink(testfile_path)

        #Returning a dictionary containing configuration parameters
        return conf
 
    #Handling faulty configuration   
    except IOError as err:
        if '.s3synctestfile.test' in str(err):
            logging.critical('Cannot access local directory. ' + str(err) + '  Exiting...')
        else:
            logging.critical('Configuration file is not found. Exiting...')
    except KeyError:
        logging.critical('Configuration file is missing parameters. Exiting...')
    except ValueError as err:
        if str(err) == 'Unknown retention policy':
            logging.critical('Configuration file contains invalid value for retention_policy parameter. Exiting...')
        elif 'Invalid control character at' in str(err):
           logging.critical('Configuration file contains error: ' + str(err) + '. Exiting...')
        else:
            logging.critical('Configuration file contains invalid value for retention_period parameter. Exiting...')
    except:
        logging.critical('Error reading configuration file. Exiting...')
    sys.exit()


def s3_connect(bucket):

#Connecting to S3 bucket using system parameters and bucketname found in conf file

    logging.info('Connecting to S3 bucket')

    try:
        conn = boto.connect_s3()
        bucket = conn.get_bucket(bucket)
    except:
        logging.critical('Connection to AWS failed, exiting...')
        sys.exit()

    return bucket 

def sync_remote(retention_policy, retention_period, location):

#This mode presumes that wanted state is the one in the local directory

    logging.info('Will sync S3 bucket with local directory')

    #Determening what is the oldest a file can be
    oldest = datetime.today() + timedelta(days=-int(retention_period))
    logging.info('Will remediate all files older than ' + datetime.strftime(oldest, '%Y-%m-%d %H:%M'))

    for f in os.listdir(location):
        full_path = os.path.join(location, f)
        creation_time = datetime.fromtimestamp(os.path.getctime(full_path))
        remote_key = bucket.get_key(f)

        #If file is outdated - handle it according to configured retention policy
        if (creation_time < oldest):
            local_retention_policy[retention_policy](full_path)

            if remote_key:
                remote_retention_policy[retention_policy](f)

            logging.info(full_path + ' is older than ' + retention_period + ' days, handling accordind to configured retention policy.')

        elif remote_key:
            remote_md5 = remote_key.etag.strip('"')
            local_md5 = hashlib.md5(open(full_path, 'rb').read()).hexdigest()

            #If local and remote files have the same md5 sum, then do nothing. Else update remote file.
            if remote_md5 == local_md5:
                logging.info(full_path + ' backup is up to date')
            else:
                remote_key.set_contents_from_filename(full_path)
                logging.info(full_path + ' backup updated')
        else:
            #If remote file does not exist - create it.
            k = Key(bucket)
            k.key = f
            k.set_contents_from_filename(full_path)
            logging.info(full_path + ' backup created')

def sync_local(retention_policy, retention_period, location):

#This mode presumes that wanted state is the one in the remote directory

    logging.info('Will sync local directory with S3 bucket')

    #Determening what is the oldest a file can be
    oldest = datetime.today() + timedelta(days=-int(retention_period))
    logging.info('Will remediate all files older than ' + datetime.strftime(oldest, '%Y-%m-%d %H:%M'))

    for key in bucket.list():
        f = bucket.get_key(key.name)
        creation_time = datetime.strptime(str(f.date), '%a, %d %b %Y %H:%M:%S %Z')
        full_path = os.path.join(location, key.name)
        local_key = os.path.exists(full_path)

        #If file is outdated - handle it according to configured retention policy
        if (creation_time < oldest):
            remote_retention_policy[retention_policy](key.name)

            if local_key:
                local_retention_policy[retention_policy](full_path)

            logging.info(key.name + ' is older than ' + retention_period + ' days, handling accordind to configured retention policy.')

        elif local_key:
            #If local and remote files have the same md5 sum, then do nothing. Else update local file.
            remote_md5 = f.etag.strip('"')
            local_md5 = hashlib.md5(open(full_path, 'rb').read()).hexdigest()
            if remote_md5 == local_md5:
                logging.info(full_path + ' file is up to date')
            else:
                with open(full_path, "wb") as location:
                    f.get_file(location)
                logging.info(full_path + ' file updated')
        else:
            #If local file does not exist - create it.
            with open(full_path, "wb") as location:
                f.get_file(location)
            logging.info(full_path + ' file created')  

#Initializing logging
try:
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO, filename='s3backuptest.log')
except OSError as err:
    print('Could not create/access logfile: ' + err)
logging.info('Script started.')

conf = read_config()
bucket = s3_connect(conf['backup_bucket'])

local_retention_policy = {
                    'delete_all'    :   os.unlink,
                    'delete_local'  :   os.unlink,
                    'delete_remote' :   lambda: None,
                    'skip'          :   lambda: None,
}
remote_retention_policy = {
                    'delete_all'    :   bucket.delete_key,
                    'delete_local'  :   lambda: None,
                    'delete_remote' :   bucket.delete_key,
                    'skip'          :   lambda: None,
}

if len(sys.argv) < 2:
    print('Argument missing. Run "./s3backup.py sync_remote" to sync AWS S3 bucket with local directory or "./s3backup.py sync_local" to sync local directory with AWS S3 bucket.')
elif (sys.argv[1] == 'sync_local'):
    sync_local(conf['retention_policy'], conf['retention_period'], conf['location'])
elif (sys.argv[1] == 'sync_remote'):
    sync_remote(conf['retention_policy'], conf['retention_period'], conf['location'])
else:
    print('Invalid argument. Run "./s3backup.py sync_remote" to sync AWS S3 bucket with local directory or "./s3backup.py sync_local" to sync local directory with AWS S3 bucket.')

logging.info('Script finished')
