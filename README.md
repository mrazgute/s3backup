# S3 buckets sync script

### -----INTRO-----

This script will syncronize AWS S3 bucket and a local repository (which one is primary source can be configured). Script will remediate all the files which were created earlier N days before the run (N is set in configuration file).

### -----SETTING UP-----

AWS access should be configured for the script to work. You can do it through aws cli (run `aws configure`) or manually edit ~/.aws/credentials file:
```
[default]
aws_access_key_id = YOUR_ACCESS_KEY
aws_secret_access_key = YOUR_SECRET_KEY
```

### -----CONFIGURATION-----

Depending on configuration, this script can work in a several ways. File `s3backuptest_conf.json` should be located in the same directory as the script.
`s3backuptest_conf.json` should be written in json format!
Available settings:
+ **retention_period** - how old can file be (in days) before it should be deleted;
+ **retention_policy** - what to do with old files:
   - **delete_all**		- delete the files from S3 bucket and local directory;
   - **delete_remote**	- delete the file from S3 bucket, skip the one in local directory;
   - **delete_local**	- skip the file in S3 bucket, delete the one in local directory;
   - **skip**		- skip the files in S3 bucket and local repository;
+ **location**	   - full path to remote folder you want to sync (default `/tmp/s3backuptest/`);
+ **backup_bucket**    - name of the AWS S3 bucket you are using.

### -----LOGGING-----

Logs can be found in the same directory as a script in `s3backuptest.log` file.

### -----TODO-----

Add ability to choose if you want to remediate files created earlier than N days back or those that were updated earlier than N days back.
