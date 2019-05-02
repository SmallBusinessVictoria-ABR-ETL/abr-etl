## ABR ETL Process

* ABR Requires a Static IP to whitelist for SFTP access 
  * we choose AWS elastic IPs and start and stop the instance when not in use (Saving on hosting costs).

## Deltas

* Created after uploading the weekly extract
* Run as part of abr-etl application
* Executed as SQL Queries in Athena
* 2 Parts 
  * Updated (record exists in previous but something changed)
  * Added (record didn't exists in previous)
* Athena SQL output file `<query-id>.csv` renamed to `Agency_Data_updated.csv` and `Agency_data_added.csv` 
* Store in S3 
  * `DELTA/UPDATED/Agency_Data/importdate=<Import Date>/Agency_Data_updated.csv`
  * `DELTA/ADDED/Agency_Data/importdate=<Import Date>/Agency_Data_added.csv`


## SFTP Access to Deltas

1. https://ap-southeast-2.console.aws.amazon.com/transfer/home?region=ap-southeast-2#/
2. Create Server
3. Set Hostname
4. Select "Service managed" for Identity Provider
5. Select an IAM role with access to CloudWatch Logs
6. Create a user - With correct Role (eg Data Engineer - See Section describing Roles for more)
7. Add SSH Key
8. Test SFTP ()

**Note: Both the Server and User roles must allow "Assume:Role from Transfer"**

## Deployment

* CircleCI builds the go ETL application and syncs the ETL Bash scripts and application to and s3 folder.
  * This is because the server is not running
* When the ETL is run (via CRON) it syncs the contents to the local filesystem and runs the bash script.
  ```bash
  #!/bin/bash
  
  /home/ubuntu/.local/bin/aws s3 sync s3://sbv-abr-etl/code/ /home/ubuntu/abr-etl/ && \
    /home/ubuntu/abr-etl/abr-etl.sh
  ```

