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

## Deployment

* CircleCI builds the go ETL application and syncs the ETL Bash scripts and application to and s3 folder.
  * This is because the server is not running
* When the ETL is run (via CRON) it syncs the contents to the local filesystem and runs the bash script.
  ```bash
  #!/bin/bash
  
  /home/ubuntu/.local/bin/aws s3 sync s3://sbv-abr-etl/code/ /home/ubuntu/abr-etl/ && \
    /home/ubuntu/abr-etl/abr-etl.sh
  ```

 