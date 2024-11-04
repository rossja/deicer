# deicer

![an image showing a cartoon version of a glacier splitting and melting.](https://github.com/rossja/deicer/blob/main/docs/img/logo.jpg)

Python script to delete all archives from an s3 Glacier Vault and then
the vault itself.

## install dependencies

this code uses poetry to manage dependencies. if you hate that, a requirements.txt
file is also provided for you.

### using poetry

1. set the proper env vars: `cp .env.example .env` and edit .env
2. install the packages needed: `poetry install`
3. start the venv: `poetry shell`


## running the script

1. run the script: `python3 deicer.py`
2. you should see some debug information checking the auth configuration and then get a warning
3. type `no` to quit, or type `yes` to run
4. wait. then keep waiting. because glacier. *(see notes for details)*

## command line options

the script takes a number of parameters:

* **Log File**: using `--log-file`
* **Log Directory**: using `--log-dir`
* **Debug mode**: set by using `--debug --yes`

### examples

* **Specify a custom log file**:  `python deicer.py --log-file=/path/to/custom.log`
* **Specify a directory for logs (will create timestamped files)**:  `python deicer.py --log-dir=/path/to/logs`
* **enable debug mode**:  `python deicer.py --debug --yes`


## notes

* The script will run for a potentially **verrrrry** long time depending on how much data you have (up to days), but will sleep 900 seconds in between checking for the status.
* Once all archives are deleted, the script will attempt to delete the vault itself.
  * **The vault delete attempt will almost certainly fail the first time!**
    * This is because S3 Glacier won't allow recently changed vaults to be deleted.
    * The script will continue trying with an increasing wait time until it is able to delete the vault.


### example output

```shell
❯ python3 deicer.py
Starting credential validation...
Loading .env file
AWS_ACCESS_KEY_ID is set (length: 20)
AWS_ACCESS_KEY_ID preview: AK[REDACTED]EO
AWS_SECRET_ACCESS_KEY is set (length: 40)
AWS_SECRET_ACCESS_KEY preview: jw[REDACTED]M9
AWS_SESSION_TOKEN is not set
AWS_DEFAULT_REGION is set (length: 9)
AWS_DEFAULT_REGION preview: us-e...st-1
Optional variable AWS_SESSION_TOKEN not set
This will delete ALL vaults and their contents. Are you sure? (yes/no): yes
Found 3 vaults
Processing vault: Disk[REDACTED]AF0_1
Initiated inventory retrieval job C4j3[REDACTED]1sUo-X for vault Disk[REDACTED]AF0_1
Job still in progress for vault Disk[REDACTED]AF0_1. Waiting 900 seconds...
Job still in progress for vault Disk[REDACTED]AF0_1. Waiting 900 seconds...
...
Job still in progress for vault Disk[REDACTED]AF0_1. Waiting 900 seconds...
Job still in progress for vault Disk[REDACTED]AF0_1. Waiting 900 seconds...
Retrieved inventory for vault Disk[REDACTED]AF0_1
Processing inventory for vault Disk[REDACTED]AF0_1
Found 103639 archives in vault Disk[REDACTED]AF0_1
Deleted archive tqhO[REDACTED]a8Bg from vault Disk[REDACTED]AF0_1
Deleted archive cYs2[REDACTED]6srA from vault Disk[REDACTED]AF0_1
Deleted archive N2sC[REDACTED]shxg from vault Disk[REDACTED]AF0_1
...