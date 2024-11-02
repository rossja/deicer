# deicer

![an image showing a cartoon version of a glacier splitting and melting.](https://github.com/rossja/deicer/blob/main/docs/img/logo.jpg)

Python script to delete all archives from an s3 Glacier Vault and then
the vault itself.

## running

this code uses poetry to manage dependencies. to run the script:

1. set the proper env vars: `cp .env.example .env` and edit .env
2. install the packages needed: `poetry install`
3. start the venv: `poetry shell`
4. run the script: `python3 deicer.py`

You should see some debug information checking the auth configuration
and then get a warning: type `no` to quit or just hit enter to accept
the default `yes`:

```shell
❯ python3 deicer.py
INFO:__main__:Starting credential validation...
INFO:__main__:Loading .env file
INFO:__main__:AWS_ACCESS_KEY_ID is set (length: 20)
INFO:__main__:AWS_ACCESS_KEY_ID preview: AKIA...EFEF
INFO:__main__:AWS_SECRET_ACCESS_KEY is set (length: 40)
INFO:__main__:AWS_SECRET_ACCESS_KEY preview: kd2...hbc
INFO:__main__:AWS_SESSION_TOKEN is not set
INFO:__main__:AWS_DEFAULT_REGION is set (length: 9)
INFO:__main__:AWS_DEFAULT_REGION preview: us-e...st-1
INFO:__main__:Optional variable AWS_SESSION_TOKEN not set
INFO:__main__:Initializing Glacier client in region: us-east-1
INFO:__main__:Using permanent credentials (no session token)
This will delete ALL vaults and their contents. Are you sure? (yes/no): yes
INFO:__main__:Found 3 vaults
INFO:__main__:Processing vault: Dis...F0_1
INFO:__main__:Initiated inventory retrieval job oJZf...yeTh for vault Dis...F0_1
INFO:__main__:Job still in progress for vault Dis...F0_1. Waiting 900 seconds...
```

The script will run for a potentially **verrrrry** long time depending on how much data you have (up to days), but will sleep 900 seconds in between checking for the status.
