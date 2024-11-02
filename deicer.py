import boto3
import time
import os
from botocore.exceptions import ClientError
import logging
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_aws_credentials():
    """
    Load AWS credentials from environment variables or .env file.
    Returns True if credentials are found, False otherwise.
    """
    logger.info("Starting credential validation...")
    # Try to load .env file if it exists
    env_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
    if os.path.exists(env_file):
        logger.info("Loading .env file")
        load_dotenv(env_file)

    # Check for required AWS credentials
    required_vars = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']
    optional_vars = ['AWS_SESSION_TOKEN', 'AWS_DEFAULT_REGION']

    # Debug credential presence and format
    for var in required_vars + optional_vars:
        value = os.getenv(var)
        if value:
            logger.info(f"{var} is set (length: {len(value)})")
            # Show first 4 and last 4 characters if value is long enough
            if len(value) > 8:
                logger.info(f"{var} preview: {value[:4]}...{value[-4:]}")
        else:
            logger.info(f"{var} is not set")

    # Check required variables
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"Missing required AWS credentials: {
                     ', '.join(missing_vars)}")
        logger.error("Please set them in your environment or .env file")
        return False

    # Log status of optional variables
    for var in optional_vars:
        if not os.getenv(var):
            logger.info(f"Optional variable {var} not set")

    return True


class GlacierCleanup:
    def __init__(self, region_name=None):
        """
        Initialize Glacier client with specified region or default from environment.
        """
        region_name = region_name or os.getenv(
            'AWS_DEFAULT_REGION', 'us-east-1')
        logger.info(f"Initializing Glacier client in region: {region_name}")

        # Create session with explicit credential check
        credentials = {
            'aws_access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
            'aws_secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
            'region_name': region_name
        }

        # Only add session token if it's present
        session_token = os.getenv('AWS_SESSION_TOKEN')
        if session_token:
            credentials['aws_session_token'] = session_token
            logger.info("Using temporary credentials (session token present)")
        else:
            logger.info("Using permanent credentials (no session token)")

        session = boto3.Session(**credentials)

        self.glacier = session.client('glacier')

    def list_vaults(self):
        """List all Glacier vaults in the account."""
        try:
            vaults = []
            paginator = self.glacier.get_paginator('list_vaults')

            for page in paginator.paginate():
                vaults.extend(page.get('VaultList', []))

            return vaults
        except ClientError as e:
            logger.error(f"Error listing vaults: {e}")
            raise

    def initiate_inventory_retrieval(self, vault_name):
        """Initiate an inventory retrieval job for a vault."""
        try:
            job_params = {'Type': 'inventory-retrieval'}
            response = self.glacier.initiate_job(
                vaultName=vault_name,
                jobParameters=job_params
            )
            return response['jobId']
        except ClientError as e:
            logger.error(f"Error initiating inventory retrieval for vault {
                         vault_name}: {e}")
            raise

    def get_job_output(self, vault_name, job_id):
        """Get the output of a completed inventory retrieval job."""
        try:
            response = self.glacier.get_job_output(
                vaultName=vault_name,
                jobId=job_id
            )
            return response['body'].read()
        except ClientError as e:
            logger.error(f"Error getting job output for vault {
                         vault_name}: {e}")
            raise

    def wait_for_job_completion(self, vault_name, job_id, check_interval=900):
        """Wait for an inventory job to complete."""
        while True:
            try:
                response = self.glacier.describe_job(
                    vaultName=vault_name,
                    jobId=job_id
                )
                if response['Completed']:
                    return True
                logger.info(f"Job still in progress for vault {
                            vault_name}. Waiting {check_interval} seconds...")
                time.sleep(check_interval)
            except ClientError as e:
                logger.error(f"Error checking job status for vault {
                             vault_name}: {e}")
                raise

    def delete_archive(self, vault_name, archive_id):
        """Delete a single archive from a vault."""
        try:
            self.glacier.delete_archive(
                vaultName=vault_name,
                archiveId=archive_id
            )
            logger.info(f"Deleted archive {
                        archive_id} from vault {vault_name}")
        except ClientError as e:
            logger.error(f"Error deleting archive {
                         archive_id} from vault {vault_name}: {e}")
            raise

    def delete_vault(self, vault_name):
        """Delete an empty vault."""
        try:
            self.glacier.delete_vault(vaultName=vault_name)
            logger.info(f"Deleted vault {vault_name}")
        except ClientError as e:
            logger.error(f"Error deleting vault {vault_name}: {e}")
            raise

    def cleanup_all_vaults(self):
        """Main function to clean up all vaults and their archives."""
        vaults = self.list_vaults()
        logger.info(f"Found {len(vaults)} vaults")

        for vault in vaults:
            vault_name = vault['VaultName']
            logger.info(f"Processing vault: {vault_name}")

            try:
                # Initiate inventory retrieval
                job_id = self.initiate_inventory_retrieval(vault_name)
                logger.info(f"Initiated inventory retrieval job {
                            job_id} for vault {vault_name}")

                # Wait for job completion
                self.wait_for_job_completion(vault_name, job_id)

                # Get inventory
                inventory = self.get_job_output(vault_name, job_id)
                archives = inventory.get('ArchiveList', [])

                # Delete all archives
                for archive in archives:
                    self.delete_archive(vault_name, archive['ArchiveId'])

                # Delete the empty vault
                self.delete_vault(vault_name)

            except Exception as e:
                logger.error(f"Error processing vault {vault_name}: {e}")
                continue


def main():
    """Main execution function."""
    try:
        # Check for AWS credentials
        if not load_aws_credentials():
            return

        # Initialize the cleanup class
        cleanup = GlacierCleanup()

        # Confirm with user before proceeding
        response = input(
            "This will delete ALL vaults and their contents. Are you sure? (yes/no): ")
        if response.lower() != 'yes':
            logger.info("Operation cancelled by user")
            return

        # Proceed with cleanup
        cleanup.cleanup_all_vaults()
        logger.info("Cleanup completed successfully")

    except Exception as e:
        logger.error(f"An error occurred during cleanup: {e}")
        raise


if __name__ == "__main__":
    main()
