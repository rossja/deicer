import boto3
import time
import os
import argparse
from botocore.exceptions import ClientError
import logging
from dotenv import load_dotenv

# Set up logging
logger = logging.getLogger(__name__)


def setup_logging(debug=False, log_file=None):
    """
    Configure logging to output to both file and console.

    Args:
        debug (bool): Whether to enable debug logging
        log_file (str): Path to log file. If None, generates a timestamped file name
    """
    log_level = logging.DEBUG if debug else logging.INFO

    # Create formatters
    console_formatter = logging.Formatter('%(message)s')
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Set up the root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Clear any existing handlers
    root_logger.handlers = []

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(log_level)
    root_logger.addHandler(console_handler)

    # File handler
    if log_file is None:
        # Generate default log file name with timestamp
        timestamp = time.strftime('%Y%m%d-%H%M%S')
        log_file = f'glacier_cleanup_{timestamp}.log'

    # Ensure log directory exists
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(log_level)
    root_logger.addHandler(file_handler)

    logger.debug("Logging initialized: console and file (%s)", log_file)


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
        logger.debug("Initializing Glacier client in region: %s", region_name)

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
            logger.debug("Using temporary credentials (session token present)")
        else:
            logger.debug("Using permanent credentials (no session token)")

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
            import json
            response = self.glacier.get_job_output(
                vaultName=vault_name,
                jobId=job_id
            )
            # Read the response body and decode it as JSON
            body_bytes = response['body'].read()
            inventory_json = json.loads(body_bytes.decode('utf-8'))
            logger.info(f"Retrieved inventory for vault {vault_name}")
            return inventory_json
        except ClientError as e:
            logger.error(f"Error getting job output for vault {
                         vault_name}: {e}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding inventory JSON for vault {
                         vault_name}: {e}")
            logger.debug(f"Raw response: {body_bytes}")
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

    def delete_vault(self, vault_name, max_retries=5, initial_wait=300):
        """
        Delete an empty vault with retries.

        Args:
            vault_name: Name of the vault to delete
            max_retries: Maximum number of deletion attempts
            initial_wait: Initial wait time in seconds between archive deletion and first vault deletion attempt
        """
        logger.info(
            "Waiting %d seconds for vault %s to be ready for deletion...", initial_wait, vault_name)
        time.sleep(initial_wait)  # Initial wait after archive deletions

        for attempt in range(max_retries):
            try:
                self.glacier.delete_vault(vaultName=vault_name)
                logger.info("Successfully deleted vault %s", vault_name)
                return True
            except ClientError as e:
                if e.response['Error']['Code'] == 'InvalidParameterValueException' and 'Vault not empty' in str(e):
                    # Exponential backoff
                    wait_time = (attempt + 1) * initial_wait
                    if attempt < max_retries - 1:  # Don't wait after the last attempt
                        logger.info("Vault %s not ready for deletion. Waiting %d seconds before retry %d/%d...",
                                    vault_name, wait_time, attempt + 1, max_retries)
                        time.sleep(wait_time)
                    else:
                        logger.error(
                            "Failed to delete vault %s after %d attempts", vault_name, max_retries)
                        raise
                else:
                    logger.error("Error deleting vault %s: %s",
                                 vault_name, str(e))
                    raise

    def cleanup_all_vaults(self):
        """Main function to clean up all vaults and their archives."""
        vaults = self.list_vaults()
        logger.info("Found %d vaults", len(vaults))

        for vault in vaults:
            vault_name = vault['VaultName']
            logger.info("Processing vault: %s", vault_name)

            try:
                # Initiate inventory retrieval
                job_id = self.initiate_inventory_retrieval(vault_name)
                logger.info(
                    "Initiated inventory retrieval job %s for vault %s", job_id, vault_name)

                # Wait for job completion
                self.wait_for_job_completion(vault_name, job_id)

                # Get inventory
                inventory = self.get_job_output(vault_name, job_id)
                logger.info("Processing inventory for vault %s", vault_name)

                # The inventory structure should contain an ArchiveList
                if 'ArchiveList' not in inventory:
                    logger.error(
                        "Unexpected inventory format for vault %s", vault_name)
                    logger.debug("Inventory contents: %s", inventory)
                    continue

                archives = inventory['ArchiveList']
                logger.info("Found %d archives in vault %s",
                            len(archives), vault_name)

                # Delete all archives
                for archive in archives:
                    self.delete_archive(vault_name, archive['ArchiveId'])

                if archives:
                    logger.info(
                        "All archives deleted from vault %s. Proceeding with vault deletion.", vault_name)
                else:
                    logger.info(
                        "No archives found in vault %s. Proceeding with vault deletion.", vault_name)

                # Delete the empty vault with retries
                self.delete_vault(vault_name)

            except Exception as e:
                logger.error(f"Error processing vault {vault_name}: {e}")
                continue


def main():
    """Main execution function."""
    try:
        # Parse command line arguments
        parser = argparse.ArgumentParser(
            description='AWS Glacier Vault Cleanup Tool')
        parser.add_argument('--debug', action='store_true',
                            help='Enable debug logging')
        parser.add_argument('--yes', '-y', action='store_true',
                            help='Skip confirmation prompt')
        parser.add_argument('--log-file',
                            help='Path to log file (default: glacier_cleanup_TIMESTAMP.log)')
        parser.add_argument('--log-dir',
                            help='Directory to store log files (default: current directory)')
        args = parser.parse_args()

        # Determine log file path
        log_file = args.log_file
        if args.log_dir and not args.log_file:
            timestamp = time.strftime('%Y%m%d-%H%M%S')
            log_file = os.path.join(
                args.log_dir, f'glacier_cleanup_{timestamp}.log')

        # Setup logging based on debug flag
        setup_logging(args.debug, log_file)

        # Log start of execution with script version or other relevant info
        logger.info("Starting AWS Glacier cleanup")

        # Check for AWS credentials
        if not load_aws_credentials():
            return

        # Initialize the cleanup class
        cleanup = GlacierCleanup()

        # Confirm with user before proceeding
        if not args.yes:
            response = input(
                "This will delete ALL vaults and their contents. Are you sure? (yes/no): ")
            if response.lower() != 'yes':
                logger.info("Operation cancelled by user")
                return

        # Proceed with cleanup
        cleanup.cleanup_all_vaults()
        logger.info("Cleanup completed successfully")

    except Exception as e:
        logger.error("An error occurred during cleanup: %s", str(e))
        if args.debug:
            logger.exception("Detailed error information:")
        raise


if __name__ == "__main__":
    main()
