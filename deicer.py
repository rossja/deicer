import boto3
import time
import os
import json
import argparse
from datetime import datetime, timezone
from botocore.exceptions import ClientError
import logging
from dotenv import load_dotenv

# Set up logging
logger = logging.getLogger(__name__)


def setup_logging(debug=False, log_file=None):
    """Configure logging to output to both file and console."""
    log_level = logging.DEBUG if debug else logging.INFO

    # Create formatters
    console_formatter = logging.Formatter('%(message)s')
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Set up the root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.handlers = []

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(log_level)
    root_logger.addHandler(console_handler)

    # File handler
    if log_file is None:
        timestamp = time.strftime('%Y%m%d-%H%M%S')
        log_file = f'glacier_cleanup_{timestamp}.log'

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
    # Try to load .env file if it exists
    env_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
    if os.path.exists(env_file):
        logger.debug("Loading .env file from: %s", env_file)
        load_dotenv(env_file)
    else:
        logger.debug("No .env file found at: %s", env_file)

    # Check for required AWS credentials
    required_vars = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']
    optional_vars = ['AWS_SESSION_TOKEN', 'AWS_DEFAULT_REGION']

    # Debug credential presence and format
    for var in required_vars + optional_vars:
        value = os.getenv(var)
        if value:
            logger.debug("%s is set (length: %d)", var, len(value))
            # Show first 4 and last 4 characters if value is long enough
            if len(value) > 8:
                logger.debug("%s preview: %s...%s", var, value[:4], value[-4:])
        else:
            logger.debug("%s is not set", var)

    # Check required variables
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.error("Missing required AWS credentials: %s",
                     ', '.join(missing_vars))
        logger.error("Please set them in your environment or .env file")
        return False

    logger.debug("AWS credentials validation completed successfully")
    return True


class GlacierStateManager:
    """Manages the state of Glacier vault deletion process."""

    def __init__(self, state_file):
        """Initialize with path to state file."""
        self.state_file = state_file
        self.state = self.load_state()

    def load_state(self):
        """Load state from file or create new if doesn't exist."""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    return json.load(f)
            except json.JSONDecodeError:
                logger.error("Error reading state file. Creating new state.")
                return {}
        return {}

    def save_state(self):
        """Save current state to file."""
        with open(self.state_file, 'w') as f:
            json.dump(self.state, f, indent=2)
        logger.debug("State saved to %s", self.state_file)

    def add_vault(self, vault_id):
        """Add a new vault to track."""
        if vault_id not in self.state:
            self.state[vault_id] = {
                "job_id": None,
                "status": None,
                "job_updated": None,
                "archives": []
            }
            self.save_state()

    def update_vault_job(self, vault_id, job_id, status):
        """Update job information for a vault."""
        if vault_id in self.state:
            self.state[vault_id]["job_id"] = job_id
            self.state[vault_id]["status"] = status
            self.state[vault_id]["job_updated"] = datetime.now(
                timezone.utc).isoformat()
            self.save_state()

    def update_vault_archives(self, vault_id, archives):
        """Update archive list for a vault."""
        if vault_id in self.state:
            self.state[vault_id]["archives"] = archives
            self.save_state()


class GlacierCleanup:
    def __init__(self, state_manager, region_name=None):
        """Initialize Glacier client with state manager."""
        region_name = region_name or os.getenv(
            'AWS_DEFAULT_REGION', 'us-east-1')
        self.state_manager = state_manager

        session = boto3.Session(
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            aws_session_token=os.getenv('AWS_SESSION_TOKEN'),
            region_name=region_name
        )

        self.glacier = session.client('glacier')
        logger.debug("Initialized Glacier client in region: %s", region_name)

    def list_vaults(self):
        """List and record all Glacier vaults."""
        try:
            vaults = []
            paginator = self.glacier.get_paginator('list_vaults')

            for page in paginator.paginate():
                for vault in page.get('VaultList', []):
                    vault_name = vault['VaultName']
                    self.state_manager.add_vault(vault_name)
                    vaults.append(vault_name)

            logger.info("Found %d vaults", len(vaults))
            return vaults
        except ClientError as e:
            logger.error("Error listing vaults: %s", str(e))
            raise

    def initiate_inventory_jobs(self):
        """Start inventory jobs for vaults without active jobs."""
        for vault_id, vault_data in self.state_manager.state.items():
            if not vault_data["job_id"] or vault_data["status"] in ["error", "complete"]:
                try:
                    response = self.glacier.initiate_job(
                        vaultName=vault_id,
                        jobParameters={'Type': 'inventory-retrieval'}
                    )
                    job_id = response['jobId']
                    self.state_manager.update_vault_job(
                        vault_id, job_id, "in-progress")
                    logger.info(
                        "Initiated inventory job %s for vault %s", job_id, vault_id)
                except ClientError as e:
                    logger.error(
                        "Error initiating job for vault %s: %s", vault_id, str(e))
                    self.state_manager.update_vault_job(
                        vault_id, None, "error")

    def check_job_status(self):
        """Check status of all in-progress jobs."""
        jobs_checked = 0
        for vault_id, vault_data in self.state_manager.state.items():
            if vault_data["status"] == "in-progress" and vault_data["job_id"]:
                try:
                    response = self.glacier.describe_job(
                        vaultName=vault_id,
                        jobId=vault_data["job_id"]
                    )
                    jobs_checked += 1

                    # Update the job timestamp even if not complete
                    self.state_manager.update_vault_job(
                        vault_id, vault_data["job_id"], "in-progress")

                    logger.info("Checking job %s for vault %s: %s",
                                vault_data["job_id"],
                                vault_id,
                                "COMPLETED" if response['Completed'] else "IN PROGRESS")

                    if response['Completed']:
                        self.get_job_output(vault_id, vault_data["job_id"])
                except ClientError as e:
                    logger.error("Error checking job %s for vault %s: %s",
                                 vault_data["job_id"], vault_id, str(e))
                    self.state_manager.update_vault_job(
                        vault_id, vault_data["job_id"], "error")

        if jobs_checked == 0:
            logger.info("No in-progress jobs to check")

    def get_job_output(self, vault_id, job_id):
        """Get and store output from completed inventory job."""
        try:
            response = self.glacier.get_job_output(
                vaultName=vault_id,
                jobId=job_id
            )
            inventory = json.loads(response['body'].read())
            archives = [
                {
                    'id': archive['ArchiveId'],
                    'description': archive.get('ArchiveDescription', ''),
                    'size': archive['Size']
                }
                for archive in inventory.get('ArchiveList', [])
            ]
            self.state_manager.update_vault_archives(vault_id, archives)
            self.state_manager.update_vault_job(vault_id, job_id, "complete")
            logger.info("Retrieved %d archives for vault %s",
                        len(archives), vault_id)
        except ClientError as e:
            logger.error(
                "Error getting job output for vault %s: %s", vault_id, str(e))
            self.state_manager.update_vault_job(vault_id, job_id, "error")

    def process_completed_jobs(self):
        """Process vaults with completed inventory jobs older than 24 hours."""
        current_time = datetime.now(timezone.utc)

        for vault_id, vault_data in self.state_manager.state.items():
            if vault_data["status"] == "complete" and vault_data["job_updated"]:
                job_time = datetime.fromisoformat(vault_data["job_updated"])
                hours_elapsed = (
                    current_time - job_time).total_seconds() / 3600

                if hours_elapsed >= 24:
                    logger.info(
                        "Processing vault %s (%.1f hours elapsed)", vault_id, hours_elapsed)
                    self.delete_vault_contents(
                        vault_id, vault_data["archives"])

    def delete_vault_contents(self, vault_id, archives):
        """Delete archives and attempt vault deletion."""
        try:
            for archive in archives:
                try:
                    self.glacier.delete_archive(
                        vaultName=vault_id,
                        archiveId=archive['id']
                    )
                    logger.info("Deleted archive %s from vault %s",
                                archive['id'], vault_id)
                except ClientError as e:
                    logger.error("Error deleting archive %s: %s",
                                 archive['id'], str(e))

            # After deleting archives, mark the vault for future deletion
            self.state_manager.update_vault_job(
                vault_id, None, "pending_deletion")
            logger.info("Marked vault %s for deletion", vault_id)

        except Exception as e:
            logger.error("Error processing vault %s: %s", vault_id, str(e))


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description='AWS Glacier Vault Cleanup Tool')
    parser.add_argument('--debug', action='store_true',
                        help='Enable debug logging')
    parser.add_argument('--log-file', help='Path to log file')
    parser.add_argument('--state-file', default='glacier_state.json',
                        help='Path to state file (default: glacier_state.json)')
    parser.add_argument('--scan', action='store_true',
                        help='Scan for new vaults and initiate inventory jobs')
    parser.add_argument('--status', action='store_true',
                        help='Show current status of all vaults and jobs')
    args = parser.parse_args()

    try:
        # Setup logging
        setup_logging(args.debug, args.log_file)

        # Load AWS credentials
        if not load_aws_credentials():
            logger.error("Failed to load AWS credentials")
            return 1

        # Initialize state manager and cleanup
        state_manager = GlacierStateManager(args.state_file)
        cleanup = GlacierCleanup(state_manager)

        # If scan flag is set, scan for new vaults
        if args.scan:
            cleanup.list_vaults()
            cleanup.initiate_inventory_jobs()

        # Always check existing jobs and process completed ones
        cleanup.check_job_status()
        cleanup.process_completed_jobs()

        # Show status if requested
        if args.status:
            logger.info("\nCurrent Status:")
            for vault_id, vault_data in state_manager.state.items():
                status = vault_data["status"] or "not started"
                job_id = vault_data["job_id"] or "no job"
                updated = vault_data["job_updated"] or "never"
                if updated != "never":
                    # Convert UTC timestamp to local time for display
                    updated_dt = datetime.fromisoformat(
                        updated).replace(tzinfo=timezone.utc)
                    updated = updated_dt.astimezone().strftime('%Y-%m-%d %H:%M:%S %Z')
                archive_count = len(vault_data.get("archives", []))
                logger.info(f"Vault: {vault_id}")
                logger.info(f"  Status: {status}")
                logger.info(f"  Job ID: {job_id}")
                logger.info(f"  Last Updated: {updated}")
                logger.info(f"  Archives: {archive_count}")
                logger.info("")

        logger.info("Command completed")

    except Exception as e:
        logger.error("An error occurred: %s", str(e))
        if args.debug:
            logger.exception("Detailed error information:")
        raise


if __name__ == "__main__":
    main()
