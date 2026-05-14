#!/usr/bin/env python3
"""
Main Application - Uranium Stock Data Collection Pipeline
Process-based execution:
- process1 = most followed stocks fetcher (uranium stocks)
- process2 = insider transactions fetcher (uranium stocks)

IMPORTANT: Process is updated BEFORE execution to prevent getting stuck on failures
"""

import logging
import sys
from database_config import get_curser
from database_operations import update_process_status
from most_followed import get_most_followed_data
from insider_transactions_fetcher import main as run_insider_transactions_fetcher

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)


def get_current_process():
    """Get current process from database"""
    try:
        connection, cursor = get_curser()
        cursor.execute("SELECT current_process FROM process_python2 LIMIT 1")
        result = cursor.fetchone()
        cursor.close()
        connection.close()
        return result[0].strip() if result else "process1"
    except:
        return "process1"


def main():
    """Main pipeline function - runs based on current process"""
    current_process = get_current_process()
    logging.info(f"🔍 Current process: '{current_process}'")

    if current_process == "process1":
        # Most followed stocks fetcher
        logging.info("🚀 STARTING PROCESS 1: MOST FOLLOWED STOCKS FETCHER")

        connection, cursor = get_curser()
        logging.info("✅ Updating to process2 BEFORE execution")
        update_process_status(cursor, connection, "process2")
        cursor.close()
        connection.close()

        try:
            get_most_followed_data()
            logging.info("✅ Process 1 completed successfully")
        except Exception as e:
            logging.error(f"❌ Error in process1: {e}")

    elif current_process == "process2":
        # Insider transactions fetcher
        logging.info("🚀 STARTING PROCESS 2: INSIDER TRANSACTIONS FETCHER")

        connection, cursor = get_curser()
        logging.info("✅ Updating to process1 BEFORE execution (cycling back)")
        update_process_status(cursor, connection, "process1")
        cursor.close()
        connection.close()

        try:
            logging.info("📊 About to run insider transactions fetcher...")
            run_insider_transactions_fetcher()
            logging.info("✅ Process 2 completed successfully")
        except Exception as e:
            logging.error(f"❌ Error in process2: {e}")
            raise

    else:
        # Default to process1 if unknown process
        logging.warning(f"❌ Unknown process: '{current_process}', defaulting to process1")
        connection, cursor = get_curser()
        update_process_status(cursor, connection, "process1")
        cursor.close()
        connection.close()

        try:
            get_most_followed_data()
            logging.info("✅ Fallback process 1 completed")
        except Exception as e:
            logging.error(f"❌ Error in fallback process1: {e}")


if __name__ == "__main__":
    main()
