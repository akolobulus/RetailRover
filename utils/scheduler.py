import time
import threading
import logging
from datetime import datetime, timedelta

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("Scheduler")

# Global variables for scheduling
scheduled_task = None
schedule_thread = None
stop_flag = threading.Event()

def scrape_on_schedule(interval_seconds, scrape_function):
    """
    Run the scraping function periodically on the given interval.
    
    Args:
        interval_seconds (int): Interval between scrapes in seconds
        scrape_function (function): Function to run for scraping
    """
    global stop_flag
    
    while not stop_flag.is_set():
        try:
            logger.info(f"Running scheduled scrape at {datetime.now()}")
            scrape_function()
        except Exception as e:
            logger.error(f"Error in scheduled scrape: {str(e)}")
        
        # Sleep until next interval
        logger.info(f"Next scrape scheduled at {datetime.now() + timedelta(seconds=interval_seconds)}")
        stop_flag.wait(interval_seconds)

def schedule_scraping(schedule_type, scrape_function):
    """
    Schedule regular scraping based on the specified interval.
    
    Args:
        schedule_type (str): Type of schedule (Hourly, Daily, Weekly)
        scrape_function (function): Function to call for scraping
    """
    global scheduled_task, schedule_thread, stop_flag
    
    # Stop any existing scheduled task
    if schedule_thread and schedule_thread.is_alive():
        stop_flag.set()
        schedule_thread.join(timeout=5)
        logger.info("Stopped previous scheduled task")
    
    # Reset stop flag
    stop_flag.clear()
    
    # Set interval based on schedule type
    if schedule_type == "Hourly":
        interval_seconds = 3600  # 1 hour
    elif schedule_type == "Daily":
        interval_seconds = 86400  # 24 hours
    elif schedule_type == "Weekly":
        interval_seconds = 604800  # 7 days
    else:
        # Disabled or invalid schedule
        logger.info(f"Scheduling disabled or invalid schedule type: {schedule_type}")
        return
    
    # Start the scheduler thread
    scheduled_task = schedule_type
    schedule_thread = threading.Thread(
        target=scrape_on_schedule,
        args=(interval_seconds, scrape_function),
        daemon=True
    )
    schedule_thread.start()
    logger.info(f"Started {schedule_type} scraping schedule")

def get_schedule_status():
    """
    Get the current schedule status.
    
    Returns:
        str: Current schedule status or None if not scheduled
    """
    global scheduled_task, schedule_thread
    
    if scheduled_task and schedule_thread and schedule_thread.is_alive():
        return scheduled_task
    return None
