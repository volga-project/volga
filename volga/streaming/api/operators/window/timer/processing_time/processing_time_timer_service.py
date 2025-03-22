from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.memory import MemoryJobStore
import uuid
import logging
import datetime
from datetime import timezone

logger = logging.getLogger(__name__)

class ProcessingTimeTimerService:
    """
    Service for scheduling timer tasks using APScheduler.
    """
    
    def __init__(self):
        """Initialize the timer service."""
        # Configure the scheduler with a memory job store
        jobstores = {
            'default': MemoryJobStore()
        }
        self.scheduler = BackgroundScheduler(jobstores=jobstores, timezone=timezone.utc)
        self.jobs = {}  # task_id -> job_id
    
    def start(self):
        """Start the timer service."""
        self.scheduler.start()
    
    def stop(self):
        """Stop the timer service."""
        self.scheduler.shutdown()
    
    def schedule_once(self, delay, callback, task_id=None, args=None, kwargs=None):
        """
        Schedule a one-time task.
        
        Args:
            delay: Delay in seconds before executing the task
            callback: Function to call when the timer fires
            task_id: Optional unique identifier for the task
            args: Arguments to pass to the callback
            kwargs: Keyword arguments to pass to the callback
            
        Returns:
            The task ID
        """
        if task_id is None:
            task_id = str(uuid.uuid4())
        
        if args is None:
            args = []
        
        if kwargs is None:
            kwargs = {}
        
        # Wrap the callback to handle exceptions
        def safe_callback(*cb_args, **cb_kwargs):
            try:
                callback(*cb_args, **cb_kwargs)
            except Exception as e:
                logger.error(f"Error executing timer task {task_id}: {e}")
            finally:
                # Clean up after execution
                if task_id in self.jobs:
                    del self.jobs[task_id]
        
        # Calculate the run date
        run_date = datetime.datetime.now(timezone.utc) + datetime.timedelta(seconds=delay)
        
        # Schedule the job
        job = self.scheduler.add_job(
            safe_callback,
            'date',
            args=args,
            kwargs=kwargs,
            id=task_id,
            run_date=run_date,
            replace_existing=True
        )
        
        self.jobs[task_id] = job.id
        return task_id
    
    def cancel(self, task_id):
        """
        Cancel a scheduled task.
        
        Args:
            task_id: The ID of the task to cancel
            
        Returns:
            True if the task was cancelled, False if it wasn't found
        """
        if task_id in self.jobs:
            try:
                self.scheduler.remove_job(task_id)
            except:
                logger.warning(f"Failed to cancel job {task_id}")
            
            del self.jobs[task_id]
            return True
        return False
