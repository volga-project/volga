import unittest
import time
import threading
from decimal import Decimal

from volga.streaming.api.operators.window.timer.processing_time.processing_time_timer_service import ProcessingTimeTimerService

class TestTimerService(unittest.TestCase):
    def setUp(self):
        self.timer_service = ProcessingTimeTimerService()
        self.timer_service.start()
        self.results = []
        self.lock = threading.Lock()
    
    def tearDown(self):
        self.timer_service.stop()
    
    def record_result(self, value):
        """Helper method to record results from timer callbacks"""
        with self.lock:
            self.results.append(value)
    
    def test_timer_execution(self):
        """Test that timers execute at the expected times"""
        # Schedule three tasks with different delays
        self.timer_service.schedule_once(0.1, self.record_result, args=["task1"])
        self.timer_service.schedule_once(0.2, self.record_result, args=["task2"])
        self.timer_service.schedule_once(0.3, self.record_result, args=["task3"])
        
        # Wait for all tasks to complete
        time.sleep(0.5)
        
        # Check that all tasks executed in the correct order
        self.assertEqual(self.results, ["task1", "task2", "task3"])
    
    def test_timer_cancellation(self):
        """Test that cancelled timers don't execute"""
        # Schedule two tasks
        task1_id = self.timer_service.schedule_once(0.1, self.record_result, args=["task1"])
        task2_id = self.timer_service.schedule_once(0.2, self.record_result, args=["task2"])
        
        # Cancel the second task
        self.timer_service.cancel(task2_id)
        
        # Wait for tasks to complete
        time.sleep(0.3)
        
        # Check that only the first task executed
        self.assertEqual(self.results, ["task1"])
    
    def test_timer_replacement(self):
        """Test that scheduling a task with the same ID replaces the previous task"""
        task_id = "same_id"
        
        # Schedule a task
        self.timer_service.schedule_once(0.2, self.record_result, task_id=task_id, args=["original"])
        
        # Replace it with another task with the same ID but shorter delay
        self.timer_service.schedule_once(0.1, self.record_result, task_id=task_id, args=["replacement"])
        
        # Wait for tasks to complete
        time.sleep(0.3)
        
        # Check that only the replacement task executed
        self.assertEqual(self.results, ["replacement"])
    
    def test_multiple_cancellations(self):
        """Test that cancelling a task multiple times doesn't cause issues"""
        task_id = self.timer_service.schedule_once(0.2, self.record_result, args=["task"])
        
        # Cancel the task multiple times
        self.assertTrue(self.timer_service.cancel(task_id))
        self.assertFalse(self.timer_service.cancel(task_id))
        self.assertFalse(self.timer_service.cancel(task_id))
        
        # Wait for the original task time
        time.sleep(0.3)
        
        # Check that the task didn't execute
        self.assertEqual(self.results, [])
    
    def test_exception_handling(self):
        """Test that exceptions in timer callbacks are properly handled"""
        def failing_callback():
            raise ValueError("Expected test exception")
        
        # Schedule a task that will raise an exception
        self.timer_service.schedule_once(0.1, failing_callback)
        
        # Schedule a task after the failing one
        self.timer_service.schedule_once(0.2, self.record_result, args=["after_exception"])
        
        # Wait for tasks to complete
        time.sleep(0.3)
        
        # Check that the second task still executed despite the exception
        self.assertEqual(self.results, ["after_exception"])
    
    def test_many_timers(self):
        """Test handling many timers simultaneously"""
        # Schedule 100 timers with the same delay
        for i in range(100):
            self.timer_service.schedule_once(0.1, self.record_result, args=[i])
        
        # Wait for all tasks to complete
        time.sleep(0.3)
        
        # Check that all 100 tasks executed
        self.assertEqual(len(self.results), 100)
        self.assertEqual(set(self.results), set(range(100)))

if __name__ == '__main__':
    unittest.main()
    # t = TestTimerService()
    # t.setUp()
    # t.test_timer_execution()
    # t.test_timer_cancellation()
    # t.test_timer_replacement()
    # t.test_multiple_cancellations()
    # t.test_exception_handling()
    # t.test_many_timers()
    # t.tearDown()

