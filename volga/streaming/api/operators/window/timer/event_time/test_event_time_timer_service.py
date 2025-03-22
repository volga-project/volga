import unittest
from unittest.mock import Mock, call
from decimal import Decimal
from volga.streaming.api.operators.window.timer.event_time.event_time_timer_service import EventTimeTimerService, EventTimer

class TestEventTimeTimerService(unittest.TestCase):
    
    def setUp(self):
        self.callback = Mock()
        self.timer_service = EventTimeTimerService(self.callback)
    
    def test_initial_watermark(self):
        """Test that the initial watermark is 0."""
        self.assertEqual(self.timer_service.get_current_watermark(), Decimal('0'))
    
    def test_register_timer(self):
        """Test registering a timer."""
        timer_id = self.timer_service.register_timer("key1", "window1", Decimal('1000.5'))
        
        # Verify timer_id is a string
        self.assertIsInstance(timer_id, str)
        
        # Verify timer is stored correctly
        self.assertIn(timer_id, self.timer_service.timers_by_id)
        self.assertIn(Decimal('1000.5'), self.timer_service.timers_by_time)
        self.assertIn(timer_id, self.timer_service.timers_by_time[Decimal('1000.5')])
        self.assertIn("key1", self.timer_service.timers_by_key_time)
        self.assertIn(Decimal('1000.5'), self.timer_service.timers_by_key_time["key1"])
        self.assertIn(timer_id, self.timer_service.timers_by_key_time["key1"][Decimal('1000.5')])
        
        # Verify timer details
        timer = self.timer_service.timers_by_id[timer_id]
        self.assertEqual(timer.timestamp, Decimal('1000.5'))
        self.assertEqual(timer.key, "key1")
        self.assertEqual(timer.namespace, "window1")
    
    def test_delete_timer(self):
        """Test deleting a timer."""
        timer_id = self.timer_service.register_timer("key1", "window1", Decimal('1000.5'))
        
        # Delete the timer
        result = self.timer_service.delete_timer(timer_id)
        
        # Verify deletion was successful
        self.assertTrue(result)
        
        # Verify timer is removed from all collections
        self.assertNotIn(timer_id, self.timer_service.timers_by_id)
        self.assertNotIn(Decimal('1000.5'), self.timer_service.timers_by_time)
        self.assertNotIn("key1", self.timer_service.timers_by_key_time)
    
    def test_delete_nonexistent_timer(self):
        """Test deleting a timer that doesn't exist."""
        result = self.timer_service.delete_timer("nonexistent")
        
        # Verify deletion failed
        self.assertFalse(result)
    
    def test_delete_timers_for_key(self):
        """Test deleting all timers for a key."""
        self.timer_service.register_timer("key1", "window1", Decimal('1000.5'))
        self.timer_service.register_timer("key1", "window2", Decimal('2000.75'))
        self.timer_service.register_timer("key2", "window3", Decimal('1500.25'))
        
        # Delete timers for key1
        count = self.timer_service.delete_timers_for_key("key1")
        
        # Verify correct number of timers deleted
        self.assertEqual(count, 2)
        
        # Verify key1 timers are removed
        self.assertNotIn("key1", self.timer_service.timers_by_key_time)
        
        # Verify key2 timers still exist
        self.assertIn("key2", self.timer_service.timers_by_key_time)
    
    def test_delete_timers_for_key_with_timestamp(self):
        """Test deleting timers for a key at a specific timestamp."""
        self.timer_service.register_timer("key1", "window1", Decimal('1000.5'))
        self.timer_service.register_timer("key1", "window2", Decimal('2000.75'))
        
        # Delete timers for key1 at timestamp 1000.5
        count = self.timer_service.delete_timers_for_key("key1", Decimal('1000.5'))
        
        # Verify correct number of timers deleted
        self.assertEqual(count, 1)
        
        # Verify key1 still exists (has timer at timestamp 2000.75)
        self.assertIn("key1", self.timer_service.timers_by_key_time)
        self.assertIn(Decimal('2000.75'), self.timer_service.timers_by_key_time["key1"])
        self.assertNotIn(Decimal('1000.5'), self.timer_service.timers_by_key_time["key1"])
    
    def test_advance_watermark_no_timers(self):
        """Test advancing watermark with no timers."""
        count = self.timer_service.advance_watermark(Decimal('1000.5'))
        
        # Verify no timers fired
        self.assertEqual(count, 0)
        
        # Verify watermark was updated
        self.assertEqual(self.timer_service.get_current_watermark(), Decimal('1000.5'))
        
        # Verify callback was not called
        self.callback.assert_not_called()
    
    def test_advance_watermark_with_timers(self):
        """Test advancing watermark with timers."""
        # Register timers
        timer_id1 = self.timer_service.register_timer("key1", "window1", Decimal('500.25'))
        timer_id2 = self.timer_service.register_timer("key2", "window2", Decimal('1000.5'))
        timer_id3 = self.timer_service.register_timer("key3", "window3", Decimal('1500.75'))
        
        # Get timer objects for verification
        timer1 = self.timer_service.timers_by_id[timer_id1]
        timer2 = self.timer_service.timers_by_id[timer_id2]
        
        # Advance watermark to 1000.5
        count = self.timer_service.advance_watermark(Decimal('1000.5'))
        
        # Verify correct number of timers fired
        self.assertEqual(count, 2)
        
        # Verify watermark was updated
        self.assertEqual(self.timer_service.get_current_watermark(), Decimal('1000.5'))
        
        # Verify callback was called with correct timer objects - now grouped by key and timestamp
        self.callback.assert_has_calls([
            call([timer1]),  # key1, 500.25
            call([timer2])   # key2, 1000.5
        ], any_order=False)
        
        # Verify fired timers were removed
        self.assertNotIn(Decimal('500.25'), self.timer_service.timers_by_time)
        self.assertNotIn(Decimal('1000.5'), self.timer_service.timers_by_time)
        self.assertIn(Decimal('1500.75'), self.timer_service.timers_by_time)
    
    def test_advance_watermark_backwards(self):
        """Test that advancing watermark backwards has no effect."""
        self.timer_service.advance_watermark(Decimal('1000.5'))
        
        # Try to advance watermark backwards
        count = self.timer_service.advance_watermark(Decimal('500.25'))
        
        # Verify no timers fired
        self.assertEqual(count, 0)
        
        # Verify watermark was not updated
        self.assertEqual(self.timer_service.get_current_watermark(), Decimal('1000.5'))
    
    def test_decimal_precision(self):
        """Test that Decimal timestamps maintain their precision."""
        # Register timers with different decimal precisions
        timer_id1 = self.timer_service.register_timer("key1", "window1", Decimal('1000.5'))
        timer_id2 = self.timer_service.register_timer("key2", "window2", Decimal('1000.50'))
        timer_id3 = self.timer_service.register_timer("key3", "window3", Decimal('1000.500'))
        
        # Verify that these are treated as the same timestamp (Decimal equality)
        self.assertEqual(len(self.timer_service.timers_by_time), 1)
        self.assertIn(Decimal('1000.5'), self.timer_service.timers_by_time)
        
        # Verify different timestamps are distinguished
        timer_id4 = self.timer_service.register_timer("key4", "window4", Decimal('1000.51'))
        self.assertEqual(len(self.timer_service.timers_by_time), 2)
        self.assertIn(Decimal('1000.51'), self.timer_service.timers_by_time)
    
    def test_timers_execute_in_time_order(self):
        """Test that timers are executed in timestamp order regardless of registration order."""
        # Register timers in non-chronological order
        timer_id3 = self.timer_service.register_timer("key3", "window3", Decimal('1500.75'))
        timer_id1 = self.timer_service.register_timer("key1", "window1", Decimal('500.25'))
        timer_id2 = self.timer_service.register_timer("key2", "window2", Decimal('1000.5'))
        
        # Get timer objects for verification
        timer1 = self.timer_service.timers_by_id[timer_id1]
        timer2 = self.timer_service.timers_by_id[timer_id2]
        timer3 = self.timer_service.timers_by_id[timer_id3]
        
        # Advance watermark to cover all timers
        count = self.timer_service.advance_watermark(Decimal('2000'))
        
        # Verify all timers fired
        self.assertEqual(count, 3)
        
        # Verify callback was called with timers in timestamp order, now grouped by key and timestamp
        self.callback.assert_has_calls([
            call([timer1]),  # key1, 500.25
            call([timer2]),  # key2, 1000.5
            call([timer3])   # key3, 1500.75
        ], any_order=False)
    
    def test_timers_with_same_timestamp(self):
        """Test that timers with the same timestamp are executed in registration order."""
        # Register multiple timers with the same timestamp but different keys
        timer_id1 = self.timer_service.register_timer("key1", "window1", Decimal('1000.5'))
        timer_id2 = self.timer_service.register_timer("key2", "window2", Decimal('1000.5'))
        timer_id3 = self.timer_service.register_timer("key3", "window3", Decimal('1000.5'))
        
        # Get timer objects for verification
        timer1 = self.timer_service.timers_by_id[timer_id1]
        timer2 = self.timer_service.timers_by_id[timer_id2]
        timer3 = self.timer_service.timers_by_id[timer_id3]
        
        # Advance watermark to trigger all timers
        count = self.timer_service.advance_watermark(Decimal('1500'))
        
        # Verify all timers fired
        self.assertEqual(count, 3)
        
        # Verify callback was called with timers grouped by key and timestamp
        # The order of keys is not guaranteed, but each key should be called separately
        self.assertEqual(self.callback.call_count, 3)
        self.callback.assert_has_calls([
            call([timer1]),
            call([timer2]),
            call([timer3])
        ], any_order=True)
    
    def test_timers_grouped_by_key_and_timestamp(self):
        """Test that timers with the same key and timestamp are grouped together."""
        # Register multiple timers with the same key and timestamp
        timer_id1 = self.timer_service.register_timer("key1", "window1", Decimal('1000.5'))
        timer_id2 = self.timer_service.register_timer("key1", "window2", Decimal('1000.5'))
        timer_id3 = self.timer_service.register_timer("key2", "window3", Decimal('1000.5'))
        
        # Get timer objects for verification
        timer1 = self.timer_service.timers_by_id[timer_id1]
        timer2 = self.timer_service.timers_by_id[timer_id2]
        timer3 = self.timer_service.timers_by_id[timer_id3]
        
        # Advance watermark to trigger all timers
        count = self.timer_service.advance_watermark(Decimal('1500'))
        
        # Verify all timers fired
        self.assertEqual(count, 3)
        
        # Verify callback was called with timers grouped by key and timestamp
        self.assertEqual(self.callback.call_count, 2)  # 2 calls for 2 distinct (key, timestamp) pairs
        
        # Check that timers for key1 were grouped together
        self.callback.assert_has_calls([
            call([timer1, timer2]),  # key1, 1000.5 (both timers in one call)
            call([timer3])           # key2, 1000.5
        ], any_order=True)

if __name__ == '__main__':
    unittest.main()