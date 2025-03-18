from pprint import pprint
import unittest
from decimal import Decimal
from typing import Dict, Any

from volga.streaming.api.function.retractable_aggregate_function import AggregationType
from volga.streaming.api.message.message import Record, KeyRecord
from volga.streaming.api.operators.window.window_operator import WindowOperator
from volga.streaming.api.operators.window.models import SlidingWindowConfig
from volga.streaming.api.collector.collector import Collector

import logging
logging.basicConfig(level=logging.DEBUG, format='[%(asctime)s][%(levelname)s] %(message)s')

class TestCollector(Collector):
    def __init__(self):
        self.collected_records = []
    
    def collect(self, record: Record):
        self.collected_records.append(record)
    
    def close(self):
        pass


class TestWindowOperator(unittest.TestCase):
    # Define test cases as a class variable
    TEST_CASES = {
        "single_key_single_window": {
            "configs": [
                SlidingWindowConfig(
                    duration='10s',
                    agg_type=AggregationType.SUM,
                    agg_on_func=lambda x: x,
                    name="test_sum",
                    fixed_interval='5s'
                )
            ],
            "test_data": {
                'events': [
                    ('user1', 10, Decimal('100')),
                    ('user1', 20, Decimal('103')),
                    ('user1', 30, Decimal('107')),
                    ('user1', 40, Decimal('112')),
                    ('user1', 50, Decimal('118')),
                ],
                'watermarks': [
                    Decimal('110'),
                    Decimal('120')
                ],
                'expected_results': [
                    {'time': Decimal('110'), 'test_sum': 60},  # sum of 10, 20, 30
                    {'time': Decimal('115'), 'test_sum': 70},  # sum of 30, 40
                    {'time': Decimal('120'), 'test_sum': 90},  # sum of 40, 50
                ]
            }
        },
        
        "multi_key_single_window": {
            "configs": [
                SlidingWindowConfig(
                    duration='10s',
                    agg_type=AggregationType.SUM,
                    agg_on_func=lambda x: x,
                    name="test_sum",
                    fixed_interval='5s'
                )
            ],
            "test_data": {
                'events': [
                    # First batch - some events at same time, some at different times
                    ('user1', 10, Decimal('100')),
                    ('user2', 15, Decimal('101')),  # Different time and value
                    ('user1', 20, Decimal('103')),
                    ('user2', 20, Decimal('103')),  # Same time as user1
                    ('user1', 30, Decimal('107')),
                    ('user2', 25, Decimal('105')),  # Different time and value
                    
                    # Second batch - after first watermark
                    ('user1', 40, Decimal('112')),
                    ('user2', 45, Decimal('114')),  # Different time and value
                    ('user1', 50, Decimal('118')),
                    ('user2', 50, Decimal('118')),  # Same time as user1
                ],
                'watermarks': [
                    Decimal('110'),
                    Decimal('120')
                ],
                'expected_results': [
                    # Results for user1
                    {'time': Decimal('110'), 'stream_name': 'user1', 'test_sum': 60},  # sum of 10, 20, 30
                    {'time': Decimal('110'), 'stream_name': 'user2', 'test_sum': 60},  # sum of 15, 20, 25
                    
                    {'time': Decimal('115'), 'stream_name': 'user1', 'test_sum': 70},  # sum of 30, 40
                    {'time': Decimal('115'), 'stream_name': 'user2', 'test_sum': 70},  # sum of 25, 45
                    
                    {'time': Decimal('120'), 'stream_name': 'user1', 'test_sum': 90},  # sum of 40, 50
                    {'time': Decimal('120'), 'stream_name': 'user2', 'test_sum': 95},  # sum of 45, 50
                ]
            }
        },
        
        "single_key_multi_window": {
            "configs": [
                SlidingWindowConfig(
                    duration='10s',
                    agg_type=AggregationType.SUM,
                    agg_on_func=lambda x: x,
                    name="test_sum_1",
                    fixed_interval='5s'
                ),
                SlidingWindowConfig(
                    duration='15s',
                    agg_type=AggregationType.SUM,
                    agg_on_func=lambda x: x,
                    name="test_sum_2",
                    fixed_interval='5s'
                )
            ],
            "test_data": {
                'events': [
                    ('user1', 10, Decimal('100')),
                    ('user1', 20, Decimal('103')),
                    ('user1', 30, Decimal('107')),
                    ('user1', 40, Decimal('112')),
                    ('user1', 50, Decimal('118')),
                ],
                'watermarks': [
                    Decimal('110'),
                    Decimal('120')
                ],
                'expected_results': [
                    # Results at time 110
                    {'time': Decimal('110'), 'test_sum_1': 60, 'test_sum_2': 60},  # sum of 10, 20, 30
                    
                    # Results at time 115
                    {'time': Decimal('115'), 'test_sum_1': 70, 'test_sum_2': 100},  # sum of 30, 40 and sum of 10, 20, 30, 40
                    
                    # Results at time 120
                    {'time': Decimal('120'), 'test_sum_1': 90, 'test_sum_2': 140},  # sum of 40, 50 and sum of 20, 30, 40, 50
                ]
            }
        },
        
        "multi_key_multi_window_multi_watermark": {
            "configs": [
                SlidingWindowConfig(
                    duration='10s',
                    agg_type=AggregationType.SUM,
                    agg_on_func=lambda x: x,
                    name="test_sum_1",
                    fixed_interval='5s'
                ),
                SlidingWindowConfig(
                    duration='15s',
                    agg_type=AggregationType.SUM,
                    agg_on_func=lambda x: x,
                    name="test_sum_2",
                    fixed_interval='5s'
                )
            ],
            "test_data": {
                'events': [
                    ('user1', 10, Decimal('100')),
                    ('user2', 15, Decimal('100')),
                    ('user1', 20, Decimal('103')),
                    ('user2', 25, Decimal('103')),
                    ('user1', 30, Decimal('107')),
                    ('user2', 35, Decimal('107')),
                    ('user1', 40, Decimal('112')),
                    ('user2', 45, Decimal('112')),
                    ('user1', 50, Decimal('118')),
                    ('user2', 55, Decimal('118')),
                    ('user1', 60, Decimal('122')),
                    ('user2', 65, Decimal('122')),
                ],
                'watermarks': [
                    Decimal('110'),
                    Decimal('120'),
                    Decimal('130')
                ],
                'expected_results': [
                    # Results at time 110
                    {'time': Decimal('110'), 'stream_name': 'user1', 'test_sum_1': 60, 'test_sum_2': 60},
                    {'time': Decimal('110'), 'stream_name': 'user2', 'test_sum_1': 75, 'test_sum_2': 75},
                    
                    # Results at time 115
                    {'time': Decimal('115'), 'stream_name': 'user1', 'test_sum_1': 70, 'test_sum_2': 100},
                    {'time': Decimal('115'), 'stream_name': 'user2', 'test_sum_1': 80, 'test_sum_2': 120},
                    
                    # Results at time 120
                    {'time': Decimal('120'), 'stream_name': 'user1', 'test_sum_1': 90, 'test_sum_2': 140},
                    {'time': Decimal('120'), 'stream_name': 'user2', 'test_sum_1': 100, 'test_sum_2': 160},
                    
                    # Results at time 125
                    {'time': Decimal('125'), 'stream_name': 'user1', 'test_sum_1': 110, 'test_sum_2': 180},
                    {'time': Decimal('125'), 'stream_name': 'user2', 'test_sum_1': 120, 'test_sum_2': 200},
                ]
            }
        }
    }
    
    def create_watermark(self, event_time: Decimal) -> Record:
        """Helper to create a watermark record"""
        record = Record(value=None, event_time=event_time)
        record.is_watermark = True
        return record
    
    def create_key_record(self, key: Any, value: Any, event_time: Decimal) -> KeyRecord:
        """Helper to create a keyed record"""
        return KeyRecord(key=key, value=value, event_time=event_time)
    
    def test_fixed_interval(self, test_case_name):
        """
        Test fixed interval windows with the given test case.
        
        Args:
            test_case_name: Name of the test case to run
        """
        test_case = self.TEST_CASES[test_case_name]
        configs = test_case["configs"]
        test_data = test_case["test_data"]
        
        events = test_data['events']
        watermarks = test_data['watermarks']
        expected_results = test_data['expected_results']
        
        # Extract unique keys from events
        keys = sorted(set(key for key, _, _ in events))
        num_keys = len(keys)
        num_windows = len(configs)
        
        print(f"\nRunning test case: {test_case_name}")
        print(f"  {num_keys} keys, {num_windows} windows, {len(watermarks)} watermarks")
        
        # Create operator and collector
        operator = WindowOperator(configs)
        collector = TestCollector()
        operator.open([collector], None)
        
        # Process events and watermarks in time order
        all_events = [(time, 'event', (key, value, time)) for key, value, time in events]
        all_watermarks = [(time, 'watermark', time) for time in watermarks]
        all_actions = sorted(all_events + all_watermarks)
        
        for time, action_type, action_data in all_actions:
            if action_type == 'event':
                key, value, event_time = action_data
                print(f"  Processing event: key={key}, value={value}, time={event_time}")
                operator.process_element(self.create_key_record(key, value, event_time))
            else:  # watermark
                watermark_time = action_data
                print(f"  Sending watermark at time {watermark_time}")
                operator.process_element(self.create_watermark(watermark_time))
        
        # Verify results
        # Filter out the watermarks
        results = [r for r in collector.collected_records if not hasattr(r, 'is_watermark')]
        print(f"  Collected {len(results)} result records")
        
        # Convert results to a comparable format
        actual_results = []
        for record in results:
            result = {'time': record.event_time}
            
            # Add stream_name if it exists
            if hasattr(record, 'stream_name') and record.stream_name:
                result['stream_name'] = record.stream_name
                
            # Add window values
            for config in configs:
                window_name = config.name
                if window_name in record.value:
                    result[window_name] = record.value[window_name]
            
            actual_results.append(result)
            print(f"  Result: {result}")
        
        # Sort both expected and actual results for comparison
        # Sort by time first, then by stream_name if present
        def sort_key(result):
            return (result['time'], result.get('stream_name', ''))
        
        expected_results_sorted = sorted(expected_results, key=sort_key)
        actual_results_sorted = sorted(actual_results, key=sort_key)
        
        # Verify results match expectations
        self.assertEqual(len(actual_results_sorted), len(expected_results_sorted),
                        f"[{test_case_name}] Expected {len(expected_results_sorted)} results, got {len(actual_results_sorted)}")
        
        for i, (expected, actual) in enumerate(zip(expected_results_sorted, actual_results_sorted)):
            self.assertEqual(expected, actual, 
                            f"[{test_case_name}] Result {i} doesn't match expected value.\n"
                            f"Expected: {expected}\nActual: {actual}")
        
        # Clean up
        operator.close()
        
        return results
    
    def test_window_configurations(self):
        """Test all window configurations defined in TEST_CASES."""
        failed_test_cases = []
        # for test_case_name in self.TEST_CASES:
        for test_case_name in ['single_key_multi_window']:
            try:
                self.test_fixed_interval(test_case_name)
                print(f"✅ Test case '{test_case_name}' passed")
            except Exception as e:
                print(f"❌ Test case '{test_case_name}' failed: {str(e)}")
                failed_test_cases.append((test_case_name, str(e)))
                # raise  # Re-raise the exception to fail the test

        if len(failed_test_cases) > 0:
            error_message = ""
            for test_case, error in failed_test_cases:
                error_message += f"  {test_case}: {error}\n"
            raise Exception(f"{[f[0] for f in failed_test_cases]} test cases failed: {error_message}")


if __name__ == '__main__':
    # unittest.main()
    test = TestWindowOperator()
    # test.test_event_based_window()
    test.test_window_configurations()   