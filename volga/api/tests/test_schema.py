from datetime import datetime
import unittest
import pytest
from typing import Dict

from volga.api.aggregate import Count, Sum, Avg, Min, Max
from volga.api.entity import Entity, entity, field
from volga.api.operators import Join, GroupBy, Transform, Rename, Drop, DropNull, Assign


class TestSchema(unittest.TestCase):

    def test_left_join_different_keys(self):
        @entity
        class User:
            user_id: str = field(key=True)
            registered_at: datetime = field(timestamp=True)
            name: str

        @entity
        class Order:
            buyer_id: str = field(key=True)
            product_id: str = field(key=True)
            product_type: str
            purchased_at: datetime = field(timestamp=True)
            product_price: float

        @entity
        class UserOrder:
            user_id: str = field(key=True)
            registered_at: datetime = field(timestamp=True)
            name: str
            buyer_id: str
            product_id: str
            product_type: str
            product_price: float
            purchased_at: datetime

        join = Join(
            left=Entity(User),
            right=Entity(Order),
            how='left',
            left_on=['user_id'],
            right_on=['buyer_id']
        )
        joined_schema = join.schema()
        expected_schema = UserOrder._entity_metadata.schema()
        
        assert joined_schema.keys == expected_schema.keys
        assert joined_schema.timestamp == expected_schema.timestamp
        assert joined_schema.values == expected_schema.values

    def test_right_join(self):
        @entity
        class User:
            user_id: str = field(key=True)
            registered_at: datetime = field(timestamp=True)
            name: str

        @entity
        class Order:
            buyer_id: str = field(key=True)
            product_id: str = field(key=True)
            product_type: str
            purchased_at: datetime = field(timestamp=True)
            product_price: float

        @entity
        class OrderUser:
            buyer_id: str = field(key=True)
            purchased_at: datetime = field(timestamp=True)
            user_id: str
            registered_at: datetime
            name: str
            product_id: str
            product_type: str
            product_price: float

        join = Join(
            left=Entity(User),
            right=Entity(Order),
            how='right',
            left_on=['user_id'],
            right_on=['buyer_id']
        )
        joined_schema = join.schema()
        expected_schema = OrderUser._entity_metadata.schema()
        
        assert joined_schema.keys == expected_schema.keys
        assert joined_schema.timestamp == expected_schema.timestamp
        assert joined_schema.values == expected_schema.values

    def test_join_with_field_name_conflicts(self):
        @entity
        class UserWithType:
            user_id: str = field(key=True)
            registered_at: datetime = field(timestamp=True)
            name: str
            type: str

        @entity
        class OrderWithType:
            buyer_id: str = field(key=True)
            product_id: str = field(key=True)
            type: str
            purchased_at: datetime = field(timestamp=True)
            product_price: float

        @entity
        class UserOrderWithType:
            user_id: str = field(key=True)
            registered_at: datetime = field(timestamp=True)
            name: str
            left_type: str
            buyer_id: str
            product_id: str
            right_type: str
            product_price: float
            purchased_at: datetime

        join = Join(
            left=Entity(UserWithType),
            right=Entity(OrderWithType),
            how='left',
            left_on=['user_id'],
            right_on=['buyer_id']
        )
        joined_schema = join.schema()
        expected_schema = UserOrderWithType._entity_metadata.schema()
        
        assert joined_schema.keys == expected_schema.keys
        assert joined_schema.timestamp == expected_schema.timestamp
        assert joined_schema.values == expected_schema.values

    def test_stream_key_functions(self):
        @entity
        class User:
            user_id: str = field(key=True)
            registered_at: datetime = field(timestamp=True)
            name: str
            type: str = field(key=True)

        @entity
        class Order:
            buyer_id: str = field(key=True)
            order_id: str 
            type: str = field(key=True)
            purchased_at: datetime = field(timestamp=True)
            product_price: float

        # Test data
        left_element = {
            'user_id': '123',
            'registered_at': datetime(2024, 1, 1),
            'name': 'John',
            'type': 'premium'
        }
        right_element = {
            'buyer_id': '123',
            'order_id': 'order_1',
            'type': 'sale',
            'purchased_at': datetime(2024, 1, 2),
            'product_price': 99.99
        }

        # Test single key matching
        join = Join(
            left=Entity(User),
            right=Entity(Order),
            how='left',
            left_on=['user_id'],
            right_on=['buyer_id']
        )
        assert join._stream_left_key_func(left_element) == '123', \
            "Left key function should extract 'user_id' value"
        assert join._stream_right_key_func(right_element) == '123', \
            "Right key function should extract 'buyer_id' value"

        # Test composite keys
        join_composite = Join(
            left=Entity(User),
            right=Entity(Order),
            how='left',
            left_on=['user_id', 'type'],
            right_on=['buyer_id', 'type']
        )
        
        left_key = join_composite._stream_left_key_func(left_element)
        right_key = join_composite._stream_right_key_func(right_element)
        
        # Test that keys are order-independent
        expected_left = frozenset([('user_id', '123'), ('type', 'premium')])
        expected_right = frozenset([('buyer_id', '123'), ('type', 'sale')])
        assert left_key == expected_left, \
            "Left composite key should be a frozenset of (field, value) pairs"
        assert right_key == expected_right, \
            "Right composite key should be a frozenset of (field, value) pairs"
        
        # Test different key order produces same result
        join_reversed = Join(
            left=Entity(User),
            right=Entity(Order),
            how='left',
            left_on=['type', 'user_id'],  # Reversed order
            right_on=['type', 'buyer_id']  # Reversed order
        )
        assert join_composite._stream_left_key_func(left_element) == join_reversed._stream_left_key_func(left_element), \
            "Left key function should produce same result regardless of key order in join specification"
        assert join_composite._stream_right_key_func(right_element) == join_reversed._stream_right_key_func(right_element), \
            "Right key function should produce same result regardless of key order in join specification"

    def test_stream_join_function(self):
        @entity
        class User:
            user_id: str = field(key=True)
            registered_at: datetime = field(timestamp=True)
            name: str
            type: str

        @entity
        class Order:
            buyer_id: str = field(key=True)
            order_id: str = field(key=True)
            type: str
            purchased_at: datetime = field(timestamp=True)
            product_price: float

        # Test data
        left_element = {
            'user_id': '123',
            'registered_at': datetime(2024, 1, 1),
            'name': 'John',
            'type': 'premium'
        }
        right_element = {
            'buyer_id': '123',
            'order_id': 'order_1',
            'type': 'sale',
            'purchased_at': datetime(2024, 1, 2),
            'product_price': 99.99
        }

        # Test left join
        join = Join(
            left=Entity(User),
            right=Entity(Order),
            how='left',
            left_on=['user_id'],
            right_on=['buyer_id']
        )
        joined = join._stream_join_func(left_element, right_element)
        expected_joined = {
            'user_id': '123',  # Key from left
            'registered_at': datetime(2024, 1, 1),  # Timestamp from left
            'name': 'John',
            'left_type': 'premium',  # Prefixed due to conflict
            'buyer_id': '123',
            'order_id': 'order_1',
            'right_type': 'sale',  # Prefixed due to conflict
            'purchased_at': datetime(2024, 1, 2),
            'product_price': 99.99
        }
        assert joined == expected_joined, \
            "Left join should preserve left side key and timestamp, prefix conflicting fields, and include all fields from both sides"

        # Test right join
        join_right = Join(
            left=Entity(User),
            right=Entity(Order),
            how='right',
            left_on=['user_id'],
            right_on=['buyer_id']
        )
        joined_right = join_right._stream_join_func(left_element, right_element)
        expected_joined_right = {
            'buyer_id': '123',  # Key from right
            'purchased_at': datetime(2024, 1, 2),  # Timestamp from right
            'user_id': '123',
            'registered_at': datetime(2024, 1, 1),
            'name': 'John',
            'left_type': 'premium',  # Prefixed due to conflict
            'order_id': 'order_1',
            'right_type': 'sale',  # Prefixed due to conflict
            'product_price': 99.99
        }
        assert joined_right == expected_joined_right, \
            "Right join should preserve right side key and timestamp, prefix conflicting fields, and include all fields from both sides"

    def test_group_by_aggregate_schema(self):
        @entity
        class Order:
            user_id: str = field(key=True)
            product_id: str = field(key=True)
            purchased_at: datetime = field(timestamp=True)
            product_price: float
            quantity: int

        # Test simple count aggregation with single key
        @entity
        class OrderCount:
            user_id: str = field(key=True)
            purchased_at: datetime = field(timestamp=True)
            order_count: int

        group_by = GroupBy(Entity(Order), keys=['user_id'])
        agg = group_by.aggregate([
            Count(field='quantity', window='forever', into='order_count')
        ])
        
        assert agg.schema() == OrderCount._entity_metadata.schema(), \
            "Count aggregation should match OrderCount entity schema"

        # Test multiple aggregations with multiple keys
        @entity
        class OrderProductStats:
            user_id: str = field(key=True)
            product_id: str = field(key=True)
            purchased_at: datetime = field(timestamp=True)
            total_spent: float
            avg_price: float
            min_price: float
            max_price: float
            order_count: int

        group_by_multi = GroupBy(Entity(Order), keys=['user_id', 'product_id'])
        agg_multiple = group_by_multi.aggregate([
            Sum(field='product_price', window='forever', into='total_spent'),
            Avg(field='product_price', window='forever', into='avg_price'),
            Min(field='product_price', window='forever', into='min_price'),
            Max(field='product_price', window='forever', into='max_price'),
            Count(field='quantity', window='forever', into='order_count')
        ])
        
        assert agg_multiple.schema() == OrderProductStats._entity_metadata.schema(), \
            "Multiple aggregations with multiple keys should match OrderProductStats entity schema"

        # Test error cases
        with pytest.raises(ValueError, match='Aggregate expects at least one aggregation operation'):
            group_by.aggregate([])

        # Test key function
        test_event = {
            'user_id': 'user1',
            'product_id': 'prod1',
            'purchased_at': datetime(2024, 1, 1),
            'product_price': 10.0,
            'quantity': 2
        }

        # Test single key
        single_key = group_by._stream_key_by_func(test_event)
        assert single_key == 'user1', \
            "Single key should return the value directly"

        # Test multiple keys
        multi_key = group_by_multi._stream_key_by_func(test_event)
        expected_key = frozenset([('user_id', 'user1'), ('product_id', 'prod1')])
        assert multi_key == expected_key, \
            "Multiple keys should return frozenset of (key, value) pairs"

        # Test key order independence
        group_by_reversed = GroupBy(Entity(Order), keys=['product_id', 'user_id'])
        reversed_key = group_by_reversed._stream_key_by_func(test_event)
        assert reversed_key == multi_key, \
            "Key function should produce same result regardless of key order in GroupBy specification"

        # Test missing key
        with pytest.raises(RuntimeError, match='key .* not in event'):
            group_by._stream_key_by_func({'wrong_key': 'value'})

    def test_transform_schema(self):
        @entity
        class User:
            user_id: str = field(key=True)
            registered_at: datetime = field(timestamp=True)
            name: str
            age: int

        # Test schema preservation when no schema changes
        def uppercase_name(event: Dict) -> Dict:
            event['name'] = event['name'].upper()
            return event

        transform = Transform(Entity(User), func=uppercase_name)
        assert transform.schema() == User._entity_metadata.schema(), \
            "Transform should preserve schema when no new fields are added"

        # Test schema with new fields
        @entity
        class UserWithAgeGroup:
            user_id: str = field(key=True)
            registered_at: datetime = field(timestamp=True)
            name: str
            age: int
            age_group: str

        def add_age_group(event: Dict) -> Dict:
            event['age_group'] = 'adult' if event['age'] >= 18 else 'minor'
            return event

        transform_with_new_schema = Transform(
            Entity(User), 
            func=add_age_group,
            new_schema_dict={
                'name': str,
                'age': int,
                'age_group': str
            }
        )
        assert transform_with_new_schema.schema() == UserWithAgeGroup._entity_metadata.schema(), \
            "Transform should update schema when new fields are added"

    def test_transform_map_function(self):
        @entity
        class User:
            user_id: str = field(key=True)
            registered_at: datetime = field(timestamp=True)
            name: str
            age: int

        test_event = {
            'user_id': 'user1',
            'registered_at': datetime(2024, 1, 1),
            'name': 'john',
            'age': 25
        }

        # Test simple transform
        def uppercase_name(event: Dict) -> Dict:
            result = event.copy()
            result['name'] = result['name'].upper()
            return result

        transform = Transform(Entity(User), func=uppercase_name)
        result = transform._stream_map_func(test_event)
        expected = {
            'user_id': 'user1',
            'registered_at': datetime(2024, 1, 1),
            'name': 'JOHN',
            'age': 25
        }
        assert result == expected, \
            "Transform should correctly apply the mapping function"

        # Test transform with schema change
        def add_age_group(event: Dict) -> Dict:
            result = event.copy()
            result['age_group'] = 'adult' if event['age'] >= 18 else 'minor'
            return result

        transform_with_new = Transform(
            Entity(User), 
            func=add_age_group,
            new_schema_dict={
                'name': str,
                'age': int,
                'age_group': str
            }
        )
        result_with_new = transform_with_new._stream_map_func(test_event)
        expected_with_new = {
            'user_id': 'user1',
            'registered_at': datetime(2024, 1, 1),
            'name': 'john',
            'age': 25,
            'age_group': 'adult'
        }
        assert result_with_new == expected_with_new, \
            "Transform should correctly apply the mapping function and add new fields"

        # Test error cases
        with pytest.raises(ValueError, match='Input event fields .* do not match parent schema fields'):
            transform._stream_map_func({'invalid': 'event'})

        def invalid_transform(event: Dict) -> Dict:
            result = event.copy()
            result['invalid_field'] = 'value'
            return result

        transform_invalid = Transform(Entity(User), func=invalid_transform)
        with pytest.raises(ValueError, match='Output event fields .* do not match target schema fields'):
            transform_invalid._stream_map_func(test_event)

        def drop_field_transform(event: Dict) -> Dict:
            result = event.copy()
            del result['user_id']
            return result

        transform_missing = Transform(Entity(User), func=drop_field_transform)
        with pytest.raises(ValueError, match='Output event fields .* do not match target schema fields'):
            transform_missing._stream_map_func(test_event)

    def test_rename_schema(self):
        @entity
        class User:
            user_id: str = field(key=True)
            registered_at: datetime = field(timestamp=True)
            first_name: str
            last_name: str

        @entity
        class UserRenamed:
            id: str = field(key=True)
            created_at: datetime = field(timestamp=True)
            fname: str
            lname: str

        # Test renaming all fields including timestamp
        rename = Rename(
            Entity(User),
            columns={
                'user_id': 'id',
                'registered_at': 'created_at',
                'first_name': 'fname',
                'last_name': 'lname'
            }
        )
        assert rename.schema() == UserRenamed._entity_metadata.schema(), \
            "Rename should handle timestamp field renaming"

        # Test partial rename with timestamp
        rename_partial = Rename(
            Entity(User),
            columns={
                'registered_at': 'created_at',
                'first_name': 'fname'
            }
        )
        assert rename_partial.schema().timestamp == 'created_at', \
            "Rename should update timestamp field name"

    def test_rename_map_function(self):
        @entity
        class User:
            user_id: str = field(key=True)
            registered_at: datetime = field(timestamp=True)
            first_name: str
            last_name: str

        test_event = {
            'user_id': 'user1',
            'registered_at': datetime(2024, 1, 1),
            'first_name': 'John',
            'last_name': 'Doe'
        }

        # Test simple rename
        rename = Rename(
            Entity(User),
            columns={
                'user_id': 'uid',
                'first_name': 'fname',
                'last_name': 'lname'
            }
        )
        result = rename._stream_map_func(test_event)
        expected = {
            'uid': 'user1',
            'registered_at': datetime(2024, 1, 1),
            'fname': 'John',
            'lname': 'Doe'
        }
        assert result == expected, \
            "Rename should correctly map field names while preserving values"

        # Test error cases
        with pytest.raises(ValueError, match='Input event fields .* do not match parent schema fields'):
            rename._stream_map_func({'invalid': 'event'})

        # Test partial rename
        rename_partial = Rename(
            Entity(User),
            columns={'first_name': 'fname'}
        )
        result_partial = rename_partial._stream_map_func(test_event)
        expected_partial = {
            'user_id': 'user1',
            'registered_at': datetime(2024, 1, 1),
            'fname': 'John',
            'last_name': 'Doe'
        }
        assert result_partial == expected_partial, \
            "Rename should handle partial renames correctly"

    def test_drop_schema(self):
        @entity
        class User:
            user_id: str = field(key=True)
            registered_at: datetime = field(timestamp=True)
            first_name: str
            last_name: str
            age: int
            email: str

        # Test dropping multiple fields
        @entity
        class UserMinimal:
            user_id: str = field(key=True)
            registered_at: datetime = field(timestamp=True)
            first_name: str
            last_name: str

        drop = Drop(Entity(User), columns=['age', 'email'])
        assert drop.schema() == UserMinimal._entity_metadata.schema(), \
            "Drop should remove specified fields from schema"

        # Test error - dropping key field
        with pytest.raises(ValueError, match='Cannot drop key fields'):
            Drop(Entity(User), columns=['user_id']).schema()

        # Test error - dropping timestamp field
        with pytest.raises(ValueError, match='Cannot drop timestamp field'):
            Drop(Entity(User), columns=['registered_at']).schema()

    def test_drop_map_function(self):
        @entity
        class User:
            user_id: str = field(key=True)
            registered_at: datetime = field(timestamp=True)
            first_name: str
            last_name: str
            age: int
            email: str

        test_event = {
            'user_id': 'user1',
            'registered_at': datetime(2024, 1, 1),
            'first_name': 'John',
            'last_name': 'Doe',
            'age': 30,
            'email': 'john@example.com'
        }

        # Test dropping multiple fields
        drop = Drop(Entity(User), columns=['age', 'email'])
        result = drop._stream_map_func(test_event)
        expected = {
            'user_id': 'user1',
            'registered_at': datetime(2024, 1, 1),
            'first_name': 'John',
            'last_name': 'Doe'
        }
        assert result == expected, \
            "Drop should remove specified fields from event"

        # Test error - invalid input
        with pytest.raises(ValueError, match='Input event fields .* do not match parent schema fields'):
            drop._stream_map_func({'invalid': 'event'})

        # Test dropping non-existent field
        drop_nonexistent = Drop(Entity(User), columns=['nonexistent'])
        result_nonexistent = drop_nonexistent._stream_map_func(test_event)
        assert result_nonexistent == test_event, \
            "Drop should ignore non-existent fields"

    def test_dropnull_filter_function(self):
        @entity
        class User:
            user_id: str = field(key=True)
            registered_at: datetime = field(timestamp=True)
            first_name: str
            last_name: str
            age: int
            email: str

        base_event = {
            'user_id': 'user1',
            'registered_at': datetime(2024, 1, 1),
            'first_name': 'John',
            'last_name': 'Doe',
            'age': 30,
            'email': 'john@example.com'
        }

        # Test with specific columns
        drop_null = DropNull(Entity(User), columns=['first_name', 'email'])

        # Test event with no nulls
        assert drop_null._stream_filter_func(base_event), \
            "Should keep event with no nulls"

        # Test event with null in specified column
        event_with_null = base_event.copy()
        event_with_null['email'] = None
        assert not drop_null._stream_filter_func(event_with_null), \
            "Should drop event with null in specified column"

        # Test event with null in unspecified column
        event_with_other_null = base_event.copy()
        event_with_other_null['age'] = None
        assert drop_null._stream_filter_func(event_with_other_null), \
            "Should keep event with null in unspecified column"

        # Test with no columns specified (should check all value fields)
        drop_null_all = DropNull(Entity(User))

        # Test event with no nulls
        assert drop_null_all._stream_filter_func(base_event), \
            "Should keep event with no nulls when checking all columns"

        # Test event with any null
        event_with_any_null = base_event.copy()
        event_with_any_null['age'] = None
        assert not drop_null_all._stream_filter_func(event_with_any_null), \
            "Should drop event with any null when checking all columns"

        # Test error - invalid input
        with pytest.raises(ValueError, match='Input event fields .* do not match parent schema fields'):
            drop_null._stream_filter_func({'invalid': 'event'})

        # Test with non-existent columns (should ignore them)
        drop_null_nonexistent = DropNull(Entity(User), columns=['nonexistent'])
        assert drop_null_nonexistent._stream_filter_func(base_event), \
            "Should ignore non-existent columns"

    def test_assign_schema(self):
        @entity
        class User:
            user_id: str = field(key=True)
            registered_at: datetime = field(timestamp=True)
            first_name: str
            last_name: str
            age: int

        # Test adding new column
        @entity
        class UserWithFullName:
            user_id: str = field(key=True)
            registered_at: datetime = field(timestamp=True)
            first_name: str
            last_name: str
            age: int
            full_name: str

        assign = Assign(
            Entity(User),
            column='full_name',
            output_type=str,
            func=lambda e: f"{e['first_name']} {e['last_name']}"
        )
        assert assign.schema() == UserWithFullName._entity_metadata.schema(), \
            "Assign should add new column to schema"

        # Test updating existing column
        assign_existing = Assign(
            Entity(User),
            column='age',
            output_type=float,
            func=lambda e: float(e['age'])
        )
        assert assign_existing.schema().values['age'] == float, \
            "Assign should update type of existing column"

        # Test error - assigning to key field
        with pytest.raises(ValueError, match='Cannot assign to key field'):
            Assign(
                Entity(User),
                column='user_id',
                output_type=str,
                func=lambda e: e['user_id']
            ).schema()

        # Test error - assigning to timestamp field
        with pytest.raises(ValueError, match='Cannot assign to timestamp field'):
            Assign(
                Entity(User),
                column='registered_at',
                output_type=datetime,
                func=lambda e: e['registered_at']
            ).schema()

    def test_assign_map_function(self):
        @entity
        class User:
            user_id: str = field(key=True)
            registered_at: datetime = field(timestamp=True)
            first_name: str
            last_name: str
            age: int

        test_event = {
            'user_id': 'user1',
            'registered_at': datetime(2024, 1, 1),
            'first_name': 'John',
            'last_name': 'Doe',
            'age': 30
        }

        # Test adding new column
        assign = Assign(
            Entity(User),
            column='full_name',
            output_type=str,
            func=lambda e: f"{e['first_name']} {e['last_name']}"
        )
        result = assign._stream_map_func(test_event)
        expected = {
            'user_id': 'user1',
            'registered_at': datetime(2024, 1, 1),
            'first_name': 'John',
            'last_name': 'Doe',
            'age': 30,
            'full_name': 'John Doe'
        }
        assert result == expected, \
            "Assign should add new column with computed value"

        # Test updating existing column
        assign_existing = Assign(
            Entity(User),
            column='age',
            output_type=float,
            func=lambda e: float(e['age']) + 0.5
        )
        result_existing = assign_existing._stream_map_func(test_event)
        expected_existing = {
            'user_id': 'user1',
            'registered_at': datetime(2024, 1, 1),
            'first_name': 'John',
            'last_name': 'Doe',
            'age': 30.5
        }
        assert result_existing == expected_existing, \
            "Assign should update existing column with computed value"

        # Test error - invalid input
        with pytest.raises(ValueError, match='Input event fields .* do not match parent schema fields'):
            assign._stream_map_func({'invalid': 'event'})

        # Test error - function raises exception
        assign_error = Assign(
            Entity(User),
            column='error_field',
            output_type=str,
            func=lambda e: e['nonexistent']
        )
        with pytest.raises(KeyError):
            assign_error._stream_map_func(test_event)


if __name__ == '__main__':
    unittest.main()
    # t = TestSchema()
    # t.test_stream_key_functions()
    # t.test_stream_join_function()
    # t.test_group_by_aggregate_schema()
    # t.test_transform_schema()
    # t.test_transform_map_function()
    # t.test_rename_schema()
    # t.test_rename_map_function()
    # t.test_drop_schema()
    # t.test_drop_map_function()
    # t.test_dropnull_filter_function()
    # t.test_assign_schema()
    # t.test_assign_map_function()
    # t.test_filter_schema()
    # t.test_filter_map_function()
