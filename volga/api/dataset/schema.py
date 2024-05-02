from dataclasses import dataclass
import datetime
from typing import Dict, Type, List


@dataclass
class Schema:
    keys: Dict[str, Type]
    values: Dict[str, Type]
    timestamp: str
    name: str = ""

    def to_dict(self) -> Dict[str, Type]:
        schema = {**self.keys, **self.values, self.timestamp: datetime.datetime}
        return schema

    def fields(self) -> List[str]:
        return (
            [x for x in self.keys.keys()]
            + [x for x in self.values.keys()]
            + [self.timestamp]
        )

    def get_type(self, field) -> Type:
        if field in self.keys:
            return self.keys[field]
        elif field in self.values:
            return self.values[field]
        elif field == self.timestamp:
            return datetime.datetime
        else:
            raise Exception(
                f"field `{field}` not found in schema of `{self.name}`"
            )
