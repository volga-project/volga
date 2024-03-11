from decimal import Decimal
from typing import List

import dateutil.parser

Duration = str


def duration_to_s(duration_string: Duration) -> Decimal:
    total_seconds = Decimal('0')
    prev_num: List[str] = []
    for character in duration_string:
        if character.isalpha():
            if prev_num:
                num = Decimal("".join(prev_num))
                if character == "y":
                    total_seconds += num * 365 * 24 * 60 * 60
                elif character == "w":
                    total_seconds += num * 7 * 24 * 60 * 60
                elif character == "d":
                    total_seconds += num * 60 * 60 * 24
                elif character == "h":
                    total_seconds += num * 60 * 60
                elif character == "m":
                    total_seconds += num * 60
                elif character == "s":
                    total_seconds += num
                else:
                    raise ValueError(f'Invalid character {character} in duration {duration_string}')
                prev_num = []
            elif character != ' ':
                raise ValueError(f'Invalid character {character} in duration {duration_string}')
        elif character.isnumeric() or character == '.':
            prev_num.append(character)
        elif character != ' ':
            raise ValueError(f'Invalid character {character} in duration {duration_string}')
    return total_seconds


def duration_to_ms(duration: Duration) -> Decimal:
    return duration_to_s(duration) * 1000


def datetime_str_to_ts(dt_str: str) -> Decimal:
    dt = dateutil.parser.isoparse(dt_str)
    return Decimal(dt.timestamp())
