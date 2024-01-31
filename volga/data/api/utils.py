import typing


def is_optional(type_):
    return (
        typing.get_origin(type_) is typing.Union
        and type(None) is typing.get_args(type_)[1]
    )