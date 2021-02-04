import typing


def coalesce(*values):
    output: typing.List = [a for a in values if a is not None]
    return output[0]
