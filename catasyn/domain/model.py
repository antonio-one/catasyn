import typing

TableId = typing.NewType("TableId", str)

ListOfTables = typing.NewType("ListOfTables", typing.List[TableId])
