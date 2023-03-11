import sqlite3
from inspect import get_annotations
from typing import Any

from bookkeeper.repository.abstract_repository import AbstractRepository, T


class SQliteRepository(AbstractRepository[T]):
    def __init__(self, db_file: str, cls: type) -> None:
        self.db_file = db_file
        self.table_name = cls.__name__.lower()
        self.type = cls
        self.fields = get_annotations(cls, eval_str=True)
        self.fields.pop('pk')

    def add(self, obj: T) -> int:
        if getattr(obj, 'pk', None) != 0:
            raise ValueError(f'trying to add object {obj} with filled `pk` attribute')
        names = ', '.join(self.fields.keys())
        p = ', '.join("?" * len(self.fields))
        values = [getattr(obj, x) for x in self.fields]
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
        cur.execute('PRAGMA foreign_keys = ON')
        cur.execute(
            f'INSERT INTO {self.table_name} ({names}) VALUES ({p})',
            values
        )
        obj.pk = cur.lastrowid
        cur.execute('COMMIT')
        con.close()
        return obj.pk

    def get(self, pk: int) -> T | None:
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
        cur.execute(f'SELECT *, rowid FROM {self.table_name} WHERE rowid={pk}')
        result = None
        for row in cur:
            result = self.type(*row)
        con.close()
        return result

    def get_all(self, where: dict[str, Any] | None = None) -> list[T]:
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
        if where is None:
            cur.execute(f'SELECT *, rowid FROM {self.table_name}')
        else:
            condition = ", ".join([f"{key}=\"{value}\"" for (key, value) in where.items()])
            cur.execute(f'SELECT *, rowid FROM {self.table_name} WHERE {condition}')
        result = []
        for row in cur:
            record = self.type(*row)
            result.append(record)
        con.close()
        return result

    def update(self, obj: T) -> None:
        if obj.pk == 0:
            raise ValueError('attempt to update object with unknown primary key')
        names = ', '.join(self.fields.keys())
        p = ', '.join("?" * len(self.fields))
        values = [getattr(obj, x) for x in self.fields]
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
        cur.execute(
            f'UPDATE {self.table_name} SET ({names}) = ({p}) WHERE rowid={obj.pk}',
            values
        )
        cur.execute('COMMIT')
        con.close()

    def delete(self, pk: int) -> None:
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
        cur.execute(f'DELETE FROM {self.table_name} WHERE rowid={pk}')
        cur.execute('COMMIT')
        con.close()
