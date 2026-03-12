from typing import Type, TypeVar, Optional, List, Dict, Any, Union
from sqlmodel import SQLModel
from sqlalchemy import select, delete, func, not_
from sqlalchemy.dialects.sqlite import insert 
from front_end.connect import sql_engine as engine
from core.template import TemplateClass
import re

T = TypeVar("T", bound="SQLDocument")


class SQLDocument(SQLModel, TemplateClass):

    @classmethod
    def _table(cls):
        return cls.__table__

    @classmethod
    def _pk(cls) -> str:
        for name, field in cls.model_fields.items():
            if getattr(field, "primary_key", False):
                return name
        raise ValueError(f"No primary key for {cls.__name__}")

    def save(self: T) -> T:
        table = self._table()
        pk = self._pk()

        data = {
            c.name: getattr(self, c.name)
            for c in table.columns
        }

        stmt = insert(table).values(data)

        update_cols = {
            c.name: stmt.excluded[c.name]
            for c in table.columns
            if c.name != pk
        }

        stmt = stmt.on_conflict_do_update(
            index_elements=[pk],
            set_=update_cols
        )

        with engine.begin() as conn:
            conn.execute(stmt)

    @classmethod
    def mass_save(cls, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return

        table = cls._table()
        pk = cls._pk()

        stmt = insert(table).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=[pk],
            set_={
                c.name: stmt.excluded[c.name]
                for c in table.columns
                if c.name != pk
            }
        )

        with engine.begin() as conn:
            conn.execute(stmt)

    @classmethod
    def find_by_id(cls: Type[T], id_value: Any) -> Optional[T]:
        table = cls._table()
        pk = cls._pk()
        with engine.connect() as conn:
            row = conn.execute(select(table).where(table.c[pk] == id_value)).mappings().first()
            return cls(**row) if row else None

    @classmethod
    def find_all(cls: Type[T]) -> List[T]:
        table = cls._table()
        with engine.connect() as conn:
            rows = conn.execute(select(table)).mappings().all()
            return [cls(**r) for r in rows]

    @classmethod
    def find_by_field_num(
        cls,
        field: str,
        condition: Dict[str, Any],
        multiple: bool = False
    ):
        col = cls._table().c.get(field)
        if col is None:
            raise ValueError(f"{field} not valid")

        ops = {
            "$eq": lambda v: col == v,
            "$gt": lambda v: col > v,
            "$gte": lambda v: col >= v,
            "$lt": lambda v: col < v,
            "$lte": lambda v: col <= v,
            "$ne": lambda v: col != v,
            "$in": lambda v: col.in_(v),
            "$nin": lambda v: not_(col.in_(v)),
        }

        filters = [ops[k](v) for k, v in condition.items()]

        stmt = select(cls._table()).where(*filters)

        with engine.connect() as conn:
            rows = conn.execute(stmt).mappings().all()

        return rows if multiple else (rows[0] if rows else None)

    @classmethod
    def find_by_field(
        cls,
        query: Dict[str, Any],
        *,
        multiple: bool = True,
    ):
        filters = []

        for field, condition in query.items():
            col = cls._table().c.get(field)
            if col is None:
                raise ValueError(f"{field} not valid")

            if isinstance(condition, dict):
                if "$sub" in condition:
                    filters.append(
                        func.lower(col).like(
                            f"%{condition['$sub'].lower()}%"
                        )
                    )
                elif "$regex" in condition:
                    like = re.sub(r"\.\*", "%", condition["$regex"])
                    filters.append(col.like(like))
                else:
                    filters.extend(
                        cls.find_by_field_num(
                            field, condition, multiple=True
                        )
                    )
            else:
                filters.append(col == condition)

        stmt = select(cls._table()).where(*filters)

        with engine.connect() as conn:
            rows = conn.execute(stmt).mappings().all()

        return rows if multiple else (rows[0] if rows else None)


    def delete(self) -> None:
        table = self._table()
        pk = self._pk()
        pk_val = getattr(self, pk)

        stmt = delete(table).where(table.c[pk] == pk_val)

        with engine.begin() as conn:
            conn.execute(stmt)

    @classmethod
    def mass_delete(cls, ids: List[Any]) -> None:
        if not ids:
            return

        table = cls._table()
        pk = cls._pk()

        stmt = delete(table).where(table.c[pk].in_(ids))

        with engine.begin() as conn:
            conn.execute(stmt)
