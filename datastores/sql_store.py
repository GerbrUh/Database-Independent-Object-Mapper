
from front_end.connect import sql_engine as engine
from sqlmodel import Session, SQLModel, select
from sqlalchemy.dialects.sqlite import insert
from typing import TypeVar, Type, Optional, Union, List, Any, Tuple, Dict

from sqlalchemy import func
from sqlalchemy import or_, not_

from core.template import TemplateClass

import re

T = TypeVar('T', bound='SQLDocument')

class SQLDocument(SQLModel, TemplateClass):
    class Config:
        arbitrary_types_allowed = True

    def save(self: T) -> T:
        table = self.__table__
        pk_name = list(self.__mapper__.primary_key)[0].name  

        obj_dict = {c.name: getattr(self, c.name) for c in table.columns}

        stmt = insert(table).values(obj_dict)

        update_cols = {
            c.name: stmt.excluded[c.name]
            for c in table.columns
            if c.name != pk_name
        }

        stmt = stmt.on_conflict_do_update(
            index_elements=[pk_name],
            set_=update_cols
        )

        with Session(engine) as session:
            session.execute(stmt)
            session.commit()
            refreshed = session.get(type(self), getattr(self, pk_name))
            for c in table.columns:
                setattr(self, c.name, getattr(refreshed, c.name))

        return self


    @classmethod
    def find_by_id(cls: Type[T], id_value: str) -> Optional[T]:
        pk_name = None
        for name, field in cls.model_fields.items():  
            if getattr(field, "primary_key", False):
                pk_name = name
                break

        if pk_name is None:
            raise ValueError(f"No primary key found for {cls.__name__}")

        with Session(engine) as session:
            stmt = select(cls).where(getattr(cls, pk_name) == id_value)
            result = session.exec(stmt).first()
            return result

    @classmethod
    def find_all(cls: Type[T]) -> List[T]:
        with Session(engine) as session:
            return list(session.exec(select(cls)))

    def delete(self):
        cls = self.__class__

        pk_name = None
        for name, field in cls.model_fields.items():
            if getattr(field, "primary_key", False):
                pk_name = name
                break

        if pk_name is None:
            raise ValueError(f"No primary key found for {cls.__name__}")

        pk_value = getattr(self, pk_name)

        with Session(engine) as session:
            stmt = select(cls).where(getattr(cls, pk_name) == pk_value)
            obj = session.exec(stmt).first()

            if obj:
                session.delete(obj)
                session.commit()

    @classmethod
    def find_by_field_spec(cls: Type[T], field: str, value, multiple: bool = False) -> Optional[T] | List[T]:
        with Session(engine) as session:
            stmt = select(cls).where(getattr(cls, field) == value)
            if multiple:
                results = session.exec(stmt).all()
                return results
            else:
                return session.exec(stmt).first()

    @classmethod
    def find_by_field_sub(
        cls: Type[T],
        field: str,
        substring: str,
        case_insensitive: bool = True,
        multiple: bool = False
    ) -> Optional[List[T]]:
        column = getattr(cls, field, None)
        if column is None:
            raise ValueError(f"{field} is not a valid column of {cls.__name__}")

        with Session(engine) as session:
            if case_insensitive:
                stmt = select(cls).where(func.lower(column).like(f"%{substring.lower()}%"))
            else:
                stmt = select(cls).where(column.like(f"%{substring}%"))

            results = session.exec(stmt).all()

        if multiple:
            return results
        else:
            return results[0] if results else None
        
    @classmethod
    def find_by_field_num(
        cls: Type[T],
        field: str,
        condition: Dict[str, Any],
        multiple: bool = False,
    ) -> Union[Optional[T], List[T]]:
        if not isinstance(condition, dict):
            raise ValueError("condition must be a dict of MongoDB operators (e.g. {'$gt': 5})")

        column = getattr(cls, field, None)
        if column is None:
            raise ValueError(f"{field} is not a valid column of {cls.__name__}")

        filters = []

        for op, val in condition.items():
            if op == "$eq":
                filters.append(column == val)
            elif op == "$gt":
                filters.append(column > val)
            elif op == "$gte":
                filters.append(column >= val)
            elif op == "$lt":
                filters.append(column < val)
            elif op == "$lte":
                filters.append(column <= val)
            elif op == "$ne":
                filters.append(column != val)
            elif op == "$in":
                if not isinstance(val, list):
                    raise ValueError("$in value must be a list")
                filters.append(column.in_(val))
            elif op == "$nin":
                if not isinstance(val, list):
                    raise ValueError("$nin value must be a list")
                filters.append(not_(column.in_(val)))
            else:
                raise ValueError(f"Unsupported operator: {op}")

        stmt = select(cls).where(*filters)

        with Session(engine) as session:
            results = session.exec(stmt).all()

        if multiple:
            return results
        else:
            return results[0] if results else None

    @classmethod
    def find_by_field(
        cls: Type[T],
        query: Dict[str, Any],
        *,
        ref_map: Optional[dict[str, Type["SQLDocument"]]] = None,
        multiple: bool = True,
        indexed: bool = False,
    ) -> Union[
        Optional[T],
        List[T],
        List[tuple[T, Optional["SQLDocument"]]],
    ]:
        
        if not isinstance(query, dict):
            raise ValueError("Query must be a dict of field → condition/value")

        if ref_map is None:
            ref_map = {}

        _ = indexed

        normal_conditions: Dict[str, Any] = {}
        reference_lookups: List[tuple[str, str, Any]] = []

        for field, condition in query.items():
            if "." in field:
                class_name, inner_field = field.split(".", 1)
                if class_name not in ref_map:
                    raise ValueError(f"No reference class found for '{class_name}' in ref_map")
                reference_lookups.append((class_name, inner_field, condition))
            else:
                normal_conditions[field] = condition

        results: Optional[List[T]] = None

        for field, condition in normal_conditions.items():
            if isinstance(condition, dict):

                if "$sub" in condition:
                    res = cls.find_by_field_sub(
                        field,
                        condition["$sub"],
                        case_insensitive=condition.get("case", True),
                        multiple=True,
                    )

                elif "$regex" in condition:
                    pattern = condition["$regex"]
                    like = re.sub(r"\.\*", "%", pattern)
                    like = re.sub(r"\$", "", like)
                    res = cls.find_by_field_sub(
                        field,
                        like.replace("%", ""),
                        case_insensitive=True,
                        multiple=True,
                    )

                else:
                    res = cls.find_by_field_num(
                        field,
                        condition,
                        multiple=True,
                    )
            else:
                res = cls.find_by_field_spec(
                    field,
                    condition,
                    multiple=True,
                )

            results = res if results is None else [
                r for r in results if r in res
            ]

        if results is None:
            results = []

        if reference_lookups:
            paired_results: List[tuple[T, Optional["SQLDocument"]]] = []

            for class_name, inner_field, condition in reference_lookups:
                ref_cls = ref_map[class_name]

                if isinstance(condition, dict):
                    if "$sub" in condition:
                        ref_objs = ref_cls.find_by_field_sub(
                            inner_field,
                            condition["$sub"],
                            multiple=True,
                        )
                    elif "$regex" in condition:
                        ref_objs = ref_cls.find_by_field_sub(
                            inner_field,
                            condition["$regex"],
                            multiple=True,
                        )
                    else:
                        ref_objs = ref_cls.find_by_field_num(
                            inner_field,
                            condition,
                            multiple=True,
                        )
                else:
                    ref_objs = ref_cls.find_by_field_spec(
                        inner_field,
                        condition,
                        multiple=True,
                    )

                if not ref_objs:
                    continue

                ref_by_id = {obj.id: obj for obj in ref_objs}
                ref_id_field = f"{class_name.lower()}_id"

                main_objs = cls.find_by_field_spec(
                    ref_id_field,
                    list(ref_by_id.keys()),
                    multiple=True,
                )

                for obj in main_objs:
                    ref_id = getattr(obj, ref_id_field, None)
                    paired_results.append((obj, ref_by_id.get(ref_id)))

            return paired_results if multiple else (paired_results[:1] if paired_results else [])

        if not multiple:
            return results[0] if results else None
        return results
        
    @classmethod
    def get_upsert_column(cls):
        pk_cols = cls.__mapper__.primary_key
        if len(pk_cols) != 1:
            raise ValueError("Only single-column primary keys supported for upsert")
        return pk_cols[0].name

    @classmethod
    def mass_save(cls, rows: List[dict]) -> None:
        with Session(engine) as session:
            if not rows:
                return

            table = cls.__table__
            pk_name = cls.get_upsert_column()

            stmt = insert(table).values(rows)

            update_cols = {
                c.name: stmt.excluded[c.name]
                for c in table.columns
                if c.name != pk_name
            }

            stmt = stmt.on_conflict_do_update(
                index_elements=[pk_name],
                set_=update_cols
            )

            session.execute(stmt)
            session.commit()


    @classmethod
    def mass_delete(cls, ids: List) -> None:
        if not ids:
            return

        pk_name = None
        for name, field in cls.model_fields.items():
            if getattr(field, "primary_key", False):
                pk_name = name
                break

        if pk_name is None:
            raise ValueError(f"No primary key found for {cls.__name__}")

        pk_col = getattr(cls, pk_name)

        with Session(engine) as session:
            session.query(cls).filter(pk_col.in_(ids)).delete(
                synchronize_session=False
            )
            session.commit()



