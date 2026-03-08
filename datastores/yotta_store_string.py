from front_end.connect import yottadb
import json
import uuid
from core.template import TemplateClass
from typing import Optional, List, TypeVar, Type
from pydantic import Field, PrivateAttr
from typing import *
import re

T = TypeVar('T', bound='YottaDocument')

class YottaDocument(TemplateClass): 
    _root_node: Optional[str] = PrivateAttr()
    
    @classmethod
    def purge_data(cls):
        yottadb.Key(cls.get_root_node()).delete_tree()

    @classmethod
    def get_root_node(cls):
        return "^" + str(cls.__name__)
    
    @classmethod
    def _field_names(cls) -> list[str]:
        return list(cls.model_fields.keys())


    @classmethod
    def _serialize(cls, obj: "YottaDocument") -> str:
        values = []
        for field in cls._field_names():
            val = getattr(obj, field, "")
            values.append("" if val is None else str(val))
        return "^".join(values)


    @classmethod
    def _deserialize(cls, raw: str) -> dict:
        parts = raw.split("^")
        fields = cls._field_names()
        data = {}
        for i, field in enumerate(fields):
            data[field] = parts[i] if i < len(parts) and parts[i] != "" else None
        return data
    

    def save(self):
        if not self.id:
            self.id = str(uuid.uuid4())

        node = yottadb.Key(self.__class__.get_root_node())[str(self.id)]
        node.value = self.__class__._serialize(self)
        return self

    def delete(self):
        yottadb.Key(self.__class__.get_root_node())[str(self.id)].delete_tree()

    @classmethod
    def find_by_id(cls, id: str):
        node = yottadb.Key(cls.get_root_node())[str(id)]
        raw = node.value

        if not raw:
            return None

        if isinstance(raw, bytes):
            raw = raw.decode()

        data = cls._deserialize(raw)
        return cls(**data)

    @classmethod
    def find_all(cls) -> list["YottaDocument"]:
        results = []
        root = yottadb.Key(cls.get_root_node())

        for sub in root.subscripts:
            id_str = sub.decode() if isinstance(sub, bytes) else str(sub)
            obj = cls.find_by_id(id_str)
            if obj:
                results.append(obj)

        return results


    @classmethod
    def find_by_field(
        cls,
        query: dict[str, Any],
        *,
        ref_map: Optional[dict[str, type["YottaDocument"]]] = None,
        multiple: bool = True,
        indexed: bool = False,
    ) -> Union[Optional[T], List[T]]:

        if not isinstance(query, dict):
            raise ValueError("Query must be a dict")

        results: list[T] = []
        root = yottadb.Key(cls.get_root_node())
        fields = cls._field_names()

        for sub in root.subscripts:
            id_str = sub.decode() if isinstance(sub, bytes) else str(sub)
            raw = root[id_str].value

            if not raw:
                continue

            if isinstance(raw, bytes):
                raw = raw.decode()

            parts = raw.split("^")

            match = True

            for field, condition in query.items():
                if field not in fields:
                    raise ValueError(f"Invalid field: {field}")

                idx = fields.index(field)
                field_val = parts[idx] if idx < len(parts) else ""

                if isinstance(condition, dict):
                    for op, val in condition.items():
                        if op == "$sub":
                            flags = re.IGNORECASE
                            if not re.search(re.escape(val), field_val, flags):
                                match = False
                                break

                        elif op == "$regex":
                            if not re.search(val, field_val):
                                match = False
                                break

                        elif op in {"$gt", "$gte", "$lt", "$lte", "$eq", "$ne"}:
                            try:
                                num_val = float(field_val)
                            except ValueError:
                                match = False
                                break

                            if op == "$eq" and num_val != val:
                                match = False
                            elif op == "$ne" and num_val == val:
                                match = False
                            elif op == "$gt" and num_val <= val:
                                match = False
                            elif op == "$gte" and num_val < val:
                                match = False
                            elif op == "$lt" and num_val >= val:
                                match = False
                            elif op == "$lte" and num_val > val:
                                match = False
                        else:
                            raise ValueError(f"Unsupported operator: {op}")

                    if not match:
                        break
                else:
                    if str(field_val) != str(condition):
                        match = False
                        break

            if match:
                obj = cls.find_by_id(id_str)
                if obj:
                    results.append(obj)
                    if not multiple:
                        return obj

        return results if multiple else (results[0] if results else None)

