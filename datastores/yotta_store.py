from front_end.connect import yottadb
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
    
    def save(self):
        if not self.id:
            self.id = str(uuid.uuid4())

        node = yottadb.Key(self.__class__.get_root_node())[str(self.id)]
        node.value = "exists"

        def save_field(node, prefix, value):
            if isinstance(value, dict):
                for subfield, subval in value.items():
                    save_field(node, f"{prefix}.{subfield}", subval)
            else:
                node[prefix].value = str(value)

        data = self.dict(by_alias=False, exclude_none=True)
        for field, value in data.items():
            save_field(node, field, value)


    def delete(self):
        yottadb.Key(self.__class__.get_root_node())[str(self.id)].delete_tree()

    @classmethod
    def find_by_id(cls, id: str):
        node = yottadb.Key(cls.get_root_node())[str(id)]

        sentinel = node.value
        if isinstance(sentinel, bytes):
            sentinel = sentinel.decode()
        if sentinel != "exists":
            return None

        flat_data = {}
        for sub in node.subscripts:
            sub_str = sub.decode() if isinstance(sub, bytes) else str(sub)
            value = node[sub_str].value
            if isinstance(value, bytes):
                value = value.decode()
            flat_data[sub_str] = value

        nested_data = {}
        for key, value in flat_data.items():
            parts = key.split(".")
            current = nested_data
            for p in parts[:-1]:
                if p not in current:
                    current[p] = {}
                current = current[p]
            current[parts[-1]] = value

        def instantiate(model_cls, data_dict):
            out = {}
            for field_name, field_info in model_cls.model_fields.items():
                if field_name not in data_dict:
                    continue
                val = data_dict[field_name]
                field_type = field_info.annotation
                if hasattr(field_type, "model_fields") and isinstance(val, dict):
                    out[field_name] = instantiate(field_type, val)
                else:
                    out[field_name] = val
            return model_cls(**out)

        return instantiate(cls, nested_data)

    @classmethod
    def find_all(cls) -> List["YottaDocument"]:
        results = []
        for subscript in yottadb.Key(cls.get_root_node()).subscripts:
            results.append(cls.find_by_id(subscript.decode()))
        return results


    @classmethod
    def find_by_field(
        cls: Type[T],
        query: dict[str, Any],
        *,
        multiple: bool = True,
    ) -> Union[Optional[T], List[T], List[tuple[T, Optional["YottaDocument"]]]]:
        if not isinstance(query, dict):
            raise ValueError("Query must be a dict of field → condition/value")

        results = []
        root = yottadb.Key(cls.get_root_node())

        for sub in root.subscripts:
            id_str = sub.decode() if isinstance(sub, bytes) else str(sub)
            node = root[id_str]

            sentinel = node.value
            if isinstance(sentinel, bytes):
                sentinel = sentinel.decode()
            if sentinel != "exists":
                continue

            decoded_subs = [s.decode() if isinstance(s, bytes) else str(s) for s in node.subscripts]
            field_data = {
                s: (node[s].value.decode() if isinstance(node[s].value, bytes) else node[s].value)
                for s in decoded_subs
            }

            obj = cls.find_by_id(id_str)
            if not obj:
                continue

            match = True
            referenced_instance = None

            for f, condition in query.items():
                field_val = None

                field_val = field_data.get(f, getattr(obj, f, None))

                if field_val is None:
                    match = False
                    break

                if isinstance(condition, dict):
                    for op, val in condition.items():
                        if op == "$sub":
                            case = condition.get("case", True)
                            flags = re.IGNORECASE if case else 0
                            if not re.search(re.escape(val), str(field_val), flags=flags):
                                match = False
                                break
                        elif op == "$regex":
                            if not re.search(val, str(field_val)):
                                match = False
                                break
                        elif op in {"$gt", "$gte", "$lt", "$lte", "$eq", "$ne", "$in", "$nin"}:
                            try:
                                num_val = float(field_val)
                            except (ValueError, TypeError):
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
                            elif op == "$in" and num_val not in val:
                                match = False
                            elif op == "$nin" and num_val in val:
                                match = False
                        else:
                            raise ValueError(f"Unsupported operator: {op}")

                        if not match:
                            break
                else:
                    if str(field_val) != str(condition):
                        match = False

                if not match:
                    break

            if match:
                if referenced_instance:
                    results.append((obj, referenced_instance))
                else:
                    results.append(obj)
                if not multiple:
                    return results[0]

        return results if multiple else (results[0] if results else None)

