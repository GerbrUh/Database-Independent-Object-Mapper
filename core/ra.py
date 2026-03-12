from typing import Any, Dict, List, Optional, Tuple, Type, Union
from pydantic import BaseModel, create_model
import re

class RA:
    @staticmethod
    def _normalize_input(data):
        if isinstance(data, type) and issubclass(data, BaseModel):
            return [r.model_dump() for r in data.find_all()]

        if isinstance(data, List):
            normalized = []
            for item in data:
                if isinstance(item, BaseModel):
                    normalized.append(item.model_dump())
                elif isinstance(item, dict):
                    normalized.append(item)
                else:
                    raise TypeError(f"Unsupported item type in list: {type(item)}")
            return normalized

        raise TypeError(f"Unsupported input type: {type(data)}")
    
    @staticmethod
    def _get_field_types(data):
        if isinstance(data, type) and issubclass(data, BaseModel):
            return {name: field.annotation for name, field in data.model_fields.items()}

        if isinstance(data, list) and data:
            first = data[0]

            if isinstance(first, BaseModel):
                return {name: field.annotation for name, field in first.model_fields.items()}

            if isinstance(first, dict):
                return {k: Any for k in first.keys()}
            
        return {}


    @staticmethod
    def Union_op(a, b):

        data_a = RA._normalize_input(a)
        data_b = RA._normalize_input(b)

        fields_a = RA._get_field_types(a)
        fields_b = RA._get_field_types(b)
        print(data_a, data_b)

        if fields_a != fields_b:
            raise ValueError(
                f"Union requires matching fields.\n"
                f"Left fields:  {fields_a}\n"
                f"Right fields: {fields_b}"
            )

        JoinModel = create_model("RA", **fields_a)
        results = []
        
        for item in data_a:
            results.append(JoinModel(**item))
        for item in data_b:
            results.append(JoinModel(**item))
        return results

    @staticmethod
    def Difference(a, b):

        data_a = RA._normalize_input(a)
        data_b = RA._normalize_input(b)

        fields_a = RA._get_field_types(a)
        fields_b = RA._get_field_types(b)


        if fields_a != fields_b:
            raise ValueError(
                f"Union requires matching fields.\n"
                f"Left fields:  {fields_a}\n"
                f"Right fields: {fields_b}"
            )

        JoinModel = create_model("RA", **fields_a)
        results = []
        
        set_b = {tuple(sorted(item.items())) for item in data_b}

        for item in data_a:
            key = tuple(sorted(item.items()))
            if key not in set_b:
                results.append(JoinModel(**item))
        
        return results
    
    @staticmethod
    def Intersect(a, b):

        data_a = RA._normalize_input(a)
        data_b = RA._normalize_input(b)

        fields_a = RA._get_field_types(a)
        fields_b = RA._get_field_types(b)

        if fields_a != fields_b:
            raise ValueError(
                f"Union requires matching fields.\n"
                f"Left fields:  {fields_a}\n"
                f"Right fields: {fields_b}"
            )

        JoinModel = create_model("RA", **fields_a)
        results = []
        
        set_b = {tuple(sorted(item.items())) for item in data_b}
        for item in data_a:
            key = tuple(sorted(item.items()))
            if key in set_b:
                results.append(JoinModel(**item))
        
        return results
    
    @staticmethod
    def Projection(a, fields: List):
        data_a = RA._normalize_input(a)
        field_a = RA._get_field_types(a)

        missing = [f for f in fields if f not in field_a]
        if missing:
            raise ValueError(
                f"Projection fields not found: {missing}\n"
                f"Available fields: {list(field_a.keys())}"
            )
        
        fields_dict = {key: field_a[key] for key in fields}
        JoinModel = create_model("RA", **fields_dict)
        results = []

        for item in data_a:
            results.append(JoinModel(**item))

        return results

    @staticmethod
    def Rename(a, renames: dict[str, str]):
        data = RA._normalize_input(a)
        field_types = RA._get_field_types(a)

        missing = [old for old in renames.keys() if old not in field_types]
        if missing:
            raise ValueError(
                f"Cannot rename missing fields: {missing}\n"
                f"Available fields: {list(field_types.keys())}"
            )

        new_fields = {}
        for old, typ in field_types.items():
            if old in renames:
                new_fields[renames[old]] = typ
            else:
                new_fields[old] = typ

        JoinModel = create_model("RA", **new_fields)
        results = []

        for item in data:
            new_item = {}
            for old_key, value in item.items():
                new_key = renames.get(old_key, old_key)
                new_item[new_key] = value
            results.append(JoinModel(**new_item))

        return results

    @staticmethod
    def CartesianJoin(a, b):
        data_a = RA._normalize_input(a)
        data_b = RA._normalize_input(b)

        fields_a = RA._get_field_types(a)
        fields_b = RA._get_field_types(b)

        overlap = set(fields_a.keys()) & set(fields_b.keys())
        if overlap:
            raise ValueError(
                f"Cartesian join requires unique field names in each input.\n"
                f"Overlapping fields: {overlap}"
            )

        fields = {**fields_a, **fields_b}

        JoinModel = create_model("RA", **fields)

        results = []
        for row_a in data_a:
            for row_b in data_b:
                merged = {**row_a, **row_b}
                results.append(JoinModel(**merged))

        return results

    @staticmethod
    def NaturalJoin(a, b):
        data_a = RA._normalize_input(a)
        data_b = RA._normalize_input(b)

        fields_a = RA._get_field_types(a)
        fields_b = RA._get_field_types(b)

        shared_fields = list(set(fields_a.keys()) & set(fields_b.keys()))

        if shared_fields:
            for f in shared_fields:
                if f in fields_a and f in fields_b:
                    if fields_a[f] is not Any and fields_b[f] is not Any:
                        if fields_a[f] != fields_b[f]:
                            raise TypeError(
                                f"NaturalJoin type mismatch on field '{f}': "
                                f"{fields_a[f]} vs {fields_b[f]}"
                            )

            fields = {**fields_a, **fields_b}
            JoinModel = create_model("RA", **fields)

            results = []
            lookup = {}

            for row_b in data_b:
                key = tuple(row_b[f] for f in shared_fields)
                lookup.setdefault(key, []).append(row_b)
                
            for row_a in data_a:
                key = tuple(row_a[f] for f in shared_fields)
                if key in lookup:
                    for row_b in lookup[key]:
                        combined = {**row_a, **row_b}
                        results.append(JoinModel(**combined))

            return results
        
        else:
            fields = {**fields_a, **fields_b}
            JoinModel = create_model("RA", **fields)
            results = []

            for row_a in data_a:
                for row_b in data_b:
                    results.append(JoinModel(**{**row_a, **row_b}))

            return results

    @staticmethod
    def Join(a, b, match_fields: Optional[list[tuple[str, str]]] = None):
        data_a = RA._normalize_input(a)
        data_b = RA._normalize_input(b)
        fields_a = RA._get_field_types(a)
        fields_b = RA._get_field_types(b)

        if match_fields:
            for lf, rf in match_fields:
                if lf not in fields_a:
                    raise ValueError(f"Left join field '{lf}' not found in A.")
                if rf not in fields_b:
                    raise ValueError(f"Right join field '{rf}' not found in B.")

        else:
            shared = list(set(fields_a.keys()) & set(fields_b.keys()))
            match_fields = [(f, f) for f in shared]

        if not match_fields:
            fields = {**fields_a, **fields_b}
            JoinModel = create_model("RA", **fields)
            return [JoinModel(**({**ra, **rb})) for ra in data_a for rb in data_b]
        
        fields = {**fields_a, **fields_b}
        JoinModel = create_model("RA", **fields)

        lookup = {}
        for row_b in data_b:
            key = tuple(row_b[rf] for _, rf in match_fields)
            lookup.setdefault(key, []).append(row_b)

        results = []
        for row_a in data_a:
            key = tuple(row_a[lf] for lf, _ in match_fields)
            if key in lookup:
                for row_b in lookup[key]:
                    combined = {**row_a, **row_b}
                    results.append(JoinModel(**combined))

        return results
   

    @staticmethod
    def Select(
        data: Union[type, List[dict], List[Any]],
        query: Dict[str, Any],
        *,
        multiple: bool = True,
    ):
        if isinstance(data, type) and hasattr(data, "find_by_field"):
            return data.find_by_field(
                query,
                multiple=multiple,
            )

        rows = RA._normalize_input(data)
        results = []

        def match_value(field_val: Any, condition: Any) -> bool:
            if isinstance(condition, dict):
                if "$sub" in condition:
                    sub_val = str(condition["$sub"])
                    case_insensitive = condition.get("case", True)
                    if not isinstance(field_val, str):
                        field_val = str(field_val)
                    return (
                        sub_val.lower() in field_val.lower()
                        if case_insensitive
                        else sub_val in field_val
                    )
                if "$regex" in condition:
                    pattern = condition["$regex"]
                    flags = re.IGNORECASE if condition.get("$options", "") == "i" else 0
                    return bool(re.search(pattern, str(field_val), flags))
                for op, val in condition.items():
                    if op == "$eq" and not field_val == val:
                        return False
                    if op == "$ne" and not field_val != val:
                        return False
                    if op == "$gt" and not field_val > val:
                        return False
                    if op == "$gte" and not field_val >= val:
                        return False
                    if op == "$lt" and not field_val < val:
                        return False
                    if op == "$lte" and not field_val <= val:
                        return False
                    if op == "$in" and field_val not in val:
                        return False
                    if op == "$nin" and field_val in val:
                        return False
                return True
            else:
                return str(field_val) == str(condition)

        for row in rows:
            if not isinstance(row, dict):
                if hasattr(row, "dict"):
                    row = row.dict()
                else:
                    continue

            for field, _ in query.items():
                if not field in row:
                     raise ValueError(f"Field {field} is not present in the row: {row.keys()}")

            if all(
                match_value(row.get(field), condition)
                for field, condition in query.items()
            ):
                results.append(row)
                if not multiple:
                    break

        if isinstance(data, list) and data and hasattr(data[0], "__class__") and hasattr(data[0], "dict"):
            Model = type(data[0])
            return [Model(**r) for r in results] if results else None
        else:
            return results

    @staticmethod
    def Distinct(data, fields: Optional[List[str]] = None, keep: str = "first"):
        rows = RA._normalize_input(data)

        field_types = RA._get_field_types(data)

        if fields is not None:
            invalid = [f for f in fields if f not in rows[0]]
            if invalid:
                raise ValueError(f"Invalid distinct fields: {invalid}")

        seen = {}
        for item in rows:
            if fields:
                key = tuple((f, item.get(f)) for f in fields)
            else:
                key = tuple(sorted(item.items()))

            if keep == "first":
                if key not in seen:
                    seen[key] = item
            elif keep == "last":
                seen[key] = item
            else:
                raise ValueError(f"Invalid 'keep' value: {keep}. Use 'first' or 'last'.")

        JoinModel = create_model("RA", **field_types)
        results = [JoinModel(**item) for item in seen.values()]

        return results

    @staticmethod
    def Order(data, order_by: List[Union[str, tuple]]):
        rows = RA._normalize_input(data)
        field_types = RA._get_field_types(data)

        if not rows:
            return []

        order_spec = []
        for item in order_by:
            if isinstance(item, str):
                field, direction = item, "asc"
            elif isinstance(item, (tuple, list)) and len(item) == 2:
                field, direction = item
            else:
                raise ValueError(f"Invalid order_by item: {item}")

            if field not in rows[0]:
                raise ValueError(f"Invalid field for ordering: {field}")
            if direction.lower() not in ("asc", "desc"):
                raise ValueError(f"Invalid sort direction: {direction}")
            order_spec.append((field, direction.lower()))

        for field, direction in reversed(order_spec):
            reverse = direction == "desc"
            rows.sort(key=lambda x: (x.get(field) is None, x.get(field)), reverse=reverse)

        JoinModel = create_model("RA", **field_types)
        results = [JoinModel(**item) for item in rows]
        return results


