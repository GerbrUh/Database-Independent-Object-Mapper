from abc import ABC, abstractmethod
from typing import Optional, Type, TypeVar, List, Tuple, Union, Dict, Any
from pydantic import BaseModel, Field
import json
from tqdm import tqdm

T = TypeVar('T', bound='TemplateClass')

class TemplateClass(BaseModel, ABC):
    id: Optional[str] = Field(default=None, alias="_id")

    class Config:
        validate_by_name = True

    @abstractmethod
    def save(self: T) -> None:
        pass

    @classmethod
    @abstractmethod
    def find_by_id(cls: Type[T], id: str) -> Optional[T]:
        pass

    @classmethod
    @abstractmethod
    def find_all(cls: Type[T]) -> List[T]:
        pass

    @classmethod
    @abstractmethod
    def find_by_field(cls: Type[T],
        query: Dict[str, Any],
        *,
        multiple: bool = True,
    ) -> Union[Optional[T], List[T]]:
        pass

    @abstractmethod
    def delete(self: T)-> None:
        pass

    @classmethod
    def mass_delete(cls: Type[T], ids: List[str]) -> None:
        for id in ids:
            instance = cls.find_by_id(id)
            if instance:
                instance.delete()

    @classmethod
    def mass_save(cls: Type[T], data: List[Tuple]) -> None:
        all_fields = list(cls.model_fields.keys())

        for i, item in enumerate(data):
            if len(item) not in (len(all_fields), len(all_fields) - 1):
                raise ValueError(
                    f"Row {i}: expected {len(all_fields)} or {len(all_fields) - 1} fields, got {len(item)} — {item}"
                )

            if len(item) == len(all_fields):
                obj_dict = {k: item.get(k) for k in all_fields}
            else:
                obj_dict["id"] = None  
                obj_dict = {k: item.get(k) for k in all_fields[1:]}

            instance = cls(**obj_dict)
            instance.save()
    
    @classmethod
    def save_to_json(cls: Type[T], json_path: str = 'data'):
        items = cls.find_all()
        item_data = [i.model_dump(by_alias=True, exclude_none=False) for i in items]
        file_path = f"{json_path}.json"
        with open(file_path, "w") as f:
            json.dump(item_data, f, indent=4)

        return file_path

    @classmethod
    def load_from_json(cls: Type[T], json_path: str = "data") -> List[T]:
        file_path = f"{json_path}.json"

        with open(file_path, "r") as f:
            raw_data = json.load(f)  
        cls.mass_save(raw_data)

    
    @classmethod
    def purge_data(cls: Type[T]):
        for item in tqdm(cls.find_all()):
            item.delete()


    
