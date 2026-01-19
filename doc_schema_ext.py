# %%
from typing import Any, Set, List
from collections.abc import Mapping
from collections import defaultdict
import copy

from general_opr import _dict_to_json_serializable

class DataSchemaNode:

    list_data_type = "array"
    null_data_type = "null"
    mapping_data_type = "object"

    root_data_nm = "root"
    array_item_data_nm = "[]"

    def __init__(self, data_nm, data_type, parent_data_nm=None, parent_data_type=None):
        self.data_nm = data_nm
        self.data_type = data_type
        self.parent_data_nm = parent_data_nm
        self.parent_data_type = parent_data_type
        self.children: List[DataSchemaNode] = []

    def add_child(self, child):
        self.children.append(child)

    def to_dict(self):

        return {
            "data_nm": self.data_nm,
            "data_type": self.data_type,
            "parent_data_nm": self.parent_data_nm,
            "parent_data_type": self.parent_data_type,
            "children": [child.to_dict() for child in self.children],
        }


class FlattenSchemaItemModel:

    def __init__(self, path: str, data_type: str, parent_path: str = None, parent_data_type: str = None):
        self.path = path
        self.data_type = data_type
        self.parent_path = parent_path
        self.parent_data_type = parent_data_type

    def to_dict(self):
        return {
            "path": self.path,
            "data_type": self.data_type,
            "parent_path": self.parent_path,
            "parent_data_type": self.parent_data_type
        }

class DataSchemaModel:

    def __init__(
        self, path: str = None, 
        data_type: Set[str] = None, 
        nullable: bool = False,
        n_docs: int = 0, 
        contains_object: bool = False,
        contains_array: bool = False,
        parent_path: Set[str] = None, 
        parent_data_type: Set[str] = None
    ):

        self.path = path
        self.data_type = data_type if data_type is not None else set[str]()
        self.nullable = nullable
        self.contains_object = contains_object
        self.contains_array = contains_array
        self.n_docs = n_docs if n_docs is not None else 0
        self.parent_path = parent_path if parent_path is not None else set[str]()
        self.parent_data_type = parent_data_type if parent_data_type is not None else set[str]()

    def to_dict(self):
        return {
            "path": self.path,
            "data_type": self.data_type,
            "n_docs": self.n_docs,
            "nullable": self.nullable,
            "contains_object": self.contains_object,
            "contains_array": self.contains_array,
            # "parent_path": self.parent_path,
            # "parent_data_type": self.parent_data_type
        }



class DataSchemaUtils:

    @staticmethod
    def chk_list_data_type(obj: Any):
        return isinstance(obj, list)

    @staticmethod
    def chk_mapping_data_type(obj: Any):
        return isinstance(obj, Mapping)

    @staticmethod
    def get_data_type(obj: Any):

        if obj is None:
            return DataSchemaNode.null_data_type
        elif DataSchemaUtils.chk_list_data_type(obj):
            return DataSchemaNode.list_data_type
        elif DataSchemaUtils.chk_mapping_data_type(obj):
            return DataSchemaNode.mapping_data_type
        else:
            return type(obj).__name__

    @staticmethod
    def ext_schema_tree(
        obj: Any, data_nm: str = DataSchemaNode.root_data_nm,
        parent_data_nm: str = None,
        parent_data_type: str = None
    ) -> DataSchemaNode:

        if DataSchemaUtils.chk_mapping_data_type(obj):

            data_node = DataSchemaNode(
                data_nm=data_nm,
                data_type=DataSchemaUtils.get_data_type(obj),
                parent_data_nm=parent_data_nm,
                parent_data_type=parent_data_type
            )
            
            for k, v in obj.items():
                child = DataSchemaUtils.ext_schema_tree(
                    obj=v, data_nm=k, 
                    parent_data_nm=data_node.data_nm, parent_data_type=data_node.data_type
                    
                )
                data_node.add_child(child)
        elif DataSchemaUtils.chk_list_data_type(obj):
            data_node = DataSchemaNode(data_nm, DataSchemaUtils.get_data_type(obj))

            for item in obj:
                child = DataSchemaUtils.ext_schema_tree(
                    obj=item, data_nm=data_nm + DataSchemaNode.array_item_data_nm,
                    parent_data_nm=data_node.data_nm, parent_data_type=data_node.data_type
                )
                data_node.add_child(child)
        else:
            data_node = DataSchemaNode(
                data_nm = data_nm, 
                data_type = DataSchemaUtils.get_data_type(obj),
                parent_data_nm = parent_data_nm,
                parent_data_type = parent_data_type
            )

        return data_node

    @staticmethod
    def flatten_schema_tree(
        data_node: DataSchemaNode, parent_path: str = "", parent_data_type: str = None
    ) -> List[FlattenSchemaItemModel]:
        flat_schema: List[FlattenSchemaItemModel] = []

        if parent_data_type == DataSchemaNode.list_data_type:
            cur_path = (
                f"{parent_path}{DataSchemaNode.array_item_data_nm}"
                if parent_path
                else data_node.data_nm
            )
        else:
            cur_path = (
                f"{parent_path}.{data_node.data_nm}"
                if parent_path
                else data_node.data_nm
            )
        

        flat_schema.append(
            FlattenSchemaItemModel(path=cur_path, data_type=data_node.data_type, parent_path=parent_path, parent_data_type=parent_data_type)
        )

        for child in data_node.children:
            flat_schema.extend(
                DataSchemaUtils.flatten_schema_tree(child, cur_path, data_node.data_type)
            )

        return flat_schema

    @staticmethod
    def merge_deduped_flatschema(
        ls_flat_schema: List[List[FlattenSchemaItemModel]],
        agg_derived_schema: defaultdict[str, DataSchemaModel] = None,
    ) -> defaultdict[str, DataSchemaModel]:

        deduped = copy.deepcopy(agg_derived_schema) if agg_derived_schema is not None else defaultdict[str, DataSchemaModel](DataSchemaModel)
        
        for flat_schema in ls_flat_schema:
            # Track unique paths in this document to count n_docs correctly
            unique_paths_in_doc = set[str]()
            
            for item in flat_schema:
                if item.path: deduped[item.path].path = item.path
                if item.data_type:deduped[item.path].data_type.add(item.data_type)
                if item.parent_path: 
                    deduped[item.path].parent_path.add(item.parent_path)
                    if len(deduped[item.path].parent_path) > 1: raise ValueError(f"Multiple parent paths for {item.path}")
                if item.parent_data_type: 
                    deduped[item.path].parent_data_type.add(item.parent_data_type)
                    if len(deduped[item.path].parent_data_type) > 1: raise ValueError(f"Multiple parent data types for {item.path}")
                
                if item.path: unique_paths_in_doc.add(item.path)
            
            
            for path in unique_paths_in_doc:
                deduped[path].n_docs += 1

        return deduped

    @staticmethod
    def derive_schema(
        docs: List[Any],
        agg_derived_schema: defaultdict[str, DataSchemaModel] = None,
    ) -> defaultdict[str, DataSchemaModel]:

        ls_flat_schema = []
        for doc in docs:
            data_node = DataSchemaUtils.ext_schema_tree(doc)
            ls_flat_schema.append(DataSchemaUtils.flatten_schema_tree(data_node))

        derived_schema_results = DataSchemaUtils.merge_deduped_flatschema(
            ls_flat_schema, agg_derived_schema
        )


        
        
        ls_data_nms = list(derived_schema_results.keys())
        for data_nm in ls_data_nms:

            data_schema = derived_schema_results[data_nm]
            # add array with item type in parent data type
            if DataSchemaNode.list_data_type in data_schema.parent_data_type:
                for parent_path in data_schema.parent_path:
                    if parent_path not in derived_schema_results: continue
                    for data_type in data_schema.data_type:
                        if data_type == DataSchemaNode.null_data_type: continue
                        derived_schema_results[parent_path].data_type.add(f"{DataSchemaNode.list_data_type}<{data_type}>")

            # add nullable
            data_schema.nullable = (data_schema.nullable or (DataSchemaNode.null_data_type in data_schema.data_type))

            # add contains object
            if DataSchemaNode.list_data_type in data_schema.data_type:
                data_schema.contains_array = True

            # add contains array
            if DataSchemaNode.mapping_data_type in data_schema.data_type:
                data_schema.contains_object = True
            


        return derived_schema_results
    

# %%
# Simple Test 1
docs = [{"x21": 1}, {"x22": 33}, {"x22": 33, "x44": 123}, 33]

derived_schema_results = DataSchemaUtils.derive_schema(docs)
{
    k: v.to_dict()
    for k, v in derived_schema_results.items()
}
#%%
derived_schema_results = DataSchemaUtils.derive_schema([])
{
    k: v.to_dict()
    for k, v in derived_schema_results.items()
}
#%%
# Simple Test 2
from datetime import datetime
docs = [
    {
        # "x3": [],
        # "x2": [{"x21": 1}, {"x22": 33}, {"x22": 33, "x44": 123}, 33],
        "x1": [1, "a", None],
        # 'x4': None
    },
    {
        # "x3": [],
        "x2": [{"x21": 1}, {"x22": 33}, {"x22": 33, "x44": 123}, 33, None],
        "x1": [1, "a", None, datetime.now()],
        # 'x4': 1
    },
    {'x1': None}
]

derived_schema_results = DataSchemaUtils.derive_schema(docs)
{
    k: v.to_dict()
    for k, v in derived_schema_results.items()
}
# %%
_dict_to_json_serializable(
    obj = derived_schema_results
)
