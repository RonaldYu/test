from typing import Any, Dict, Set
from collections.abc import Mapping
from collections import defaultdict

def f_chk_list(obj: Any):
  return isinstance(obj, list)

def f_chk_mapping(obj: Any):
  return isinstance(obj, Mapping)

def f_obj_type(obj: Any) -> str:

  if f_chk_list(obj): return 'array'
  elif f_chk_mapping(obj): return 'object'
  else: return type(obj).__name__

class SchemaNode:
    array_node_type = "array"
    obj_node_type = "object"

    root_node_nm = "root"
    array_item_node_nm = "[]"

    def __init__(self, name, node_type):
        self.name = name
        self.node_type = node_type
        self.children = []

    def add_child(self, child):
        self.children.append(child)

    def to_dict(self):
        return {
            "name": self.name,
            "node_type": self.node_type,
            "children": [child.to_dict() for child in self.children]
        }


class SchemaOpr:
  @staticmethod
  def extract_schema_tree(obj, name=SchemaNode.root_node_nm):
      if f_chk_mapping(obj):
          node = SchemaNode(name, SchemaNode.obj_node_type)
          for k, v in obj.items():
              child = SchemaOpr.extract_schema_tree(v, k)
              node.add_child(child)
          return node
      elif f_chk_list(obj):
          node = SchemaNode(name, SchemaNode.array_node_type)
          # Use first element to infer schema, or merge schemas for all elements
          for item in obj:
            child = SchemaOpr.extract_schema_tree(item, name + SchemaNode.array_item_node_nm)
            node.add_child(child)

          return node
      else:
          return SchemaNode(name, type(obj).__name__)
        
  @staticmethod
  def flatten_schema_tree(node, parent_path="", parent_type=None):
    flat = []
    if parent_type == SchemaNode.array_node_type:
      path = f"{parent_path}{SchemaNode.array_item_node_nm}" if parent_path else node.name
    else:
      path = f"{parent_path}.{node.name}" if parent_path else node.name

    flat.append({"path": path, "node_type": node.node_type})
    for child in node.children:
        flat.extend(SchemaOpr.flatten_schema_tree(child, path, node.node_type))
    return flat

  @staticmethod
  def deduped_flatschema(flat_schema):
    deduped = defaultdict(set)
    for item in flat_schema:
      deduped[item["path"]].add(item["node_type"])

    return dict(deduped)
  
  @staticmethod
  def merge_schema(ls_schema_dict):
    deduped = defaultdict(set)

    for schema_dict in ls_schema_dict:
      for path, node_types in schema_dict.items:
        deduped[path].update(node_types)

    return dict(deduped)
  
  @staticmethod
  def derive_schema(docs: List[Any]) -> Dict[str, Set[str]]:
    ls_schema_dict = []
    for doc in docs:
      
# Simple Test:
tree_schematree_schema = SchemaOpr.extract_schema_tree([
      {'x21': 1}, {'x22': 33}, {'x22': 33, 'x44': 123}, 33
    ])

flat_schema = SchemaOpr.flatten_schema_tree(tree_schematree_schema)
SchemaOpr.deduped_flatschema(flat_schema)

# Simple Test:
tree_schematree_schema = SchemaOpr.extract_schema_tree({
    'x3': [],
    'x2': [
      {'x21': 1}, {'x22': 33}, {'x22': 33, 'x44': 123}, 33
    ],
    'x1': [1, 'a']
})

flat_schema = SchemaOpr.flatten_schema_tree(tree_schematree_schema)
SchemaOpr.deduped_flatschema(flat_schema)
