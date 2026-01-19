import json
from datetime import datetime, date
from typing import Any
from decimal import Decimal


def _default_json_handler(obj: Any) -> Any:
    
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    
    if hasattr(obj, '__str__') and type(obj).__name__ == 'ObjectId':
        return str(obj)
    
    if isinstance(obj, set):
        return list(obj)
    
    if isinstance(obj, Decimal):
        return float(obj)
    
    if isinstance(obj, bytes):
        return obj.decode('utf-8', errors='replace')
    
    if hasattr(obj, '__dict__'):
        return obj.__dict__
    
    if hasattr(obj, 'to_dict'):
        return obj.to_dict()
    
    return str(obj)


def _dict_to_json_serializable(obj: Any, handler = _default_json_handler) -> Any:
    
    if isinstance(obj, dict):
        return {key: _dict_to_json_serializable(value, handler) for key, value in obj.items()}
    
    if isinstance(obj, (list, tuple)):
        return [_dict_to_json_serializable(item, handler) for item in obj]
    
    if isinstance(obj, set):
        return [_dict_to_json_serializable(item, handler) for item in obj]
    

    try:
        json.dumps(obj)
        return obj
    except (TypeError, ValueError):
        converted = handler(obj)
        if converted != obj:
            return _dict_to_json_serializable(converted, handler)
        return converted
