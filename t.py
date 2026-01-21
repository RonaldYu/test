
    @staticmethod
    def merge_deduped_flatschema(
        ls_flat_schema: List[List[FlattenDataSchemaInfoModel]],
        agg_derived_schema: defaultdict[str, DocumentSchemaInfoModel] = None,
    ) -> defaultdict[str, DocumentSchemaInfoModel]:
        deduped = copy.deepcopy(agg_derived_schema) if agg_derived_schema is not None else defaultdict[str, DocumentSchemaInfoModel](DocumentSchemaInfoModel)
        
        def _process_flat_schema(
            flat_schema: List[FlattenDataSchemaInfoModel],
            unique_paths: set[str]
        ) -> None:
            """Recursively process items in a flat schema list"""
            if not flat_schema:
                return
            
            # Process first item
            item = flat_schema[0]
            if item.path:
                deduped[item.path].path = item.path
                unique_paths.add(item.path)
                
                if item.data_type:
                    deduped[item.path].data_type.add(item.data_type)
                if item.parent_path:
                    deduped[item.path].parent_path.add(item.parent_path)
                    if len(deduped[item.path].parent_path) > 1:
                        raise ValueError(f"Multiple parent paths for {item.path}")
                if item.parent_data_type:
                    deduped[item.path].parent_data_type.add(item.parent_data_type)
                    if len(deduped[item.path].parent_data_type) > 1:
                        raise ValueError(f"Multiple parent data types for {item.path}")
            
            # Recursively process remaining items
            _process_flat_schema(flat_schema[1:], unique_paths)
        
        def _process_schema_list(
            schema_list: List[List[FlattenDataSchemaInfoModel]]
        ) -> None:
            """Recursively process list of flat schemas"""
            if not schema_list:
                return
            
            # Process first flat schema
            unique_paths_in_doc = set[str]()
            _process_flat_schema(schema_list[0], unique_paths_in_doc)
            
            # Update document counts for unique paths
            for path in unique_paths_in_doc:
                deduped[path].n_docs += 1
            
            # Recursively process remaining schemas
            _process_schema_list(schema_list[1:])
        
        _process_schema_list(ls_flat_schema)
        return deduped
