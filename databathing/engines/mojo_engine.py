from mo_sql_parsing import format
from .base_engine import BaseEngine


class MojoEngine(BaseEngine):
    """Mojo 🔥 high-performance data processing code generator [PROTOTYPE]
    
    ⚠️  WARNING: This is a conceptual prototype engine that generates
    experimental Mojo-style code. The generated code:
    - Uses fictional functions like load_table(), simd_filter(), simd_groupby()
    - Will NOT compile or run in actual Mojo environment
    - Is intended for demonstration and future development
    
    For production use, please use 'spark' or 'duckdb' engines.
    
    Future vision - generates Mojo code for SQL operations leveraging:
    - SIMD vectorized operations  
    - Zero-copy data structures
    - Memory-safe array operations
    - High-performance numerical computing
    """
    
    @property
    def engine_name(self):
        return "mojo"
    
    def _from_analyze(self, from_stmt):
        if not from_stmt:
            return 
        if type(from_stmt) is str:
            # Mojo: Create DataFrame-like structure from table
            self.from_ans += f'var {from_stmt}_data = load_table("{from_stmt}")\n'
        elif type(from_stmt) is dict:
            if "name" in from_stmt.keys():
                # Table with alias: FROM table AS alias
                self.from_ans += f'var {from_stmt["name"]}_data = load_table("{from_stmt["value"]}")\n'
            elif "left join" in from_stmt.keys():
                # Mojo high-performance join operation
                join_table = from_stmt['left join']['value']
                join_alias = from_stmt['left join']['name']
                left_col = from_stmt["on"]["eq"][0]
                right_col = from_stmt["on"]["eq"][1]
                self.from_ans += f'var {join_alias}_data = load_table("{join_table}")\n'
                self.from_ans += f'var joined_data = simd_left_join(main_data, {join_alias}_data, "{left_col}", "{right_col}")\n'
            elif "inner join" in from_stmt.keys():
                join_table = from_stmt['inner join']['value']
                join_alias = from_stmt['inner join']['name']
                left_col = from_stmt["on"]["eq"][0]
                right_col = from_stmt["on"]["eq"][1]
                self.from_ans += f'var {join_alias}_data = load_table("{join_table}")\n'
                self.from_ans += f'var joined_data = simd_inner_join(main_data, {join_alias}_data, "{left_col}", "{right_col}")\n'
            elif "right join" in from_stmt.keys():
                join_table = from_stmt['right join']['value']
                join_alias = from_stmt['right join']['name']
                left_col = from_stmt["on"]["eq"][0]
                right_col = from_stmt["on"]["eq"][1]
                self.from_ans += f'var {join_alias}_data = load_table("{join_table}")\n'
                self.from_ans += f'var joined_data = simd_right_join(main_data, {join_alias}_data, "{left_col}", "{right_col}")\n'
                
        elif type(from_stmt) is list:
            for item_from in from_stmt:
                self._from_analyze(item_from)    
    
    def _select_analyze(self, select_stmt):
        if not select_stmt:
            return

        if type(select_stmt) is str:
            self.select_ans += f'"{select_stmt}", '
            return  
        if type(select_stmt) is dict and 'value' in select_stmt and type(select_stmt['value']) is str:
            if 'name' in select_stmt:
                self.select_ans += f'Column("{select_stmt["value"]}", alias="{select_stmt["name"]}"), '
            else:
                self.select_ans += f'"{select_stmt["value"]}", '
            return
        if type(select_stmt) is dict:
            if 'value' in select_stmt and isinstance(select_stmt["value"], dict) and list(select_stmt["value"].keys())[0].lower() in self.agg_list:
                # Aggregation function with SIMD optimization
                func_name = list(select_stmt["value"].keys())[0]
                func_arg = select_stmt["value"][func_name]
                alias = select_stmt.get('name', '')
                
                # Map to high-performance Mojo SIMD functions
                mojo_func_map = {
                    'sum': 'simd_sum',
                    'avg': 'simd_mean', 
                    'mean': 'simd_mean',
                    'count': 'simd_count',
                    'max': 'simd_max',
                    'min': 'simd_min'
                }
                
                mojo_func = mojo_func_map.get(func_name.lower(), f'simd_{func_name.lower()}')
                
                if alias:
                    self.select_ans += f'Aggregate("{func_arg}", {mojo_func}, alias="{alias}"), '
                else:
                    self.select_ans += f'Aggregate("{func_arg}", {mojo_func}), '
            else:
                # Other expressions - use vectorized operations when possible
                formatted = format({"select": select_stmt})[7:]
                self.select_ans += f'Expression("{formatted}"), '
        elif type(select_stmt) is list:
            for inner_item in select_stmt:
                self._select_analyze(inner_item)

    def _where_analyze(self, where_stmt):
        # Convert WHERE clause to high-performance Mojo filter
        where_expr = format({"where": where_stmt})[6:]
        self.where_ans = f'simd_filter(data, "{where_expr}")'

    def _groupby_analyze(self, groupby_stmt):
        # Convert GROUP BY to vectorized Mojo grouping
        groupby_expr = format({"groupby": groupby_stmt})[9:]
        self.groupby_ans = f'simd_groupby(data, ["{groupby_expr}"])'

    def _agg_analyze(self, agg_stmt):
        # High-performance SIMD aggregation operations
        if type(agg_stmt) is dict:
            if 'value' in agg_stmt and type(agg_stmt["value"]) is dict and list(agg_stmt["value"].keys())[0].lower() in self.agg_list:
                for funct, alias in agg_stmt["value"].items():
                    # Map to SIMD-optimized functions
                    mojo_func_map = {
                        'sum': 'simd_sum',
                        'avg': 'simd_mean',
                        'mean': 'simd_mean', 
                        'count': 'simd_count',
                        'max': 'simd_max',
                        'min': 'simd_min'
                    }
                    mojo_func = mojo_func_map.get(funct.lower(), f'simd_{funct.lower()}')
                    agg_name = agg_stmt.get("name", f"{funct}_{alias}")
                    self.agg_ans += f'{mojo_func}(data["{alias}"]).alias("{agg_name}"), '

        elif type(agg_stmt) is list:
            for item in agg_stmt:
                self._agg_analyze(item)
                    
        self.agg_ans = self.agg_ans.replace("\n", "")

    def _having_analyze(self, having_stmt):
        having_expr = format({"having": having_stmt})[7:]
        self.having_ans = f'simd_filter(grouped_data, "{having_expr}")'

    def _orderby_analyze(self, order_stmt):
        if type(order_stmt) is dict:
            direction = "desc" if order_stmt.get("sort", "asc").upper() == "DESC" else "asc"
            self.orderby_ans += f'("{order_stmt["value"]}", "{direction}"), '
        else:
            for item in order_stmt:
                self._orderby_analyze(item)

    def _limit_analyze(self, limit_stmt):
        self.limit_ans = limit_stmt

    def _set_operation_analyze(self, operation_type, queries_list):
        """Handle UNION, UNION ALL, INTERSECT, EXCEPT operations with SIMD optimization"""
        if not queries_list or len(queries_list) < 2:
            return
        
        # Generate Mojo code for each subquery
        subquery_results = []
        for i, query in enumerate(queries_list):
            # Create a new MojoEngine instance for each subquery
            subquery_parser = MojoEngine(query)
            subquery_code = subquery_parser.parse()
            subquery_var = f"subquery_{i}"
            subquery_results.append(subquery_var)
            self.set_operation_ans += f"var {subquery_var} = {subquery_code}\n"
        
        # Combine subqueries with high-performance SIMD set operations
        if operation_type == "union":
            # UNION in SQL = simd_union().distinct() in Mojo
            result_expr = f"simd_union_distinct([{', '.join(subquery_results)}])"
        elif operation_type == "union_all":
            # UNION ALL in SQL = simd_union() in Mojo  
            result_expr = f"simd_union([{', '.join(subquery_results)}])"
        elif operation_type == "intersect":
            # INTERSECT in SQL = simd_intersect() in Mojo
            result_expr = f"simd_intersect([{', '.join(subquery_results)}])"
        elif operation_type == "except":
            # EXCEPT in SQL = simd_except() in Mojo
            result_expr = f"simd_except({subquery_results[0]}, {subquery_results[1]})"
        
        self.set_operation_ans += f"var result = {result_expr}\n"

    def parse(self):
        """Generate high-performance Mojo code for SQL operations"""
        # Check if this is a set operation query first
        set_operations = ["union", "union_all", "intersect", "except"]
        for op in set_operations:
            if op in self.parsed_json_query:
                self._set_operation_analyze(op, self.parsed_json_query[op])
                return self.set_operation_ans

        # Build Mojo code components
        mojo_code = []
        
        # Import necessary Mojo modules
        mojo_code.append("from collections import List, DynamicVector")
        mojo_code.append("from memory import memset_zero, memcpy")
        mojo_code.append("from algorithm import vectorize, parallelize")
        mojo_code.append("from sys.info import simdwidthof")
        mojo_code.append("from tensor import Tensor")
        mojo_code.append("")
        
        # Handle FROM clause first
        main_table = None
        for method, stmt in self.parsed_json_query.items():
            if str(method).lower() == "from":
                self._from_analyze(stmt)
                if isinstance(stmt, str):
                    main_table = stmt
                elif isinstance(stmt, dict) and "value" in stmt:
                    main_table = stmt["value"]
                break
        
        if self.from_ans:
            mojo_code.append(self.from_ans)
        
        # Initialize main data variable
        if main_table:
            mojo_code.append(f"var data = {main_table}_data")
        else:
            mojo_code.append("var data = main_data")
        
        # Handle WHERE clause with SIMD filtering
        for method, stmt in self.parsed_json_query.items():
            if str(method).lower() == "where":
                self._where_analyze(stmt)
                mojo_code.append(f"data = {self.where_ans}")
                break

        # Handle GROUP BY with vectorized operations
        grouped_data = False
        for method, stmt in self.parsed_json_query.items():
            if str(method).lower() == "groupby":
                self._groupby_analyze(stmt)
                mojo_code.append(f"var grouped_data = {self.groupby_ans}")
                grouped_data = True
                
                # Handle aggregations
                agg_stmt = self.parsed_json_query.get("select", self.parsed_json_query.get("select_distinct", []))
                if agg_stmt:
                    self._agg_analyze(agg_stmt)
                    if self.agg_ans:
                        agg_operations = self.agg_ans.rstrip(", ")
                        mojo_code.append(f"var aggregated_data = simd_aggregate(grouped_data, [{agg_operations}])")
                        mojo_code.append("data = aggregated_data")
                break

        # Handle HAVING clause
        for method, stmt in self.parsed_json_query.items():
            if str(method) == "having":
                self._having_analyze(stmt)
                mojo_code.append(f"data = {self.having_ans}")
                break

        # Handle SELECT clause with column projection
        select_columns = []
        for method, stmt in self.parsed_json_query.items():
            if str(method).lower() in ["select", "select_distinct"]:
                self._select_analyze(stmt)
                if self.select_ans:
                    select_columns = self.select_ans.rstrip(", ").split(", ")
                self.distinct_flag = True if str(method) == "select_distinct" else False
                break
        
        if select_columns:
            columns_str = "[" + ", ".join(select_columns) + "]"
            mojo_code.append(f"data = simd_select(data, {columns_str})")
        
        if self.distinct_flag:
            mojo_code.append("data = simd_distinct(data)")

        # Handle ORDER BY with SIMD sorting
        for method, stmt in self.parsed_json_query.items():
            if str(method) == "orderby":
                self._orderby_analyze(stmt)
                if self.orderby_ans:
                    order_columns = self.orderby_ans.rstrip(", ")
                    mojo_code.append(f"data = simd_sort(data, [{order_columns}])")
                break

        # Handle LIMIT
        for method, stmt in self.parsed_json_query.items():
            if str(method).lower() == "limit":
                self._limit_analyze(stmt)
                mojo_code.append(f"data = simd_limit(data, {self.limit_ans})")
                break

        # Final result assignment
        mojo_code.append("")
        mojo_code.append("# Return high-performance result")
        mojo_code.append("var result = data")
        
        return "\n".join(mojo_code)