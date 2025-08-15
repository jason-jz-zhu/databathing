from mo_sql_parsing import format
from .base_engine import BaseEngine


class DuckDBEngine(BaseEngine):
    """DuckDB Relational API code generator"""
    
    @property
    def engine_name(self):
        return "duckdb"
    
    def _from_analyze(self, from_stmt):
        if not from_stmt:
            return 
        if type(from_stmt) is str:
            # DuckDB: duckdb.sql("SELECT * FROM table_name")
            self.from_ans += f'duckdb.sql("SELECT * FROM {from_stmt}")'
        elif type(from_stmt) is dict:
            if "name" in from_stmt.keys():
                # Table with alias: FROM table AS alias -> duckdb.sql("SELECT * FROM table").alias("alias")
                self.from_ans += f'duckdb.sql("SELECT * FROM {from_stmt["value"]}").alias("{from_stmt["name"]}")'
            elif "left join" in from_stmt.keys():
                # DuckDB doesn't have direct join method, so we generate SQL
                join_table = from_stmt['left join']['value']
                join_alias = from_stmt['left join']['name']
                on_condition = f'{from_stmt["on"]["eq"][0]} = {from_stmt["on"]["eq"][1]}'
                self.from_ans += f'duckdb.sql("SELECT * FROM {{}} LEFT JOIN {join_table} AS {join_alias} ON {on_condition}")'
            elif "inner join" in from_stmt.keys():
                join_table = from_stmt['inner join']['value']
                join_alias = from_stmt['inner join']['name']
                on_condition = f'{from_stmt["on"]["eq"][0]} = {from_stmt["on"]["eq"][1]}'
                self.from_ans += f'duckdb.sql("SELECT * FROM {{}} INNER JOIN {join_table} AS {join_alias} ON {on_condition}")'
            elif "right join" in from_stmt.keys():
                join_table = from_stmt['right join']['value']
                join_alias = from_stmt['right join']['name']
                on_condition = f'{from_stmt["on"]["eq"][0]} = {from_stmt["on"]["eq"][1]}'
                self.from_ans += f'duckdb.sql("SELECT * FROM {{}} RIGHT JOIN {join_table} AS {join_alias} ON {on_condition}")'
                
        elif type(from_stmt) is list:
            for item_from in from_stmt:
                self._from_analyze(item_from)    
    
    def _select_analyze(self, select_stmt):
        if not select_stmt:
            return

        if type(select_stmt) is str:
            self.select_ans += f'{select_stmt},'
            return  
        if type(select_stmt) is dict and 'value' in select_stmt and type(select_stmt['value']) is str:
            if 'name' in select_stmt:
                self.select_ans += f'{select_stmt["value"]} AS {select_stmt["name"]},'
            else:
                self.select_ans += f'{select_stmt["value"]},'
            return
        if type(select_stmt) is dict:
            if 'value' in select_stmt and isinstance(select_stmt["value"], dict) and list(select_stmt["value"].keys())[0].lower() in self.agg_list:
                # Aggregation function
                func_name = list(select_stmt["value"].keys())[0]
                func_arg = select_stmt["value"][func_name]
                alias = select_stmt.get('name', '')
                if alias:
                    self.select_ans += f'{func_name}({func_arg}) AS {alias},'
                else:
                    self.select_ans += f'{func_name}({func_arg}),'
            else:
                # Other expressions - use format but clean up quotes
                formatted = format({"select": select_stmt})[7:]
                self.select_ans += f'{formatted},'
        elif type(select_stmt) is list:
            for inner_item in select_stmt:
                self._select_analyze(inner_item)

    def _where_analyze(self, where_stmt):
        # Convert WHERE clause to DuckDB filter
        self.where_ans = format({"where": where_stmt})[6:]

    def _groupby_analyze(self, groupby_stmt):
        # Convert GROUP BY to DuckDB format
        self.groupby_ans = format({"groupby": groupby_stmt})[9:]

    def _agg_analyze(self, agg_stmt):
        # DuckDB uses standard SQL aggregation functions
        if type(agg_stmt) is dict:
            if type(agg_stmt["value"]) is dict and list(agg_stmt["value"].keys())[0].lower() in self.agg_list:
                for funct, alias in agg_stmt["value"].items():
                    self.agg_ans += f'{funct}({alias}) AS {agg_stmt["name"]},'

        elif type(agg_stmt) is list:
            for item in agg_stmt:
                self._agg_analyze(item)
                    
        self.agg_ans = self.agg_ans.replace("\n", "")

    def _having_analyze(self, having_stmt):
        self.having_ans = format({"having": having_stmt})[7:]

    def _orderby_analyze(self, order_stmt):
        if type(order_stmt) is dict:
            direction = "DESC" if order_stmt.get("sort", "asc").upper() == "DESC" else "ASC"
            self.orderby_ans += f'{order_stmt["value"]} {direction},'
        else:
            for item in order_stmt:
                self._orderby_analyze(item)

    def _limit_analyze(self, limit_stmt):
        self.limit_ans = limit_stmt

    def _set_operation_analyze(self, operation_type, queries_list):
        """Handle UNION, UNION ALL, INTERSECT, EXCEPT operations for DuckDB"""
        if not queries_list or len(queries_list) < 2:
            return
        
        # Generate DuckDB SQL for each subquery
        subquery_sqls = []
        for query in queries_list:
            # Create a new DuckDBEngine instance for each subquery
            subquery_parser = DuckDBEngine(query)
            subquery_code = subquery_parser.parse()
            # Extract the SQL part from the DuckDB code
            if 'duckdb.sql("' in subquery_code:
                sql_start = subquery_code.find('duckdb.sql("') + 11
                sql_end = subquery_code.rfind('")')
                subquery_sql = subquery_code[sql_start:sql_end]
                subquery_sqls.append(f"({subquery_sql})")
            else:
                subquery_sqls.append(f"({subquery_code})")
        
        # Combine subqueries with appropriate SQL set operation
        if operation_type == "union":
            combined_sql = " UNION ".join(subquery_sqls)
        elif operation_type == "union_all":
            combined_sql = " UNION ALL ".join(subquery_sqls)
        elif operation_type == "intersect":
            combined_sql = " INTERSECT ".join(subquery_sqls)
        elif operation_type == "except":
            combined_sql = " EXCEPT ".join(subquery_sqls)
        
        self.set_operation_ans = f'duckdb.sql("{combined_sql}")'

    def parse(self):
        from_final_ans = where_final_ans = groupby_final_ans = agg_final_ans = select_final_ans = orderby_final_ans = limit_final_ans = having_final_ans = set_operation_final_ans = ""

        # Check if this is a set operation query first
        set_operations = ["union", "union_all", "intersect", "except"]
        for op in set_operations:
            if op in self.parsed_json_query:
                self._set_operation_analyze(op, self.parsed_json_query[op])
                return self.set_operation_ans

        # Build SQL components
        sql_parts = []
        
        # Handle SELECT
        for method, stmt in self.parsed_json_query.items():
            if str(method).lower() in ["select", "select_distinct"]:
                self._select_analyze(stmt)
                select_final_ans = self.select_ans[:-1] if self.select_ans.endswith(',') else self.select_ans
                self.distinct_flag = True if str(method) == "select_distinct" else False
                
                if self.distinct_flag:
                    sql_parts.append(f"SELECT DISTINCT {select_final_ans}")
                else:
                    sql_parts.append(f"SELECT {select_final_ans}")
                break
        
        # If no SELECT found, use SELECT *
        if not sql_parts:
            sql_parts.append("SELECT *")
        
        # Handle FROM
        for method, stmt in self.parsed_json_query.items():
            if str(method).lower() == "from":
                if isinstance(stmt, str):
                    sql_parts.append(f"FROM {stmt}")
                elif isinstance(stmt, dict) and "value" in stmt:
                    if "name" in stmt:
                        sql_parts.append(f'FROM {stmt["value"]} AS {stmt["name"]}')
                    else:
                        sql_parts.append(f'FROM {stmt["value"]}')
                break

        # Handle WHERE
        for method, stmt in self.parsed_json_query.items():
            if str(method).lower() == "where":
                self._where_analyze(stmt)
                sql_parts.append(f"WHERE {self.where_ans}")
                break

        # Handle GROUP BY
        for method, stmt in self.parsed_json_query.items():
            if str(method).lower() == "groupby":
                self._groupby_analyze(stmt)
                sql_parts.append(f"GROUP BY {self.groupby_ans}")
                break

        # Handle HAVING
        for method, stmt in self.parsed_json_query.items():
            if str(method) == "having":
                self._having_analyze(stmt)
                sql_parts.append(f"HAVING {self.having_ans}")
                break

        # Handle ORDER BY
        for method, stmt in self.parsed_json_query.items():
            if str(method) == "orderby":
                self._orderby_analyze(stmt)
                orderby_final_ans = self.orderby_ans[:-1] if self.orderby_ans.endswith(',') else self.orderby_ans
                sql_parts.append(f"ORDER BY {orderby_final_ans}")
                break

        # Handle LIMIT
        for method, stmt in self.parsed_json_query.items():
            if str(method).lower() == "limit":
                self._limit_analyze(stmt)
                sql_parts.append(f"LIMIT {self.limit_ans}")
                break

        # Combine all SQL parts
        final_sql = " ".join(sql_parts)
        return f'duckdb.sql("{final_sql}")'