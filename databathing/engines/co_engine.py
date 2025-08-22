import re
from typing import Dict, List, Set
from mo_sql_parsing import format
from .base_engine import BaseEngine


class COEngine(BaseEngine):
    """Capital One Banking Engine - PySpark code generator with financial compliance features"""
    
    def __init__(self, parsed_json_query, security_config=None):
        super().__init__(parsed_json_query)
        self.security_config = security_config or {}
        
        # Banking-specific aggregation functions
        self.agg_list = [
            "sum", "avg", "max", "min", "mean", "count", "collect_list", "collect_set",
            # Financial aggregations
            "stddev", "variance", "percentile_approx", "corr", "covar_pop", "covar_samp",
            "first", "last", "skewness", "kurtosis"
        ]
        
        # Sensitive data patterns for detection
        self.sensitive_patterns = {
            'ssn': r'\b\d{3}-?\d{2}-?\d{4}\b',
            'account_number': r'\b\d{10,16}\b',
            'credit_card': r'\b(?:\d{4}[-\s]?){3}\d{4}\b',
            'routing_number': r'\b\d{9}\b',
            'email': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        }
        
        # Banking compliance flags
        self.requires_audit_trail = False
        self.sensitive_columns = set()
        self.encryption_columns = set()
    
    @property
    def engine_name(self):
        return "co_banking"
    
    def _detect_sensitive_data(self, column_name, select_stmt):
        """Detect sensitive data patterns in column names and expressions"""
        sensitive_indicators = [
            'ssn', 'social', 'account_num', 'account_number', 'credit_card', 'cc_num',
            'routing', 'bank_num', 'email', 'phone', 'address', 'dob', 'birth',
            'salary', 'income', 'balance', 'customer_id', 'cust_id'
        ]
        
        column_lower = column_name.lower()
        for indicator in sensitive_indicators:
            if indicator in column_lower:
                self.sensitive_columns.add(column_name)
                return True
        return False
    
    def _apply_data_masking(self, column_name, expression):
        """Apply data masking for sensitive columns"""
        if column_name in self.sensitive_columns:
            if 'ssn' in column_name.lower():
                return f"concat('XXX-XX-', substring({expression}, -4, 4)) as {column_name}_masked"
            elif 'account' in column_name.lower():
                return f"concat('****', substring({expression}, -4, 4)) as {column_name}_masked"
            elif 'credit_card' in column_name.lower() or 'cc_' in column_name.lower():
                return f"concat('****-****-****-', substring({expression}, -4, 4)) as {column_name}_masked"
            elif 'email' in column_name.lower():
                return f"concat('***@', substring_index({expression}, '@', -1)) as {column_name}_masked"
            else:
                return f"'***MASKED***' as {column_name}_masked"
        return expression
    
    def _add_audit_columns(self):
        """Add audit trail columns to generated code"""
        audit_cols = [
            "current_timestamp() as co_processing_timestamp",
            "current_user() as co_processed_by",
            "'databathing_co_engine' as co_processing_system"
        ]
        return audit_cols
    
    def _from_analyze(self, from_stmt):
        """Enhanced FROM analysis with banking compliance checks"""
        if not from_stmt:
            return 
            
        if type(from_stmt) is str:
            # Check if table name suggests sensitive data
            if any(indicator in from_stmt.lower() for indicator in ['customer', 'account', 'transaction', 'payment']):
                self.requires_audit_trail = True
            self.from_ans += from_stmt + "."
            
        elif type(from_stmt) is dict:
            if "name" in from_stmt.keys():
                table_name = from_stmt['value']
                alias_name = from_stmt['name']
                if any(indicator in table_name.lower() for indicator in ['customer', 'account', 'transaction', 'payment']):
                    self.requires_audit_trail = True
                self.from_ans += f"{table_name}.alias(\"{alias_name}\")."
                
            elif "left join" in from_stmt.keys():
                self.from_ans += self._joinfeat(from_stmt, 'left', 'left')
            elif "inner join" in from_stmt.keys():
                self.from_ans += self._joinfeat(from_stmt, 'inner', 'inner')
            elif "right join" in from_stmt.keys():
                self.from_ans += self._joinfeat(from_stmt, 'right', 'right')
            elif "join" in from_stmt.keys():
                self.from_ans += self._joinfeat(from_stmt, '', 'inner')
                
        elif type(from_stmt) is list:
            for item_from in from_stmt:
                self._from_analyze(item_from)
    
    def _joinfeat(self, from_stmt, srcjointype, dstjointype):
        """Banking-compliant JOIN processing"""
        join_target = from_stmt.get(f'{srcjointype} join', from_stmt.get('join'))
        if isinstance(join_target, dict) and 'value' in join_target and 'name' in join_target:
            join_expr = join_target['value']+".alias(\""+join_target['name']+"\")"
        elif isinstance(join_target, str):
            join_expr = join_target
        else:
            join_expr = str(join_target)

        # Handle complex ON conditions with AND
        if "and" in from_stmt['on']:
            eachand = []
            for theandvalue in from_stmt['on']['and']:
                eachand.append("col(\""+str(theandvalue['eq'][0])+"\")" + "==" + "col(\""+str(theandvalue['eq'][1])+"\")")
            return "join({}, {}, \"{}\").".format( 
                join_expr, 
                " & ".join(eachand), 
                dstjointype)
        else:
            # Simple ON condition
            eachand = "col(\""+str(from_stmt['on']['eq'][0])+"\")" + "==" + "col(\""+str(from_stmt['on']['eq'][1])+"\")"
            return "join({}, {}, \"{}\").".format( 
                join_expr, 
                eachand, 
                dstjointype)
    
    def _select_analyze(self, select_stmt):
        """Enhanced SELECT analysis with sensitive data detection and masking"""
        if not select_stmt:
            return

        if type(select_stmt) is str:
            self._detect_sensitive_data(select_stmt, select_stmt)
            if self.security_config.get('enable_masking', False):
                masked_expr = self._apply_data_masking(select_stmt, select_stmt)
                self.select_ans += "\"" + masked_expr + "\","
            else:
                self.select_ans += "\"" + format({"select": select_stmt})[7:] + "\","
            return
            
        if type(select_stmt) is dict and 'value' in select_stmt and type(select_stmt['value']) is str:
            column_name = select_stmt.get('name', select_stmt['value'])
            self._detect_sensitive_data(column_name, select_stmt)
            if self.security_config.get('enable_masking', False):
                masked_expr = self._apply_data_masking(column_name, select_stmt['value'])
                self.select_ans += "\"" + masked_expr + "\","
            else:
                self.select_ans += "\"" + format({"select": select_stmt})[7:] + "\","
            return
            
        if type(select_stmt) is dict:
            if 'value' in select_stmt and isinstance(select_stmt["value"], dict) and list(select_stmt["value"].keys())[0].lower() in self.agg_list:
                if 'name' in select_stmt:
                    column_name = select_stmt['name']
                    self._detect_sensitive_data(column_name, select_stmt)
                    self.select_ans += "\"" + column_name + "\","
                else:
                    self.select_ans += "\"" + format({"select": select_stmt})[7:] + "\","
            elif 'value' in select_stmt and isinstance(select_stmt["value"], dict) and list(select_stmt["value"].keys())[0].lower() == "create_struct":
                self.select_ans += "\"" + format({"select": select_stmt})[14:] + "\","
            else:
                column_name = select_stmt.get('name', str(select_stmt.get('value', '')))
                if column_name:
                    self._detect_sensitive_data(column_name, select_stmt)
                self.select_ans += "\"" + format({"select": select_stmt})[7:] + "\","
                
        elif type(select_stmt) is list:
            for inner_item in select_stmt:
                self._select_analyze(inner_item)

    def _where_analyze(self, where_stmt):
        """Enhanced WHERE analysis with compliance checks"""
        self.where_ans = format({"where": where_stmt})[6:]
        
        # Check for potential PII in WHERE conditions
        where_str = str(where_stmt)
        for pattern_name, pattern in self.sensitive_patterns.items():
            if re.search(pattern, where_str):
                self.requires_audit_trail = True

    def _groupby_analyze(self, groupby_stmt):
        """Banking-compliant GROUP BY analysis"""
        self.groupby_ans = format({"groupby": groupby_stmt})[9:]

    def _agg_analyze(self, agg_stmt):
        """Enhanced aggregation analysis for banking metrics"""
        if type(agg_stmt) is dict:
            if type(agg_stmt["value"]) is dict and list(agg_stmt["value"].keys())[0].lower() in self.agg_list:
                for funct, alias in agg_stmt["value"].items():
                    if "name" in agg_stmt:
                        column_name = agg_stmt["name"]
                        self._detect_sensitive_data(column_name, agg_stmt)
                        self.agg_ans += "{}(col(\"{}\")).alias(\"{}\"),".format(funct, alias, column_name)
                    else:
                        self.agg_ans += "{}(col(\"{}\")).alias(\"{}\"),".format(funct, alias, f"{funct}_{alias}")

        elif type(agg_stmt) is list:
            for item in agg_stmt:
                self._agg_analyze(item)
                    
        self.agg_ans = self.agg_ans.replace("\n", "")

    def _having_analyze(self, having_stmt):
        """Banking-compliant HAVING analysis"""
        self.having_ans = format({"having": having_stmt})[7:]

    def _orderby_analyze(self, order_stmt):
        """Enhanced ORDER BY analysis"""
        if type(order_stmt) is dict:
            odr = "desc()" if order_stmt.get("sort", "asc") == "desc" else "asc()"
            self.orderby_ans += "col(\"{}\").{},".format(str(order_stmt["value"]), odr)
        else:
            for item in order_stmt:
                self._orderby_analyze(item)

    def _limit_analyze(self, limit_stmt):
        """Banking-compliant LIMIT analysis"""
        self.limit_ans = limit_stmt

    def _set_operation_analyze(self, operation_type, queries_list):
        """Banking-compliant set operations"""
        if not queries_list or len(queries_list) < 2:
            return
        
        subquery_results = []
        for query in queries_list:
            subquery_parser = COEngine(query, self.security_config)
            subquery_code = subquery_parser.parse()
            subquery_results.append(f"({subquery_code})")
        
        if operation_type == "union":
            base_df = subquery_results[0]
            for subquery in subquery_results[1:]:
                base_df = f"{base_df}.union({subquery})"
            self.set_operation_ans = f"{base_df}.distinct()"
        elif operation_type == "union_all":
            base_df = subquery_results[0]
            for subquery in subquery_results[1:]:
                base_df = f"{base_df}.union({subquery})"
            self.set_operation_ans = base_df
        elif operation_type == "intersect":
            base_df = subquery_results[0]
            for subquery in subquery_results[1:]:
                base_df = f"{base_df}.intersect({subquery})"
            self.set_operation_ans = base_df
        elif operation_type == "except":
            base_df = subquery_results[0]
            for subquery in subquery_results[1:]:
                base_df = f"{base_df}.exceptAll({subquery})"
            self.set_operation_ans = base_df

    def parse(self):
        """Generate banking-compliant PySpark code with audit trails and security"""
        from_final_ans = where_final_ans = groupby_final_ans = agg_final_ans = select_final_ans = orderby_final_ans = limit_final_ans = having_final_ans = set_operation_final_ans = ""

        # Check for set operations first
        set_operations = ["union", "union_all", "intersect", "except"]
        for op in set_operations:
            if op in self.parsed_json_query:
                self._set_operation_analyze(op, self.parsed_json_query[op])
                set_operation_final_ans = self.set_operation_ans
                break

        # Process query components
        for method, stmt in self.parsed_json_query.items():
            if str(method).lower() == "from":
                if isinstance(stmt, dict):
                    for op in set_operations:
                        if op in stmt:
                            self._set_operation_analyze(op, stmt[op])
                            set_operation_final_ans = self.set_operation_ans
                            break
                if not set_operation_final_ans:
                    self._from_analyze(stmt)
                    from_final_ans = self.from_ans[:-1] if self.from_ans and self.from_ans[-1] == '.' else self.from_ans

            elif str(method).lower() == "where":
                self._where_analyze(stmt)
                where_final_ans = self.where_ans

            elif str(method).lower() == "groupby":
                self._groupby_analyze(stmt)
                groupby_final_ans = self.groupby_ans
                agg_stmt = self.parsed_json_query.get("select", self.parsed_json_query.get("select_distinct"))
                self._agg_analyze(agg_stmt)
                agg_final_ans = self.agg_ans[:-1] if self.agg_ans else ""

            elif str(method).lower() in ["select", "select_distinct"]:
                self._select_analyze(stmt)
                select_final_ans = self.select_ans[:-1] if self.select_ans else ""
                self.distinct_flag = True if str(method) == "select_distinct" else False

            elif str(method) == "having": 
                self._having_analyze(stmt)
                having_final_ans = self.having_ans

            elif str(method) == "orderby":
                self._orderby_analyze(stmt)
                orderby_final_ans = self.orderby_ans[:-1] if self.orderby_ans else ""

            elif str(method).lower() == "limit":
                self._limit_analyze(stmt)
                limit_final_ans = self.limit_ans

        # Add audit columns if processing sensitive data
        if self.requires_audit_trail and self.security_config.get('enable_audit', True):
            audit_cols = self._add_audit_columns()
            if select_final_ans:
                select_final_ans += "," + ",".join(f'"{col}"' for col in audit_cols)
            else:
                select_final_ans = ",".join(f'"{col}"' for col in audit_cols)

        # Handle set operations
        if set_operation_final_ans:
            final_ans = set_operation_final_ans
            if orderby_final_ans:
                final_ans += f"\n.orderBy({orderby_final_ans})"
            if limit_final_ans:
                final_ans += f"\n.limit({limit_final_ans})"
            return self._add_banking_comments(final_ans)

        # Build regular query
        final_ans = ""
        if from_final_ans:
            final_ans += from_final_ans + "\\"
        if where_final_ans:
            final_ans += f"\n.filter(\"{where_final_ans}\")\\"
        if groupby_final_ans:
            final_ans += f"\n.groupBy(\"{groupby_final_ans}\")\\"
        if agg_final_ans:
            final_ans += f"\n.agg({agg_final_ans})\\"
        if having_final_ans:
            final_ans += f"\n.filter(\"{having_final_ans}\")\\"
        if select_final_ans:
            final_ans += f"\n.selectExpr({select_final_ans})\\"
        if self.distinct_flag:
            final_ans += "\n.distinct()\\"
        if orderby_final_ans:
            final_ans += f"\n.orderBy({orderby_final_ans})\\"
        if limit_final_ans:
            final_ans += f"\n.limit({limit_final_ans})\\"
        
        final_code = final_ans[:-1] if final_ans.endswith('\\') else final_ans
        return self._add_banking_comments(final_code)
    
    def _add_banking_comments(self, code):
        """Add banking compliance comments to generated code"""
        comments = ["# Generated by Capital One DataBathing Engine"]
        
        if self.sensitive_columns:
            comments.append(f"# COMPLIANCE: Sensitive columns detected: {', '.join(self.sensitive_columns)}")
        
        if self.requires_audit_trail:
            comments.append("# COMPLIANCE: Audit trail enabled for this query")
            
        if self.security_config.get('enable_masking', False):
            comments.append("# SECURITY: Data masking enabled")
            
        return "\n".join(comments) + "\n\n" + code
    
    def get_compliance_report(self):
        """Generate compliance report for the processed query"""
        return {
            'sensitive_columns_detected': list(self.sensitive_columns),
            'audit_trail_required': self.requires_audit_trail,
            'masking_enabled': self.security_config.get('enable_masking', False),
            'audit_enabled': self.security_config.get('enable_audit', True),
            'compliance_score': self._calculate_compliance_score()
        }
    
    def _calculate_compliance_score(self):
        """Calculate compliance score (0-100)"""
        score = 100
        
        # Deduct points for unmasked sensitive data
        if self.sensitive_columns and not self.security_config.get('enable_masking', False):
            score -= 30
            
        # Deduct points for missing audit trail on sensitive operations
        if self.requires_audit_trail and not self.security_config.get('enable_audit', True):
            score -= 25
            
        return max(0, score)