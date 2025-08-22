from curses import nonl
# from mo_sql_parsing import parse
from mo_sql_parsing import parse_bigquery as parse
from mo_sql_parsing import format
import json
import copy

from databathing.py_bathing import py_bathing
from databathing.engines import SparkEngine, DuckDBEngine, MojoEngine, COEngine
from databathing.validation.validator_factory import validate_code


class Pipeline:
    def __init__(self, query, engine="spark", validate=True, security_config=None):
        # print(query)
        self.original_query = query
        self.parsed_whole_query = parse(query)
        self.parsed_json_whole_query = json.loads(json.dumps(self.parsed_whole_query,indent=4))
        self.parsed_json_whole_query = self.parsed_json_whole_query
        self.engine = engine.lower()
        self.with_ans = ""
        self.last_ans = ""
        self.validate_code = validate
        self.validation_report = None
        self.security_config = security_config or {}
        
        # Validate engine choice
        if self.engine not in ["spark", "duckdb", "mojo", "co"]:
            raise ValueError(f"Unsupported engine: {engine}. Choose from: spark, duckdb, mojo, co")

    def _get_engine_instance(self, query_data):
        """Factory method to create engine instances based on selected engine"""
        if self.engine == "spark":
            return py_bathing(query_data)  # Keep backward compatibility
        elif self.engine == "duckdb":
            return DuckDBEngine(query_data)
        elif self.engine == "mojo":
            return MojoEngine(query_data)
        elif self.engine == "co":
            return COEngine(query_data, self.security_config)
    
    def gen_with_pipeline(self, query):
        if "with" in query:
            with_stmts =  query["with"]
            if type(with_stmts) is dict:
                self.gen_with_pipeline(with_stmts)
            else:
                for with_stmt in with_stmts:
                    self.gen_with_pipeline(with_stmt)   
        else:
            engine_instance = self._get_engine_instance(query["value"])
            self.with_ans += query["name"] + " = " + engine_instance.parse() + "\n\n"


    def gen_last_pipeline(self, query):
        tmp_query = copy.deepcopy(query) 

        # Handle CTE references in the main query
        if "with" in query:
            # Extract CTE names for reference replacement
            cte_names = []
            if isinstance(query["with"], list):
                cte_names = [cte["name"] for cte in query["with"]]
            elif isinstance(query["with"], dict):
                cte_names = [query["with"]["name"]]
            
            # Replace CTE references in FROM clause with variable names
            self._replace_cte_references(tmp_query, cte_names)
            
            del tmp_query["with"]
        
        engine_instance = self._get_engine_instance(tmp_query)
        
        # Different variable naming based on engine
        if self.engine == "duckdb":
            self.last_ans = "result = " + engine_instance.parse() + "\n\n"
        elif self.engine == "mojo":
            self.last_ans = "# Mojo 🔥 High-Performance Code\n" + engine_instance.parse() + "\n\n"
        elif self.engine == "co":
            self.last_ans = "co_secure_df = " + engine_instance.parse() + "\n\n"
        else:
            self.last_ans = "final_df = " + engine_instance.parse() + "\n\n"
    
    def _replace_cte_references(self, query, cte_names):
        """Replace CTE references in query with actual variable names"""
        if not cte_names:
            return
            
        # Replace in FROM clause
        if "from" in query:
            self._replace_cte_in_from(query["from"], cte_names)
    
    def _replace_cte_in_from(self, from_clause, cte_names):
        """Replace CTE references in FROM clause"""
        if isinstance(from_clause, str):
            # Simple table reference
            if from_clause in cte_names:
                # This is a CTE reference, replace with variable name
                return from_clause  # Variable name is same as CTE name
        elif isinstance(from_clause, dict):
            # Table with alias or JOIN
            if "value" in from_clause and from_clause["value"] in cte_names:
                # This references a CTE, keep the variable name
                pass  # Variable name is same as CTE name
            if "join" in from_clause and "value" in from_clause["join"]:
                if from_clause["join"]["value"] in cte_names:
                    # JOIN references a CTE, keep the variable name
                    pass  # Variable name is same as CTE name
            # Handle other join types
            for join_type in ["left join", "inner join", "right join"]:
                if join_type in from_clause and "value" in from_clause[join_type]:
                    if from_clause[join_type]["value"] in cte_names:
                        # JOIN references a CTE, keep the variable name
                        pass  # Variable name is same as CTE name
        elif isinstance(from_clause, list):
            # Multiple FROM items
            for item in from_clause:
                self._replace_cte_in_from(item, cte_names)

    def parse(self):
        final_ans = ""
        if "with" in self.parsed_json_whole_query:
            self.gen_with_pipeline(self.parsed_json_whole_query)
            final_ans += self.with_ans
        self.gen_last_pipeline(self.parsed_json_whole_query)
        final_ans += self.last_ans
        
        # Validate generated code if requested
        if self.validate_code and self.engine not in ["mojo", "co"]:  # Skip validation for mojo and co for now
            try:
                self.validation_report = validate_code(final_ans, self.engine, self.original_query, use_cache=True)
            except ImportError as e:
                print(f"Warning: Validation dependencies not available: {e}")
                print("Install validation dependencies with: pip install databathing[validation]")
                self.validation_report = None
            except ValueError as e:
                print(f"Warning: Validation configuration error: {e}")
                self.validation_report = None
            except Exception as e:
                print(f"Warning: Unexpected validation error: {e}")
                print("Please report this issue at: https://github.com/jason-jz-zhu/databathing/issues")
                self.validation_report = None
        
        return final_ans
    
    def get_validation_report(self):
        """Get the validation report for the generated code"""
        return self.validation_report
    
    def get_code_score(self):
        """Get the overall code quality score (0-100)"""
        if self.validation_report:
            return self.validation_report.metrics.overall_score
        return None
    
    def get_code_grade(self):
        """Get the code quality grade (A-F)"""
        if self.validation_report:
            return self.validation_report.get_grade()
        return None
    
    def is_code_valid(self):
        """Check if the generated code is syntactically valid"""
        if self.validation_report:
            return self.validation_report.is_valid
        return None
    
    def parse_with_validation(self):
        """Parse and return both code and validation report"""
        code = self.parse()
        return {
            'code': code,
            'validation_report': self.validation_report,
            'score': self.get_code_score(),
            'grade': self.get_code_grade(),
            'is_valid': self.is_code_valid()
        }









# query = """

# select
# df1.firstname,
# count(*) cnt
# from df as df1
# inner join df as df2
# on df1.firstname = df2.firstname
# group by df1.firstname
# having cnt > 0

# """

# query = """
# with step1 as (
#     select firstname, id from df
# ), step2 as (
#     select gender, salary, id from df
# ), step3 as (
#     select 
#         s1.id, s1.firstname, s2.gender, s2.salary
#     from step1 as s1
#     inner join step2 as s2
#     on s1.id = s2.id
# )
# select
#     *,
#     RANK() OVER (PARTITION BY id ORDER BY salary DESC) AS seq
# from step3
# """

# query = """
# SELECT b.id, b.title, a.last_name AS author, e.last_name AS editor,
#             t.last_name AS translator
#         FROM book b
#         LEFT JOIN author a
#         ON b.author_id = a.id
#         LEFT JOIN editor e
#         ON b.editor_id = e.id
#         LEFT JOIN translator t
#         ON b.translator_id = t.id
#         ORDER BY b.id, a.id desc
# """



# query = """
#     WITH namePreDF AS (
#         SELECT 
#             distinct glbl_ptnt_id, 
#             patient_name,
#             struct(split(patient_name, ',')[0] as firstname, split(patient_name, ',')[1] as lastname) as patient_name_info
#         FROM overviewDF
#         WHERE patient_name != ''
#         ORDER BY filled_date desc
#         )
#         SELECT
#             glbl_ptnt_id,
#             collect_set(patient_name_info) as patient_name_info
#         FROM namePreDF
#         GROUP BY glbl_ptnt_id
#         """

# query = """
#             SELECT 
#                 struct(firstname as firstname, lastname as lastname) as name
#             FROM df
# """

# pipeline = Pipeline(query)

# ans = pipeline.parse()
# print(ans)