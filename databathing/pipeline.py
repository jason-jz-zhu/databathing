from curses import nonl
# from mo_sql_parsing import parse
from mo_sql_parsing import parse_bigquery as parse
from mo_sql_parsing import format
import json
import copy

from databathing.py_bathing import py_bathing


class Pipeline:
    def __init__(self, query):
        # print(query)
        self.parsed_whole_query = parse(query)
        self.parsed_json_whole_query = json.loads(json.dumps(self.parsed_whole_query,indent=4))
        self.parsed_json_whole_query = self.parsed_json_whole_query
        self.with_ans = ""
        self.last_ans = ""

    def gen_with_pipeline(self, query):
        if "with" in query:
            with_stmts =  query["with"]
            if type(with_stmts) is dict:
                self.gen_with_pipeline(with_stmts)
            else:
                for with_stmt in with_stmts:
                    self.gen_with_pipeline(with_stmt)   
        else:
            dbing = py_bathing(query["value"])
            self.with_ans += query["name"] + " = " + dbing.parse() + "\n\n"


    def gen_last_pipeline(self, query):
        tmp_query = copy.deepcopy(query) 

        if "with" in query:
            del tmp_query["with"]
        
        dbing = py_bathing(tmp_query)
        self.last_ans = "final_df = " + dbing.parse() + "\n\n"

    def parse(self):
        final_ans = ""
        if "with" in self.parsed_json_whole_query:
            self.gen_with_pipeline(self.parsed_json_whole_query)
            final_ans += self.with_ans
        self.gen_last_pipeline(self.parsed_json_whole_query)
        final_ans += self.last_ans
        return final_ans









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