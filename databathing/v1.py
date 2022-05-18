from curses import nonl
# from mo_sql_parsing import parse
from mo_sql_parsing import parse_bigquery as parse

from mo_sql_parsing import format
import json

# query = """
# SELECT product_id as new_product_id,
#     Count(star_rating) as total_rating,
#     Max(star_rating)   AS best_rating,
#     Min(star_rating)   AS worst_rating,
#     ROW_NUMBER() OVER (PARTITION BY firstname ORDER BY salary DESC) AS SEQUENCE
# FROM   tbl_books
# WHERE  verified_purchase = 'Y'
#     AND review_date BETWEEN '1995-07-22' AND '2015-08-31'
#     AND marketplace IN ( 'DE', 'US', 'UK', 'FR', 'JP' )
# GROUP  BY product_id
# ORDER  BY total_rating asc,product_id desc,best_rating
# LIMIT  10;
# """

# query = """select distinct 
# firstname,
# lastname,
# case when gender == "M" then "m" else "f" end as new,
# ROW_NUMBER() OVER (PARTITION BY firstname ORDER BY salary DESC) AS SEQUENCE
# from test1 t1
# where t1.cc = 'cc'"""


query = """
select distinct t1 as tt2, t2 as tt2
from test
"""

# query = """select 
# t1.a, t2.b
# from test1 t1
# left join test2 t2
# on t1.a = t2.a
# inner join test3 t3
# on t2.b = t3.b
# where t1.cc = 'cc'"""

# query = """select 
# t1.a, t2.b
# from test1 t1
# where t1.cc = 'cc'"""

# query = """
# with tmp as (
# SELECT product_id as new_product_id,
#     Count(star_rating) as total_rating,
#     Max(star_rating)   AS best_rating,
#     Min(star_rating)   AS worst_rating,
#     ROW_NUMBER() OVER (PARTITION BY firstname ORDER BY salary DESC) AS SEQUENCE
# FROM   tbl_books
# WHERE  verified_purchase = 'Y'
#     AND review_date BETWEEN '1995-07-22' AND '2015-08-31'
#     AND marketplace IN ( 'DE', 'US', 'UK', 'FR', 'JP' )
# GROUP  BY product_id
# ORDER  BY total_rating asc,product_id desc,best_rating
# LIMIT  10
# ), aa as (select * from tmp)
# select * from aa
# """


# query = """
# with step1 as (
#     select * from t1
# ), step2 as (
#     select * from t2
# ), step3 as (
#     select 
#         s1.a, s2.b
#     from step1 as s1
#     inner join step2 as s2
#     on s1.a = s2.a
# )
# select
#     a, b, 
#     ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC) AS seq
# from step3
# """


v_parse = parse(query)
v_json = json.loads(json.dumps(v_parse,indent=4))
print(v_json)

distinct_flag = False

result_from=""
def fn_from(value):
    global result_from
    if not value:
        return 
    if type(value) is str:
        result_from += format({ "from": value })[5:]
    elif type(value) is dict:
        if "name" in value.keys():
            result_from += value['value']+".alias(\""+value['name']+"\")."
        elif "left join" in value.keys():
            result_from += "join({}, {}, \"{}\").".format( 
                value['left join']['value']+".alias(\""+value['left join']['name']+"\")", 
                "col(\""+str(value['on']['eq'][0])+"\")" + "===" + "col(\""+str(value['on']['eq'][1])+"\")" , 
                'left')
        elif "inner join" in value.keys():
            result_from += "join({}, {}, \"{}\").".format( 
                value['inner join']['value']+".alias(\""+value['inner join']['name']+"\")", 
                "col(\""+str(value['on']['eq'][0])+"\")" + "===" + "col(\""+str(value['on']['eq'][1])+"\")" , 
                'inner')
        elif "right join" in value.keys():
            result_from += "join({}, {}, \"{}\").".format( 
                value['right join']['value']+".alias(\""+value['right join']['name']+"\")", 
                "col(\""+str(value['on']['eq'][0])+"\")" + "===" + "col(\""+str(value['on']['eq'][1])+"\")" , 
                'right')
            
    elif type(value) is list:
        for item_from in value:
            fn_from(item_from)




# def fn_from(value):
#     print("------")
#     print(value)
#     print("------")
#     result_from=""
#     if type(value) is str:
#         result_from = format({ "from": value })
#         result_from = result_from[5:]
#     # elif type(value) is dict:
#     #     if "name" in value.keys():
#     #         result_from = result_from + value['value']+".alias(\""+value['name']+"\")"
#     #     else:
#     #         result_from = result_from + value['value']+""
#     elif type(value) is list:
#         for item_from in value:
#             if type(item_from) is dict:
#                 if "name" in item_from.keys():
#                     result_from = result_from + item_from['value']+".alias(\""+item_from['name']+"\"),"
#                 else:
#                     result_from = result_from + item_from['value']+","
#             elif type(item_from) is str:
#                 result_from = result_from + item_from+","
#     return result_from
        

agg_list = ["sum", "avg", "max", "min", "mean", "count"]
result_select = ""
level_select =0
def fn_select(value):
    global distinct_flag
    global result_select
    global level_select
    if not value:
        return
    
    if  type(value) is str:
        result_select += "\"" + format({ "select": value })[7:] + "\","
        return  
    if type(value) is dict and type(value['value']) is str:
        result_select += "\"" + format({ "select": value })[7:] + "\","
        return
    if type(value) is dict:
        if "distinct" in value["value"].keys():
            distinct_flag = True
            level_select += 1
            fn_select(value["value"]["distinct"])
        elif list(value["value"].keys())[0].lower() in agg_list:
            result_select += "\""+ value['name'] +"\","
        else:
            result_select += "\"" + format({ "select": value })[7:] + "\","
    elif type(value) is list and (level_select == 0 or (level_select == 1 and distinct_flag)):
        for inner_item in value:
            fn_select(inner_item)

def fn_where(value):
    result_where=""
    result_where = format({ "where": value })[6:]
    return result_where


def fn_groupby(value):
    result_groupby=""
    result_groupby = format({ "groupby": value })[9:]
    return result_groupby

def fn_agg(query):
    v_parse = parse(query)
    v_agg = ""
    for i in v_parse["select"]:
        if type(i["value"]) is dict:
            for key,value in i["value"].items():
                v_agg = v_agg + (key+"("+"col(\""+str(value)+"\")"+").alias('"+i["name"]+"')") +","
    v_agg = v_agg.replace("\n", "")
    return v_agg[:-1]


def fn_orderby(query):
    v_parse = parse(query)
    v_orderby_collist=""
    v_orderby = v_parse["orderby"]
    for i in v_orderby:
        if i.get("sort", "asc") == "desc":
            v_sortorder = "desc()"
        else:
            v_sortorder = "asc()"
        v_orderby_collist = v_orderby_collist + "col(\""+str(i.get("value", ""))+"\")." +v_sortorder+","
    return v_orderby_collist[:-1]


def fn_limit(query):
    v_parse = parse(query)
    v_limit = v_parse["limit"]
    return v_limit


def fn_genSQL(data):
    v_fn_from = v_fn_where = v_fn_groupby = v_fn_agg = v_fn_select = v_fn_orderby = v_fn_limit = ""
    for key,value in data.items():
        # handle from
        if str(key)=="from":
            fn_from(value)
            v_fn_from = result_from[:-1]

        #handle where
        if str(key) =="where":
            v_fn_where = fn_where(value)

        #handle groupby
        if str(key) =="groupby":
            v_fn_groupby = fn_groupby(value)

        #handle agg
        if str(key) =="groupby":
            v_fn_agg = fn_agg(query)

        #handle select
        if str(key) =="select":
            fn_select(value)
            v_fn_select = result_select[:-1]


        #handle sort
        if str(key) =="orderby":
            v_fn_orderby = fn_orderby(query)

        #handle limit
        if str(key) =="limit":
            v_fn_limit = fn_limit(query)

    v_final_stmt = ""
    if v_fn_from:
        v_final_stmt = v_final_stmt + v_fn_from
    if v_fn_where:
        v_final_stmt = v_final_stmt + "\n.filter(\""+v_fn_where+"\")"
    if v_fn_groupby:
        v_final_stmt = v_final_stmt + "\n.groupBy(\""+v_fn_groupby+"\")"
    if v_fn_agg:
        v_final_stmt = v_final_stmt + "\n.agg("+v_fn_agg+"\")"
    if v_fn_select:
        v_final_stmt = v_final_stmt + "\n.selectExpr("+v_fn_select+")"
    if distinct_flag:
        v_final_stmt = v_final_stmt + "\n.distinct()"
    if v_fn_orderby:
        v_final_stmt = v_final_stmt + "\n.orderBy("+v_fn_orderby+")"
    if v_fn_limit:
        v_final_stmt = v_final_stmt + "\n.limit("+str(v_fn_limit)+")"
    
    return v_final_stmt
    

print (fn_genSQL(v_json))
