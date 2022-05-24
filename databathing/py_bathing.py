from curses import nonl
# from mo_sql_parsing import parse
from mo_sql_parsing import parse_bigquery as parse
from mo_sql_parsing import format
import json


class py_bathing:
    def __init__(self, parsed_json_whole_query):
        self.parsed_json_whole_query = parsed_json_whole_query

        self.distinct_flag = False
        self.from_ans = ""
        self.select_ans = ""
        self.level_select = 0
        self.where_ans = ""
        self.groupby_ans = ""
        self.limit_ans = ""
        self.agg_ans = ""
        self.having_ans = ""
        self.orderby_ans = ""
        self.agg_list = ["sum", "avg", "max", "min", "mean", "count", "collect_list", "collect_set"]


    def _from_analyze(self, from_stmt):
        if not from_stmt:
            return 
        if type(from_stmt) is str:
            self.from_ans += format({ "from": from_stmt })[5:]
        elif type(from_stmt) is dict:
            if "name" in from_stmt.keys():
                self.from_ans += from_stmt['value']+".alias(\""+from_stmt['name']+"\")."
            elif "left join" in from_stmt.keys():
                self.from_ans += "join({}, {}, \"{}\").".format( 
                    from_stmt['left join']['value']+".alias(\""+from_stmt['left join']['name']+"\")", 
                    "col(\""+str(from_stmt['on']['eq'][0])+"\")" + "==" + "col(\""+str(from_stmt['on']['eq'][1])+"\")" , 
                    'left')
            elif "inner join" in from_stmt.keys():
                self.from_ans += "join({}, {}, \"{}\").".format( 
                    from_stmt['inner join']['value']+".alias(\""+from_stmt['inner join']['name']+"\")", 
                    "col(\""+str(from_stmt['on']['eq'][0])+"\")" + "==" + "col(\""+str(from_stmt['on']['eq'][1])+"\")" , 
                    'inner')
            elif "right join" in from_stmt.keys():
                self.from_ans += "join({}, {}, \"{}\").".format( 
                    from_stmt['right join']['value']+".alias(\""+from_stmt['right join']['name']+"\")", 
                    "col(\""+str(from_stmt['on']['eq'][0])+"\")" + "==" + "col(\""+str(from_stmt['on']['eq'][1])+"\")" , 
                    'right')
                
        elif type(from_stmt) is list:
            for item_from in from_stmt:
                self._from_analyze(item_from)    
    
    def _select_analyze(self, select_stmt):
        # print(select_stmt)

        if not select_stmt:
            return

        if  type(select_stmt) is str:
            self.select_ans  += "\"" + format({ "select": select_stmt })[7:] + "\","
            return  
        if type(select_stmt) is dict and type(select_stmt['value']) is str:
            self.select_ans  += "\"" + format({ "select": select_stmt })[7:] + "\","
            return
        if type(select_stmt) is dict:
            if list(select_stmt["value"].keys())[0].lower() in self.agg_list:
                self.select_ans  += "\""+ select_stmt['name'] +"\","
            elif list(select_stmt["value"].keys())[0].lower() == "create_struct":
                self.select_ans  += "\"" + format({ "select": select_stmt })[14:] + "\","
            else:
                self.select_ans  += "\"" + format({ "select": select_stmt })[7:] + "\","
        elif type(select_stmt) is list and (self.level_select == 0):
            self.level_select += 1
            for inner_item in select_stmt:
                self._select_analyze(inner_item)


    def _where_analyze(self, where_stmt):
        self.where_ans = format({ "where": where_stmt })[6:]

    def _groupby_analyze(self, groupby_stmt):
        self.groupby_ans = format({ "groupby": groupby_stmt })[9:]

    def _agg_analyze(self, agg_stmt):
        if type(agg_stmt) is dict:
            if type(agg_stmt["value"]) is dict and list(agg_stmt["value"].keys())[0].lower() in self.agg_list:
                for funct, alias in agg_stmt["value"].items():
                    self.agg_ans += "{}(col(\"{}\")).alias(\"{}\"),".format(funct, alias, agg_stmt["name"])

        elif type(agg_stmt) is list:
            for item in agg_stmt:
                self._agg_analyze(item)

                # if type(item["value"]) is dict and list(item["value"].keys())[0].lower() in self.agg_list:
                #     for funct, alias in item["value"].items():
                #         self.agg_ans += "{}(col(\"{}\")).alias(\"{}\"),".format(funct, alias, item["name"])
                    
        self.agg_ans = self.agg_ans.replace("\n", "")


    def _having_analyze(self, having_stmt):
            self.having_ans = format({ "having": having_stmt })[7:]

    def _orderby_analyze(self, order_stmt):
        # print(order_stmt)
        if type(order_stmt) is dict:
            odr = "desc()" if order_stmt.get("sort", "asc") == "desc" else "asc()"
            self.orderby_ans += "col(\"{}\").{},".format(str(order_stmt["value"]), odr)
        else:
            for item in order_stmt:
                self._orderby_analyze(item)
        # for item in order_stmt:
        #     odr = "desc()" if item.get("sort", "asc") == "desc" else "asc()"
        #     self.orderby_ans += "col(\"{}\").{},".format(str(item["value"]), odr)

    def _limit_analyze(self, limit_stmt):
        self.limit_ans = limit_stmt

    def parse(self):
        from_final_ans = where_final_ans = groupby_final_ans = agg_final_ans = select_final_ans = orderby_final_ans = limit_final_ans = having_final_ans = ""

        for method, stmt in self.parsed_json_whole_query.items():
            # handle from
            if str(method).lower() == "from":
                self._from_analyze(stmt)
                from_final_ans = self.from_ans[:-1] if self.from_ans[-1] == '.' else self.from_ans

            #handle where
            elif str(method).lower() == "where":
                self._where_analyze(stmt)
                where_final_ans = self.where_ans

            #handle groupby and agg
            elif str(method).lower() == "groupby":
                # group by
                self._groupby_analyze(stmt)
                groupby_final_ans = self.groupby_ans
                # agg
                agg_stmt = self.parsed_json_whole_query["select"] \
                    if "select" in self.parsed_json_whole_query.keys() \
                    else self.parsed_json_whole_query["select_distinct"]
                self._agg_analyze(agg_stmt)
                agg_final_ans = self.agg_ans[:-1]

            #handle select
            elif str(method).lower() in ["select", "select_distinct"]:
                self._select_analyze(stmt)
                select_final_ans = self.select_ans[:-1]
                self.distinct_flag = True if str(method) == "select_distinct" else  False

            # handle having
            elif str(method) =="having": 
                self._having_analyze(stmt)
                having_final_ans = self.having_ans

            #handle sort
            elif str(method) =="orderby":
                self._orderby_analyze(stmt)
                orderby_final_ans = self.orderby_ans[:-1]

            #handle limit
            elif str(method).lower() =="limit":
                self._limit_analyze(stmt)
                limit_final_ans = self.limit_ans

        final_ans = ""
        if from_final_ans:
            final_ans += from_final_ans + "\\"
        if where_final_ans:
            final_ans += "\n.filter(\"{}\")\\".format(where_final_ans) 
        if groupby_final_ans:
            final_ans += "\n.groupBy(\"{}\")\\".format(groupby_final_ans)
        if agg_final_ans:
            final_ans += "\n.agg({})\\".format(agg_final_ans)
        if having_final_ans:
            final_ans += "\n.filter(\"{}\")\\".format(having_final_ans) 
        if select_final_ans:
            final_ans += "\n.selectExpr({})\\".format(select_final_ans)
        if self.distinct_flag:
            final_ans += "\n.distinct()\\"
        if orderby_final_ans:
            final_ans += "\n.orderBy("+orderby_final_ans+")\\"
        if limit_final_ans:
            final_ans +=  "\n.limit("+str(limit_final_ans)+")\\"
        
        return final_ans[:-1]




# query = """
#         SELECT 
#             distinct glbl_ptnt_id, 
#             patient_name,
#             split(patient_name, ',')[0] as first_name,
#             split(patient_name, ',')[1] as last_name,
#             struct(first_name as firstname, last_name as lastname) as patient_name_info
#         FROM overviewDF
#         WHERE patient_name != ''
#         ORDER BY filled_date desc
#         """


# parsed_whole_query = parse(query)
# parsed_json_whole_query = json.loads(json.dumps(parsed_whole_query,indent=4))

# # print(parsed_json_whole_query)

# dbing = py_bathing(parsed_json_whole_query)
# ans = dbing.parse()
# print(ans)