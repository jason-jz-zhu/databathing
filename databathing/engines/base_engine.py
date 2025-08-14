from abc import ABC, abstractmethod


class BaseEngine(ABC):
    """Abstract base class for SQL-to-code engines"""
    
    def __init__(self, parsed_json_query):
        self.parsed_json_query = parsed_json_query
        self.distinct_flag = False
        self.from_ans = ""
        self.select_ans = ""
        self.where_ans = ""
        self.groupby_ans = ""
        self.limit_ans = ""
        self.agg_ans = ""
        self.having_ans = ""
        self.orderby_ans = ""
        self.set_operation_ans = ""
        self.agg_list = ["sum", "avg", "max", "min", "mean", "count", "collect_list", "collect_set"]
    
    @abstractmethod
    def _from_analyze(self, from_stmt):
        """Analyze FROM clause and generate engine-specific code"""
        pass
    
    @abstractmethod
    def _select_analyze(self, select_stmt):
        """Analyze SELECT clause and generate engine-specific code"""
        pass
    
    @abstractmethod
    def _where_analyze(self, where_stmt):
        """Analyze WHERE clause and generate engine-specific code"""
        pass
    
    @abstractmethod
    def _groupby_analyze(self, groupby_stmt):
        """Analyze GROUP BY clause and generate engine-specific code"""
        pass
    
    @abstractmethod
    def _agg_analyze(self, agg_stmt):
        """Analyze aggregation functions and generate engine-specific code"""
        pass
    
    @abstractmethod
    def _having_analyze(self, having_stmt):
        """Analyze HAVING clause and generate engine-specific code"""
        pass
    
    @abstractmethod
    def _orderby_analyze(self, order_stmt):
        """Analyze ORDER BY clause and generate engine-specific code"""
        pass
    
    @abstractmethod
    def _limit_analyze(self, limit_stmt):
        """Analyze LIMIT clause and generate engine-specific code"""
        pass
    
    @abstractmethod
    def _set_operation_analyze(self, operation_type, queries_list):
        """Handle UNION, INTERSECT, EXCEPT operations"""
        pass
    
    @abstractmethod
    def parse(self):
        """Parse the complete query and return engine-specific code"""
        pass
    
    @property
    @abstractmethod
    def engine_name(self):
        """Return the name of this engine"""
        pass