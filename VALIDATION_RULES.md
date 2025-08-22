# 📋 **DataBathing Validation Rules Documentation**

## 🎯 **Overview**

DataBathing includes a comprehensive validation system that analyzes generated code for syntax correctness, performance issues, best practices, and maintainability. The system supports multiple engines (Spark, DuckDB, Mojo) with engine-specific rules.

---

## 🏗️ **Validation Architecture**

### **Scoring System**
- **Overall Score**: Weighted combination of 5 metrics (0-100 scale)
- **Grading**: A (90+), B (80-89), C (70-79), D (60-69), F (<60)

### **Score Weights**
```python
{
    'syntax': 30%,          # Code compiles/parses correctly
    'complexity': 15%,      # Code complexity (lower is better)
    'readability': 15%,     # Code readability and formatting  
    'performance': 30%,     # Performance optimization (critical!)
    'maintainability': 10%  # Long-term code maintainability
}
```

---

## ⚡ **Spark Engine Validation Rules**

### **🚨 Performance Anti-Patterns** (High Impact)

| Rule | Pattern | Message | Penalty | Severity |
|------|---------|---------|---------|----------|
| **SELECT * Usage** | `.selectExpr("*")` | Avoid SELECT * in selectExpr for performance | **45** | 🔴 Critical |
| **Collect on Large Data** | `.collect()` | Avoid collect() on large datasets | **40** | 🔴 Critical |
| **Iterate Collected Data** | `for.*\.collect()` | Avoid iterating over collected data | **35** | 🔴 High |
| **Collect after GroupBy** | `.groupBy().count().collect()` | Avoid collect() after groupBy operations | **35** | 🔴 High |
| **RDD API Usage** | `.rdd\.` | Using RDD API reduces DataFrame optimization | **30** | 🔴 High |
| **toPandas() Usage** | `.toPandas()` | Use toPandas() carefully with large data | **25** | 🟠 Medium |
| **Complex Multiple Joins** | `.join().join().join()` | Complex multiple joins may need optimization | **25** | 🟠 Medium |
| **Multiple Count Calls** | `.count().*\.count()` | Multiple count() calls can be expensive | **20** | 🟡 Medium |
| **Multiple Cache Calls** | `.cache().*\.cache()` | Unnecessary multiple cache() calls | **15** | 🟡 Low |
| **Multiple Show Calls** | `.show().*\.show()` | Multiple show() calls impact performance | **10** | 🟡 Low |

### **📖 Best Practice Rules**

| Rule | Detection | Message | Category |
|------|-----------|---------|----------|
| **Long Method Chains** | `code.count('.') > 5 and '\\' not in code` | Consider breaking long method chains across lines | Readability |
| **Column References** | `'"' in code and 'col(' not in code` | Consider using col() function for column references | Best Practice |
| **Deprecated Methods** | `.unionAll(` | unionAll() is deprecated, use union() instead | Deprecated |
| **Inefficient GroupBy** | `.groupBy() without .agg() but with .count()` | Consider using agg() with groupBy for better performance | Performance |
| **Missing Aliases** | Complex expressions without `AS` | Consider adding aliases to complex expressions | Readability |

### **✅ Performance Bonuses**

| Pattern | Bonus | Reason |
|---------|-------|---------|
| **Filter Before Select** | +5 points | Efficient data processing order |
| **Function Usage** | +2 points per function | Using optimized Spark functions |

---

## 🦆 **DuckDB Engine Validation Rules**

### **🚨 Performance Anti-Patterns**

| Rule | Pattern | Message | Severity |
|------|---------|---------|----------|
| **SELECT * with ORDER BY** | `SELECT\s+\*.*ORDER BY` | SELECT * with ORDER BY can be inefficient on large tables | 🔴 High |
| **Leading LIKE Wildcards** | `WHERE.*LIKE\s+["\']%.*%["\']` | Leading wildcard in LIKE can prevent index usage | 🟠 Medium |
| **Correlated Subqueries** | `WHERE.*\(\s*SELECT` | Correlated subqueries can be slow | 🟠 Medium |
| **DISTINCT with ORDER BY** | `DISTINCT.*ORDER BY` | DISTINCT with ORDER BY may require sorting large datasets | 🟡 Medium |
| **HAVING with COUNT(*)** | `GROUP BY.*HAVING.*COUNT\(\*\)` | HAVING with COUNT(*) processes all groups before filtering | 🟡 Medium |

### **📖 Best Practice Rules**

| Rule | Detection | Message | Category |
|------|-----------|---------|----------|
| **SELECT * Usage** | `SELECT\s+\*` | Avoid SELECT *, specify column names explicitly | Best Practice |
| **Missing Table Aliases** | Multiple JOINs without `AS` | Consider using table aliases in complex joins | Readability |
| **Complex WHERE Clauses** | WHERE clause > 200 characters | Consider breaking complex WHERE clauses into CTEs | Readability |
| **ORDER BY without LIMIT** | `ORDER BY` without `LIMIT` | Consider adding LIMIT to ORDER BY queries for better performance | Best Practice |
| **HAVING vs WHERE** | `HAVING` without `WHERE` | Consider using WHERE instead of HAVING when possible | Performance |
| **NULL Handling** | `NULL` without `COALESCE`/`IS NULL` | Consider explicit NULL handling with COALESCE or IS NULL checks | Best Practice |

---

## 🔧 **Syntax Validation Rules**

### **Spark Syntax Validation**
- ✅ **Python AST Parsing**: Code must be valid Python expression
- ✅ **Spark Structure**: Must use valid DataFrame methods
- ✅ **Method Chaining**: Proper dot notation and parentheses
- ✅ **Import Detection**: Checks for required Spark imports

### **DuckDB Syntax Validation**
- ✅ **SQL Parsing**: Valid SQL syntax using sqlparse (optional)
- ✅ **Quote Balancing**: Matched single/double quotes
- ✅ **Parentheses Balancing**: Matched parentheses
- ✅ **Statement Structure**: Valid SELECT/WITH/etc. statements
- ✅ **Keyword Order**: Proper SQL keyword sequence

---

## 🎛️ **Custom Rules Framework**

### **RegexRule Class**
Create pattern-based validation rules:

```python
rule = RegexRule(
    name="no_hardcoded_values",
    description="Avoid hardcoded values in queries", 
    pattern=r"WHERE\s+\w+\s*=\s*\d+",
    message="Consider parameterizing hardcoded values",
    severity=ValidationStatus.WARNING,
    applicable_engines=["spark", "duckdb"]
)
```

### **FunctionRule Class**
Create custom logic-based rules:

```python
def check_table_naming(code: str, engine_type: str) -> List[ValidationIssue]:
    # Custom validation logic
    pass

rule = FunctionRule(
    name="table_naming_convention",
    description="Check table naming conventions",
    check_function=check_table_naming
)
```

---

## 📊 **Rule Priority Matrix**

### **Critical Rules** (40+ penalty points)
1. 🔴 **SELECT * Usage** (45 points) - Major performance impact
2. 🔴 **collect() on Large Data** (40 points) - Memory/performance killer

### **High Priority Rules** (25-39 penalty points)  
3. 🟠 **Iterate Collected Data** (35 points)
4. 🟠 **Collect after GroupBy** (35 points)
5. 🟠 **RDD API Usage** (30 points)
6. 🟠 **toPandas() Usage** (25 points)
7. 🟠 **Complex Multiple Joins** (25 points)

### **Medium Priority Rules** (10-24 penalty points)
8. 🟡 **Multiple Count Calls** (20 points)
9. 🟡 **Multiple Cache Calls** (15 points)
10. 🟡 **Multiple Show Calls** (10 points)

---

## 💡 **Examples & Fixes**

### ❌ **Bad Code Example** (Grade D/F)
```python
# Multiple violations: SELECT *, collect(), no line breaks
final_df = large_table.selectExpr("*").orderBy("created_at").collect()
```
**Issues**: SELECT * (45 points), collect() (40 points) = 85 point penalty

### ✅ **Good Code Example** (Grade A)
```python
# Best practices: specific columns, proper formatting, no collect()
final_df = large_table\
    .filter(col("status") == "active")\
    .selectExpr("id", "name", "created_at")\
    .orderBy(col("created_at").desc())\
    .limit(1000)
```

---

## 🔧 **Configuration & Usage**

### **Enable/Disable Validation**
```python
# With validation (default)
pipeline = Pipeline(query, engine="spark", validate=True)

# Without validation  
pipeline = Pipeline(query, engine="spark", validate=False)
```

### **Direct Validation**
```python
from databathing.validation.validator_factory import validate_code

# Validate generated code
report = validate_code(code, engine="spark")
print(f"Score: {report.metrics.overall_score}/100 (Grade: {report.get_grade()})")
```

### **Custom Rules Registration**
```python
from databathing.validation.custom_rules import get_custom_rules_registry

registry = get_custom_rules_registry()
registry.register_rule(my_custom_rule)
```

---

## 📈 **Performance Impact Analysis**

### **High Impact Rules** (30%+ score reduction)
- **SELECT * + collect()**: Can reduce score from A to F
- **Multiple performance anti-patterns**: Cumulative penalties

### **Medium Impact Rules** (10-30% score reduction)  
- **Single performance violation**: Grade drop of 1-2 levels
- **Multiple best practice violations**: Gradual score reduction

### **Low Impact Rules** (<10% score reduction)
- **Readability issues**: Minor score impact
- **Style violations**: Warning level only

---

## 🚀 **Getting Started**

1. **Basic Usage**: Validation is enabled by default in all Pipeline operations
2. **Review Reports**: Check `pipeline.validation_report` for detailed feedback
3. **Custom Rules**: Extend the system with your organization's coding standards
4. **Performance Focus**: Pay special attention to rules with 25+ penalty points

This comprehensive validation system ensures DataBathing generates high-quality, performant, and maintainable code across all supported engines! 🚀