"""
SQL text retrieval tools.

Tools for fetching SQL text associated with queries.
"""

from typing import Optional
from sim.rca.tools.base import RCATool, ToolResult, DatabaseContext


class FetchSQLTextTool(RCATool):
    """
    Fetch SQL text for a query.
    
    Retrieves the SQL text from the plan cache or query store,
    truncating if too long.
    """
    
    def __init__(self, db_context: Optional[DatabaseContext] = None, max_chars: int = 4000):
        self._db_context = db_context
        self._max_chars = max_chars
    
    @property
    def name(self) -> str:
        return "fetch_sql_text"
    
    @property
    def description(self) -> str:
        return (
            "Fetch the SQL text for a query by its hash or plan handle. "
            "Use this to understand what a query is doing. "
            "Long queries are truncated."
        )
    
    @property
    def parameters_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "query_hash": {
                    "type": "string",
                    "description": "Query hash to look up (provide one of query_hash, plan_handle, or sql_handle)"
                },
                "plan_handle": {
                    "type": "string",
                    "description": "Plan handle (alternative to query_hash)"
                },
                "sql_handle": {
                    "type": "string",
                    "description": "SQL handle (alternative to query_hash)"
                },
            },
        }
    
    def execute(  # type: ignore[override]
        self,
        query_hash: Optional[str] = None,
        plan_handle: Optional[str] = None,
        sql_handle: Optional[str] = None,
    ) -> ToolResult:
        """Fetch SQL text."""
        if not any([query_hash, plan_handle, sql_handle]):
            return ToolResult.fail("One of query_hash, plan_handle, or sql_handle is required")
        
        if self._db_context is None:
            identifier = query_hash or plan_handle or sql_handle
            return ToolResult.ok({
                "status": "unavailable",
                "reason": "No database connection - SQL text should be available in analytics JSON snapshots",
                "identifier": identifier,
            })
        
        try:
            sql_text = self._fetch_sql(query_hash, plan_handle, sql_handle)
            if not sql_text:
                return ToolResult.fail("SQL text not found")
            
            truncated = False
            if len(sql_text) > self._max_chars:
                sql_text = sql_text[:self._max_chars] + "\n-- TRUNCATED --"
                truncated = True
            
            return ToolResult.ok({
                "sql_text": sql_text,
                "truncated": truncated,
                "length": len(sql_text),
            })
            
        except Exception as e:
            return ToolResult.fail(f"Failed to fetch SQL text: {str(e)}")
    
    def _fetch_sql(
        self,
        query_hash: Optional[str] = None,
        plan_handle: Optional[str] = None,
        sql_handle: Optional[str] = None,
    ) -> Optional[str]:
        """Fetch SQL text from database."""
        assert self._db_context is not None, "db_context required for _fetch_sql"
        if query_hash:
            sql = """
                SELECT TOP 1 SUBSTRING(st.text, 
                    (qs.statement_start_offset/2) + 1,
                    ((CASE qs.statement_end_offset
                        WHEN -1 THEN DATALENGTH(st.text)
                        ELSE qs.statement_end_offset
                    END - qs.statement_start_offset)/2) + 1) AS sql_text
                FROM sys.dm_exec_query_stats qs
                CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
                WHERE qs.query_hash = CONVERT(varbinary(8), ?, 1)
            """
            results = self._db_context.query(sql, (query_hash,))
        elif plan_handle:
            sql = """
                SELECT TOP 1 st.text AS sql_text
                FROM sys.dm_exec_query_stats qs
                CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
                WHERE qs.plan_handle = CONVERT(varbinary(64), ?, 1)
            """
            results = self._db_context.query(sql, (plan_handle,))
        else:  # sql_handle
            sql = """
                SELECT text AS sql_text
                FROM sys.dm_exec_sql_text(CONVERT(varbinary(64), ?, 1))
            """
            results = self._db_context.query(sql, (sql_handle,))
        
        if results and results[0].get("sql_text"):
            sql_text: str = results[0]["sql_text"]
            return sql_text
        return None


class AnalyzeSQLTextTool(RCATool):
    """
    Analyze SQL text to identify potential issues.
    
    Performs static analysis of SQL text to find:
    - Missing WHERE clauses
    - SELECT * usage
    - NOLOCK hints
    - Potential cartesian products
    """
    
    @property
    def name(self) -> str:
        return "analyze_sql_text"
    
    @property
    def description(self) -> str:
        return (
            "Analyze SQL text to identify potential issues. "
            "Checks for anti-patterns like missing WHERE clauses, "
            "SELECT *, and NOLOCK hints."
        )
    
    @property
    def parameters_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "sql_text": {
                    "type": "string",
                    "description": "SQL text to analyze"
                },
            },
            "required": ["sql_text"]
        }
    
    def execute(self, sql_text: str) -> ToolResult:  # type: ignore[override]
        """Analyze SQL text."""
        sql_upper = sql_text.upper()
        
        issues = []
        
        # Check for SELECT *
        if "SELECT *" in sql_upper or "SELECT  *" in sql_upper:
            issues.append({
                "type": "select_star",
                "severity": "warning",
                "description": "SELECT * used - may return unnecessary columns",
            })
        
        # Check for missing WHERE on UPDATE/DELETE
        if ("UPDATE " in sql_upper or "DELETE " in sql_upper) and " WHERE " not in sql_upper:
            issues.append({
                "type": "missing_where",
                "severity": "critical",
                "description": "UPDATE/DELETE without WHERE clause",
            })
        
        # Check for NOLOCK hint
        if "NOLOCK" in sql_upper or "READUNCOMMITTED" in sql_upper:
            issues.append({
                "type": "nolock_hint",
                "severity": "warning",
                "description": "NOLOCK hint may cause dirty reads",
            })
        
        # Check for potential cartesian product (multiple tables without JOIN)
        if sql_upper.count(" FROM ") == 1 and "," in sql_upper.split("WHERE")[0] if "WHERE" in sql_upper else sql_upper:
            if " JOIN " not in sql_upper:
                issues.append({
                    "type": "potential_cartesian",
                    "severity": "warning",
                    "description": "Multiple tables without explicit JOIN may cause cartesian product",
                })
        
        # Check for functions on columns in WHERE
        if " WHERE " in sql_upper:
            where_clause = sql_upper.split(" WHERE ")[1].split(" ORDER ")[0].split(" GROUP ")[0]
            funcs = ["CONVERT(", "CAST(", "DATEPART(", "YEAR(", "MONTH(", "ISNULL("]
            for func in funcs:
                if func in where_clause:
                    issues.append({
                        "type": "function_on_column",
                        "severity": "performance",
                        "description": f"Function {func[:-1]} in WHERE may prevent index usage",
                    })
                    break
        
        # Check for ORDER BY without TOP/OFFSET when in subquery
        if "SELECT" in sql_upper.split("FROM")[0] if "FROM" in sql_upper else sql_upper:
            if " ORDER BY " in sql_upper and " TOP " not in sql_upper and " OFFSET " not in sql_upper:
                if sql_upper.count("SELECT") > 1:  # Has subqueries
                    issues.append({
                        "type": "order_in_subquery",
                        "severity": "warning",
                        "description": "ORDER BY in subquery without TOP is ignored",
                    })
        
        return ToolResult.ok({
            "issues_found": len(issues),
            "issues": issues,
            "sql_length": len(sql_text),
        })

