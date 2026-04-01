"""
Execution plan analysis tools.

Tools for fetching, parsing, and comparing SQL Server execution plans.
"""

import xml.etree.ElementTree as ET
from typing import Optional
from sim.rca.tools.base import RCATool, ToolResult, DatabaseContext


class FetchPlanSummaryTool(RCATool):
    """
    Fetch and parse an execution plan into a structured summary.
    
    Extracts key information from execution plan XML:
    - Join order and types
    - Access paths (scans, seeks, lookups)
    - Heavy operators (sorts, hashes, spills)
    - Memory grants
    - Warnings
    """
    
    def __init__(self, db_context: Optional[DatabaseContext] = None):
        self._db_context = db_context
    
    @property
    def name(self) -> str:
        return "fetch_plan_summary"
    
    @property
    def description(self) -> str:
        return (
            "Fetch and parse a SQL Server execution plan into a structured summary. "
            "Returns join order, access paths, heavy operators, memory grants, and warnings. "
            "Use this to understand query execution behavior."
        )
    
    @property
    def parameters_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "query_hash": {
                    "type": "string",
                    "description": "Query hash to look up the plan for (provide this OR plan_handle)"
                },
                "plan_handle": {
                    "type": "string",
                    "description": "Plan handle to look up (provide this OR query_hash)"
                },
            },
        }
    
    def execute(self, query_hash: str = None, plan_handle: str = None) -> ToolResult:
        """Execute the tool to fetch and parse a plan."""
        if not query_hash and not plan_handle:
            return ToolResult.fail("Either query_hash or plan_handle is required")
        
        if self._db_context is None:
            identifier = query_hash or plan_handle
            return ToolResult.ok({
                "status": "unavailable",
                "reason": "No database connection - execution plan details should be analyzed from analytics JSON query_stats",
                "identifier": identifier,
            })
        
        try:
            # Fetch plan XML from database
            plan_xml = self._fetch_plan_xml(query_hash, plan_handle)
            if not plan_xml:
                return ToolResult.fail("Plan not found in cache")
            
            # Parse the plan
            summary = self._parse_plan_xml(plan_xml)
            return ToolResult.ok(summary)
            
        except Exception as e:
            return ToolResult.fail(f"Failed to fetch plan: {str(e)}")
    
    def _fetch_plan_xml(self, query_hash: str = None, plan_handle: str = None) -> Optional[str]:
        """Fetch plan XML from the database."""
        if query_hash:
            sql = """
                SELECT TOP 1 qp.query_plan
                FROM sys.dm_exec_query_stats qs
                CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp
                WHERE qs.query_hash = CONVERT(varbinary(8), ?, 1)
            """
            results = self._db_context.query(sql, (query_hash,))
        else:
            sql = """
                SELECT query_plan
                FROM sys.dm_exec_query_plan(CONVERT(varbinary(64), ?, 1))
            """
            results = self._db_context.query(sql, (plan_handle,))
        
        if results and results[0].get("query_plan"):
            return results[0]["query_plan"]
        return None
    
    def _parse_plan_xml(self, plan_xml: str) -> dict:
        """Parse execution plan XML into structured summary."""
        try:
            root = ET.fromstring(plan_xml)
        except ET.ParseError:
            return {"error": "Invalid plan XML", "raw_length": len(plan_xml)}
        
        # SQL Server plan namespace
        ns = {"p": "http://schemas.microsoft.com/sqlserver/2004/07/showplan"}
        
        summary = {
            "operators": [],
            "access_paths": [],
            "joins": [],
            "warnings": [],
            "memory_grant": None,
            "estimated_cost": None,
            "estimated_rows": None,
        }
        
        # Extract operators
        for op in root.findall(".//p:RelOp", ns):
            op_info = {
                "type": op.get("PhysicalOp"),
                "logical_op": op.get("LogicalOp"),
                "estimated_rows": float(op.get("EstimateRows", 0)),
                "estimated_cost": float(op.get("EstimatedTotalSubtreeCost", 0)),
            }
            summary["operators"].append(op_info)
            
            # Identify access paths
            physical_op = op.get("PhysicalOp", "")
            if "Scan" in physical_op or "Seek" in physical_op:
                obj = op.find(".//p:Object", ns)
                if obj is not None:
                    summary["access_paths"].append({
                        "type": physical_op,
                        "table": obj.get("Table", "").strip("[]"),
                        "index": obj.get("Index", "").strip("[]"),
                        "estimated_rows": op_info["estimated_rows"],
                    })
            
            # Identify joins
            if "Join" in physical_op:
                summary["joins"].append({
                    "type": physical_op,
                    "estimated_rows": op_info["estimated_rows"],
                })
        
        # Extract warnings
        for warning in root.findall(".//p:Warnings", ns):
            for child in warning:
                tag = child.tag.replace(f"{{{ns['p']}}}", "")
                summary["warnings"].append({
                    "type": tag,
                    "details": child.attrib,
                })
        
        # Extract memory grant
        mem_grant = root.find(".//p:MemoryGrantInfo", ns)
        if mem_grant is not None:
            summary["memory_grant"] = {
                "requested_kb": int(mem_grant.get("RequestedMemory", 0)),
                "granted_kb": int(mem_grant.get("GrantedMemory", 0)),
            }
        
        # Get top-level estimates
        stmt = root.find(".//p:StmtSimple", ns)
        if stmt is not None:
            summary["estimated_cost"] = float(stmt.get("StatementSubTreeCost", 0))
            summary["estimated_rows"] = float(stmt.get("StatementEstRows", 0))
        
        return summary


class DiffPlansTool(RCATool):
    """
    Compare two execution plans and identify differences.
    
    Compares baseline and incident plans to find:
    - Join order changes
    - Access path changes
    - Row estimate deltas
    - Memory grant differences
    """
    
    def __init__(self, db_context: Optional[DatabaseContext] = None):
        self._db_context = db_context
        self._plan_tool = FetchPlanSummaryTool(db_context)
    
    @property
    def name(self) -> str:
        return "diff_plans"
    
    @property
    def description(self) -> str:
        return (
            "Compare two execution plans and identify differences. "
            "Use this to detect plan regressions, join order changes, "
            "index changes, and memory grant differences."
        )
    
    @property
    def parameters_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "baseline_query_hash": {
                    "type": "string",
                    "description": "Query hash for the baseline plan"
                },
                "incident_query_hash": {
                    "type": "string",
                    "description": "Query hash for the incident plan"
                },
            },
            "required": ["baseline_query_hash", "incident_query_hash"]
        }
    
    def execute(
        self,
        baseline_query_hash: str,
        incident_query_hash: str,
    ) -> ToolResult:
        """Execute plan comparison."""
        # Fetch both plans
        baseline_result = self._plan_tool.execute(query_hash=baseline_query_hash)
        incident_result = self._plan_tool.execute(query_hash=incident_query_hash)
        
        if not baseline_result.success:
            return ToolResult.fail(f"Failed to fetch baseline plan: {baseline_result.error}")
        if not incident_result.success:
            return ToolResult.fail(f"Failed to fetch incident plan: {incident_result.error}")
        
        baseline = baseline_result.data
        incident = incident_result.data
        
        # Compare plans
        diff = self._compare_plans(baseline, incident)
        
        return ToolResult.ok(diff)
    
    def _compare_plans(self, baseline: dict, incident: dict) -> dict:
        """Compare two plan summaries."""
        diff = {
            "has_changes": False,
            "changes": [],
            "baseline_summary": {
                "operator_count": len(baseline.get("operators", [])),
                "estimated_cost": baseline.get("estimated_cost"),
                "memory_grant_kb": baseline.get("memory_grant", {}).get("requested_kb") if baseline.get("memory_grant") else None,
            },
            "incident_summary": {
                "operator_count": len(incident.get("operators", [])),
                "estimated_cost": incident.get("estimated_cost"),
                "memory_grant_kb": incident.get("memory_grant", {}).get("requested_kb") if incident.get("memory_grant") else None,
            },
        }
        
        # Check operator changes
        baseline_ops = set(op["type"] for op in baseline.get("operators", []))
        incident_ops = set(op["type"] for op in incident.get("operators", []))
        
        new_ops = incident_ops - baseline_ops
        removed_ops = baseline_ops - incident_ops
        
        if new_ops:
            diff["has_changes"] = True
            diff["changes"].append({
                "type": "new_operators",
                "operators": list(new_ops),
            })
        
        if removed_ops:
            diff["has_changes"] = True
            diff["changes"].append({
                "type": "removed_operators",
                "operators": list(removed_ops),
            })
        
        # Check access path changes
        baseline_paths = {(p["table"], p["type"]) for p in baseline.get("access_paths", [])}
        incident_paths = {(p["table"], p["type"]) for p in incident.get("access_paths", [])}
        
        if baseline_paths != incident_paths:
            diff["has_changes"] = True
            diff["changes"].append({
                "type": "access_path_change",
                "baseline": [{"table": t, "access": a} for t, a in baseline_paths],
                "incident": [{"table": t, "access": a} for t, a in incident_paths],
            })
        
        # Check memory grant changes
        b_grant = baseline.get("memory_grant", {}).get("requested_kb") if baseline.get("memory_grant") else 0
        i_grant = incident.get("memory_grant", {}).get("requested_kb") if incident.get("memory_grant") else 0
        
        if b_grant and i_grant and abs(i_grant - b_grant) > b_grant * 0.2:  # 20% change
            diff["has_changes"] = True
            diff["changes"].append({
                "type": "memory_grant_change",
                "baseline_kb": b_grant,
                "incident_kb": i_grant,
                "factor": round(i_grant / b_grant, 2) if b_grant > 0 else None,
            })
        
        # Check for new warnings
        baseline_warnings = set(w["type"] for w in baseline.get("warnings", []))
        incident_warnings = set(w["type"] for w in incident.get("warnings", []))
        
        new_warnings = incident_warnings - baseline_warnings
        if new_warnings:
            diff["has_changes"] = True
            diff["changes"].append({
                "type": "new_warnings",
                "warnings": list(new_warnings),
            })
        
        # Check cost change
        b_cost = baseline.get("estimated_cost", 0)
        i_cost = incident.get("estimated_cost", 0)
        
        if b_cost and i_cost and abs(i_cost - b_cost) > b_cost * 0.5:  # 50% cost change
            diff["has_changes"] = True
            diff["changes"].append({
                "type": "cost_increase",
                "baseline_cost": b_cost,
                "incident_cost": i_cost,
                "factor": round(i_cost / b_cost, 2) if b_cost > 0 else None,
            })
        
        return diff


class FetchPlanXMLTool(RCATool):
    """
    Fetch raw execution plan XML.
    
    Returns the raw XML for advanced analysis or debugging.
    Truncates if too large.
    """
    
    def __init__(self, db_context: Optional[DatabaseContext] = None, max_kb: int = 100):
        self._db_context = db_context
        self._max_kb = max_kb
    
    @property
    def name(self) -> str:
        return "fetch_plan_xml"
    
    @property
    def description(self) -> str:
        return (
            "Fetch the raw execution plan XML for a query. "
            "Use this for detailed analysis when the summary is insufficient. "
            "Output is truncated if over size limit."
        )
    
    @property
    def parameters_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "query_hash": {
                    "type": "string",
                    "description": "Query hash to look up the plan for"
                },
                "max_kb": {
                    "type": "integer",
                    "description": "Maximum KB to return (default: 100)",
                    "default": 100,
                }
            },
            "required": ["query_hash"]
        }
    
    def execute(self, query_hash: str, max_kb: int = None) -> ToolResult:
        """Fetch raw plan XML."""
        max_kb = max_kb or self._max_kb
        
        if self._db_context is None:
            return ToolResult.ok({
                "status": "unavailable",
                "reason": "No database connection - raw plan XML not available in offline mode",
                "query_hash": query_hash,
            })
        
        try:
            sql = """
                SELECT TOP 1 CAST(qp.query_plan AS NVARCHAR(MAX)) as query_plan
                FROM sys.dm_exec_query_stats qs
                CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp
                WHERE qs.query_hash = CONVERT(varbinary(8), ?, 1)
            """
            results = self._db_context.query(sql, (query_hash,))
            
            if not results or not results[0].get("query_plan"):
                return ToolResult.fail("Plan not found in cache")
            
            xml = results[0]["query_plan"]
            truncated = False
            
            # Truncate if too large
            max_bytes = max_kb * 1024
            if len(xml.encode('utf-8')) > max_bytes:
                xml = xml[:max_bytes].rsplit('<', 1)[0]  # Truncate at XML boundary
                xml += "\n<!-- TRUNCATED -->"
                truncated = True
            
            return ToolResult.ok({
                "xml": xml,
                "truncated": truncated,
                "size_kb": len(xml.encode('utf-8')) // 1024,
            })
            
        except Exception as e:
            return ToolResult.fail(f"Failed to fetch plan XML: {str(e)}")

