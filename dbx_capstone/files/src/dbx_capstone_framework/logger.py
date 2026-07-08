"""Logger Class to Log all Pipeline runs and errors"""
 
import uuid 
import traceback
import json
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from pyspark.sql.types import *
from pyspark.sql import SparkSession
 
class EtlLogger:
    """Reusable ETL logger for Databricks notebooks and pipelines.
 
    Logs run-level metadata and detailed errors to Delta tables.
    """
 
    def __init__(
        self,
        spark_session: SparkSession,
        pipeline_name: str,
        environment: str,
        run_id: Optional[str] = None
    ) -> None:
        self.spark = spark_session
        self.pipeline_name = pipeline_name
        self.environment = environment
        self.run_id = run_id or str(uuid.uuid4())
        # self.log_catalog = f"dbx_capstone_log{self.environment}"
        # self.run_log_table = f"{self.log_catalog}.logs.etl_run_log"
        # self.error_log_table = f"{self.log_catalog}.logs.etl_error_log"
        self.log_catalog = "dbx_capstone"
        self.run_log_table = f"{self.log_catalog}.log.etl_run_log"
        self.error_log_table = f"{self.log_catalog}.log.etl_error_log"
 
        self.cluster_id = self.spark.conf.get(
            "spark.databricks.clusterUsageTags.clusterId",
            "unknown",
        )
 
        self.start_time = datetime.now(timezone.utc)
 
 
    # ---------- helpers ----------
    def _format_exc(self, exc: Exception) -> Optional[str]:
        if exc is None:
            return None
        try:
            return "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        except (TypeError, ValueError):
            return f"{type(exc).__name__}: {exc}"
 
    def _extract_last_frame(self, exc: Exception) -> Dict[str, Optional[str]]:
        try:
            frames = traceback.extract_tb(exc.__traceback__)
            if frames:
                last = frames[-1]
                return {
                    "file": str(last.filename), 
                    "line": str(last.lineno), 
                    "function": str(last.name)
                }
        except (TypeError, ValueError):
            pass
        return {"file": None, "line": None, "function": None}
 
    def _format_extra_ctx(self, extra_ctx: Optional[dict]) -> Optional[str]:
        if not extra_ctx:
            return None
        try:
            return json.dumps(extra_ctx, ensure_ascii=False)
        except (TypeError, ValueError):
            return str(extra_ctx)
 
    def _truncate(self, s: Optional[str], limit: int = 20000) -> Optional[str]:
        if s is None:
            return None
        return s if len(s) <= limit else (s[:limit] + "...<truncated>")
 
    def log_run(
        self,
        status: str,
        records_processed: int = 0,
        info: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log a single ETL run (or stage) event."""
        end_time = datetime.now(timezone.utc)
 
        schema = StructType([
            StructField("run_id", StringType(), False),
            StructField("pipeline_name", StringType(), False),
            StructField("environment", StringType(), False),
            StructField("cluster_id", StringType(), True),
            StructField("start_time", TimestampType(), False),
            StructField("end_time", TimestampType(), True),
            StructField("status", StringType(), False),
            StructField("error_message", StringType(), True),
            StructField("additional_info", StringType(), True)
        ])
 
        data = [{
            "run_id": str(self.run_id),
            "pipeline_name": str(self.pipeline_name),
            "environment": str(self.environment),
            "cluster_id": str(self.cluster_id) if self.cluster_id else None,
            "start_time": self.start_time,  # already datetime
            "end_time": end_time,           # datetime or None
            "status": str(status),
            "error_message": None,
            "additional_info": str(info) if info else None
        }]
 
        df = self.spark.createDataFrame(data, schema=schema)
 
        df.write.format("delta").mode("append").saveAsTable(self.run_log_table)
 
 
    def log_error(
        self,
        stage: str,
        error_type: str,
        error_message: str,
        record: Optional[str] = None,
        exc: Exception = None,
        extra_ctx: dict = None,
    ) -> None:
        """Log an error with full stack trace and optional context."""
 
        schema = StructType([
                StructField("ts_utc", TimestampType(), False),
                StructField("run_id", StringType(), False),
                StructField("pipeline_name", StringType(), False),
                StructField("environment", StringType(), False),
                StructField("cluster_id", StringType(), True),
                StructField("stage", StringType(), False),
                StructField("error_type", StringType(), True),
                StructField("error_message", StringType(), True),
                StructField("record", StringType(), True),
                # Diagnostics
                StructField("traceback", StringType(), True),
                StructField("cause_traceback", StringType(), True),
                StructField("context_traceback", StringType(), True),
                StructField("file", StringType(), True),
                StructField("line", StringType(), True),
                StructField("function", StringType(), True),
                StructField("extra_ctx", StringType(), True),
        ])
 
        # Build diagnostics
        trace = self._format_exc(exc)
        cause_trace = self._format_exc(getattr(exc, "__cause__", None)) if exc else None
        context_trace = self._format_exc(getattr(exc, "__context__", None)) if exc else None
        frame = self._extract_last_frame(exc) if exc else {
            "file": None, 
            "line": None, 
            "function": None
        }
 
        data = [{
            "ts_utc": datetime.now(timezone.utc),
            "run_id": str(self.run_id),
            "pipeline_name": str(self.pipeline_name),
            "environment": str(self.environment),
            "cluster_id": str(self.cluster_id) if self.cluster_id else None,
            "stage": str(stage),
            "error_type": str(error_type) if error_type else None,
            "error_message": self._truncate(str(error_message)) if error_message else None,
            "record": self._truncate(str(record)) if record else None,
            "traceback": self._truncate(trace),
            "cause_traceback": self._truncate(cause_trace),
            "context_traceback": self._truncate(context_trace),
            "file": frame["file"],
            "line": frame["line"],
            "function": frame["function"],
            "extra_ctx": self._format_extra_ctx(extra_ctx),
        }]
 
        df = self.spark.createDataFrame(data, schema=schema)
 
        df.write.format("delta").mode("append").saveAsTable(self.error_log_table)