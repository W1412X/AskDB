"""
统一的日志配置模块
提供结构化的日志记录功能，包括工作流程、异常、输入输出等。

- 控制台：仅输出简要信息（detail=True 的日志被过滤）。
- 文件：仅「每次用户请求」一个文件，路径为 log/request_{request_id}_{时间}.log，
  由 pipeline 入口调用 attach_request_log_file / detach_request_log_file 挂载；
  该文件包含本次请求全链路（意图分解、autolink、关系代数、SQL 生成等）的完整日志，可据此重现执行路径与过程。
  不再使用 data/logs 或按日期/按 logger 的独立文件。
"""

import logging
import sys
import threading
from datetime import datetime
from typing import Any, Dict, Optional
import json
import traceback
from pathlib import Path
from utils.data_paths import DataPaths


def _format_message_with_extra(message: str, kwargs: Dict[str, Any]) -> str:
    """将 kwargs 序列化追加到 message，便于 request 单文件包含完整信息。"""
    if not kwargs:
        return message
    try:
        tail = json.dumps(kwargs, ensure_ascii=False, default=str)
        return message + " | " + tail
    except Exception:
        return message + " | " + str(kwargs)


class _DetailFilter(logging.Filter):
    """过滤掉 detail=True 的日志，不输出到控制台（仅用于控制台 handler）。"""
    def filter(self, record: logging.LogRecord) -> bool:
        return not getattr(record, "detail", False)


class StructuredLogger:
    """结构化日志记录器。仅控制台 + 请求级文件（由 attach_request_log_file 挂载），无 data/logs。"""

    def __init__(
        self,
        name: str,
        level: int = logging.INFO,
        enable_console_logging: bool = True,
    ):
        """
        初始化结构化日志记录器。
        不再支持按日期/按 logger 写 data/logs；文件输出仅通过 attach_request_log_file 按请求写 log/。
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        self.logger.handlers.clear()

        brief_format = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s",
            datefmt="%H:%M:%S",
        )
        if enable_console_logging:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(level)
            console_handler.setFormatter(brief_format)
            console_handler.addFilter(_DetailFilter())
            self.logger.addHandler(console_handler)

    def info(self, message: str, *args, **kwargs):
        """记录信息日志。kwargs 会追加到 message 写入 request 文件，便于完整重现。"""
        extra = kwargs.pop("extra", None) or {}
        msg = _format_message_with_extra(message, kwargs) if kwargs else message
        self.logger.info(msg, *args, extra=extra)

    def debug(self, message: str, *args, **kwargs):
        extra = kwargs.pop("extra", None) or {}
        msg = _format_message_with_extra(message, kwargs) if kwargs else message
        self.logger.debug(msg, *args, extra=extra)

    def warning(self, message: str, *args, **kwargs):
        extra = kwargs.pop("extra", None) or {}
        msg = _format_message_with_extra(message, kwargs) if kwargs else message
        self.logger.warning(msg, *args, extra=extra)

    def error(self, message: str, *args, exc_info: bool = False, **kwargs):
        extra = kwargs.pop("extra", None) or {}
        if exc_info:
            kwargs["exception"] = traceback.format_exc()
        msg = _format_message_with_extra(message, kwargs) if kwargs else message
        self.logger.error(msg, *args, exc_info=exc_info, extra=extra)

    def critical(self, message: str, *args, exc_info: bool = False, **kwargs):
        extra = kwargs.pop("extra", None) or {}
        if exc_info:
            kwargs["exception"] = traceback.format_exc()
        msg = _format_message_with_extra(message, kwargs) if kwargs else message
        self.logger.critical(msg, *args, exc_info=exc_info, extra=extra)
    
    def workflow_start(self, workflow_name: str, **kwargs):
        """记录工作流程开始"""
        message = f"工作流程开始: {workflow_name}"
        self.info(message, workflow=workflow_name, event="workflow_start", **kwargs)
    
    def workflow_end(self, workflow_name: str, duration: float, **kwargs):
        """记录工作流程结束"""
        message = f"工作流程结束: {workflow_name}, 耗时: {duration:.2f}秒"
        self.info(message, workflow=workflow_name, event="workflow_end", duration=duration, **kwargs)
    
    def workflow_node_start(self, node_name: str, **kwargs):
        """记录工作流节点开始"""
        message = f"工作流节点开始: {node_name}"
        self.info(message, node=node_name, event="node_start", **kwargs)
    
    def workflow_node_end(self, node_name: str, duration: float, **kwargs):
        """记录工作流节点结束"""
        message = f"工作流节点结束: {node_name}, 耗时: {duration:.2f}秒"
        self.info(message, node=node_name, event="node_end", duration=duration, **kwargs)
    
    def function_call(self, function_name: str, inputs: Dict[str, Any], **kwargs):
        """记录函数调用"""
        message = f"函数调用: {function_name}"
        self.debug(message, function=function_name, event="function_call", inputs=inputs, **kwargs)
    
    def function_result(self, function_name: str, result: Any, duration: float = None, **kwargs):
        """记录函数返回结果"""
        message = f"函数返回: {function_name}"
        extra = {"function": function_name, "event": "function_result", "result": str(result)[:500]}
        if duration is not None:
            extra["duration"] = duration
        extra.update(kwargs)
        self.debug(message, **extra)
    
    def exception(self, message: str, exception: Exception, **kwargs):
        """记录异常信息"""
        error_type = type(exception).__name__
        error_message = str(exception)
        full_message = f"{message} | 异常类型: {error_type} | 异常信息: {error_message}"
        self.error(full_message, exc_info=True, exception_type=error_type, exception_message=error_message, **kwargs)
    
    def input_output(self, operation: str, input_data: Any = None, output_data: Any = None, **kwargs):
        """记录输入输出"""
        message = f"操作: {operation}"
        extra = {
            "operation": operation,
            "event": "input_output"
        }
        if input_data is not None:
            extra["input"] = str(input_data)[:1000]  # 限制长度
        if output_data is not None:
            extra["output"] = str(output_data)[:1000]  # 限制长度
        extra.update(kwargs)
        self.debug(message, **extra)


# 全局日志记录器实例
_loggers: Dict[str, StructuredLogger] = {}
_loggers_lock = threading.Lock()

# 每次用户请求会挂到这些 logger 上的同一份文件（整条 pipeline 共用一个请求日志文件）
REQUEST_LOG_LOGGER_NAMES = [
    "autolink",
    "intent_divide",
    "sql_generation_pipeline",
    "intent_runtime",
    "sql_generation_worker_runtime",
    "database_tool",
]

_request_file_handler: Optional[logging.FileHandler] = None
_request_handler_lock = threading.RLock()


def attach_request_log_file(request_id: str, log_dir: Optional[Path] = None) -> str:
    """
    为本次用户请求绑定一个日志文件，将同一 FileHandler 挂到 REQUEST_LOG_LOGGER_NAMES 中的每个 logger 上。
    在 pipeline 入口（如 run_sql_generation_stage）开始时调用。
    返回日志文件路径。
    """
    global _request_file_handler
    with _request_handler_lock:
        detach_request_log_file()
        if log_dir is None:
            log_dir = Path(DataPaths.default().project_root) / "log"
        log_dir = Path(log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = log_dir / f"request_{request_id}_{ts}.log"
        path_str = str(path)
        _request_file_handler = logging.FileHandler(path_str, encoding="utf-8")
        _request_file_handler.setLevel(logging.DEBUG)
        _request_file_handler.setFormatter(
            logging.Formatter(
                "%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s:%(lineno)d | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        for name in REQUEST_LOG_LOGGER_NAMES:
            logging.getLogger(name).addHandler(_request_file_handler)
        return path_str


def add_request_log_file_in_process(log_path: str) -> None:
    """
    在子进程（如 Worker 进程）内调用：将同一请求日志文件挂到 REQUEST_LOG_LOGGER_NAMES 的每个 logger 上，
    使 intent_runtime、autolink 等在子进程中的日志也写入主进程创建的 request 文件。
    多进程写同一文件时每次 write 通常为原子，可安全使用。
    """
    import logging as _log
    import os as _os

    target = _os.path.abspath(str(log_path))

    def _has_handler(logger: _log.Logger) -> bool:
        for h in logger.handlers:
            if isinstance(h, _log.FileHandler) and _os.path.abspath(getattr(h, "baseFilename", "")) == target:
                return True
        return False

    # Idempotent: a worker (thread or process) may run multiple tasks and call this
    # repeatedly. Avoid duplicating handlers (which would duplicate every log line).
    with _request_handler_lock:
        if any(_has_handler(_log.getLogger(name)) for name in REQUEST_LOG_LOGGER_NAMES):
            missing = []
            for name in REQUEST_LOG_LOGGER_NAMES:
                lg = _log.getLogger(name)
                if not _has_handler(lg):
                    missing.append(lg)
            if not missing:
                return
            handler = _log.FileHandler(log_path, encoding="utf-8")
            handler.setLevel(_log.DEBUG)
            handler.setFormatter(
                _log.Formatter(
                    "%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s:%(lineno)d | %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                )
            )
            for lg in missing:
                lg.addHandler(handler)
            return

        handler = _log.FileHandler(log_path, encoding="utf-8")
        handler.setLevel(_log.DEBUG)
        handler.setFormatter(
            _log.Formatter(
                "%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s:%(lineno)d | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        for name in REQUEST_LOG_LOGGER_NAMES:
            _log.getLogger(name).addHandler(handler)
        return


def detach_request_log_file() -> None:
    """在 pipeline 结束时调用，从各 logger 移除请求日志文件 handler 并关闭文件。"""
    global _request_file_handler
    with _request_handler_lock:
        if _request_file_handler is None:
            return
        for name in REQUEST_LOG_LOGGER_NAMES:
            try:
                logging.getLogger(name).removeHandler(_request_file_handler)
            except Exception:
                pass
        try:
            _request_file_handler.close()
        except Exception:
            pass
        _request_file_handler = None


def get_logger(
    name: str,
    level: int = logging.INFO,
    enable_console_logging: bool = True,
) -> StructuredLogger:
    """
    获取日志记录器实例（单例模式）。
    文件输出仅由 pipeline 通过 attach_request_log_file 挂载到 log/request_xxx.log，不写 data/logs。
    """
    with _loggers_lock:
        if name not in _loggers:
            _loggers[name] = StructuredLogger(
                name=name,
                level=level,
                enable_console_logging=enable_console_logging,
            )
        return _loggers[name]
