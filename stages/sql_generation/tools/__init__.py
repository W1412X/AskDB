"""
SQL generation shared tools.

This package provides a minimal, stable tool surface used by:
- AutoLink (schema discovery/enrichment)
- Intent SQL generation (validate/execute)

Tools are implemented as LangChain-compatible callables with `.invoke()`.
"""

from stages.sql_generation.tools.db import (
    describe_table_tool,
    dry_run_tool,
    execute_select_with_limit_tool,
    get_table_indexes_tool,
    get_foreign_keys_tool,
    get_primary_key_tool,
    get_table_comment_tool,
    list_databases_tool,
    list_tables_tool,
    parse_sql_tool,
    search_columns_tool,
    search_tables_tool,
    validate_sql_select_only_tool,
)

__all__ = [
    "describe_table_tool",
    "dry_run_tool",
    "execute_select_with_limit_tool",
    "get_table_indexes_tool",
    "get_foreign_keys_tool",
    "get_primary_key_tool",
    "get_table_comment_tool",
    "list_databases_tool",
    "list_tables_tool",
    "parse_sql_tool",
    "search_columns_tool",
    "search_tables_tool",
    "validate_sql_select_only_tool",
]
