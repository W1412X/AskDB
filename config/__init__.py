"""
全局配置管理模块。
"""

from .app_config import AppConfig, get_app_config, reload_app_config


def get_llm(model_name: str | None = None):
    from .llm_config import get_llm as _get_llm

    return _get_llm(model_name)

__all__ = [
    "AppConfig",
    "get_app_config",
    "reload_app_config",
    "get_llm",
]
