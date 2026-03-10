from langchain_qwq import ChatQwen
from langchain_deepseek import ChatDeepSeek
from langchain_openai import ChatOpenAI

from config.app_config import get_app_config


def get_llm(model_name: str | None = None):
    """
    获取 LLM 实例
    
    Args:
        model_name: 模型名称，支持 'qwen3-max', 'deepseek-chat', 'gpt-5.2', 'gpt-4o'
    Returns:
        LangChain ChatModel 实例
    """
    cfg = get_app_config()
    alias = str(model_name or cfg.models.default_model)
    spec = cfg.get_model(alias)

    if spec.provider == "qwen":
        model = ChatQwen(
            model_name=spec.model_name,
            api_key=spec.api_key,
            base_url=spec.base_url or None,
        )
    elif spec.provider == "deepseek":
        model = ChatDeepSeek(
            model_name=spec.model_name,
            api_key=spec.api_key,
            base_url=spec.base_url or None,
        )
    elif spec.provider == "openai":
        model = ChatOpenAI(
            model_name=spec.model_name,
            api_key=spec.api_key,
            base_url=spec.base_url or None,
        )
    else:
        raise ValueError(f"Unsupported model provider: {spec.provider}")
    setattr(model, "_codex_model_name", alias)
    setattr(model, "_codex_model_factory", get_llm)
    return model
