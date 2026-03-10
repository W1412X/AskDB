# 配置说明

项目配置由 `config/app_config.py` 统一读取，JSON 文件位于 `config/json/`。敏感信息（API Key、数据库密码）通过环境变量注入，推荐使用项目根目录下的 `.env` 文件。

## 一、敏感信息与 .env

**请勿在 JSON 中填写真实 API Key 或数据库密码。** 使用环境变量并在本地维护 `.env`（该文件已加入 `.gitignore`，不会提交）。

### 1. 首次使用

```bash
# 在项目根目录
cp .env.example .env
# 编辑 .env，填入真实值
```

程序启动时会自动加载项目根目录下的 `.env`（通过 `python-dotenv`），因此无需在 shell 中手动 `export`。

### 2. .env 示例与变量说明

`.env.example` 中列出了所有可选变量，复制为 `.env` 后按需填写：

| 变量名 | 用途 | 对应配置 |
|--------|------|----------|
| `DB_PASSWORD` | 数据库密码 | `database.json` 中连接的 `password_env` |
| `OPENAI_API_KEY` | OpenAI 系列模型 API Key | `models.json` 中 `api_key_env: "OPENAI_API_KEY"` 的模型 |
| `QWEN_API_KEY` | 通义千问 API Key | 同上，Qwen 模型 |
| `DEEPSEEK_API_KEY` | DeepSeek API Key | 同上，DeepSeek 模型 |
| `APP_CONFIG_DIR` | 自定义配置目录（可选） | 覆盖默认 `config/json/` |

若某条未在 `.env` 中设置，则使用对应 JSON 中的 `api_key` / `password` 等字段（通常留空，便于全部走环境变量）。

---

## 二、配置文件结构

```
config/
├── README.md          # 本说明
├── app_config.py      # 配置加载与解析
├── llm_config.py      # LLM 实例构造
└── json/
    ├── database.json  # 数据库连接与库列表
    ├── models.json    # LLM 模型别名与调用策略
    └── stages.json    # 各阶段参数（意图分解、SQL 生成、初始化等）
```

### 自定义配置目录

若希望使用项目外部的配置目录：

```bash
export APP_CONFIG_DIR=/path/to/your/config
# 该目录下需包含 database.json、models.json、stages.json
```

---

## 三、database.json

配置数据库连接与“默认使用的库列表”。

### 字段说明

| 字段 | 类型 | 说明 |
|------|------|------|
| `default_connection` | string | 默认使用的连接名，对应 `connections` 中的 key |
| `default_scope` | string[] | 默认查询/可见的数据库名列表 |
| `initialize_databases` | string[] | 需要跑“初始化”（列描述 + 嵌入）的库列表，为空时沿用 default_scope |
| `query_databases` | string[] | 意图分解与查询时使用的库列表，为空时沿用 default_scope |
| `connections` | object | 连接名 → 连接参数 |

每个连接支持：

| 字段 | 类型 | 说明 |
|------|------|------|
| `host` | string | 主机 |
| `port` | number | 端口，默认 3306 |
| `user` | string | 用户名 |
| `password` | string | 密码，可为空 |
| `password_env` | string | 密码环境变量名，若设置则优先使用环境变量 |
| `database` | string | 默认数据库名（可选） |
| `charset` | string | 字符集，默认 utf8mb4 |

### 示例

```json
{
  "default_connection": "primary",
  "default_scope": ["my_db"],
  "initialize_databases": ["my_db"],
  "query_databases": ["my_db"],
  "connections": {
    "primary": {
      "host": "localhost",
      "port": 3306,
      "user": "root",
      "password": "",
      "password_env": "DB_PASSWORD",
      "database": "my_db",
      "charset": "utf8mb4"
    }
  }
}
```

---

## 四、models.json

配置 LLM 模型别名、默认模型、回退顺序及调用策略。**API Key 一律通过 `api_key_env` 从环境变量（如 .env）读取，JSON 中 `api_key` 留空。**
### 字段说明

| 字段 | 类型 | 说明 |
|------|------|------|
| `default_model` | string | 默认使用的模型别名 |
| `fallback_order` | string[] | 调用失败时依次尝试的模型别名 |
| `call_policy` | object | 超时、重试等策略（见下） |
| `models` | object | 模型别名 → 模型规格 |

每个模型规格：

| 字段 | 类型 | 说明 |
|------|------|------|
| `provider` | string | 固定为 `openai` / `qwen` / `deepseek` 之一 |
| `model_name` | string | 对应厂商的模型名 |
| `api_key` | string | 留空，由环境变量提供 |
| `api_key_env` | string | 环境变量名，如 `OPENAI_API_KEY`、`DEEPSEEK_API_KEY` |
| `base_url` | string | API 地址，可选 |
| `base_url_env` | string | 覆盖 base_url 的环境变量名，可选 |

`call_policy` 常用字段：`timeout_seconds`、`max_transport_retries`、`max_format_retries`、`retryable_error_classes`。

### 示例

```json
{
  "default_model": "deepseek-chat",
  "fallback_order": ["qwen3-max", "deepseek-chat"],
  "call_policy": {
    "timeout_seconds": 90,
    "max_transport_retries": 2,
    "max_format_retries": 3,
    "retryable_error_classes": ["timeout", "transport", "rate_limit"]
  },
  "models": {
    "deepseek-chat": {
      "provider": "deepseek",
      "model_name": "deepseek-chat",
      "api_key": "",
      "api_key_env": "DEEPSEEK_API_KEY",
      "base_url": "https://api.deepseek.com/v1",
      "base_url_env": "DEEPSEEK_BASE_URL"
    },
    "qwen3-max": {
      "provider": "qwen",
      "model_name": "qwen3-max",
      "api_key": "",
      "api_key_env": "QWEN_API_KEY",
      "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
      "base_url_env": "QWEN_BASE_URL"
    }
  }
}
```

---

## 五、stages.json

配置各阶段使用的模型名及业务参数（意图分解、SQL 生成、初始化、摘要等）。仅列常用顶层键，子结构见仓库内 `config/json/stages.json`。

| 键 | 说明 |
|----|------|
| `intent_divide` | 意图分解：`model_name`、`max_retry_attempts`、`max_tool_rounds`、`column_query_top_k` |
| `sql_generation` | SQL 生成：`pipeline`（model_name、max_concurrency、max_rows、max_rounds_per_intent）、`intent_runtime`、`autolink` |
| `initialize` | 初始化：`agent.model_name`、`embedding`（model_name、model_path_name、normalize_embeddings、batch_size 等） |
| `general` | 通用：如 `summary.model_name`、`max_input_length` |
| `column_agent` | 列描述 Agent：sampling、token、retry、parallel 等 |

---

## 六、代码中使用配置

```python
from config import get_app_config, get_llm

cfg = get_app_config()

# 数据库
scope = cfg.get_default_database_scope()
conn_cfg = cfg.get_database_connection()

# 模型（api_key / base_url 已从环境变量注入）
spec = cfg.get_model("deepseek-chat")
model = get_llm("deepseek-chat")

# 阶段
stage_model = cfg.get_stage_model_name("sql_generation.pipeline")
```

---

## 七、环境变量优先级

对 `api_key`、`password`、`base_url` 等支持“环境变量覆盖”的字段：

1. 若配置中指定了 `*_env`（如 `api_key_env: "DEEPSEEK_API_KEY"`），则优先使用该环境变量的值。
2. 若环境变量未设置或为空，则使用 JSON 中对应字段的值（如 `api_key`，通常留空）。

因此推荐做法：JSON 中敏感字段留空，在 `.env` 中设置全部 `*_env` 对应的变量。
