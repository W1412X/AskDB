# SQL Generation 阶段 - 设计方案与技术实现

## 概述

SQL Generation 阶段负责将结构化的意图（Intent）转换为可执行的 SQL 查询。该阶段采用**自研 DAG 调度系统**协调具有依赖关系的多个意图，使用**AutoLink 模式发现引擎**进行动态模式解析，并实现了带有**多阶段执行流水线**和**内置澄清对话机制**的完整处理流程。

---

## 系统架构

### 核心组件结构

```mermaid
graph TB
    subgraph "SQL Generation 阶段"
        A[Pipeline 协调器] --> B[DAG 调度器]
        B --> C[Worker 运行时]
        C --> D[Intent 运行时]
        
        subgraph "DAG 调度器"
            B1[状态管理器]
            B2[事件处理器]
            B3[依赖追踪器]
            B4[就绪队列]
        end
        
        subgraph "Worker 运行时"
            C1[进程池执行器]
            C2[任务序列化器]
            C3[结果合并器]
        end
        
        subgraph "Intent 运行时"
            D1[阶段控制器]
            D2[收敛保护器]
            D3[检查点管理器]
        end
        
        subgraph "Intent Agents"
            E1[Schema 构建器]
            E2[RA 规划器]
            E3[SQL 渲染器]
            E4[SQL 验证器]
            E5[结果解释器]
            E6[澄清器]
        end
        
        D --> E1
        D --> E2
        D --> E3
        D --> E4
        D --> E5
        D --> E6
        
        subgraph "AutoLink 引擎"
            F1[Schema 规划器]
            F2[工具 Agents]
            F3[轮次裁判器]
            F4[语义增强器]
        end
        
        E1 --> F1
        E1 --> F2
        
        subgraph "工具层"
            G1[schema_retrieval<br/>向量检索]
            G2[schema_meta<br/>初始化数据]
            G3[sql_explore<br/>采样值获取]
            G4[sql_draft<br/>SQL 试跑]
            G5[db_execute<br/>数据库执行]
        end
        
        F2 --> G1
        F2 --> G2
        F2 --> G3
        F2 --> G4
        E4 --> G5
        E5 --> G5
        
        subgraph "对话系统"
            H1[Ticket 管理器]
            H2[队列处理器]
            H3[Clarifier Agent]
        end
        
        E6 --> H1
        E6 --> H3
    end
    
    I[用户查询] --> A
    A --> J[Intent Divide 阶段]
    J --> I
    A --> K[SQL 结果集]
```

### 模块依赖关系

```mermaid
graph LR
    subgraph "核心模块"
        A[pipeline.py<br/>流程协调]
        B[worker_runtime.py<br/>Worker 执行]
        C[main.py<br/>入口函数]
    end
    
    subgraph "DAG 模块"
        D[dag/scheduler.py<br/>调度器]
        E[dag/models.py<br/>数据模型]
        F[dag/deps.py<br/>依赖解析]
        G[dag/serialize.py<br/>序列化]
    end
    
    subgraph "Intent 模块"
        H[intent/runtime.py<br/>运行时]
        I[intent/models.py<br/>数据模型]
        J[intent/dialog.py<br/>对话管理]
        K[intent/middleware.py<br/>中间件]
    end
    
    subgraph "Intent Agents"
        L[ra_planner.py<br/>RA 规划]
        M[sql_renderer.py<br/>SQL 渲染]
        N[sql_validator.py<br/>SQL 验证]
        O[result_interpreter.py<br/>结果解释]
        P[clarifier.py<br/>澄清器]
    end
    
    subgraph "AutoLink 模块"
        Q[autolink/runtime.py<br/>运行时]
        R[autolink/models.py<br/>数据模型]
        S[autolink/agents/planner.py<br/>规划器]
        T[autolink/agents/tool_agents.py<br/>工具 Agent]
        U[autolink/tools/*.py<br/>工具实现]
    end
    
    A --> D
    A --> H
    A --> J
    B --> D
    B --> H
    B --> G
    C --> D
    H --> L
    H --> M
    H --> N
    H --> O
    H --> P
    K --> Q
    Q --> S
    Q --> T
    T --> U
```

---

## 数据流设计

### 端到端处理流程

```mermaid
sequenceDiagram
    participant Client as 客户端
    participant Pipeline as Pipeline 协调器
    participant DAG as DAG 调度器
    participant Worker as Worker 执行器
    participant Intent as Intent 运行时
    participant AutoLink as AutoLink 引擎
    participant Dialog as 对话系统
    participant DB as 数据库
    
    Client->>Pipeline: run_sql_generation_stage(query, context)
    Pipeline->>Pipeline: divide_intents_with_audit(query)
    Pipeline->>DAG: DAGScheduler(intents, config)
    
    loop DAG 执行循环
        DAG->>DAG: poll_work(limit=max_concurrency)
        DAG->>Worker: submit(work_item)
        Worker->>Intent: run_intent_node(node, state)
        
        alt 阶段：BUILDING_SCHEMA
            Intent->>AutoLink: run_autolink(request=BUILD)
            AutoLink->>AutoLink: SchemaPlanner → ToolAgents → RoundJudge
            AutoLink-->>Intent: schema {databases, tables, columns}
        end
        
        alt 阶段：PLANNING_RA
            Intent->>Intent: run_ra_planner(intent, deps, schema)
            Note over Intent: 构建关系代数计划
        end
        
        alt 阶段：RENDERING_SQL
            Intent->>Intent: run_sql_renderer(ra_plan, schema, dialect)
            Note over Intent: 生成 SQL 候选
        end
        
        alt 阶段：VALIDATING_SQL
            Intent->>DB: execute_select_with_limit(sql, limit=1)
            Note over Intent: 验证 SQL 语法与语义
        end
        
        alt 阶段：EXECUTING_SQL
            Intent->>DB: execute_select_with_limit(sql, limit=max_rows)
            DB-->>Intent: rows[{col: value}]
        end
        
        alt 阶段：INTERPRETING_RESULT
            Intent->>Intent: run_result_interpreter(sql, rows)
            Note over Intent: 生成自然语言答案
        end
        
        alt 需要用户澄清
            Intent->>Dialog: create_dialog_ticket(payload)
            Dialog-->>Intent: ticket_id
            Intent-->>Worker: WAIT_USER, {ticket}
            Worker-->>DAG: submit_work_result(WAIT_USER)
            DAG-->>Pipeline: StageStatus.WAIT_USER
            Pipeline-->>Client: dialog_ticket
        end
        
        Intent-->>Worker: OK/FINAL_BUNDLE
        Worker->>DAG: submit_work_result(ok, payload)
        DAG->>DAG: mark_completed(intent_id)
        DAG->>DAG: unblock_dependents()
    end
    
    DAG-->>Pipeline: StageStatus.SUCCESS
    Pipeline-->>Client: SQLStageResult {state, results}
```

### Intent 阶段状态机

```mermaid
stateDiagram-v2
    [*] --> INIT
    INIT --> BUILDING_SCHEMA
    BUILDING_SCHEMA --> PLANNING_RA: schema_built
    BUILDING_SCHEMA --> WAITING_USER: empty_schema
    BUILDING_SCHEMA --> FAILED: guard_triggered
    
    PLANNING_RA --> RENDERING_SQL: ra_plan_ok
    PLANNING_RA --> BUILDING_SCHEMA: ra_plan_failed
    
    RENDERING_SQL --> VALIDATING_SQL: candidates_ready
    RENDERING_SQL --> BUILDING_SCHEMA: render_failed
    
    VALIDATING_SQL --> EXECUTING_SQL: validation_passed
    VALIDATING_SQL --> RENDERING_SQL: validation_failed
    
    EXECUTING_SQL --> INTERPRETING_RESULT: exec_ok
    EXECUTING_SQL --> BUILDING_SCHEMA: exec_schema_error
    EXECUTING_SQL --> RENDERING_SQL: exec_failed
    
    INTERPRETING_RESULT --> COMPLETED: interpretation_ok
    INTERPRETING_RESULT --> FAILED: interpretation_failed
    
    WAITING_USER --> BUILDING_SCHEMA: user_resolved
    WAITING_USER --> FAILED: user_timeout
    
    COMPLETED --> [*]
    FAILED --> [*]
    
    note right of BUILDING_SCHEMA
        AutoLink 发现
        最小完备 schema
    end note
    
    note right of PLANNING_RA
        构建关系代数
        计划
    end note
    
    note right of RENDERING_SQL
        生成 SQL
        候选语句
    end note
    
    note right of VALIDATING_SQL
        LIMIT 1
        试跑验证
    end note
    
    note right of EXECUTING_SQL
        执行查询
        获取结果
    end note
    
    note right of INTERPRETING_RESULT
        将结果转换为
        自然语言
    end note
```

### DAG 调度器状态转换

```mermaid
stateDiagram-v2
    [*] --> PENDING: build_global_state
    
    PENDING --> READY: deps_resolved
    READY --> RUNNING: pop_ready
    RUNNING --> COMPLETED: result_ok
    RUNNING --> FAILED: result_error
    RUNNING --> WAIT_USER: need_clarification
    RUNNING --> BLOCKED_BY_FAILED_DEP: dep_failed
    
    WAIT_USER --> READY: user_reply_received
    WAIT_USER --> FAILED: user_timeout
    
    FAILED --> BLOCKED_BY_FAILED_DEP: cascade
    
    COMPLETED --> [*]
    FAILED --> [*]
    
    state "就绪队列 Ready Queue" as RQ {
        READY
    }
    
    state "运行集合 Running Set" as RS {
        RUNNING
    }
    
    state "完成集合 Completed Set" as CS {
        COMPLETED
    }
```

### AutoLink 引擎工作流程

```mermaid
graph TB
    A[AutolinkRequest] --> B[SchemaPlanner]
    B --> C{当前阶段？}
    
    C -->|DISCOVER| D[SCHEMA_RETRIEVAL]
    C -->|HYDRATE_METADATA| E[SCHEMA_META]
    C -->|EXPLORE_DATA| F[SCHEMA_EXPLORER]
    C -->|ASSESS| G[RoundJudge]
    
    D --> H[工具执行器]
    E --> H
    F --> H
    
    H --> I[Schema 合并器]
    I --> J[RoundJudge]
    
    J --> K{是否完备？}
    K -->|是 | L[剪枝冗余项]
    K -->|否 | M{是否收敛？}
    M -->|是 | N[返回 Schema]
    M -->|否 | B
    
    L --> O[AutolinkOutput]
    N --> O
    
    subgraph "工具层"
        H1[schema_retrieval<br/>向量相似度检索]
        H2[schema_meta<br/>读取 initialize.json]
        H3[sql_explore<br/>获取采样值]
        H4[sql_draft<br/>SQL 试跑验证]
    end
    
    H --> H1
    H --> H2
    H --> H3
    H --> H4
```

---

## 核心协议设计

### 1. DAG 调度协议

**状态机流转：**

```mermaid
stateDiagram-v2
    [*] --> PENDING
    PENDING --> READY: 依赖=0
    PENDING --> BLOCKED_BY_FAILED_DEP: 依赖失败
    READY --> RUNNING: 调度执行
    RUNNING --> COMPLETED: 成功(result)
    RUNNING --> FAILED: 错误
    RUNNING --> WAIT_USER: 等待
    WAIT_USER --> READY: 已解决(dialog)
```

**事件类型：**

| 事件 | 含义 |
|------|------|
| INTENT_READY | 依赖已解析，加入就绪队列 |
| INTENT_COMPLETED | 执行成功，解锁依赖节点 |
| INTENT_FAILED | 执行失败，级联阻塞依赖节点 |
| INTENT_WAIT_USER | 需要用户澄清 |
| USER_REPLY_RECEIVED | 用户已回复，恢复执行 |
| NODE_BLOCKED | 被失败的依赖节点阻塞 |

### 2. Worker 通信协议

```mermaid
sequenceDiagram
    participant Main as 主进程
    participant Worker as Worker 进程

    Main->>Main: 构造 WorkerTaskPayload
    Note over Main: state_data (序列化)<br/>intent_id, model_name<br/>max_rows, max_rounds
    Main->>Worker: WorkerTaskPayload
    Worker->>Worker: run_intent_node()
    Worker->>Worker: 构造 WorkerTaskResult
    Note over Worker: intent_id, ok<br/>payload (最终结果包)<br/>node_data (artifacts)<br/>dialog_tickets
    Worker->>Main: WorkerTaskResult
    Main->>Main: _merge_worker_result()
    Note over Main: 更新节点 artifacts<br/>合并 dialog tickets<br/>更新 active_ticket_id
```

- **序列化方式**：同进程内做 state 快照与合并（dict 序列化）。
- **隔离性**：每个 Worker 接收完整的状态快照。

### 3. 对话澄清协议

```mermaid
sequenceDiagram
    participant Intent as Intent 运行时
    participant Dialog as 对话系统
    participant User as 用户

    Intent->>Dialog: create_dialog_ticket()
    Note right of Intent: intent_id, question_id<br/>phase, payload.ask<br/>acceptance_criteria, max_turns
    Dialog->>Dialog: 入队 ticket<br/>设置 active_ticket_id
    Dialog-->>Intent: WAIT_USER 状态

    User->>Dialog: 用户提供回复

    Intent->>Dialog: submit_dialog_user_message(ticket_id, user_message)
    Dialog->>Dialog: append_turn()<br/>run_clarifier()
    Note right of Dialog: 提取 hints<br/>判断是否解决
    Dialog-->>Intent: resolved, hints, next_ask, resolution_type

    alt resolved
        Intent->>Intent: 合并 hints<br/>恢复阶段<br/>继续执行
    else 未解决
        Intent->>Intent: 更新 ask 内容<br/>返回 WAIT_USER
    end
```

**解决类型：**

| 类型 | 含义 |
|------|------|
| RESOLVED | 用户提供了充分的澄清信息 |
| ASSUMPTIVE | 达到最大轮次，尽力恢复执行 |
| ABANDONED | 用户明确放弃 |

### 4. AutoLink Schema 发现协议

**四个阶段：**

```mermaid
flowchart TD
    A[DISCOVER 发现阶段<br/>向量检索相关表/列<br/>工具: schema_retrieval]
    B[HYDRATE_METADATA 元数据填充<br/>从 initialize.json 获取详细元数据<br/>工具: schema_meta]
    C[EXPLORE_DATA 数据探索<br/>收集关键列的采样值<br/>工具: sql_explore]
    D[ASSESS COMPLETENESS 完备性评估<br/>RoundJudge 评估完备性<br/>检查必需字段 / 识别冗余项]

    A --> B --> C --> D
```

**RoundJudge 决策逻辑：**

- `should_stop = (所有必需字段已存在 AND schema 连续 N 轮未变化) OR 达到最大轮次`
- `redundant_items = 未在任何工具结果映射中引用的列/表`

**输出：**

- **schema**：最小完备 schema（含表、列、键）
- **audit**：所有阶段、工具调用、决策的追踪记录
- **status**：SUCCESS | PARTIAL_SUCCESS | FAILED

---

## 收敛保护机制

```mermaid
graph TB
    A[Intent 运行时循环] --> B{阶段执行}
    B --> C[应用 Artifacts]
    C --> D[更新 Guard 状态]
    
    D --> E{状态指纹是否变化？}
    E -->|否 | F[no_progress_rounds++]
    E -->|是 | G[no_progress_rounds=0]
    
    F --> H{no_progress_rounds >= 3?}
    H -->|是 | I[触发：no_progress]
    H -->|否 | J{错误类型是否重复？}
    
    G --> J
    
    J -->|是 | K[repeated_error_classes[class]++]
    J -->|否 | L[记录阶段边]
    
    K --> M{计数 > 2?}
    M -->|是 | N[触发：repeated_error]
    M -->|否 | O[继续循环]
    
    L --> O
    
    I --> P[标记 Intent 为 FAILED]
    N --> P
    
    O --> Q{还有迭代次数？}
    Q -->|是 | B
    Q -->|否 | R[触发：max_iterations]
    R --> P
    
    style I fill:#ff6b6b
    style N fill:#ff6b6b
    style R fill:#ff6b6b
    style P fill:#ff6b6b
```

### 保护机制说明

| 保护类型 | 触发条件 | 处理方式 |
|---------|---------|---------|
| 无进展保护 | 状态指纹连续 3 轮未变化 | 标记 FAILED |
| 重复错误保护 | 同一错误类型出现 >2 次 | 标记 FAILED |
| 迭代次数保护 | 超过最大迭代次数 (12) | 标记 FAILED |

---

## 接口规范

### Pipeline API

```python
# 初始执行
result = run_sql_generation_stage(
    query: str,
    context: Dict[str, Any],  # database_scope, max_rows, sql_dialect
    model_name: str = "qwen3-max",
    max_concurrency: int = 3,
) -> SQLStageResult

# 用户澄清后恢复执行
result = resume_sql_generation_stage_after_user_reply(
    state: GlobalState,  # 从上次结果持久化的状态
    ticket_id: str,
    user_message: str,
    context: Optional[Dict[str, Any]] = None,
    model_name: str = "qwen3-max",
) -> SQLStageResult
```

### SQLStageResult 结构

```python
@dataclass
class SQLStageResult:
    status: StageStatus  # SUCCESS | WAIT_USER | FAILED
    state: GlobalState   # 完整 DAG 状态及所有 artifacts
    dialog_ticket: Optional[Dict[str, Any]]  # WAIT_USER 时存在
    error: str  # FAILED 时存在
```

### GlobalState 结构

```python
@dataclass
class GlobalState:
    intent_map: Dict[str, IntentNode]      # 所有 Intent 节点
    ready_queue: List[str]                 # 就绪 Intent ID
    running_set: Set[str]                  # 运行中 Intent ID
    completed_set: Set[str]                # 已完成 Intent ID
    dependency_index: Dict[str, List[str]] # 反向依赖图
    remaining_deps_count: Dict[str, int]   # 每个 Intent 未解析依赖数
    dialog_state: DialogState              # Dialog tickets 及队列
    config: Dict[str, Any]                 # 运行时配置
    audit_log: List[Dict[str, Any]]        # 事件审计日志
```

### IntentNode Artifacts

```python
@dataclass
class IntentNode:
    intent_id: str
    description: str
    deps: List[str]
    status: NodeStatus
    artifacts: Dict[str, Any] = {
        "intent_meta": {...},
        "schema": {...},           # 来自 AutoLink
        "ra_plan": {...},          # 关系代数计划
        "sql_candidates": [...],   # 生成的 SQL 候选
        "validations": [...],      # 验证报告
        "exec_result": {...},      # 执行结果
        "exec_raw": {...},         # 原始数据行
        "user_hints": {...},       # 来自对话澄清
        "facts_bundle": {...},     # 派生事实
        "checkpoint": {...},       # 阶段检查点
        "guard": {...},            # 收敛保护状态
        "final": {...},            # 最终结果包
    }
```

---

## 错误处理

### 错误分类表

| 错误类型 | 触发条件 | 恢复策略 |
|---------|---------|---------|
| `empty_schema` | AutoLink 返回空 schema | 对话澄清 |
| `ra_plan_failed` | 无法构建 RA 计划 | 返回 schema 构建 |
| `sql_render_failed` | 无法生成 SQL | 返回 RA 规划 |
| `sql_validate_failed` | 所有候选 SQL 验证失败 | 重新渲染 SQL |
| `sql_exec_schema_error` | 未知列/表 | 返回 schema 构建 |
| `sql_exec_failed` | SQL 运行时错误 | 重新渲染 SQL |
| `no_progress` | 状态 3 轮无变化 | 标记失败 |
| `repeated_error` | 同错误 >2 次 | 标记失败 |
| `iteration_limit` | 超过最大迭代 | 标记失败 |

### 级联失败传播

```mermaid
graph TB
    A[Intent A 失败] --> B{有依赖节点？}
    B -->|是 | C[标记 B 为 BLOCKED_BY_FAILED_DEP]
    B -->|否 | D[结束]
    
    C --> E{B 有依赖节点？}
    E -->|是 | F[标记 C 为 BLOCKED_BY_FAILED_DEP]
    E -->|否 | D
    
    F --> G{...}
    
    style A fill:#ff6b6b
    style C fill:#ffa502
    style F fill:#ffa502
```

---

## 性能与资源配置

### 并发控制

- **max_concurrency**: 限制并行 Intent 执行数（默认：3）
- **Worker Pool**: 单进程多线程（每个 Intent 基于 state 快照执行，完成后合并结果）
- **State Serialization**: dict 快照（无 pickle / 无进程边界）

### 资源限制参数

| 参数 | 默认值 | 用途 |
|-----|-------|-----|
| `max_rows` | 100 | 限制结果集大小 |
| `max_rounds_per_intent` | 4 | 最大迭代轮次 |
| `max_runtime_iterations` | 12 | 最大阶段转换次数 |
| `max_no_progress_rounds` | 3 | 无进展保护阈值 |
| `max_repeated_error_class` | 2 | 重复错误保护阈值 |
| `max_meta_tables` | 8 | AutoLink schema 数量限制 |

---

## 审计与追踪

每次执行都会生成完整的审计日志：

```python
state.audit_log = [
    {"event": "intent_divide", "audit": {...}},
    {"event": "build", "node_count": N, "ready_count": M},
    {"event": "scheduler_event_emitted", "event_type": "INTENT_READY", ...},
    {"event": "dispatch", "intent_ids": [...]},
    {"event": "complete", "intent_id": "..."},
    {"event": "drain_events", "count": N},
    ...
]
```

每个 Intent 的 `final` artifact 包含：
- `audit`: Intent 运行时的完整追踪
- `facts_bundle`: 派生事实、指标、约束
- `schema`: 最终使用的 schema
- `exec_raw`: 执行结果数据
