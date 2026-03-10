# DAG Scheduler 详细设计方案与技术实现

## 一、系统概述

### 1.1 设计目标

DAG Scheduler 是一个**基于有向无环图的意图调度系统**，用于管理 SQL Generation 流水线中多个意图的依赖关系、并行执行和状态流转。

**核心设计原则**：

1. **依赖驱动**：意图执行严格遵循依赖关系，只有依赖全部完成后才变为 ready 状态
2. **并发控制**：支持配置最大并发数，避免资源过载
3. **失败传播**：意图失败时自动阻塞所有下游依赖意图
4. **事件驱动**：基于事件队列实现状态流转和解耦
5. **可序列化**：支持完整状态持久化和恢复

### 1.2 系统定位

DAG Scheduler 在 SQL Generation 流水线中的位置：

```mermaid
flowchart TD
    subgraph Input[输入层]
        UserQuery[用户查询]
    end

    subgraph IntentDivide[Intent Divide Stage]
        ID[意图识别与分解]
    end

    subgraph SQLGen[SQL Generation Stage]
        DAG[DAG Scheduler<br/>意图调度]
        Runtime[Intent Runtime<br/>单意图执行]
        AL[AutoLink<br/>Schema 链接]
        RA[RA Planner<br/>关系代数]
        SQL[SQL Renderer<br/>SQL 生成]
    end

    subgraph Output[输出层]
        Exec[SQL Executor<br/>执行]
        Result[结果返回]
    end

    UserQuery --> IntentDivide
    IntentDivide -->|intents with deps| DAG
    DAG -->|ready intent| Runtime
    Runtime --> AL
    Runtime --> RA
    Runtime --> SQL
    SQL --> Output

    style DAG fill:#fff3e0
    style Runtime fill:#fce4ec
    style AL fill:#e3f2fd
```

### 1.3 核心功能

| 功能 | 说明 | 实现方式 |
|------|------|---------|
| **拓扑构建** | 从意图列表构建 DAG | Kahn 算法 + 环检测 |
| **状态管理** | 跟踪每个意图的状态 | NodeStatus 枚举 + 状态机 |
| **并发调度** | 控制同时运行的意图数 | max_concurrency 配置 |
| **事件处理** | 异步状态流转 | SchedulerEvent 队列 |
| **失败处理** | 阻塞下游依赖 | 反向遍历依赖图 |
| **用户交互** | 支持等待用户输入 | WAIT_USER 状态 |

---

## 二、数据模型设计

### 2.1 核心数据结构

#### 2.1.1 IntentNode（意图节点）

```mermaid
classDiagram
    class IntentNode {
        +intent_id: str
        +description: str
        +deps: List[str]
        +status: NodeStatus
        +artifacts: Dict[str, Any]
    }

    class NodeStatus {
        <<enumeration>>
        PENDING
        READY
        RUNNING
        WAIT_USER
        COMPLETED
        FAILED
        BLOCKED_BY_FAILED_DEP
    }

    IntentNode *-- NodeStatus

    style IntentNode fill:#e3f2fd
    style NodeStatus fill:#fff3e0
```

**IntentNode 字段说明**：

| 字段 | 类型 | 说明 |
|------|------|------|
| `intent_id` | str | 意图唯一标识 |
| `description` | str | 意图描述 |
| `deps` | List[str] | 依赖的 intent_id 列表 |
| `status` | NodeStatus | 当前状态 |
| `artifacts` | Dict | 执行产物和元数据 |

**artifacts 结构**：

```python
artifacts = {
    "intent_meta": {...},           # 意图元数据
    "schema": {...},                # Schema 信息
    "ra_plan": {...},               # 关系代数计划
    "sql_candidates": [...],        # SQL 候选列表
    "validations": [...],           # 验证结果
    "exec_result": {...},           # 执行结果
    "exec_raw": {...},              # 原始执行结果
    "user_hints": {...},            # 用户提示
    "facts_bundle": {...},          # 事实包
    "checkpoint": {...},            # 检查点
    "guard": {...},                 # 收敛性检查
    "final": {...},                 # 最终结果
    "error": str                    # 错误信息
}
```

#### 2.1.2 NodeStatus 状态机

```mermaid
stateDiagram-v2
    [*] --> PENDING: 创建节点

    PENDING --> READY: 依赖全部完成
    PENDING --> BLOCKED: 依赖失败

    READY --> RUNNING: 被调度器选取
    READY --> BLOCKED: 依赖失败

    RUNNING --> COMPLETED: 执行成功
    RUNNING --> FAILED: 执行失败
    RUNNING --> WAIT_USER: 需要用户输入

    WAIT_USER --> READY: 用户回复

    FAILED --> [*]: 终止
    BLOCKED --> [*]: 终止
    COMPLETED --> [*]: 终止

    state BLOCKED {
        BLOCKED_BY_FAILED_DEP
    }

    style PENDING fill:#e0e0e0
    style READY fill:#c8e6c9
    style RUNNING fill:#fff3e0
    style COMPLETED fill:#c8e6c9
    style FAILED fill:#ffcdd2
    style WAIT_USER fill:#f3e5f5
    style BLOCKED fill:#ffcdd2
```

#### 2.1.3 GlobalState（全局状态）

```mermaid
classDiagram
    class GlobalState {
        +intent_map: Dict[str, IntentNode]
        +ready_queue: List[str]
        +running_set: Set[str]
        +completed_set: Set[str]
        +dependency_index: Dict[str, List[str]]
        +remaining_deps_count: Dict[str, int]
        +tool_registry: Dict[str, Any]
        +config: Dict[str, Any]
        +audit_log: List[Dict]
        +dialog_state: DialogState
        +pending_events: List[SchedulerEvent]
        +processed_events: List[SchedulerEvent]
        +next_event_seq: int
        +summary() Dict
    }

    class DialogState {
        +queue: List[str]
        +tickets: Dict[str, DialogTicketRecord]
        +active_ticket_id: Optional[str]
    }

    class SchedulerEvent {
        +event_id: str
        +event_type: SchedulerEventType
        +intent_id: str
        +created_at: float
        +payload: Dict[str, Any]
    }

    GlobalState *-- IntentNode
    GlobalState *-- DialogState
    GlobalState *-- SchedulerEvent

    style GlobalState fill:#e3f2fd
    style DialogState fill:#fff3e0
    style SchedulerEvent fill:#fce4ec
```

#### 2.1.4 SchedulerEventType

```mermaid
classDiagram
    class SchedulerEventType {
        <<enumeration>>
        INTENT_READY
        INTENT_COMPLETED
        INTENT_FAILED
        INTENT_WAIT_USER
        USER_REPLY_RECEIVED
        NODE_BLOCKED
    }

    style SchedulerEventType fill:#fff3e0
```

**事件类型说明**：

| 事件类型 | 触发时机 | 效果 |
|---------|---------|------|
| `INTENT_READY` | 依赖全部完成 | 节点状态 → READY，加入 ready_queue |
| `INTENT_COMPLETED` | 执行成功 | 节点状态 → COMPLETED，更新子节点剩余依赖数 |
| `INTENT_FAILED` | 执行失败 | 节点状态 → FAILED，阻塞所有下游节点 |
| `INTENT_WAIT_USER` | 需要用户输入 | 节点状态 → WAIT_USER，等待用户回复 |
| `USER_REPLY_RECEIVED` | 用户回复 | 节点状态 → READY，重新加入 ready_queue |
| `NODE_BLOCKED` | 被失败依赖阻塞 | 节点状态 → BLOCKED_BY_FAILED_DEP |

#### 2.1.5 DialogTicketRecord（对话票据）

```mermaid
classDiagram
    class DialogTicketRecord {
        +ticket_id: str
        +intent_id: str
        +question_id: str
        +phase: str
        +created_at: float
        +payload: Dict[str, Any]
        +thread_id: str
        +turns: List[Dict]
        +resolved: bool
        +resolution_type: Optional[DialogResolutionType]
    }

    class DialogResolutionType {
        <<enumeration>>
        ANSWERED
        SKIPPED
        TIMEOUT
        CANCELLED
    }

    DialogTicketRecord *-- DialogResolutionType

    style DialogTicketRecord fill:#fce4ec
    style DialogResolutionType fill:#f3e5f5
```

---

## 三、核心算法与实现

### 3.1 DAG 构建算法

```mermaid
flowchart TD
    Start[输入：intents 列表] --> BuildNodes[构建 IntentNode 列表]
    BuildNodes --> ValidateDeps[验证依赖存在性]
    ValidateDeps --> BuildReverseIndex[构建反向依赖索引]
    BuildReverseIndex --> CycleCheck[环检测 Kahn 算法]
    CycleCheck --> HasCycle{有环?}
    HasCycle -->|Yes| Error[抛出异常：<br/>intent dependency graph has cycle]
    HasCycle -->|No| InitReady[初始化 ready_queue<br/>deps_count=0 的节点]
    InitReady --> CreateState[创建 GlobalState]
    CreateState --> EmitReadyEvents[发送 INTENT_READY 事件]
    EmitReadyEvents --> End[返回 GlobalState]

    style Start fill:#e1f5e1
    style End fill:#ffe1e1
    style CycleCheck fill:#e3f2fd
    style Error fill:#ffcdd2
    style CreateState fill:#fff3e0
```

### 3.2 环检测算法（Kahn）

```python
def _validate_acyclic(intent_map, dependency_index, remaining):
    remaining_local = dict(remaining)
    queue = [id for id, cnt in remaining_local.items() if cnt == 0]
    visited = 0

    while queue:
        cur = queue.pop(0)
        visited += 1
        for child in dependency_index.get(cur, []):
            remaining_local[child] -= 1
            if remaining_local[child] == 0:
                queue.append(child)

    if visited != len(intent_map):
        raise ValueError("intent dependency graph has cycle")
```

**算法说明**：
1. 初始化：统计每个节点的入度（剩余依赖数）
2. 将入度为 0 的节点加入队列
3. 依次处理队列中的节点，减少其子节点的入度
4. 若子节点入度变为 0，加入队列
5. 若最终访问节点数 < 总节点数，说明存在环

### 3.3 调度器主循环

```mermaid
flowchart TD
    Start[调度器启动] --> Config[读取配置<br/>max_concurrency]
    Config --> PollWork[调用 poll_work]
    PollWork --> DrainEvents[drain_events<br/>处理待处理事件]
    DrainEvents --> PopReady[pop_ready<br/>从 ready_queue 取节点]
    PopReady --> CheckConcurrency{running < max?}
    CheckConcurrency -->|No| Wait[等待完成]
    CheckConcurrency -->|Yes| Dispatch[更新状态为 RUNNING]
    Dispatch --> ReturnWork[返回 WorkItem 列表]
    ReturnWork --> Execute[执行器执行意图]
    Execute --> SubmitResult[submit_work_result]
    SubmitResult --> EventType{结果类型?}

    EventType -->|WAIT_USER| EmitWaitUser[emit_wait_user]
    EventType -->|OK| EmitCompleted[emit_completed]
    EventType -->|ERROR| EmitFailed[emit_failed]

    EmitWaitUser --> DrainEvents2[drain_events]
    EmitCompleted --> DrainEvents2
    EmitFailed --> DrainEvents2

    DrainEvents2 --> CheckFinished{is_finished?}
    CheckFinished -->|No| PollWork
    CheckFinished -->|Yes| End[调度结束]

    Wait --> CheckFinished

    style Start fill:#e1f5e1
    style End fill:#ffe1e1
    style PollWork fill:#e3f2fd
    style SubmitResult fill:#fff3e0
    style CheckFinished fill:#fce4ec
```

### 3.4 事件处理流程

```mermaid
flowchart TD
    Start[drain_events] --> HasEvents{pending_events 非空?}
    HasEvents -->|No| Return[返回处理数量]
    HasEvents -->|Yes| PopEvent[弹出队首事件]

    PopEvent --> EventType{事件类型?}

    EventType -->|INTENT_READY| HandleReady[应用 READY 状态<br/>加入 ready_queue]
    EventType -->|INTENT_COMPLETED| HandleCompleted[调用 mark_completed]
    EventType -->|INTENT_FAILED| HandleFailed[调用 mark_failed]
    EventType -->|INTENT_WAIT_USER| HandleWaitUser[调用 mark_wait_user]
    EventType -->|USER_REPLY_RECEIVED| HandleUserReply[状态→READY<br/>加入 ready_queue]
    EventType -->|NODE_BLOCKED| HandleBlocked[状态→BLOCKED<br/>从队列移除]

    HandleReady --> Log[记录 audit_log]
    HandleCompleted --> Log
    HandleFailed --> Log
    HandleWaitUser --> Log
    HandleUserReply --> Log
    HandleBlocked --> Log

    Log --> AddProcessed[加入 processed_events]
    AddProcessed --> HasEvents

    style Start fill:#e1f5e1
    style Return fill:#ffe1e1
    style HandleReady fill:#c8e6c9
    style HandleCompleted fill:#c8e6c9
    style HandleFailed fill:#ffcdd2
    style HandleWaitUser fill:#f3e5f5
    style HandleUserReply fill:#c8e6c9
    style HandleBlocked fill:#ffcdd2
```

### 3.5 失败传播算法

```mermaid
flowchart TD
    Start[mark_failed: intent_id] --> SetFailed[节点状态→FAILED]
    SetFailed --> RemoveRunning[从 running_set 移除]
    RemoveRunning --> Log[记录 audit_log]
    Log --> BlockDescendants[调用 _block_descendants]

    BlockDescendants --> GetChildren[获取直接子节点]
    GetChildren --> QueueChildren[加入 BFS 队列]
    QueueChildren --> ProcessQueue{队列非空?}

    ProcessQueue -->|No| End[返回]
    ProcessQueue -->|Yes| PopCur[弹出当前节点]
    PopCur --> Visited{已访问?}
    Visited -->|Yes| ProcessQueue
    Visited -->|No| MarkVisited[标记为已访问]

    MarkVisited --> CheckStatus{状态检查}
    CheckStatus -->|COMPLETED/FAILED/BLOCKED| ProcessQueue
    CheckStatus -->|其他 | EmitBlocked[发送 NODE_BLOCKED 事件]

    EmitBlocked --> GetGrandChildren[获取孙节点]
    GetGrandChildren --> QueueGrandChildren[加入队列]
    QueueGrandChildren --> ProcessQueue

    style Start fill:#e1f5e1
    style End fill:#ffe1e1
    style SetFailed fill:#ffcdd2
    style EmitBlocked fill:#ffcdd2
```

---

## 四、API 设计

### 4.1 DAGScheduler 公共接口

```mermaid
classDiagram
    class DAGScheduler {
        -state: GlobalState
        +__init__(intents, config, tool_registry)
        +from_state(state) DAGScheduler
        +pop_ready(limit) List[IntentNode]
        +poll_work(limit) List[WorkItem]
        +submit_work_result(intent_id, ok, payload)
        +emit_ready(intent_id, payload)
        +emit_completed(intent_id, result)
        +emit_failed(intent_id, error)
        +emit_wait_user(intent_id, payload)
        +emit_user_reply_received(intent_id, payload)
        +drain_events() int
        +mark_completed(intent_id, result)
        +mark_failed(intent_id, error)
        +mark_wait_user(intent_id, payload)
        +has_ready() bool
        +is_finished() bool
        +summary() Dict
    }

    class WorkItem {
        +intent_id: str
        +node: IntentNode
        +lease_created_at: float
    }

    class SchedulerConfig {
        +max_concurrency: int
    }

    DAGScheduler *-- GlobalState
    DAGScheduler ..> WorkItem
    DAGScheduler ..> SchedulerConfig

    style DAGScheduler fill:#e3f2fd
    style WorkItem fill:#fff3e0
    style SchedulerConfig fill:#fce4ec
```

### 4.2 使用示例

```python
from stages.sql_generation.dag import DAGScheduler, SchedulerConfig

# 1. 定义意图（带依赖）
intents = [
    {"intent_id": "I1", "intent_description": "查询工厂信息", "dependency_intent_ids": []},
    {"intent_id": "I2", "intent_description": "查询设备信息", "dependency_intent_ids": ["I1"]},
    {"intent_id": "I3", "intent_description": "查询维护记录", "dependency_intent_ids": ["I2"]},
]

# 2. 创建调度器
config = SchedulerConfig(max_concurrency=2)
scheduler = DAGScheduler(intents, config=config)

# 3. 主循环
while not scheduler.is_finished():
    # 获取可执行的工作项
    work_items = scheduler.poll_work()

    for item in work_items:
        # 执行意图（这里调用 Intent Runtime）
        result = execute_intent(item.node)

        # 提交结果
        if result.ok:
            scheduler.submit_work_result(item.intent_id, ok=True, payload=result.data)
        elif result.wait_user:
            scheduler.submit_work_result(item.intent_id, ok="WAIT_USER", payload=result.question)
        else:
            scheduler.submit_work_result(item.intent_id, ok=False, payload=result.error)

# 4. 获取最终状态
summary = scheduler.summary()
print(f"Completed: {summary['completed']}")
print(f"Failed: {summary['failed']}")
```

### 4.3 状态查询接口

```python
# 获取调度器摘要
summary = scheduler.state.summary()

# 返回结构
{
    "intent_count": 10,           # 总意图数
    "ready": 2,                   # 就绪数
    "running": 1,                 # 运行中数
    "completed": 6,               # 完成数
    "status_counts": {            # 各状态计数
        "pending": 1,
        "ready": 2,
        "running": 1,
        "completed": 6,
        "failed": 0,
        "wait_user": 0,
        "blocked_by_failed_dep": 0
    },
    "failed_intents": [],         # 失败意图列表（最多 8 个）
    "wait_user": [],              # 等待用户的意图（最多 8 个）
    "audit_events": 45,           # 审计事件数
    "active_ticket_id": "",       # 当前活跃票据 ID
    "pending_tickets": [],        # 待处理票据（最多 8 个）
    "pending_events": 0,          # 待处理事件数
    "processed_events": 45        # 已处理事件数
}
```

---

## 五、依赖管理

### 5.1 依赖数据结构

```mermaid
flowchart LR
    subgraph DependencyIndex[dependency_index]
        DI1["I1: [I2, I3]"]
        DI2["I2: [I3]"]
        DI3["I3: []"]
    end

    subgraph RemainingDeps[remaining_deps_count]
        RD1["I1: 0"]
        RD2["I2: 1"]
        RD3["I3: 2"]
    end

    subgraph IntentMap[intent_map]
        IM1["I1: IntentNode"]
        IM2["I2: IntentNode"]
        IM3["I3: IntentNode"]
    end

    style DependencyIndex fill:#e3f2fd
    style RemainingDeps fill:#fff3e0
    style IntentMap fill:#fce4ec
```

### 5.2 collect_ancestors 算法

```python
def collect_ancestors(intent_ids, intent_map):
    """收集所有祖先节点（传递依赖）"""
    queue = [x for x in intent_ids if x in intent_map]
    seen = set(queue)
    out = []

    while queue:
        cur = queue.pop(0)
        out.append(cur)
        for dep in intent_map[cur].deps:
            if dep in intent_map and dep not in seen:
                seen.add(dep)
                queue.append(dep)

    return out
```

**用途**：构建依赖 payload 时获取传递依赖信息

### 5.3 build_dependency_payload

```python
def build_dependency_payload(node, state, max_transitive=5):
    """为节点构建依赖 payload，传递给执行器"""
    deps = list(node.deps or [])
    direct_facts = []
    missing_dependencies = []

    # 1. 收集直接依赖的事实
    for dep_id in deps:
        dep = intent_map.get(dep_id)
        if dep is None or dep.status != NodeStatus.COMPLETED:
            missing_dependencies.append(dep_id)
            continue
        direct_facts.append(_facts_payload(dep))

    # 2. 收集传递依赖（最多 max_transitive 个）
    ancestors = collect_ancestors(deps, intent_map)
    transitive_ids = [x for x in ancestors if x not in deps][:max_transitive]
    transitive_facts = []
    for anc_id in transitive_ids:
        anc = intent_map.get(anc_id)
        if anc is None or anc.status != NodeStatus.COMPLETED:
            continue
        transitive_facts.append(_facts_payload(anc))

    return {
        "direct_facts": direct_facts,
        "transitive_facts": transitive_facts,
        "missing_dependencies": missing_dependencies,
        "meta": {
            "direct_dep_ids": deps,
            "transitive_selected_ids": transitive_ids,
        },
    }
```

---

## 六、对话管理

### 6.1 DialogState 结构

```mermaid
classDiagram
    class DialogState {
        +queue: List[str]
        +tickets: Dict[str, DialogTicketRecord]
        +active_ticket_id: Optional[str]
    }

    class DialogTicketRecord {
        +ticket_id: str
        +intent_id: str
        +question_id: str
        +phase: str
        +created_at: float
        +payload: Dict[str, Any]
        +thread_id: str
        +turns: List[Dict[str, Any]]
        +resolved: bool
        +resolution_type: Optional[DialogResolutionType]
    }

    DialogState "1" *-- "0..*" DialogTicketRecord

    style DialogState fill:#e3f2fd
    style DialogTicketRecord fill:#fff3e0
```

### 6.2 用户交互流程

```mermaid
sequenceDiagram
    participant S as Scheduler
    participant R as Runtime
    participant U as User

    S->>R: poll_work 返回 I1
    R->>R: 执行 I1
    R->>S: emit_wait_user(I1, {"question": "确认表名"})
    S->>S: 状态→WAIT_USER

    Note over S,U: 等待用户输入

    U->>S: 提交回复
    S->>S: emit_user_reply_received(I1)
    S->>S: 状态→READY

    S->>R: poll_work 返回 I1
    R->>R: 继续执行
    R->>S: emit_completed(I1)
    S->>S: 状态→COMPLETED
```

---

## 七、序列化与持久化

### 7.1 序列化接口

```mermaid
flowchart LR
    subgraph Serialize[序列化]
        S1[state_to_dict]
        S2[intent_node_to_dict]
    end

    subgraph Deserialize[反序列化]
        D1[state_from_dict]
        D2[intent_node_from_dict]
    end

    GlobalState --> S1
    IntentNode --> S2
    Dict --> D1
    Dict --> D2
    D1 --> GlobalState
    D2 --> IntentNode

    style Serialize fill:#e3f2fd
    style Deserialize fill:#fff3e0
```

### 7.2 持久化策略

```python
# 序列化
from stages.sql_generation.dag import state_to_dict

state_data = state_to_dict(scheduler.state)

# 保存到文件/数据库
import json
with open("scheduler_state.json", "w") as f:
    json.dump(state_data, f, ensure_ascii=False, indent=2)

# 反序列化
from stages.sql_generation.dag import state_from_dict, DAGScheduler

with open("scheduler_state.json", "r") as f:
    state_data = json.load(f)

state = state_from_dict(state_data)
scheduler = DAGScheduler.from_state(state)
```

---

## 八、错误处理

### 8.1 错误类型

| 错误类型 | 触发条件 | 处理方式 |
|---------|---------|---------|
| `ValueError: intent_id is required` | intent 缺少 intent_id | 抛出异常，终止构建 |
| `ValueError: duplicate intent_id` | 重复的 intent_id | 抛出异常，终止构建 |
| `ValueError: depends on unknown intent_id` | 依赖不存在的 intent | 抛出异常，终止构建 |
| `ValueError: intent dependency graph has cycle` | 存在循环依赖 | 抛出异常，终止构建 |
| `ValueError: cannot complete from status X` | 状态不合法 | 抛出异常 |
| `ValueError: unknown intent_id` | intent_id 不存在 | 抛出异常 |

### 8.2 失败传播

```mermaid
flowchart TD
    Start[I1 执行失败] --> MarkFailed[mark_failed I1]
    MarkFailed --> SetStatus[状态→FAILED]
    SetStatus --> EmitEvent[发送 INTENT_FAILED 事件]
    EmitEvent --> BlockDescendants[_block_descendants]

    BlockDescendants --> GetChildren[获取 I2, I3]
    GetChildren --> BlockI2[阻塞 I2<br/>状态→BLOCKED]
    GetChildren --> BlockI3[阻塞 I3<br/>状态→BLOCKED]

    BlockI2 --> GetGrandChildren[获取 I4, I5]
    BlockI3 --> GetGrandChildren

    GetGrandChildren --> BlockI4[阻塞 I4]
    GetGrandChildren --> BlockI5[阻塞 I5]

    style Start fill:#ffcdd2
    style MarkFailed fill:#ffcdd2
    style BlockI2 fill:#ffcdd2
    style BlockI3 fill:#ffcdd2
    style BlockI4 fill:#ffcdd2
    style BlockI5 fill:#ffcdd2
```

---

## 九、并发控制

### 9.1 max_concurrency 配置

```python
@dataclass(frozen=True)
class SchedulerConfig:
    max_concurrency: int = 3
```

### 9.2 并发控制逻辑

```mermaid
flowchart TD
    Start[pop_ready 调用] --> GetConfig[读取 max_concurrency]
    GetConfig --> CalcSlots[计算可用槽位：<br/>slots = max - 运行中任务数]
    CalcSlots --> CheckSlots{slots \> 0?}
    CheckSlots -->|No| ReturnEmpty[返回空列表]
    CheckSlots -->|Yes| PopFromQueue[从 ready_queue 弹出]

    PopFromQueue --> CheckStatus{节点状态=READY?}
    CheckStatus -->|No| Skip[跳过]
    CheckStatus -->|Yes| SetRunning[状态→RUNNING<br/>加入 running_set]

    Skip --> CheckLimit{已取数量 < limit?}
    SetRunning --> CheckLimit

    CheckLimit -->|Yes| PopFromQueue
    CheckLimit -->|No| Return[返回节点列表]

    ReturnEmpty --> Return

    style Start fill:#e1f5e1
    style Return fill:#ffe1e1
    style SetRunning fill:#e3f2fd
```

---

## 十、文件结构

```
stages/sql_generation/dag/
├── __init__.py              # 模块入口，导出公共接口
├── models.py                # 数据模型定义
│   ├── NodeStatus           # 节点状态枚举
│   ├── SchedulerEventType   # 事件类型枚举
│   ├── SchedulerEvent       # 事件数据类
│   ├── IntentNode           # 意图节点
│   ├── GlobalState          # 全局状态
│   ├── DialogState          # 对话状态
│   └── DialogTicketRecord   # 对话票据
│
├── scheduler.py             # 调度器核心实现
│   ├── DAGScheduler         # 调度器类
│   ├── SchedulerConfig      # 配置类
│   ├── WorkItem             # 工作项
│   ├── build_global_state   # 状态构建函数
│   └── _validate_acyclic    # 环检测函数
│
├── deps.py                  # 依赖管理
│   ├── build_dependency_payload  # 构建依赖 payload
│   └── collect_ancestors    # 收集祖先节点
│
├── serialize.py             # 序列化/反序列化
│   ├── state_to_dict        # 状态序列化
│   ├── state_from_dict      # 状态反序列化
│   ├── intent_node_to_dict  # 节点序列化
│   └── intent_node_from_dict# 节点反序列化
│
└── README.md                # 设计文档
```
