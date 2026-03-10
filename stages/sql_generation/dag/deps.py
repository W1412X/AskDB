"""
Dependency payload builder for intent execution.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Set

from stages.sql_generation.dag.models import GlobalState, IntentNode, NodeStatus


def collect_ancestors(intent_ids: Iterable[str], intent_map: Dict[str, IntentNode]) -> List[str]:
    queue = [x for x in intent_ids if x in intent_map]
    seen: Set[str] = set(queue)
    out: List[str] = []
    while queue:
        cur = queue.pop(0)
        out.append(cur)
        for dep in intent_map[cur].deps:
            if dep in intent_map and dep not in seen:
                seen.add(dep)
                queue.append(dep)
    return out


def _facts_payload(node: IntentNode) -> Dict[str, Any]:
    facts = node.artifacts.get("facts_bundle") or {}
    if not isinstance(facts, dict):
        facts = {}
    final = node.artifacts.get("final") or {}
    if not isinstance(final, dict):
        final = {}
    return {
        "intent_id": node.intent_id,
        "description": node.description,
        "intent_meta": node.artifacts.get("intent_meta") or {},
        "facts_bundle": facts,
        "final_sql_fingerprint": final.get("final_sql_fingerprint") or "",
        "status": node.status.value,
    }


def build_dependency_payload(
    node: IntentNode,
    state: GlobalState,
    *,
    max_transitive: int = 5,
) -> Dict[str, Any]:
    intent_map = state.intent_map
    deps = list(node.deps or [])
    direct_facts: List[Dict[str, Any]] = []
    missing_dependencies: List[str] = []

    for dep_id in deps:
        dep = intent_map.get(dep_id)
        if dep is None or dep.status != NodeStatus.COMPLETED:
            missing_dependencies.append(dep_id)
            continue
        direct_facts.append(_facts_payload(dep))

    ancestors = collect_ancestors(deps, intent_map=intent_map)
    transitive_ids = [x for x in ancestors if x not in deps][:max_transitive]
    transitive_facts: List[Dict[str, Any]] = []
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
