"""图谱搜索领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

import hashlib
import re
import threading
from datetime import date
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

# 同批发布并发时，「查找既有条目 → 写入」必须整体串行，
# 保证同一原版指纹只被接受一次，重复发布合并而不是叠加。
PUBLISH_LOCK = threading.RLock()

# 原版指纹取自模板的一次发布内容：任一字段变化（如同编号但跳数不同的修订版）
# 都会得到新指纹，从而按自己的图谱范围独立派发、独立持久保存。
FINGERPRINT_FIELDS = ("模板编号", "搜索类型", "查询语法", "起始实体", "关系过滤", "跳数上限", "排序规则")


def normalize_term(value: Any) -> str:
    """兼容旧模板的查询语法：折叠多余空白后再比较，同一语法不因书写格式被当成两版。"""
    return re.sub(r"\s+", " ", str(value or "")).strip()


def template_fingerprint(entry: dict[str, Any]) -> str:
    """原版指纹：同一模板内容得到稳定指纹；内容不同的修订版得到新指纹。"""
    material = "|".join(normalize_term(entry.get(field)) for field in FINGERPRINT_FIELDS)
    return hashlib.sha1(material.encode("utf-8")).hexdigest()[:12]


def legacy_result_hash(template_no: Any) -> str:
    """修复前缓存条目的哈希写法（hash_{模板编号}），用于识别并接管旧条目。"""
    return f"hash_{normalize_term(template_no)}"


class GraphSearchService(BaseModuleService):
    MODULE = "graph_search"
    ENTITY = "搜索模板"
    OPERATION_TERM = "查询"
    LIST_FIELDS = ["模板编号", "搜索类型", "查询语法", "起始实体", "关系过滤", "跳数上限", "排序规则", "使用频次"]
    REQUIRED_FIELDS = ["模板编号", "搜索类型", "查询语法"]
    STATUS_ORDER = ["草稿", "已发布", "性能告警", "已停用"]
    ACTION_RULES = {"发布模板": "已发布", "标记告警": "性能告警", "停用模板": "已停用"}
    NEGATIVE_ACTIONS = ["停用模板"]

    CACHE_CODE_PREFIX = "QUER-CACHE-"
    SUBGRAPH_CODE_PREFIX = "SUBG-SRCH-"

    # ===== 跨模块数据流实现 =====
    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, message = super().run_action(entry_id, action)
        if entry is None:
            return entry, message
        # 链路 L5/L6: graph_search → query_cache + subgraph_exp
        # 已发布模板的数据写入改为幂等合并：断线重发、重复发布都只对应具体一次发布。
        if action == "发布模板" and entry.get("status") == "已发布":
            message += "；" + self._publish_to_consumers(entry)
        return entry, message

    def _publish_to_consumers(self, entry: dict[str, Any]) -> str:
        fingerprint = template_fingerprint(entry)
        with PUBLISH_LOCK:
            generation = self._generation_for(entry, fingerprint)
            cache_row, cache_msg = self._push_to_query_cache(entry, fingerprint, generation)
            sub_row, sub_msg = self._push_to_subgraph_exp(entry, fingerprint, generation)
            # 双向回查：有效缓存 ↔ 派生种子，任何一侧都能查到对应关系。
            cache_row["探查配置"] = str(sub_row.get("配置编号", ""))
            cache_row["探查种子"] = str(sub_row.get("探查种子", ""))
            sub_row["缓存条目"] = str(cache_row.get("条目编号", ""))
        return f"{cache_msg}；{sub_msg}"

    def _generation_for(self, entry: dict[str, Any], fingerprint: str) -> int:
        """分代：同一模板编号下每个新指纹依次取下一代；重发同一指纹沿用既有代次。"""
        template_no = normalize_term(entry.get("模板编号"))
        known: dict[str, int] = {}
        for row in store.rows("query_cache"):
            if normalize_term(row.get("来源模板")) != template_no:
                continue
            fp = str(row.get("来源指纹") or "")
            try:
                gen = int(row.get("发布代次", 0) or 0)
            except (TypeError, ValueError):
                gen = 0
            if fp and gen:
                known.setdefault(fp, gen)
        if fingerprint in known:
            return known[fingerprint]
        return max(known.values(), default=0) + 1

    def _push_to_query_cache(self, entry: dict[str, Any], fingerprint: str, generation: int) -> tuple[dict[str, Any], str]:
        cache_rows = store.rows("query_cache")
        target, duplicates = self._match_cache_rows(cache_rows, entry, fingerprint)
        if target is None:
            next_id = max((int(r.get("id", 0)) for r in cache_rows), default=0) + 1
            target = {
                "id": next_id,
                "status": "有效",
                "pending": True,
                "abnormal": False,
                "条目编号": f"QUER-CACHE-{next_id:04d}",
                "命中次数": "0",
                "TTL秒数": "3600",
                "最后命中": "",
                "缓存状态": "stored",
            }
            cache_rows.append(target)
            message = f"已写入查询缓存 {target['条目编号']}（第{generation}代）"
        else:
            if target.get("status") in ("已失效", "已淘汰"):
                target["status"] = "有效"
                target["缓存状态"] = "stored"
                target["pending"] = True
            message = f"重复发布已合并到查询缓存 {target['条目编号']}（第{generation}代）"
        # 来源稳定预热数据：缓存条目固定对应具体一次发布的指纹与代次，
        # 查询模板只登记本次发布已确认的语法。
        target["查询模板"] = str(entry.get("查询语法", ""))
        target["结果哈希"] = f"hash_{fingerprint}"
        target["来源模板"] = str(entry.get("模板编号", ""))
        target["来源指纹"] = fingerprint
        target["发布代次"] = generation
        target["存储大小"] = f"{len(str(entry))}B"
        for dup in duplicates:
            dup["status"] = "已淘汰"
            dup["pending"] = False
            dup["缓存状态"] = "superseded"
        if duplicates:
            message += f"，同哈希重复条目已淘汰 {len(duplicates)} 条"
        return target, message

    def _match_cache_rows(
        self, rows: list[dict[str, Any]], entry: dict[str, Any], fingerprint: str
    ) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
        """定位本次发布对应的缓存条目：先按来源指纹，再兼容接管旧写法条目。"""
        primary: list[dict[str, Any]] = []
        legacy: list[dict[str, Any]] = []
        legacy_hash = legacy_result_hash(entry.get("模板编号"))
        syntax = normalize_term(entry.get("查询语法"))
        for row in rows:
            if row.get("来源指纹"):
                if row["来源指纹"] == fingerprint:
                    primary.append(row)
            elif (
                str(row.get("条目编号", "")).startswith(self.CACHE_CODE_PREFIX)
                and str(row.get("结果哈希", "")) == legacy_hash
                and normalize_term(row.get("查询模板")) == syntax
            ):
                legacy.append(row)
        candidates = primary + legacy
        if not candidates:
            return None, []
        return candidates[0], candidates[1:]

    def _push_to_subgraph_exp(self, entry: dict[str, Any], fingerprint: str, generation: int) -> tuple[dict[str, Any], str]:
        sub_rows = store.rows("subgraph_exp")
        target, duplicates = self._match_subgraph_rows(sub_rows, entry, fingerprint)
        if target is None:
            next_id = max((int(r.get("id", 0)) for r in sub_rows), default=0) + 1
            target = {
                "id": next_id,
                "status": "草稿",
                "pending": True,
                "abnormal": False,
                "配置编号": f"SUBG-SRCH-{next_id:04d}",
                "扩展策略": "bfs-traversal",
                "结果数量": 15,
                "缓存策略": "auto-warm",
                "上次探查": date.today().isoformat(),
            }
            sub_rows.append(target)
            message = f"已生成子图探查配置 {target['配置编号']}（第{generation}代）"
        else:
            if target.get("status") == "已丢弃":
                target["status"] = "草稿"
                target["pending"] = True
            message = f"重复发布已复用子图探查配置 {target['配置编号']}（第{generation}代）"
        target["探查种子"] = str(entry.get("起始实体", ""))
        target["关系过滤"] = str(entry.get("关系过滤", ""))
        target["深度限制"] = str(entry.get("跳数上限", "3"))
        target["来源模板"] = str(entry.get("模板编号", ""))
        target["来源指纹"] = fingerprint
        target["发布代次"] = generation
        for dup in duplicates:
            dup["status"] = "已丢弃"
            dup["pending"] = False
        if duplicates:
            message += f"，残留重复草稿已丢弃 {len(duplicates)} 份"
        return target, message

    def _match_subgraph_rows(
        self, rows: list[dict[str, Any]], entry: dict[str, Any], fingerprint: str
    ) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
        """定位本次发布对应的探查配置：先按来源指纹，再兼容接管旧写法草稿。"""
        primary: list[dict[str, Any]] = []
        legacy: list[dict[str, Any]] = []
        for row in rows:
            if row.get("来源指纹"):
                if row["来源指纹"] == fingerprint:
                    primary.append(row)
            elif (
                str(row.get("配置编号", "")).startswith(self.SUBGRAPH_CODE_PREFIX)
                and normalize_term(row.get("探查种子")) == normalize_term(entry.get("起始实体"))
                and normalize_term(row.get("关系过滤")) == normalize_term(entry.get("关系过滤"))
                and normalize_term(row.get("深度限制")) == normalize_term(entry.get("跳数上限", "3"))
            ):
                legacy.append(row)
        candidates = primary + legacy
        if not candidates:
            return None, []
        return candidates[0], candidates[1:]

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["cross_module_consumers"] = ["query_cache", "subgraph_exp"]
        # 来源稳定预热数据台账：每次发布（指纹 × 代次）对应的缓存条目与探查配置。
        base["publish_registry"] = [
            {
                "来源模板": row.get("来源模板"),
                "发布代次": row.get("发布代次"),
                "来源指纹": row.get("来源指纹"),
                "缓存条目": row.get("条目编号"),
                "探查配置": row.get("探查配置"),
            }
            for row in store.rows("query_cache")
            if row.get("来源指纹")
        ]
        return base
