"""查询缓存领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from datetime import date
from typing import Any

from app.services.base import BaseModuleService
from app.services.graph_search import normalize_term, template_fingerprint
from app.store import store


class QueryCacheService(BaseModuleService):
    MODULE = "query_cache"
    ENTITY = "缓存条目"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["条目编号", "查询模板", "结果哈希", "命中次数", "TTL秒数", "存储大小", "最后命中", "缓存状态"]
    REQUIRED_FIELDS = ["条目编号", "查询模板", "结果哈希"]
    STATUS_ORDER = ["有效", "即将过期", "已失效", "已淘汰"]
    ACTION_RULES = {"主动预热": "有效", "即时失效": "已失效"}
    NEGATIVE_ACTIONS = []

    # 来源稳定：来源模板处于已发布（含性能告警）状态时，才允许领取已确认语法。
    STABLE_SOURCE_STATUSES = ("已发布", "性能告警")

    # ===== 跨模块数据流实现 =====
    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action != "主动预热":
            return super().run_action(entry_id, action)
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        # 交付中断后重试时，尚未找到相应种子的缓存不能供预热。
        seed = self._find_seed(entry)
        if seed is None:
            return None, f"{self.ENTITY}暂不可预热：未找到对应探查种子，可能交付中断，请重新发布来源模板"
        # 查询缓存只从来源稳定预热数据领取已确认语法。
        refusal = self._confirm_syntax_from_source(entry)
        if refusal is not None:
            return None, f"{self.ENTITY}暂不可预热：{refusal}"
        entry, message = super().run_action(entry_id, action)
        assert entry is not None
        # 缓存刷新后与实际可执行草稿一致：语法已回源确认，回查引用已补齐。
        entry["缓存状态"] = "warm"
        entry["预热时间"] = date.today().isoformat()
        entry["pending"] = False
        # 预热资格结果同步到子图待办。
        seed["pending"] = False
        seed["预热资格"] = "已确认"
        message += f"；预热资格已同步子图待办 {seed.get('配置编号', '')}"
        return entry, message

    def _find_seed(self, entry: dict[str, Any]) -> dict[str, Any] | None:
        """按双向回查引用定位派生探查种子；引用不齐时自愈补齐。"""
        rows = store.rows("subgraph_exp")
        code = str(entry.get("条目编号", ""))
        seed_code = str(entry.get("探查配置", ""))
        fingerprint = str(entry.get("来源指纹", ""))
        matched: dict[str, Any] | None = None
        for row in rows:
            if row.get("status") != "已丢弃" and code and row.get("缓存条目") == code:
                matched = row
                break
        if matched is None:
            for row in rows:
                if row.get("status") != "已丢弃" and seed_code and row.get("配置编号") == seed_code:
                    matched = row
                    break
        if matched is None and fingerprint:
            for row in rows:
                if row.get("status") != "已丢弃" and row.get("来源指纹") == fingerprint:
                    matched = row
                    break
        if matched is None:
            return None
        matched["缓存条目"] = code
        entry["探查配置"] = str(matched.get("配置编号", ""))
        entry["探查种子"] = str(matched.get("探查种子", ""))
        return matched

    def _confirm_syntax_from_source(self, entry: dict[str, Any]) -> str | None:
        """从来源稳定（已发布）的同代模板领取已确认语法；来源不可用时拒绝预热。"""
        template_no = normalize_term(entry.get("来源模板"))
        if not template_no:
            return "缺少来源模板，无法确认语法"
        fingerprint = str(entry.get("来源指纹", ""))
        for row in store.rows("graph_search"):
            if normalize_term(row.get("模板编号")) != template_no:
                continue
            if fingerprint and template_fingerprint(row) != fingerprint:
                continue
            if row.get("status") not in self.STABLE_SOURCE_STATUSES:
                continue
            entry["查询模板"] = str(row.get("查询语法", ""))
            return None
        return f"来源模板 {template_no} 未处于已发布状态，语法未确认"

    def cache_seed_links(self) -> list[dict[str, Any]]:
        """回查：有效缓存条目与派生探查种子的对应关系。"""
        links: list[dict[str, Any]] = []
        for row in store.rows(self.MODULE):
            if row.get("status") != "有效" or not row.get("探查配置"):
                continue
            links.append({
                "条目编号": row.get("条目编号"),
                "来源模板": row.get("来源模板"),
                "发布代次": row.get("发布代次"),
                "结果哈希": row.get("结果哈希"),
                "探查配置": row.get("探查配置"),
                "探查种子": row.get("探查种子"),
                "缓存状态": row.get("缓存状态"),
            })
        return links

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["cross_module_consumers"] = ["subgraph_exp"]
        base["cache_seed_links"] = self.cache_seed_links()
        return base
