"""采集审计领域服务：审计记录登记与按交付标识幂等补齐。"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService


class CrawlAuditService(BaseModuleService):
    MODULE = "crawl_audit"
    ENTITY = "审计记录"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["记录标识", "操作类型", "操作对象", "操作时间", "操作人", "变更前值", "变更后值", "审计摘要"]
    REQUIRED_FIELDS = ["记录标识", "操作类型", "操作对象"]
    STATUS_ORDER = ["已记录", "已复核", "异常记录"]
    ACTION_RULES = {"标记复核": "已记录", "上报异常": "异常记录"}
    NEGATIVE_ACTIONS = []

    # 交付审计关联字段（不进入 LIST_FIELDS，CSV 列和单元格保持不变）。
    DELIVERY_FIELD = "交付标识"
    AUDIT_STATUS_FIELD = "审计状态"
    QUERY_FIELDS = [*LIST_FIELDS, DELIVERY_FIELD, AUDIT_STATUS_FIELD]
    FILTER_ALIASES = {
        "delivery_id": DELIVERY_FIELD,
        "delivery": DELIVERY_FIELD,
        "audit_status": AUDIT_STATUS_FIELD,
    }

    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        """审计列表：真实审计记录 + 交付侧尚未确认的「待补写」占位。

        交付在自动登记后、写审计前中断时，审计入口也能按交付标识查到待补
        记录，补写完成后占位消失、只剩那一条正式审计，不重复计数。
        """
        rows = list(self._audit_rows_with_pending(include_pending=True))
        aliases = self.FILTER_ALIASES
        for field, expected in (filters or {}).items():
            field = aliases.get(field, field)
            if field == self.DELIVERY_FIELD:
                rows = [row for row in rows
                        if str(row.get(field, "")) == expected]
            elif field in self.QUERY_FIELDS:
                rows = [row for row in rows
                        if expected.casefold() in str(row.get(field, "")).casefold()]
        if keyword and self.LIST_FIELDS:
            first = self.LIST_FIELDS[0]
            rows = [row for row in rows
                    if keyword.casefold() in str(row.get(first, "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def _audit_rows_with_pending(self, *, include_pending: bool) -> list[dict[str, Any]]:
        from app.store import store

        rows = list(store.rows(self.MODULE))
        seen = {
            str(row.get(self.DELIVERY_FIELD) or "")
            for row in rows
            if row.get(self.DELIVERY_FIELD)
        }
        pending: list[dict[str, Any]] = []
        if not include_pending:
            return rows
        for registration in store.rows("export_channel"):
            if registration.get("登记方式") != "auto":
                continue
            delivery_id = str(registration.get("交付标识") or "")
            if not delivery_id or delivery_id in seen:
                continue
            pending.append({
                "id": f"pending:{delivery_id}",
                "记录标识": f"PENDING-{delivery_id}",
                "操作类型": "export",
                "操作对象": registration.get("来源批次", ""),
                "操作时间": registration.get("完成时间", ""),
                "操作人": "",
                "变更前值": "",
                "变更后值": "待补写",
                "审计摘要": "采集审计待补写，可经 POST /api/crawl_audit 按交付标识补齐",
                "status": "待补写",
                "pending": True,
                "abnormal": False,
                self.DELIVERY_FIELD: delivery_id,
                self.AUDIT_STATUS_FIELD: "待补写",
            })
        return rows + pending

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """审计入口登记。

        带「交付标识」时按该标识幂等补齐交付审计（值班员从审计入口补写）；
        不带时沿用基类的普通审计登记行为。
        """
        delivery_id = str(values.get("交付标识") or values.get("delivery_id") or "").strip()
        if not delivery_id:
            return super().create_entry(values)
        missing = [
            field for field in ("操作类型", "操作对象")
            if not str(values.get(field) or "").strip()
        ]
        if missing:
            return None, missing
        entry = self.ensure_delivery_audit(
            delivery_id,
            str(values.get("操作对象")),
            str(values.get("操作类型") or "export"),
            str(values.get("操作人") or "system"),
            identifier=str(values.get("记录标识") or f"AUDIT-{delivery_id}"),
        )
        return entry, []

    def domain_audit(self, operation_type: str, target: str,
                     operator: str = "system", *,
                     delivery_id: str = "",
                     identifier: str = "") -> dict:
        """记录采集操作审计日志。

        带交付标识时幂等：同一笔交付重发只保留一条有效审计，已经写过的直接
        返回原记录，不重复追加。``identifier`` 用于让审计记录标识与交付标识
        对应（恢复时便于人工核对），省略时保持原来的 AUDIT-<时间戳>。
        """
        delivery_id = str(delivery_id or "").strip()
        if delivery_id:
            existing = self.find_delivery_audit(delivery_id)
            if existing is not None:
                return existing
        from app.store import store
        rows = store.rows(self.MODULE)
        now = time.strftime("%Y-%m-%d")
        record_id = str(identifier or "").strip() or self._next_identifier(rows)
        if any(str(r.get("记录标识") or "") == record_id for r in rows):
            record_id = self._next_identifier(rows)
        entry = {
            "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
            "记录标识": record_id,
            "操作类型": operation_type,
            "操作对象": target,
            "操作时间": now,
            "操作人": operator,
            "变更前值": "",
            "变更后值": operation_type,
            "审计摘要": f"{operator} 执行了 {operation_type} 操作于 {target}",
            "status": "已记录",
            "pending": True,
            "abnormal": False,
        }
        if delivery_id:
            entry[self.DELIVERY_FIELD] = delivery_id
            entry[self.AUDIT_STATUS_FIELD] = "已确认"
        rows.append(entry)
        return entry

    def _next_identifier(self, rows: list[dict[str, Any]]) -> str:
        """生成不与现有记录冲突的审计记录标识，保持 AUDIT-<时间戳> 的旧格式。"""
        base = int(time.time())
        existing = {str(row.get("记录标识") or "") for row in rows}
        candidate = base
        while f"AUDIT-{candidate}" in existing:
            candidate += 1
        return f"AUDIT-{candidate}"

    def find_delivery_audit(self, delivery_id: str) -> dict[str, Any] | None:
        """按交付标识查找唯一一条有效审计记录。"""
        from app.store import store

        if not delivery_id:
            return None
        for row in store.rows(self.MODULE):
            if str(row.get(self.DELIVERY_FIELD) or "") == delivery_id:
                return row
        return None

    def find_effective_audit(self, delivery_id: str) -> dict[str, Any] | None:
        return self.find_delivery_audit(delivery_id)

    def ensure_delivery_audit(
        self,
        delivery_id: str,
        target: str,
        operation_type: str = "export",
        operator: str = "system",
        *,
        identifier: str = "",
    ) -> dict[str, Any]:
        """交付链路用来补齐采集审计：没有就写，已有就复用。"""
        return self.domain_audit(
            operation_type, target, operator,
            delivery_id=delivery_id, identifier=identifier)

    # 更短的别名，便于交付链路和恢复脚本调用。
    def backfill_delivery_audit(self, delivery_id: str, target: str) -> dict[str, Any]:
        return self.ensure_delivery_audit(delivery_id, target)

    def get_delivery_audit(self, delivery_id: str) -> dict[str, Any] | None:
        return self.find_delivery_audit(delivery_id)
