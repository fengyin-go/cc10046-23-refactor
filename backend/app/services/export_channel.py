"""导出通道领域服务。

手工登记（资源列表里的「登记导出任务」）和自动交付（交付链路里的导出步骤）
共用同一套任务行构造和任务登记数据回执逻辑；自动交付额外的「是否已对外公布」
判断单独放在 :meth:`publish_decision` 里，便于交付链路和旧台账各自核对。

任务行仍然沿用原来的 8 个列表字段，CSV 的列和单元格完全不变；交付标识、
登记方式、回执状态等只作为行内扩展字段保存在 JSON 里，不进入 CSV。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService

MANUAL = "manual"
AUTO = "auto"


class ExportChannelService(BaseModuleService):
    MODULE = "export_channel"
    ENTITY = "导出任务"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["任务标识", "来源批次", "导出格式", "目标路径", "字段筛选", "导出状态", "文件大小", "完成时间"]
    REQUIRED_FIELDS = ["任务标识", "来源批次", "导出格式"]
    STATUS_ORDER = ["排队中", "导出中", "已完成", "导出失败"]
    ACTION_RULES = {"启动导出": "排队中", "取消导出": "导出中"}
    NEGATIVE_ACTIONS = []

    # 交付登记扩展字段（不进入 LIST_FIELDS，因此不会改变任务台账 CSV）。
    DELIVERY_FIELD = "交付标识"
    REGISTRATION_FIELD = "登记方式"
    PUBLISHED_FIELD = "已对外公布"
    PUBLISH_MODE_FIELD = "发布策略"
    RECEIPT_STATUS_FIELD = "回执状态"
    AUDIT_STATUS_FIELD = "审计状态"
    AUDIT_ID_FIELD = "审计记录标识"

    # 自动交付的两种发布策略：登记后立即公布（旧行为），或等审计确认后再公布。
    PUBLISH_IMMEDIATE = "登记后"
    PUBLISH_ON_AUDIT = "审计确认后"

    # 额外筛选字段只影响查询，不进入台账 CSV 的列。
    EXTRA_FILTER_FIELDS = [DELIVERY_FIELD, REGISTRATION_FIELD,
                           RECEIPT_STATUS_FIELD, AUDIT_STATUS_FIELD, PUBLISH_MODE_FIELD]
    QUERY_FIELDS = [*LIST_FIELDS, *EXTRA_FILTER_FIELDS]
    FILTER_ALIASES = {
        "delivery_id": DELIVERY_FIELD,
        "delivery": DELIVERY_FIELD,
        "registration": REGISTRATION_FIELD,
        "audit_status": AUDIT_STATUS_FIELD,
        "receipt_status": RECEIPT_STATUS_FIELD,
        "publish_mode": PUBLISH_MODE_FIELD,
    }

    # ------------------------------------------------------------------
    # 查询：旧台账入口 + 交付入口
    # ------------------------------------------------------------------
    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        """旧台账查询。

        在基类筛选之外补两件事：支持按交付标识/登记方式/回执状态筛选，并在
        返回前把审计侧已经确认的结果同步到登记行，避免两处查询出现冲突。
        """
        from app.store import store

        resolved = {
            self.FILTER_ALIASES.get(field, field): expected
            for field, expected in (filters or {}).items()
        }
        rows = list(store.rows(self.MODULE))
        for field, expected in resolved.items():
            if field == self.DELIVERY_FIELD:
                # 交付标识精确匹配，避免 DLV-1 命中 DLV-10。
                rows = [row for row in rows
                        if str(row.get(field, "")) == expected]
            elif field in self.EXTRA_FILTER_FIELDS:
                rows = [row for row in rows
                        if expected.casefold() in str(row.get(field, "")).casefold()]
            elif field in self.LIST_FIELDS:
                rows = [row for row in rows
                        if expected.casefold() in str(row.get(field, "")).casefold()]
        if keyword and self.LIST_FIELDS:
            first = self.LIST_FIELDS[0]
            rows = [row for row in rows if keyword.casefold() in str(row.get(first, "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        self._reconcile_rows(rows)
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        """单条旧台账查询也先和审计侧对齐，避免与交付入口显示冲突。"""
        entry = super().get_entry(entry_id)
        if entry is not None:
            self._reconcile_rows([entry])
        return entry

    def reconcile(self, rows: list[dict[str, Any]]) -> None:
        """公开的对齐入口：把审计侧结果同步进给定的登记行。"""
        self._reconcile_rows(rows)

    def _reconcile_rows(self, rows: list[dict[str, Any]]) -> None:
        """把审计侧已确认的交付回写到自动登记行；已对外公布的登记保持发布状态。"""
        from app.store import store

        audit_by_delivery = {
            str(row.get("交付标识") or ""): row
            for row in store.rows("crawl_audit")
            if row.get("交付标识")
        }
        for row in rows:
            if row.get(self.REGISTRATION_FIELD) != AUTO:
                continue
            delivery_id = str(row.get(self.DELIVERY_FIELD) or "")
            audit = audit_by_delivery.get(delivery_id)
            if audit is not None and row.get(self.AUDIT_STATUS_FIELD) != "已确认":
                self.attach_audit(row, audit)
            elif audit is None and row.get(self.AUDIT_STATUS_FIELD) == "已确认":
                # 审计被清理或还没写入时不虚构确认，恢复成待补状态。
                row[self.AUDIT_STATUS_FIELD] = "待补写"
                row[self.AUDIT_ID_FIELD] = ""
                row[self.RECEIPT_STATUS_FIELD] = "待补审"

    # ------------------------------------------------------------------
    # 共用的任务行与回执构造
    # ------------------------------------------------------------------
    def build_task_row(
        self,
        *,
        batch_id: str,
        export_format: str = "csv",
        fields: list[str] | None = None,
        values: dict[str, Any] | None = None,
        status: str | None = None,
        delivery_id: str = "",
        registration: str = MANUAL,
        task_id: str | None = None,
        completion_time: str | None = None,
        publish: bool | None = None,
        publish_mode: str | None = None,
    ) -> dict[str, Any]:
        """手工与自动登记共用的一条导出任务行。

        ``values`` 有值时按手工登记原样落 8 个列表字段（保持旧接口单元格不变）；
        没有值时按自动交付批次生成同样的字段。两种来源最终都经过同一段
        ``status/pending/abnormal`` 和登记回执字段的收尾逻辑。
        """
        from app.store import store

        rows = store.rows(self.MODULE)
        final_status = status or self.STATUS_ORDER[0]
        if values is not None:
            columns = {field: values.get(field) for field in self.LIST_FIELDS}
        else:
            columns = {
                "任务标识": task_id or f"EXPORT-{delivery_id or batch_id}",
                "来源批次": batch_id,
                "导出格式": export_format,
                "目标路径": f"/exports/{batch_id}.{export_format}",
                "字段筛选": ",".join(fields or []),
                "导出状态": final_status,
                "文件大小": f"{(len(batch_id) * 1024) + 512}",
                "完成时间": completion_time or time.strftime("%Y-%m-%d"),
            }

        entry: dict[str, Any] = {
            "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
        }
        entry.update(columns)
        entry["status"] = final_status
        # 与旧 create_entry / domain_export 一致：手工排队中为待处理，自动完成不待处理。
        entry["pending"] = final_status != self.STATUS_ORDER[-1]
        entry["abnormal"] = False

        entry[self.DELIVERY_FIELD] = delivery_id
        entry[self.REGISTRATION_FIELD] = registration
        # 自动交付默认沿用「登记后立即公布」；调用方也可以选择审计确认后公布。
        entry[self.PUBLISH_MODE_FIELD] = (
            (publish_mode or self.PUBLISH_IMMEDIATE)
            if registration == AUTO else ""
        )
        # 右侧先算发布判断（此时行上还没有「已对外公布」字段），再写回结果。
        entry[self.PUBLISHED_FIELD] = (
            self.publish_decision(entry) if publish is None
            else self.publish_decision(entry, audit_confirmed=publish)
        )
        entry[self.AUDIT_ID_FIELD] = ""
        if registration == AUTO:
            entry[self.AUDIT_STATUS_FIELD] = "待补写"
            entry[self.RECEIPT_STATUS_FIELD] = "待补审"
        else:
            entry[self.AUDIT_STATUS_FIELD] = "不适用"
            entry[self.RECEIPT_STATUS_FIELD] = final_status

        rows.append(entry)
        return entry

    def build_registration_receipt(
        self,
        entry: dict[str, Any],
        audit: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """根据任务行生成「任务登记数据回执」。

        回执同时给交付链路和旧台账使用：交付链路拿它按同一交付标识补齐采集
        审计，值班员拿它核对任务行、是否已对外公布、审计待补以及补写入口。
        审计的 ``操作对象`` 沿用交付链路原有的批次口径，避免改变既有审计内容。
        """
        delivery_id = str(entry.get(self.DELIVERY_FIELD) or "")
        registration = str(entry.get(self.REGISTRATION_FIELD) or MANUAL)
        is_auto = registration == AUTO
        audit_id = str((audit or {}).get("记录标识") or entry.get(self.AUDIT_ID_FIELD) or "")
        audit_confirmed = bool(audit_id) and (
            (audit or {}).get(self.AUDIT_STATUS_FIELD) in (None, "已确认")
            or (audit or {}).get("pending") is False
        )
        published = self.publish_decision(
            entry, audit_confirmed=True if audit_confirmed else None)

        if not is_auto:
            audit_status = "不适用"
            receipt_status = str(entry.get("status") or "")
            can_backfill = False
        elif audit_confirmed:
            audit_status = "已确认"
            receipt_status = "已确认"
            can_backfill = False
        else:
            audit_status = "待补写"
            receipt_status = "待补审"
            can_backfill = True

        # 审计对象沿用交付链路旧口径（来源批次），日志内容与以前一致。
        target = str(entry.get("来源批次") or "") or delivery_id
        return {
            "回执标识": f"RECEIPT-{delivery_id or entry.get('任务标识', '')}",
            "交付标识": delivery_id,
            "任务标识": entry.get("任务标识", ""),
            "来源批次": entry.get("来源批次", ""),
            "导出格式": entry.get("导出格式", ""),
            "登记方式": registration,
            "登记状态": entry.get("status", ""),
            "发布策略": entry.get(self.PUBLISH_MODE_FIELD) or (
                self.PUBLISH_IMMEDIATE if is_auto else ""),
            "已对外公布": published,
            "发布状态": "已发布" if published else "未发布",
            "是否已发布": published,
            "审计状态": audit_status,
            "审计记录标识": audit_id or (
                f"AUDIT-{delivery_id}" if delivery_id else ""),
            "审计待补": can_backfill,
            "可补写审计": can_backfill,
            "审计补写入口": "/api/crawl_audit" if can_backfill else "",
            "审计补写接口": "POST /api/crawl_audit" if can_backfill else "",
            "审计补写模块": "crawl_audit" if can_backfill else "",
            "审计操作类型": "export",
            "审计操作对象": target,
            "回执状态": receipt_status,
            "待处理": can_backfill,
            "登记明细": {
                field: entry.get(field)
                for field in self.LIST_FIELDS
            },
        }

    # 兼容更短的调用名：回执就是任务登记数据的对外凭据。
    def registration_receipt(
        self,
        entry: dict[str, Any],
        audit: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self.build_registration_receipt(entry, audit)

    # ------------------------------------------------------------------
    # 独立的发布判断
    # ------------------------------------------------------------------
    def publish_decision(
        self,
        entry: dict[str, Any],
        *,
        audit_confirmed: bool | None = None,
    ) -> bool:
        """独立判断一条登记是否已经对外公布。

        判断只看这条登记自己的状态，不读取导出通道里的其他任务：

        * 手工登记：状态走到「已完成」才算对外公布；
        * 自动登记且发布策略为「审计确认后」：必须等采集审计确认；
        * 自动登记且默认「登记后」：登记行状态为「已完成」即已对外公布，
          与原 ``domain_export`` 的返回值保持一致。

        ``audit_confirmed`` 是外部审计信号：``False`` 明确压住发布，``True``
        等价于已经确认；不传时按行上记录的发布结果判断。
        """
        if audit_confirmed is False:
            return False
        if entry.get(self.REGISTRATION_FIELD) == AUTO:
            if entry.get(self.PUBLISH_MODE_FIELD) == self.PUBLISH_ON_AUDIT:
                return audit_confirmed is True
            if audit_confirmed is True:
                return True
            if self.PUBLISHED_FIELD in entry:
                return bool(entry.get(self.PUBLISHED_FIELD))
            return (
                entry.get("status") == "已完成"
                or entry.get("导出状态") == "已完成"
            )
        # 手工任务没有自动交付的发布语义，只有状态真正走到已完成才算对外公布。
        return entry.get("status") == "已完成"

    def is_published(self, entry: dict[str, Any]) -> bool:
        return self.publish_decision(entry)

    # ------------------------------------------------------------------
    # 登记：手工（资源接口）与自动（交付链路）共用
    # ------------------------------------------------------------------
    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """手工登记导出任务，保持原有必填校验、排队状态和返回结构。"""
        missing = [
            field for field in self.REQUIRED_FIELDS
            if not str(values.get(field) or "").strip()
        ]
        if missing:
            return None, missing
        entry = self.build_task_row(
            batch_id=str(values.get("来源批次") or ""),
            export_format=str(values.get("导出格式") or "csv"),
            values=values,
            status=self.STATUS_ORDER[0],
            delivery_id="",  # 手工任务不参与自动交付标识去重，保持独立查询。
            registration=MANUAL,
            task_id=str(values.get("任务标识") or ""),
        )
        return entry, []

    def find_registration(self, delivery_id: str) -> dict[str, Any] | None:
        """按交付标识查找唯一一条自动登记，绝不按批次合并其他导出任务。"""
        from app.store import store

        if not delivery_id:
            return None
        for row in store.rows(self.MODULE):
            if (row.get(self.REGISTRATION_FIELD) == AUTO
                    and str(row.get(self.DELIVERY_FIELD) or "") == delivery_id):
                return row
        return None

    def find_effective_registration(self, delivery_id: str) -> dict[str, Any] | None:
        return self.find_registration(delivery_id)

    def list_effective_registrations(self, delivery_id: str) -> list[dict[str, Any]]:
        from app.store import store

        if not delivery_id:
            return []
        return [
            row for row in store.rows(self.MODULE)
            if row.get(self.REGISTRATION_FIELD) == AUTO
            and str(row.get(self.DELIVERY_FIELD) or "") == delivery_id
        ]

    def domain_export(self, batch_id: str, export_format: str = "csv",
                      fields: list[str] | None = None, *,
                      delivery_id: str = "",
                      task_id: str | None = None,
                      publish_mode: str | None = None) -> dict:
        """自动登记采集批次的导出任务。

        带交付标识时幂等：同一个标识再次进入（例如交付链路在写审计时中断后
        重试）直接复用已经写入的那条自动登记；不带标识时保留旧的追加行为。
        """
        delivery_id = str(delivery_id or "").strip()
        if delivery_id:
            existing = self.find_registration(delivery_id)
            if existing is not None:
                return existing
        mode = self.normalize_publish_mode(publish_mode)
        return self.build_task_row(
            batch_id=batch_id,
            export_format=export_format,
            fields=fields,
            status="已完成",
            delivery_id=delivery_id,
            registration=AUTO,
            task_id=task_id,
            publish=None if mode == self.PUBLISH_IMMEDIATE else False,
            publish_mode=mode,
        )

    def register_delivery(self, batch_id: str, export_format: str = "csv",
                          fields: list[str] | None = None, *,
                          delivery_id: str = "",
                          publish_mode: str | None = None) -> dict:
        """自动交付登记入口，语义同 :meth:`domain_export`。"""
        return self.domain_export(
            batch_id, export_format, fields,
            delivery_id=delivery_id, publish_mode=publish_mode)

    def normalize_publish_mode(self, value: Any) -> str:
        """把调用方给的中文/英文发布策略归一成两种登记行取值。"""
        text = str(value or "").strip().lower()
        if text in {"audit", "after_audit", "on_audit", "审计确认后",
                    "审计后", "等待审计", "待审计确认"}:
            return self.PUBLISH_ON_AUDIT
        return self.PUBLISH_IMMEDIATE

    def attach_audit(self, entry: dict[str, Any], audit: dict[str, Any]) -> dict[str, Any]:
        """把采集审计的确认结果回写到自动登记行，供旧台账入口核对。"""
        entry[self.AUDIT_ID_FIELD] = str(audit.get("记录标识") or "")
        entry[self.AUDIT_STATUS_FIELD] = "已确认"
        entry[self.RECEIPT_STATUS_FIELD] = "已确认"
        entry[self.PUBLISHED_FIELD] = self.publish_decision(
            entry, audit_confirmed=True)
        return entry

    def get_delivery_receipt(self, delivery_id: str) -> dict[str, Any] | None:
        """交付端凭据：登记 + 当前审计状态，恢复时可直接用于核对/补写。"""
        entry = self.find_registration(delivery_id)
        if entry is None:
            return None
        from app.services.crawl_audit import CrawlAuditService

        audit = CrawlAuditService().find_delivery_audit(delivery_id)
        return self.build_registration_receipt(entry, audit)

    def list_pending_receipts(self) -> list[dict[str, Any]]:
        """所有已登记但审计尚未确认的交付回执。"""
        from app.store import store

        candidates = [
            row for row in store.rows(self.MODULE)
            if row.get(self.REGISTRATION_FIELD) == AUTO
        ]
        # 审计可能刚从审计入口补写，先对齐再判断是否仍待处理。
        self._reconcile_rows(candidates)
        return [
            self.build_registration_receipt(row)
            for row in candidates
            if row.get(self.AUDIT_STATUS_FIELD) != "已确认"
        ]

    # 兼容更直观的名字。
    def pending_receipts(self) -> list[dict[str, Any]]:
        return self.list_pending_receipts()
