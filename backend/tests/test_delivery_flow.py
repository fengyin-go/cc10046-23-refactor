"""导出通道 + 采集审计交付链路测试：登记回执、发布判断、幂等恢复与两处核对。"""
import asyncio
import csv
import io
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from starlette.requests import Request

from app.routers.chains import (
    DeliveryInterrupted,
    delivery_detail,
    pending_deliveries,
    resume_delivery,
    resume_named_delivery,
    run_delivery,
)
from app.routers.resource import create_entry, export_entries
from app.schemas import EntryPayload
from app.services import service_for
from app.services.crawl_audit import CrawlAuditService
from app.services.export_channel import AUTO, MANUAL, ExportChannelService
from app.store import store


def _request(query: str = "") -> Request:
    return Request({
        "type": "http", "method": "GET", "path": "/", "headers": [],
        "query_string": query.encode(),
    })


def _service() -> ExportChannelService:
    return service_for("export_channel")


def _registrations(delivery_id: str) -> list[dict]:
    return _service().list_effective_registrations(delivery_id)


def _audits(delivery_id: str) -> list[dict]:
    return [
        row for row in store.rows("crawl_audit")
        if str(row.get("交付标识") or "") == delivery_id
    ]


def _csv_rows(module: str, query: str = "") -> list[list[str]]:
    response = export_entries(module, _request(query), keyword=None, status=None)

    async def collect() -> str:
        body = ""
        async for chunk in response.body_iterator:
            body += chunk
        return body

    body = asyncio.run(collect())
    return list(csv.reader(io.StringIO(body)))


def test_delivery_success_response_unchanged():
    result = run_delivery({
        "batch_id": "B-FLOW-OK",
        "delivery_id": "DLV-FLOW-OK",
        "config_name": "flow-cfg",
        "export_format": "csv",
        "fields": ["id", "title", "count"],
        "cleanup_module": "result_store",
    })
    assert result["chain"] == "delivery"
    steps = result["steps"]
    assert set(steps) == {"store_result", "export", "audit", "cleanup"}
    assert steps["store_result"]["批次标识"] == "B-FLOW-OK"
    assert steps["export"]["导出格式"] == "csv"
    assert steps["export"]["status"] == "已完成"
    assert steps["audit"]["操作类型"] == "export"
    assert steps["cleanup"]["任务标识"] == "CLEANUP-B-FLOW-OK"
    assert result["published"] is True


def test_manual_registration_keeps_queue_status_and_columns():
    result = create_entry("export_channel", EntryPayload(values={
        "任务标识": "M-FLOW-1",
        "来源批次": "B-FLOW-MANUAL",
        "导出格式": "csv",
        "导出状态": "排队中",
    }))
    assert result.ok is True
    assert result.message == "导出任务已登记"
    assert result.entry["status"] == "排队中"
    assert result.entry["pending"] is True

    rows = _csv_rows("export_channel")
    assert rows[0] == ["﻿id", *ExportChannelService.LIST_FIELDS, "status"]
    manual = [row for row in rows if row[1] == "M-FLOW-1"]
    assert len(manual) == 1
    # 手工登记的单元格原样保留，没有把回执字段塞进 CSV。
    assert manual[0][2:4] == ["B-FLOW-MANUAL", "csv"]
    assert len(manual[0]) == len(ExportChannelService.LIST_FIELDS) + 2


def test_same_delivery_id_keeps_one_registration_and_one_audit():
    first = run_delivery({"batch_id": "B-FLOW-IDEM", "delivery_id": "DLV-FLOW-IDEM"})
    second = run_delivery({"batch_id": "B-FLOW-IDEM", "delivery_id": "DLV-FLOW-IDEM"})

    assert len(_registrations("DLV-FLOW-IDEM")) == 1
    assert len(_audits("DLV-FLOW-IDEM")) == 1
    assert first["steps"]["audit"]["记录标识"] == second["steps"]["audit"]["记录标识"]


def test_other_exports_of_same_batch_are_not_merged():
    manual = create_entry("export_channel", EntryPayload(values={
        "任务标识": "M-FLOW-SAME-BATCH",
        "来源批次": "B-FLOW-SHARED",
        "导出格式": "csv",
        "导出状态": "排队中",
    }))
    assert manual.ok

    run_delivery({"batch_id": "B-FLOW-SHARED", "delivery_id": "DLV-FLOW-A"})
    run_delivery({"batch_id": "B-FLOW-SHARED", "delivery_id": "DLV-FLOW-B"})

    assert len(_registrations("DLV-FLOW-A")) == 1
    assert len(_registrations("DLV-FLOW-B")) == 1
    # 手工任务仍按原状态独立查询，批次相同也不会被合并进自动登记。
    rows, total = _service().list_entries(filters={"任务标识": "M-FLOW-SAME-BATCH"})
    assert total == 1
    assert rows[0]["status"] == "排队中"
    assert rows[0]["登记方式"] == MANUAL

    batch_rows = [row for row in store.rows("export_channel")
                  if row.get("来源批次") == "B-FLOW-SHARED"]
    assert len(batch_rows) == 3


def test_interrupt_then_resume_twice_reconciles_both_entries():
    # 同批原有手工任务保持排队。
    manual = create_entry("export_channel", EntryPayload(values={
        "任务标识": "M-FLOW-INT",
        "来源批次": "B-FLOW-INT",
        "导出格式": "csv",
        "导出状态": "排队中",
    }))
    assert manual.ok

    # 甲：自动登记写入后、写审计前中断。
    interrupted = run_delivery({
        "batch_id": "B-FLOW-INT",
        "delivery_id": "DLV-FLOW-INT",
        "interrupt_after": "export",
    })
    assert interrupted["interrupted"] is True
    assert interrupted["receipt"]["审计待补"] is True
    assert interrupted["receipt"]["审计补写入口"] == "/api/crawl_audit"
    receipt = interrupted["receipt"]
    assert receipt["已对外公布"] is True  # 登记已对外公布，可从旧台账看到

    # 旧台账入口能看到已写登记，且能定位补写审计的位置。
    ledger, total = _service().list_entries(filters={"交付标识": "DLV-FLOW-INT"})
    assert total == 1
    assert ledger[0]["任务标识"] == "EXPORT-DLV-FLOW-INT"
    assert ledger[0]["登记方式"] == AUTO
    assert ledger[0]["审计状态"] == "待补写"

    # 审计入口能看到同一条交付的待补写占位，交付端留下待处理凭据。
    assert _audits("DLV-FLOW-INT") == []
    pending_audits, pending_total = service_for("crawl_audit").list_entries(
        filters={"交付标识": "DLV-FLOW-INT"})
    assert pending_total == 1
    assert pending_audits[0]["status"] == "待补写"
    assert pending_audits[0]["id"] == "pending:DLV-FLOW-INT"
    pending = pending_deliveries()
    assert any(item["交付标识"] == "DLV-FLOW-INT" for item in pending["items"])

    # 用同一标识恢复两次。
    first = resume_delivery({"delivery_id": "DLV-FLOW-INT", "batch_id": "B-FLOW-INT"})
    second = resume_delivery({"delivery_id": "DLV-FLOW-INT", "batch_id": "B-FLOW-INT"})
    assert first["resumed"] is True and second["resumed"] is True

    assert len(_registrations("DLV-FLOW-INT")) == 1
    assert len(_audits("DLV-FLOW-INT")) == 1

    detail = delivery_detail("DLV-FLOW-INT")
    assert detail["published"] is True
    assert detail["audit_pending"] is False
    assert detail["registration"]["任务标识"] == detail["receipt"]["任务标识"]
    # 审计对象沿用交付链路旧口径（来源批次），交付标识用于关联核对。
    assert detail["audit"]["操作对象"] == "B-FLOW-INT"
    assert detail["audit"]["交付标识"] == "DLV-FLOW-INT"
    assert detail["audit"]["记录标识"] == f"AUDIT-DLV-FLOW-INT"

    # 两边核对：旧台账行与审计记录指向同一交付、同一登记明细。
    ledger, total = _service().list_entries(filters={"交付标识": "DLV-FLOW-INT"})
    assert total == 1
    assert ledger[0]["审计状态"] == "已确认"
    audit_rows, audit_total = service_for("crawl_audit").list_entries(
        filters={"交付标识": "DLV-FLOW-INT"})
    assert audit_total == 1
    assert audit_rows[0]["交付标识"] == ledger[0]["交付标识"]
    assert audit_rows[0]["status"] == "已记录"

    # 同批手工任务没有被这次恢复动到。
    manual_rows, manual_total = _service().list_entries(
        filters={"任务标识": "M-FLOW-INT"})
    assert manual_total == 1
    assert manual_rows[0]["status"] == "排队中"
    assert manual_rows[0]["pending"] is True

    # 两处独立核对：任务台账 CSV 与采集审计 CSV。
    ledger_csv = _csv_rows("export_channel")
    assert sum(1 for row in ledger_csv if row[1] == "EXPORT-DLV-FLOW-INT") == 1
    assert any(row[1] == "M-FLOW-INT" and row[9] == "排队中" for row in ledger_csv)
    audit_csv = _csv_rows("crawl_audit", "交付标识=DLV-FLOW-INT")
    # 审计 CSV 只有正式记录一行（列仍是原来的 8 列 + status），占位不落盘。
    assert len(audit_csv) == 2
    assert len(audit_csv[0]) == len(CrawlAuditService.LIST_FIELDS) + 2
    assert audit_csv[1][3] == "B-FLOW-INT"


def test_resume_after_hard_interrupt_is_idempotent():
    try:
        run_delivery({
            "batch_id": "B-FLOW-HARD",
            "delivery_id": "DLV-FLOW-HARD",
            "interrupt_after": "export",
            "hard_interrupt": True,
        })
    except DeliveryInterrupted as exc:
        assert exc.receipt["审计待补"] is True
    else:  # pragma: no cover - 中断开关失效时才走到这里
        raise AssertionError("应当中断")

    first = resume_named_delivery("DLV-FLOW-HARD", {"batch_id": "B-FLOW-HARD"})
    second = resume_named_delivery("DLV-FLOW-HARD", {"batch_id": "B-FLOW-HARD"})
    assert first["resumed"] is True and second["resumed"] is True
    assert len(_registrations("DLV-FLOW-HARD")) == 1
    assert len(_audits("DLV-FLOW-HARD")) == 1


def test_resume_unknown_delivery_reports_missing():
    result = resume_delivery({"delivery_id": "DLV-FLOW-MISSING"})
    assert result["resumed"] is False
    assert result["ok"] is False


def test_publish_decision_is_independent_of_registration():
    service = _service()
    entry = {"status": "已完成", "导出状态": "已完成", "登记方式": AUTO}
    assert service.publish_decision(entry) is True
    assert service.publish_decision(entry, audit_confirmed=False) is False
    assert service.publish_decision(entry, audit_confirmed=True) is True

    queued = {"status": "排队中", "导出状态": "排队中", "登记方式": MANUAL}
    assert service.publish_decision(queued) is False


def test_publish_mode_waits_for_audit_confirmation():
    # 选择「审计确认后」发布：交付正常完成但审计确认后才对外公布。
    result = run_delivery({
        "batch_id": "B-FLOW-WAIT",
        "delivery_id": "DLV-FLOW-WAIT",
        "publish_mode": "审计确认后",
    })
    assert result["published"] is True
    assert result["receipt"]["发布策略"] == "审计确认后"

    run_delivery({
        "batch_id": "B-FLOW-WAIT-INT",
        "delivery_id": "DLV-FLOW-WAIT-INT",
        "publish_mode": "审计确认后",
        "interrupt_after": "export",
    })
    detail = delivery_detail("DLV-FLOW-WAIT-INT")
    assert detail["published"] is False
    assert detail["audit_pending"] is True

    # 值班员从审计入口补写后，旧台账与交付入口同时变为已发布。
    CrawlAuditService().create_entry({
        "交付标识": "DLV-FLOW-WAIT-INT",
        "操作类型": "export",
        "操作对象": "B-FLOW-WAIT-INT",
    })
    detail = delivery_detail("DLV-FLOW-WAIT-INT")
    assert detail["published"] is True
    assert detail["audit_pending"] is False
    ledger, total = _service().list_entries(
        filters={"交付标识": "DLV-FLOW-WAIT-INT"})
    assert total == 1
    assert ledger[0]["审计状态"] == "已确认"
    assert ledger[0]["已对外公布"] is True
    # 审计侧占位消失，只剩一条正式审计。
    audits, audit_total = service_for("crawl_audit").list_entries(
        filters={"交付标识": "DLV-FLOW-WAIT-INT"})
    assert audit_total == 1
    assert audits[0]["status"] == "已记录"


def test_receipt_points_to_backfill_entry_and_matches_audit():
    run_delivery({"batch_id": "B-FLOW-RECEIPT", "delivery_id": "DLV-FLOW-RECEIPT"})
    receipt = _service().get_delivery_receipt("DLV-FLOW-RECEIPT")
    assert receipt is not None
    assert receipt["审计状态"] == "已确认"
    assert receipt["审计待补"] is False
    assert receipt["审计记录标识"]
    audit = CrawlAuditService().find_delivery_audit("DLV-FLOW-RECEIPT")
    assert audit is not None
    assert audit["记录标识"] == receipt["审计记录标识"]


if __name__ == "__main__":
    for name, func in sorted(globals().items()):
        if name.startswith("test_") and callable(func):
            func()
            print(f"ok {name}")
