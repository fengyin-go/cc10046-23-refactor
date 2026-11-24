"""跨模块业务链路：多条业务链把多个领域服务串成完整流程。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from app.services.crawler_config import CrawlerConfigService
from app.services.crawl_schedule import CrawlScheduleService
from app.services.batch_tracker import BatchTrackerService
from app.services.result_store import ResultStoreService
from app.services.parser_rule import ParserRuleService
from app.services.field_mapper import FieldMapperService
from app.services.html_cleaner import HtmlCleanerService
from app.services.dedup_engine import DedupEngineService
from app.services.proxy_pool import ProxyPoolService
from app.services.rate_limiter import RateLimiterService
from app.services.error_retry import ErrorRetryService
from app.services.error_logger import ErrorLoggerService
from app.services.cookie_mgr import CookieMgrService
from app.services.source_auth import SourceAuthService
from app.services.session_manager import SessionManagerService
from app.services.export_channel import ExportChannelService
from app.services.crawl_audit import CrawlAuditService
from app.services.data_cleanup import DataCleanupService

router = APIRouter(prefix="/api/chains", tags=["业务链路"])

_crawler = CrawlerConfigService()
_schedule = CrawlScheduleService()
_batch = BatchTrackerService()
_store = ResultStoreService()
_parse = ParserRuleService()
_mapper = FieldMapperService()
_cleaner = HtmlCleanerService()
_dedup = DedupEngineService()
_proxy = ProxyPoolService()
_limiter = RateLimiterService()
_retry = ErrorRetryService()
_logger = ErrorLoggerService()
_cookie = CookieMgrService()
_auth = SourceAuthService()
_session = SessionManagerService()
_export = ExportChannelService()
_audit = CrawlAuditService()
_cleanup = DataCleanupService()


@router.post("/crawl-pipeline")
def run_crawl_pipeline(payload: dict[str, Any]) -> dict[str, Any]:
    """采集流水线：配置校验 → 生成调度 → 批次追踪 → 结果存储。"""
    config_id = payload.get("config_id", 1)
    config_name = payload.get("config_name", "default")
    period = payload.get("period", "daily")
    batch_label = payload.get("batch_label", "BATCH-1")
    row_count = payload.get("row_count", 100)
    valid_count = payload.get("valid_count", 95)

    vresult = _crawler.domain_validate_config(config_id)
    sresult = _schedule.domain_create_schedule(config_name, period)
    bresult = _batch.domain_track_batch(batch_label, sresult["task_id"])
    storeresult = _store.domain_store_result(batch_label, config_name, row_count, valid_count)

    return {
        "chain": "crawl-pipeline",
        "steps": {
            "validate_config": vresult,
            "create_schedule": sresult,
            "track_batch": bresult,
            "store_result": storeresult,
        },
    }


@router.post("/data-process")
def run_data_process(payload: dict[str, Any]) -> dict[str, Any]:
    """数据处理：HTML解析 → 字段映射 → HTML清洗 → 去重。"""
    html = payload.get("html", "<html><body><p>测试内容</p></body></html>")
    source_data = payload.get("source_data", {"name": "test", "count": "42"})
    mapping_rules = payload.get("mapping_rules", [
        {"source": "name", "target": "title", "default": "", "transform": ""},
        {"source": "count", "target": "total", "default": "0", "transform": "int"},
    ])
    key_fields = payload.get("key_fields", ["title"])

    parse_result = _parse.domain_parse_html(html)
    map_result = _mapper.domain_map_fields(source_data, mapping_rules)
    clean_result = _cleaner.domain_clean_html(html)
    items = [map_result, {"title": "extra", "other": "value"}]
    dedup_result = _dedup.domain_dedup(items, key_fields)

    return {
        "chain": "data-process",
        "steps": {
            "parse_html": parse_result,
            "map_fields": map_result,
            "clean_html": {"original_length": clean_result["original_length"], "cleaned_length": clean_result["cleaned_length"]},
            "dedup": {"original_count": dedup_result["original_count"], "deduped_count": dedup_result["deduped_count"], "removed_count": dedup_result["removed_count"]},
        },
    }


@router.post("/reliability")
def run_reliability(payload: dict[str, Any]) -> dict[str, Any]:
    """可靠性保障：代理分配 → 限速检查 → 重试计算 → 错误记录。"""
    site = payload.get("site", "example.com")
    protocol = payload.get("protocol", "http")
    request_count = payload.get("request_count", 100)
    error_type = payload.get("error_type", "timeout")
    attempt = payload.get("attempt", 2)

    proxy_result = _proxy.domain_allocate_proxy(protocol)
    rate_result = _limiter.domain_check_rate(site, request_count)
    retry_result = _retry.domain_retry_calc(error_type, attempt)
    log_result = _logger.domain_log_error(
        "ERR-CHECK", f"可靠性检查 {site}", site)

    return {
        "chain": "reliability",
        "steps": {
            "allocate_proxy": proxy_result,
            "check_rate": rate_result,
            "retry_calc": retry_result,
            "log_error": {"error_code": log_result["错误码"], "error_message": log_result["错误信息"]},
        },
    }


@router.post("/auth")
def run_auth(payload: dict[str, Any]) -> dict[str, Any]:
    """认证链路：Cookie获取 → 来源认证 → 会话创建。"""
    site = payload.get("site", "example.com")
    auth_type = payload.get("auth_type", "token")
    session_type = payload.get("session_type", "http")

    cookie_result = _cookie.domain_get_cookies(site)
    auth_result = _auth.domain_authenticate(site, auth_type)
    session_id = f"{site}-session-1"
    session_result = _session.domain_create_session(session_id, site, session_type)

    return {
        "chain": "auth",
        "steps": {
            "get_cookies": cookie_result,
            "authenticate": auth_result,
            "create_session": session_result,
        },
    }


class DeliveryInterrupted(RuntimeError):
    """模拟交付在「自动登记已写入、采集审计尚未确认」之间中断。"""

    def __init__(self, receipt: dict[str, Any]) -> None:
        super().__init__("交付在自动登记后中断，采集审计待补写")
        self.receipt = receipt


_INTERRUPT_EXPORT = {"export", "registration", "register", "auto", "登记", "导出"}
_INTERRUPT_AUDIT = {"audit", "before_audit", "审计", "写审计"}


def delivery_identifier(payload: dict[str, Any], batch_id: str) -> str:
    """交付标识：调用方显式给出时用它做幂等键，否则按批次生成稳定标识。"""
    explicit = payload.get("delivery_id") or payload.get("交付标识")
    if explicit:
        return str(explicit).strip()
    return f"DLV-{batch_id}"


def interrupt_stage(payload: dict[str, Any]) -> str | None:
    """读取中断注入参数（兼容多种命名），返回中断点或 None。"""
    raw: Any = None
    for key in ("interrupt_after", "fail_after", "crash_after",
                "simulate_interrupt", "simulate_failure", "中断点"):
        if payload.get(key) not in (None, "", False):
            raw = payload[key]
            break
    if raw is None:
        return None
    if raw is True:
        return "export"
    text = str(raw).strip().lower()
    if text in _INTERRUPT_EXPORT:
        return "export"
    if text in _INTERRUPT_AUDIT:
        return "audit"
    return None


@router.post("/delivery")
def run_delivery(payload: dict[str, Any]) -> dict[str, Any]:
    """交付链路：结果存储 → 登记导出（回执）→ 审计 → 发布判断 → 数据清理。

    自动登记与审计都以交付标识幂等：同一标识重发/恢复只保留一条有效自动
    登记和一条审计。``interrupt_after`` 可把流程停在登记与审计之间，用来
    验证旧台账、审计入口和交付端凭据能否对上同一笔交付。
    """
    batch_id = payload.get("batch_id", "BATCH-DELIVERY-1")
    config_name = payload.get("config_name", "delivery")
    export_format = payload.get("export_format", "csv")
    fields = payload.get("fields", ["id", "title", "count"])
    cleanup_module = payload.get("cleanup_module", "result_store")
    delivery_id = delivery_identifier(payload, batch_id)
    publish_mode = _export.normalize_publish_mode(
        payload.get("publish_mode") or payload.get("publish_policy")
        or payload.get("release_mode"))

    store_result = _store.domain_store_result(batch_id, config_name, 200, 180)
    # 自动登记：登记得回执凭据；重复交付复用同一条登记，不合并同批其他任务。
    registration = _export.register_delivery(
        batch_id, export_format, fields, delivery_id=delivery_id,
        publish_mode=publish_mode)
    receipt = _export.build_registration_receipt(registration)

    stage = interrupt_stage(payload)
    if stage is not None:
        # 自动登记已写入、采集审计尚未确认：交付端留下待处理凭据。
        pending_receipt = _export.build_registration_receipt(registration)
        if payload.get("hard_interrupt") or payload.get("raise_on_interrupt"):
            raise DeliveryInterrupted(pending_receipt)
        return {
            "chain": "delivery",
            "ok": False,
            "interrupted": True,
            "interrupt_stage": stage,
            "message": "交付在自动登记后中断，采集审计待补写，可用同一交付标识恢复",
            "delivery_id": delivery_id,
            "batch_id": batch_id,
            "receipt": pending_receipt,
            "steps": {
                "store_result": store_result,
                "export": registration,
                "audit": None,
                "cleanup": None,
            },
            "resume": {
                "endpoint": "POST /api/chains/delivery/resume",
                "delivery_id": delivery_id,
                "batch_id": batch_id,
                "payload": {
                    "delivery_id": delivery_id,
                    "batch_id": batch_id,
                    "config_name": config_name,
                    "export_format": export_format,
                    "fields": fields,
                    "cleanup_module": cleanup_module,
                    "publish_mode": publish_mode,
                },
            },
        }

    # 交付链路消费任务登记数据回执补齐采集审计：没有就写，已有就复用，
    # 审计记录标识与交付标识一一对应，恢复后两边能按同一笔交付核对。
    audit_result = _audit.ensure_delivery_audit(
        receipt["交付标识"] or delivery_id,
        receipt["审计操作对象"],
        receipt["审计操作类型"],
        "system",
        identifier=receipt["审计记录标识"],
    )
    _export.attach_audit(registration, audit_result)
    # 独立的发布判断：发布策略决定是否已经对外公布，审计确认是外部信号。
    published = _export.publish_decision(registration, audit_confirmed=True)
    registration[_export.PUBLISHED_FIELD] = published
    receipt = _export.build_registration_receipt(registration, audit_result)
    cleanup_result = _cleanup.domain_cleanup(f"CLEANUP-{batch_id}", cleanup_module, 30)

    return {
        "chain": "delivery",
        "steps": {
            "store_result": store_result,
            "export": registration,
            "audit": audit_result,
            "cleanup": cleanup_result,
        },
        "delivery_id": delivery_id,
        "receipt": receipt,
        "published": published,
    }


def _resume_payload(delivery_id: str, payload: dict[str, Any] | None) -> dict[str, Any]:
    """恢复时用登记行上的信息补齐批次参数，避免默认值生成另一笔批次。"""
    merged = dict(payload or {})
    merged["delivery_id"] = delivery_id
    for key in ("interrupt_after", "fail_after", "crash_after",
                "simulate_interrupt", "simulate_failure", "中断点",
                "hard_interrupt", "raise_on_interrupt"):
        merged.pop(key, None)
    registration = _export.find_registration(delivery_id)
    if registration is not None:
        merged.setdefault("batch_id", registration.get("来源批次") or batch_id_for(delivery_id))
        merged.setdefault("export_format", registration.get("导出格式") or "csv")
        stored_fields = [
            field for field in str(registration.get("字段筛选") or "").split(",")
            if field
        ]
        if stored_fields:
            merged.setdefault("fields", stored_fields)
    merged.setdefault("batch_id", batch_id_for(delivery_id))
    return merged


def batch_id_for(delivery_id: str) -> str:
    return delivery_id[len("DLV-"):] if delivery_id.startswith("DLV-") else delivery_id


def _resume_delivery(delivery_id: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    registration = _export.find_registration(delivery_id)
    if registration is None:
        return {
            "chain": "delivery",
            "ok": False,
            "resumed": False,
            "message": f"交付标识 {delivery_id} 尚无自动登记，无法恢复；请先执行交付",
            "delivery_id": delivery_id,
        }
    if _audit.find_delivery_audit(delivery_id) is not None:
        # 已经补写过审计：恢复是幂等的，不再重复登记/追加审计。
        _export.reconcile([registration])
        receipt = _export.build_registration_receipt(
            registration, _audit.find_delivery_audit(delivery_id))
        return {
            "chain": "delivery",
            "ok": True,
            "resumed": True,
            "already_completed": True,
            "delivery_id": delivery_id,
            "steps": {"export": registration,
                      "audit": _audit.find_delivery_audit(delivery_id)},
            "receipt": receipt,
            "published": receipt["已对外公布"],
        }
    # 未确认审计：用登记行上的批次参数重跑交付链路；审计侧若已有占位，
    # ensure_delivery_audit 会把正式记录写成同一条，不会多出一条。
    return run_delivery(_resume_payload(delivery_id, payload))


@router.post("/delivery/resume")
def resume_delivery(payload: dict[str, Any]) -> dict[str, Any]:
    """用同一交付标识恢复中断的交付（可重复调用，结果一致）。"""
    delivery_id = str(payload.get("delivery_id") or payload.get("交付标识") or "").strip()
    if not delivery_id:
        return {"chain": "delivery", "ok": False,
                "message": "恢复交付需要提供 delivery_id"}
    result = _resume_delivery(delivery_id, payload)
    result["resumed"] = bool(result.get("ok", True))
    return result


@router.post("/deliveries/{delivery_id}/resume")
def resume_named_delivery(delivery_id: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """按路径里的交付标识恢复，无需在请求体里重复。"""
    result = _resume_delivery(delivery_id, payload)
    result["resumed"] = bool(result.get("ok", True))
    return result


@router.get("/deliveries/pending")
def pending_deliveries() -> dict[str, Any]:
    """交付端待处理凭据：已自动登记、采集审计尚未确认的交付。"""
    receipts = _export.list_pending_receipts()
    return {"items": receipts, "total": len(receipts)}


@router.get("/deliveries/{delivery_id}")
def delivery_detail(delivery_id: str) -> dict[str, Any]:
    """按交付标识核对登记明细、交付审计和待补写入口。"""
    registration = _export.find_registration(delivery_id)
    audit = _audit.find_delivery_audit(delivery_id)
    if registration is not None:
        # 审计可能从审计入口补写，核对前先让登记行和审计侧对齐。
        _export.reconcile([registration])
    if registration is None and audit is None:
        return {
            "delivery_id": delivery_id,
            "found": False,
            "message": f"交付标识 {delivery_id} 没有自动登记或审计记录",
        }
    receipt = (_export.build_registration_receipt(registration, audit)
               if registration is not None else None)
    return {
        "delivery_id": delivery_id,
        "found": True,
        "registration": registration,
        "audit": audit,
        "receipt": receipt,
        "published": bool(receipt and receipt["已对外公布"]),
        "audit_pending": bool(receipt and receipt["审计待补"]),
        "audit_backfill": (receipt or {}).get("审计补写入口", ""),
    }