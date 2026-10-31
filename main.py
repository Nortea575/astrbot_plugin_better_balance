"""AstrBot runtime entry point for Better Balance."""

from __future__ import annotations

import inspect
import sys
from collections.abc import Mapping

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, StarTools, register

from .balance import BalanceService
from .compat import QuotaCompatibility
from .configuration import TEXT_DEFAULTS
from .quota import Store
from .usage import UsageService
from .web import WebAPI


def _render_template(template: object, values: dict) -> str:
    """Format an already validated template, leaving missing values blank."""

    class BlankMissing(dict):
        def __missing__(self, key):
            return ""

    if not isinstance(template, str):
        return ""
    try:
        return template.format_map(BlankMissing(values))
    except (KeyError, ValueError):
        return template


@register(
    "astrbot_plugin_better_balance",
    "Xing",
    "每日聊天配额与模型供应商余额管理插件",
    "0.4.2",
)
class BetterBalancePlugin(Star):
    """Manage chat quotas and query provider balances without an LLM call."""

    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.store = Store(StarTools.get_data_dir(), config=config)
        self.compat = QuotaCompatibility(self.store)
        self.balance_service = BalanceService(context, self.store)
        self.usage_service = UsageService(context)
        self.web = WebAPI(
            context,
            self.store,
            compatibility_status=self.compat.status,
            balance_service=self.balance_service,
        )
        self.web.register()
        self.compat.install()
        self._scan_balance_on_loaded = not self._providers_available(context)
        self._scan_local_balance_providers()

    @staticmethod
    def _providers_available(context) -> bool:
        getter = getattr(context, "get_all_providers", None)
        if not callable(getter):
            return False
        try:
            return bool(getter())
        except Exception:
            return False

    def _scan_local_balance_providers(self) -> None:
        """Merge local provider metadata into settings without contacting providers."""
        try:
            current = self.store.get_config()
            scanned = self.balance_service.scan(current.get("balance_providers", []))
            if not isinstance(scanned, dict) or not isinstance(
                scanned.get("records"), list
            ):
                return
            if scanned["records"] == current.get("balance_providers", []):
                return
            current["balance_providers"] = scanned["records"]
            self.store.save_config(current)
        except Exception:
            logger.warning("Local balance metadata scan failed.")

    @staticmethod
    def _text_settings(config: dict) -> dict:
        value = config.get("text_settings", {})
        return value if isinstance(value, dict) else {}

    def _wake_prefix(self, event: AstrMessageEvent) -> str:
        getter = getattr(self.context, "get_config", None)
        if callable(getter):
            try:
                native = getter(event.unified_msg_origin)
            except Exception:
                native = {}
        else:
            native = getattr(self.context, "_config", {})
        prefixes = native.get("wake_prefix", []) if isinstance(native, Mapping) else []
        if isinstance(prefixes, str):
            return prefixes
        if isinstance(prefixes, (list, tuple)):
            return next((item for item in prefixes if isinstance(item, str)), "")
        return ""

    def _help_text(self, config: dict, event: AstrMessageEvent) -> str:
        prefix = self._wake_prefix(event)
        commands = "\n".join(
            (
                f"{prefix}配额 — 查看当前会话的配额",
                f"{prefix}余额 所有 — 查询所有供应商余额",
                f"{prefix}余额 当前 — 查询当前会话绑定的余额",
                f"{prefix}余额 <别名> — 查询指定供应商余额",
                f"{prefix}余额 用量 1天|3天|1周 — 查询全局 Token 用量",
                f"{prefix}帮助 — 显示本插件命令",
            )
        )
        template = self._text_settings(config).get(
            "help_reply", TEXT_DEFAULTS["help_reply"]
        )
        return _render_template(template, {"命令列表": commands, "前缀": prefix})

    def _render_balance_results(
        self, results: list, text_settings: dict, *, all_mode: bool
    ) -> str:
        if not results:
            return str(
                text_settings.get("balance_empty", TEXT_DEFAULTS["balance_empty"])
            )
        lines = []
        for result in results:
            if not isinstance(result, dict):
                continue
            alias = str(result.get("alias", ""))
            if not result.get("ok"):
                key = "balance_all_failure" if all_mode else "balance_failure"
                template = text_settings.get(key, TEXT_DEFAULTS[key])
                error = result.get("error") or result.get("error_code") or "未知错误"
                lines.append(
                    _render_template(template, {"别名": alias, "错误": str(error)})
                )
                continue
            kind = str(result.get("kind", "custom"))
            try:
                template_key = self.balance_service.template_mapping(kind)
            except Exception:
                template_key = "balance_custom"
            template = text_settings.get(
                template_key,
                TEXT_DEFAULTS.get(template_key, TEXT_DEFAULTS["balance_custom"]),
            )
            rows = result.get("rows")
            if not isinstance(rows, list) or not rows:
                rows = [{}]
            for row in rows:
                values = dict(row) if isinstance(row, dict) else {}
                values["别名"] = alias
                lines.append(_render_template(template, values))
        return "\n".join(lines) or str(
            text_settings.get("balance_empty", TEXT_DEFAULTS["balance_empty"])
        )

    @filter.event_message_type(filter.EventMessageType.ALL, priority=sys.maxsize)
    async def quota_preflight(self, event: AstrMessageEvent) -> None:
        """Check quota before chat plugins inspect media or call their providers."""
        self.compat.install()
        await self.compat.preflight_event(event)

    @filter.on_llm_request()
    async def reserve_llm_turn(self, event: AstrMessageEvent, _request: object) -> None:
        """Reserve standard and manually dispatched proactive LLM requests."""
        self.compat.install()
        if self.compat.reserve_event(event) is None:
            await self.compat.deny_event(event)

    @filter.on_astrbot_loaded()
    async def install_runtime_adapters(self) -> None:
        """Install adapters and retry the local scan once providers are ready."""
        self.compat.install()
        if self._scan_balance_on_loaded:
            self._scan_local_balance_providers()
            self._scan_balance_on_loaded = False

    @filter.command("配额", priority=sys.maxsize)
    async def show_quota(self, event: AstrMessageEvent):
        """Show the snapshot for the current private user or group."""
        scope = self.compat.scope_from_event(event)
        if scope is None:
            event.stop_event()
            yield event.plain_result("无法识别当前平台与会话范围。")
            return

        snapshot = self.store.snapshot(scope)
        config = self.store.get_config()
        if snapshot["mode"] == "blacklist":
            key = "blacklist_reply"
        elif scope.kind == "group" and snapshot["mode"] == "unlimited":
            key = "group_unlimited_reply"
        elif scope.kind == "group":
            key = "group_quota_reply"
        elif snapshot["mode"] == "unlimited":
            key = "unlimited_reply"
        else:
            key = "quota_reply"
        template = self._text_settings(config).get(key, TEXT_DEFAULTS[key])
        summary = _render_template(
            template, {"used": snapshot["used"], "total": snapshot["total"]}
        )
        event.stop_event()
        yield event.plain_result(summary)

    @filter.command("余额", priority=sys.maxsize)
    async def show_balance(
        self,
        event: AstrMessageEvent,
        target: str = "当前",
        period: str = "",
    ):
        """Query all, current, or named provider balances, or global Token totals."""
        event.stop_event()
        config = self.store.get_config()
        text_settings = self._text_settings(config)
        target = target.strip()

        if target == "用量":
            ranges = {"1天": (1, "1天"), "3天": (3, "3天"), "1周": (7, "1周")}
            selected = ranges.get(period.strip())
            if selected is None:
                template = text_settings.get(
                    "balance_usage_hint", TEXT_DEFAULTS["balance_usage_hint"]
                )
                summary = _render_template(template, {"前缀": self._wake_prefix(event)})
            else:
                token_total = await self.usage_service.get_total_tokens(selected[0])
                if token_total is None:
                    summary = str(
                        text_settings.get(
                            "usage_failure", TEXT_DEFAULTS["usage_failure"]
                        )
                    )
                else:
                    template = text_settings.get(
                        "usage_reply", TEXT_DEFAULTS["usage_reply"]
                    )
                    summary = _render_template(
                        template,
                        {"时间": selected[1], "token": f"{token_total:,}"},
                    )
            yield event.plain_result(summary)
            return

        records = config.get("balance_providers", [])
        if not isinstance(records, list):
            records = []
        if target == "所有":
            if not records:
                yield event.plain_result(
                    str(
                        text_settings.get(
                            "balance_empty", TEXT_DEFAULTS["balance_empty"]
                        )
                    )
                )
                return
            results = await self.balance_service.query_all()
            summary = self._render_balance_results(
                results, text_settings, all_mode=True
            )
            yield event.plain_result(summary)
            return

        if target == "当前":
            try:
                provider_id = str(
                    await self.context.get_current_chat_provider_id(
                        event.unified_msg_origin
                    )
                    or ""
                )
            except Exception:
                provider_id = ""
            record = next(
                (
                    item
                    for item in records
                    if isinstance(item, dict)
                    and provider_id
                    and provider_id in item.get("provider_ids", [])
                ),
                None,
            )
            if record is None:
                yield event.plain_result(
                    str(
                        text_settings.get(
                            "balance_unbound", TEXT_DEFAULTS["balance_unbound"]
                        )
                    )
                )
                return
        else:
            record = next(
                (
                    item
                    for item in records
                    if isinstance(item, dict) and item.get("alias") == target
                ),
                None,
            )
            if record is None:
                template = text_settings.get(
                    "balance_not_found", TEXT_DEFAULTS["balance_not_found"]
                )
                yield event.plain_result(_render_template(template, {"别名": target}))
                return

        result = await self.balance_service.query_record(record)
        summary = self._render_balance_results([result], text_settings, all_mode=False)
        yield event.plain_result(summary)

    @filter.command("帮助", priority=sys.maxsize)
    async def show_help(self, event: AstrMessageEvent):
        """Show only this plugin's quota, balance, usage, and help commands."""
        event.stop_event()
        yield event.plain_result(self._help_text(self.store.get_config(), event))

    async def terminate(self) -> None:
        """Restore runtime wrappers and close the plugin's local resources."""
        self.compat.close()
        close_web = getattr(self.web, "close", None)
        if callable(close_web):
            result = close_web()
            if inspect.isawaitable(result):
                await result
        close_balance = getattr(self.balance_service, "close", None)
        if callable(close_balance):
            result = close_balance()
            if inspect.isawaitable(result):
                await result
        self.store.close()
