"""Dashboard API handlers for the quota management view."""

import asyncio
from collections.abc import Mapping
from urllib.parse import urlsplit, urlunsplit

from astrbot.api.web import error_response, json_response, request

from .target_names import TargetNameResolver

PLUGIN_NAME = "astrbot_plugin_better_balance"


def _public_api_base(value: object) -> str:
    """Return an API base without embedded userinfo, query, or fragment data."""
    if not isinstance(value, str) or not value:
        return ""
    try:
        parts = urlsplit(value)
        hostname = parts.hostname
        if not parts.scheme or not hostname:
            return ""
        host = f"[{hostname}]" if ":" in hostname else hostname
        if parts.port is not None:
            host = f"{host}:{parts.port}"
        return urlunsplit((parts.scheme, host, parts.path, "", ""))
    except ValueError:
        return ""


class WebAPI:
    """Register the authenticated AstrBot plugin-page API.

    Args:
        context: AstrBot context used to register plugin web routes.
        store: Shared quota configuration and usage store.
        compatibility_status: Optional zero-argument callback returning a status dict.
    """

    TARGET_NAME_TIMEOUT_SECONDS = 3.0

    def __init__(
        self,
        context,
        store,
        compatibility_status=None,
        balance_service=None,
        music_integration=None,
    ):
        self.context = context
        self.store = store
        self.compatibility_status = compatibility_status
        self.balance_service = balance_service
        self.music_integration = music_integration
        self._active = False
        self._registered = False
        self._target_names = TargetNameResolver(context)
        self._target_name_tasks: set[asyncio.Task] = set()

    def register(self):
        """Register the view API routes with AstrBot."""
        self._active = True
        if self._registered:
            return
        self.context.register_web_api(
            f"/{PLUGIN_NAME}/config",
            self.get_config,
            ["GET"],
            "Read quota management configuration",
        )
        self.context.register_web_api(
            f"/{PLUGIN_NAME}/config",
            self.save_config,
            ["POST"],
            "Save quota management configuration",
        )
        self.context.register_web_api(
            f"/{PLUGIN_NAME}/usage",
            self.get_usage,
            ["GET"],
            "Read quota usage",
        )
        self.context.register_web_api(
            f"/{PLUGIN_NAME}/music/status",
            self.get_music_status,
            ["GET"],
            "Read Better Music integration status",
        )
        self.context.register_web_api(
            f"/{PLUGIN_NAME}/music/usage",
            self.get_music_usage,
            ["GET"],
            "Read independent music quota usage",
        )
        self.context.register_web_api(
            f"/{PLUGIN_NAME}/balance/catalog",
            self.get_balance_catalog,
            ["GET"],
            "Read supported balance adapters and provider metadata",
        )
        self.context.register_web_api(
            f"/{PLUGIN_NAME}/balance/scan",
            self.scan_balance_providers,
            ["POST"],
            "Scan local provider metadata for balance records",
        )
        self.context.register_web_api(
            f"/{PLUGIN_NAME}/balance/query",
            self.query_balance,
            ["POST"],
            "Query configured provider balances",
        )
        self._registered = True

    def close(self):
        """Deactivate this handler instance when the plugin unloads.

        AstrBot currently exposes route registration but no public unregister
        method. A reload registers new handlers at the same plugin-owned routes.
        """
        self._active = False
        for task in tuple(self._target_name_tasks):
            task.cancel()
        self._target_names.clear()

    async def get_config(self):
        """Return the saved quota configuration."""
        if not self._active:
            return error_response("插件页面 API 已停用", status_code=503)
        try:
            return json_response({"config": self.store.get_config()})
        except Exception:
            return error_response("读取配额配置失败", status_code=500)

    async def save_config(self):
        """Validate and save a complete quota configuration."""
        if not self._active:
            return error_response("插件页面 API 已停用", status_code=503)
        payload = await request.json(default=None)
        if not isinstance(payload, dict):
            return error_response("请求内容必须是 JSON 对象", status_code=400)
        config = payload.get("config", payload)
        if not isinstance(config, dict):
            return error_response("配额配置必须是 JSON 对象", status_code=400)
        try:
            normalized = self.store.save_config(config)
            if self.music_integration is not None:
                configure = getattr(self.music_integration, "configure", None)
                if callable(configure):
                    configure()
        except ValueError as exc:
            return error_response(str(exc), status_code=400)
        except Exception:
            return error_response("保存配额配置失败", status_code=500)
        return json_response({"saved": True, "config": normalized})

    async def get_usage(self):
        """Return all currently tracked quota usage rows."""
        if not self._active:
            return error_response("插件页面 API 已停用", status_code=503)
        try:
            usage = self.store.list_usage()
            compatibility = (
                self.compatibility_status()
                if callable(self.compatibility_status)
                else {}
            )
            if not isinstance(compatibility, dict):
                compatibility = {}
        except Exception:
            return error_response("读取配额用量失败", status_code=500)

        usage_rows = [dict(row) for row in usage if isinstance(row, Mapping)]
        if not await self._add_display_names(usage_rows):
            return error_response("插件页面 API 已停用", status_code=503)
        return json_response({"usage": usage_rows, "compatibility": compatibility})

    async def _add_display_names(self, usage_rows: list[dict]) -> bool:
        for row in usage_rows:
            row["display_name"] = ""
        if usage_rows:
            lookup_task = asyncio.create_task(
                self._target_names.resolve_rows(usage_rows)
            )
            self._target_name_tasks.add(lookup_task)
            lookup_task.add_done_callback(self._target_name_tasks.discard)
            try:
                names = await asyncio.wait_for(
                    lookup_task, timeout=self.TARGET_NAME_TIMEOUT_SECONDS
                )
            except asyncio.CancelledError:
                if self._active:
                    raise
                return False
            except asyncio.TimeoutError:
                names = self._target_names.cached_names(usage_rows)
            except Exception:
                names = self._target_names.cached_names(usage_rows)
            for row, name in zip(usage_rows, names):
                if isinstance(name, str):
                    row["display_name"] = name
        return self._active

    def _music_status(self) -> dict:
        service = self.music_integration
        status = service.status() if service is not None else None
        if not isinstance(status, dict):
            return {
                "installed": False,
                "compatible": False,
                "connected": False,
                "enabled": False,
                "independent_quota": False,
                "api_version": None,
                "message": "音乐额度联动状态不可用。",
            }
        return status

    async def get_music_status(self):
        """Return only the fixed integration state needed by the UI."""
        if not self._active:
            return error_response("插件页面 API 已停用", status_code=503)
        return json_response({"status": self._music_status()})

    async def get_music_usage(self):
        """Return independent music quota rows with the current connection state."""
        if not self._active:
            return error_response("插件页面 API 已停用", status_code=503)
        try:
            usage = self.store.list_music_usage()
        except Exception:
            return error_response("读取点歌用量失败", status_code=500)
        usage_rows = [dict(row) for row in usage if isinstance(row, Mapping)]
        if not await self._add_display_names(usage_rows):
            return error_response("插件页面 API 已停用", status_code=503)
        return json_response({"usage": usage_rows, "status": self._music_status()})

    def _require_balance_service(self):
        if self.balance_service is None:
            raise RuntimeError("余额查询服务不可用")
        return self.balance_service

    async def get_balance_catalog(self):
        """Return adapter metadata and only non-secret provider fields."""
        if not self._active:
            return error_response("插件页面 API 已停用", status_code=503)
        try:
            service = self._require_balance_service()
            catalog = service.catalog()
            provider_getter = getattr(self.context, "get_all_providers", None)
            loaded_providers = provider_getter() if callable(provider_getter) else []
            providers = []
            for provider in loaded_providers or []:
                provider_config = getattr(provider, "provider_config", {})
                if not isinstance(provider_config, Mapping):
                    continue
                provider_id = provider_config.get("id")
                if not isinstance(provider_id, str) or not provider_id:
                    continue
                providers.append(
                    {
                        "id": provider_id,
                        "model": str(provider_config.get("model", "")),
                        "api_base": _public_api_base(provider_config.get("api_base")),
                    }
                )
            return json_response({"catalog": catalog, "providers": providers})
        except Exception:
            return error_response("读取余额查询类型失败", status_code=500)

    async def scan_balance_providers(self):
        """Scan local metadata and persist canonical balance records."""
        if not self._active:
            return error_response("插件页面 API 已停用", status_code=503)
        payload = await request.json(default={})
        if payload is None:
            payload = {}
        if not isinstance(payload, dict):
            return error_response("请求内容必须是 JSON 对象", status_code=400)
        requested_records = payload.get("balance_providers")
        if requested_records is None and isinstance(payload.get("config"), dict):
            requested_records = payload["config"].get("balance_providers")
        if requested_records is None:
            requested_records = self.store.get_config().get("balance_providers", [])
        if not isinstance(requested_records, list):
            return error_response("balance_providers 必须是数组", status_code=400)
        try:
            service = self._require_balance_service()
            scanned = service.scan(requested_records)
            if not isinstance(scanned, dict) or not isinstance(
                scanned.get("records"), list
            ):
                raise ValueError("余额扫描结果无效")
            canonical = self.store.get_config()
            canonical["balance_providers"] = scanned["records"]
            saved = self.store.save_config(canonical)
        except ValueError as exc:
            return error_response(str(exc), status_code=400)
        except Exception:
            return error_response("扫描余额供应商失败", status_code=500)
        return json_response(
            {
                "config": saved,
                "added": scanned.get("added", []),
                "updated": scanned.get("updated", []),
                "needs_configuration": scanned.get("needs_configuration", []),
            }
        )

    async def query_balance(self):
        """Query one configured provider or all records without exposing secrets."""
        if not self._active:
            return error_response("插件页面 API 已停用", status_code=503)
        payload = await request.json(default=None)
        if not isinstance(payload, dict):
            return error_response("请求内容必须是 JSON 对象", status_code=400)
        query_all = payload.get("all") is True
        record_id = payload.get("id")
        if query_all == (isinstance(record_id, str) and bool(record_id)):
            return error_response("请指定 id 或 all=true", status_code=400)
        try:
            service = self._require_balance_service()
            if query_all:
                results = await service.query_all()
            else:
                providers = self.store.get_config().get("balance_providers", [])
                record = next(
                    (
                        item
                        for item in providers
                        if isinstance(item, dict) and item.get("id") == record_id
                    ),
                    None,
                )
                if record is None:
                    return error_response("未找到余额供应商", status_code=404)
                results = [await service.query_record(record)]
            if not isinstance(results, list):
                return error_response("余额查询结果无效", status_code=500)
            result_fields = {
                "id",
                "alias",
                "kind",
                "ok",
                "rows",
                "error_code",
                "error",
                "queried_at",
                "cached",
            }
            safe_results = [
                {key: value for key, value in item.items() if key in result_fields}
                for item in results
                if isinstance(item, dict)
            ]
        except Exception:
            return error_response("查询余额失败", status_code=500)
        return json_response({"results": safe_results})
