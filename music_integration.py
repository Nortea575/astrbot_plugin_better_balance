"""Bridge Better Balance reservations to Better Music's public quota API."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from astrbot.api import logger

from .configuration import TEXT_DEFAULTS
from .quota import Scope, Store

OWNER = "astrbot_plugin_better_balance"
API_VERSION = 2


def _format(template: object, values: Mapping[str, object]) -> str:
    class BlankMissing(dict):
        def __missing__(self, key):
            return ""

    if not isinstance(template, str):
        return ""
    try:
        return template.format_map(BlankMissing(values))
    except (KeyError, ValueError):
        return template


class MusicIntegration:
    """Manage one optional checker registration and its captured settlements."""

    def __init__(self, store: Store) -> None:
        self.store = store
        self._api: Any | None = None
        self._token: str | None = None
        self._installed = False
        self._api_version: int | None = None
        self._compatible = False
        self._registration_failed = False
        self._closed = False

    def on_music_quota_ready(self, api: Any) -> None:
        """Connect to a loaded Music quota API when it supports version 2."""
        if api is None:
            self.on_music_quota_unavailable(self._api)
            return
        if api is not self._api:
            self._unregister()
        self._api = api
        self._installed = True
        raw_version = getattr(api, "api_version", None)
        self._api_version = raw_version if isinstance(raw_version, int) else None
        self._compatible = (
            self._api_version == API_VERSION
            and callable(getattr(api, "register_checker", None))
            and callable(getattr(api, "unregister_checker", None))
        )
        self._registration_failed = False
        self.configure()

    def on_music_quota_unavailable(self, api: Any) -> None:
        """Disconnect only the API instance that announced its shutdown."""
        if api is not self._api:
            return
        self._unregister()
        self._api = None
        self._installed = False
        self._api_version = None
        self._compatible = False
        self._registration_failed = False

    def configure(self) -> None:
        """Apply the saved feature switch immediately after a web config save."""
        if self._closed:
            return
        try:
            enabled = bool(self.store.get_config()["music_integration"]["enabled"])
        except Exception:
            enabled = False
        if not enabled or not self._compatible or self._api is None:
            self._unregister()
            return
        if self._token is not None:
            return
        try:
            self._token = self._api.register_checker(
                OWNER,
                self.checker,
                on_commit=self.on_commit,
                on_release=self.on_release,
            )
            self._registration_failed = (
                not isinstance(self._token, str) or not self._token
            )
            if self._registration_failed:
                self._token = None
        except Exception:
            self._registration_failed = True
            logger.warning("Better Music quota checker registration failed.")

    def _unregister(self) -> None:
        token = self._token
        api = self._api
        self._token = None
        if token is None or api is None:
            return
        try:
            api.unregister_checker(token)
        except Exception:
            logger.warning("Better Music quota checker unregistration failed.")

    def status(self) -> dict:
        """Return fixed, non-sensitive integration state for the management UI."""
        try:
            enabled = bool(self.store.get_config()["music_integration"]["enabled"])
            independent = bool(
                self.store.get_config()["music_integration"]["independent_quota"]
            )
        except Exception:
            enabled = False
            independent = False
        if not self._installed:
            message = "未检测到 Better Music。"
        elif not self._compatible:
            message = "Better Music 配额接口版本不兼容。"
        elif not enabled:
            message = "音乐额度联动未启用。"
        elif self._registration_failed or self._token is None:
            message = "音乐额度联动暂未连接。"
        else:
            message = ""
        return {
            "installed": self._installed,
            "compatible": self._compatible,
            "connected": self._token is not None,
            "enabled": enabled,
            "independent_quota": independent,
            "api_version": self._api_version,
            "message": message,
        }

    async def checker(self, request: dict) -> dict:
        """Reserve the selected chat or independent music bucket atomically."""
        raw_scope = request.get("scope") if isinstance(request, Mapping) else None
        if not isinstance(raw_scope, Mapping):
            return {
                "allowed": False,
                "reason": self._text("music_unavailable", {}),
            }
        try:
            scope = Scope(
                str(raw_scope["platform_id"]),
                str(raw_scope["kind"]),
                str(raw_scope["target_id"]),
            )
            request_id = request.get("request_id")
            if not isinstance(request_id, str) or not request_id.strip():
                raise ValueError("request_id required")
            decision = self.store.reserve_music(scope, request_id)
        except Exception:
            logger.warning("Better Music quota reservation failed.")
            return {
                "allowed": False,
                "reason": self._text("music_unavailable", {}),
            }
        if decision.reservation is not None:
            return {"allowed": True, "reservation_id": decision.reservation.token}
        snapshot = decision.snapshot
        values = {
            "used": snapshot.get("used", 0),
            "total": snapshot.get("total", 0),
            "remaining": snapshot.get("remaining", 0),
            "cost": decision.cost,
            "multiplier": decision.multiplier,
        }
        if decision.reason == "blacklist":
            key = "music_blacklist"
        elif decision.reason == "shared_exhausted":
            key = "music_shared_exhausted"
        elif decision.reason == "exhausted":
            key = "music_quota_exhausted"
        else:
            key = "music_unavailable"
        return {"allowed": False, "reason": self._text(key, values)}

    def on_commit(self, request: dict) -> None:
        self._settle(request, commit=True)

    def on_release(self, request: dict) -> None:
        self._settle(request, commit=False)

    def _settle(self, request: object, *, commit: bool) -> None:
        if not isinstance(request, Mapping):
            return
        request_id = request.get("request_id")
        token = request.get("reservation_id")
        if not isinstance(request_id, str):
            return
        token = token if isinstance(token, str) else None
        try:
            ticket = self.store.find_music_reservation(request_id, token)
            if ticket is None:
                return
            if commit:
                self.store.commit(ticket)
            else:
                self.store.release(ticket)
        except Exception:
            logger.warning("Better Music quota settlement failed.")

    def _text(self, key: str, values: Mapping[str, object]) -> str:
        try:
            template = self.store.get_config()["text_settings"].get(
                key, TEXT_DEFAULTS[key]
            )
        except Exception:
            template = TEXT_DEFAULTS[key]
        return _format(template, values)

    def close(self) -> None:
        """Stop new Music admission while leaving captured ticket callbacks live."""
        if self._closed:
            return
        self._unregister()
        self._closed = True
