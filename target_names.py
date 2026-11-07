"""Best-effort QQ names for quota usage rows.

Names are resolved from the exact OneBot platform instance associated with a
quota scope. This deliberately uses the quota's raw target ID, so group names
remain correct when AstrBot's ``unique_session`` setting changes the UMO.
"""

from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from collections.abc import Mapping
from typing import Any


ScopeKey = tuple[str, str, str]


class TargetNameResolver:
    """Resolve and briefly cache names for private and group quota scopes."""

    def __init__(
        self,
        context: Any,
        *,
        clock=None,
        success_ttl: float = 300.0,
        failure_ttl: float = 30.0,
        max_cache_size: int = 2048,
        max_concurrency: int = 4,
        action_timeout: float = 1.5,
    ) -> None:
        self.context = context
        self._clock = clock or time.monotonic
        self._success_ttl = success_ttl
        self._failure_ttl = failure_ttl
        self._max_cache_size = max_cache_size
        self._max_concurrency = max_concurrency
        self._action_timeout = action_timeout
        self._cache: OrderedDict[ScopeKey, tuple[float, str]] = OrderedDict()

    @staticmethod
    def scope_key(row: Mapping[str, object]) -> ScopeKey | None:
        platform_id = row.get("platform_id")
        kind = row.get("kind")
        target_id = row.get("target_id")
        if not all(
            isinstance(value, str) and value.strip()
            for value in (platform_id, target_id)
        ):
            return None
        if kind not in {"private", "group"}:
            return None
        return platform_id.strip(), str(kind), target_id.strip()

    def _cached_name(self, key: ScopeKey) -> tuple[bool, str]:
        cached = self._cache.get(key)
        if cached is None:
            return False, ""
        expires_at, name = cached
        if expires_at <= self._clock():
            self._cache.pop(key, None)
            return False, ""
        self._cache.move_to_end(key)
        return True, name

    def cached_names(self, rows: list[Mapping[str, object]]) -> list[str]:
        """Return names already cached for rows without starting lookups."""
        names = [""] * len(rows)
        for index, row in enumerate(rows):
            key = self.scope_key(row)
            if key is None:
                continue
            found, name = self._cached_name(key)
            if found:
                names[index] = name
        return names

    def _cache_name(self, key: ScopeKey, name: str) -> None:
        ttl = self._success_ttl if name else self._failure_ttl
        self._cache[key] = (self._clock() + ttl, name)
        self._cache.move_to_end(key)
        while len(self._cache) > self._max_cache_size:
            self._cache.popitem(last=False)

    def clear(self) -> None:
        """Drop cached names when the owning plugin is unloaded."""
        self._cache.clear()

    async def resolve_rows(self, rows: list[Mapping[str, object]]) -> list[str]:
        """Return one best-effort display name per usage row, in row order."""
        names = [""] * len(rows)
        missing: dict[ScopeKey, list[int]] = {}
        for index, row in enumerate(rows):
            key = self.scope_key(row)
            if key is None:
                continue
            found, name = self._cached_name(key)
            if found:
                names[index] = name
            else:
                missing.setdefault(key, []).append(index)

        if not missing:
            return names

        semaphore = asyncio.Semaphore(self._max_concurrency)
        keys = list(missing)
        resolved = await asyncio.gather(
            *(self._resolve_one(key, semaphore) for key in keys),
            return_exceptions=True,
        )
        for key, value in zip(keys, resolved):
            name = value if isinstance(value, str) else ""
            if not isinstance(value, str):
                self._cache_name(key, "")
            for index in missing[key]:
                names[index] = name
        return names

    async def _resolve_one(self, key: ScopeKey, semaphore: asyncio.Semaphore) -> str:
        async with semaphore:
            try:
                name = await asyncio.wait_for(
                    self._query_onebot_name(key), timeout=self._action_timeout
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                name = ""
        self._cache_name(key, name)
        return name

    async def _query_onebot_name(self, key: ScopeKey) -> str:
        platform_id, kind, target_id = key
        get_platform_inst = getattr(self.context, "get_platform_inst", None)
        if not callable(get_platform_inst):
            return ""
        platform = get_platform_inst(platform_id)
        if platform is None:
            return ""

        get_meta = getattr(platform, "meta", None)
        if not callable(get_meta):
            return ""
        metadata = get_meta()
        if str(getattr(metadata, "name", "")).casefold() != "aiocqhttp":
            return ""

        get_client = getattr(platform, "get_client", None)
        client = get_client() if callable(get_client) else None
        call_action = getattr(client, "call_action", None)
        if not callable(call_action):
            return ""

        api_target_id: str | int = int(target_id) if target_id.isdigit() else target_id
        if kind == "group":
            response = await call_action("get_group_info", group_id=api_target_id)
            raw_name = (
                response.get("group_name") if isinstance(response, Mapping) else ""
            )
        else:
            response = await call_action(
                "get_stranger_info", user_id=api_target_id, no_cache=False
            )
            raw_name = ""
            if isinstance(response, Mapping):
                raw_name = response.get("nickname") or response.get("nick") or ""

        if not isinstance(raw_name, str):
            raw_name = str(raw_name or "")
        return raw_name.strip()[:255]
