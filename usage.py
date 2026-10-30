"""Read global Token totals from AstrBot's shared statistics service."""

from __future__ import annotations

import importlib
import logging
from typing import Any


_LOGGER = logging.getLogger(__name__)


class UsageService:
    """Lazy adapter for AstrBot Dashboard's internal ProviderStat aggregate."""

    def __init__(self, context: Any) -> None:
        self.context = context
        self._stats = None

    def _resolve_stats(self):
        if self._stats is not None:
            return self._stats
        db_helper = getattr(self.context, "_db", None)
        config = getattr(self.context, "_config", None)
        if db_helper is None or config is None:
            raise RuntimeError("AstrBot shared statistics context is unavailable")
        module = importlib.import_module("astrbot.dashboard.services.stat_service")
        service_type = getattr(module, "StatService")
        # get_provider_token_stats only needs the shared database and config;
        # it does not use core_lifecycle or Dashboard authentication state.
        self._stats = service_type(db_helper, None, config)
        return self._stats

    async def get_total_tokens(self, days: int) -> int | None:
        """Return globally aggregated input plus output Tokens for recent days.

        Args:
            days: Supported range: 1, 3, or 7 days.

        Returns:
            The total Token count, or None when AstrBot's service is unavailable.
        """
        if days not in (1, 3, 7):
            return None
        try:
            stats = await self._resolve_stats().get_provider_token_stats(days)
            total = stats.get("range_total_tokens") if isinstance(stats, dict) else None
            if isinstance(total, bool) or not isinstance(total, int):
                raise ValueError("AstrBot returned an invalid Token total")
            return total
        except Exception:
            _LOGGER.warning(
                "Unable to read AstrBot global ProviderStat totals for days=%s.",
                days,
            )
            return None
