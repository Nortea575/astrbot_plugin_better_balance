"""Adapter between Better Balance's canonical settings and AstrBot config."""

from __future__ import annotations

import copy
import threading
from collections.abc import Mapping, MutableMapping

from .configuration import (
    ALL_LEGACY_TEXT_KEYS,
    BALANCE_KINDS,
    TEXT_DEFAULTS,
    migrate_text_settings,
)

_SCALAR_KEYS = (
    "schema_version",
    "time_zone",
    "reset_time",
    "default_user_limit",
    "default_group_limit",
    "text_settings",
)
_LEGACY_EMPTY_KEYS = ("quota_management", "balance_management")
_TEMPLATE_KINDS = {"user_groups": "user_group", "group_rules": "group_rule"}


class NativeSettings:
    """Read and save canonical settings through AstrBot's config object.

    Args:
        config: AstrBot's mutable plugin config dictionary.
    """

    def __init__(self, config: MutableMapping):
        if not isinstance(config, MutableMapping):
            raise TypeError("config 必须是可变映射")
        self.config = config
        self._lock = threading.RLock()
        self._needs_migration = False

    @property
    def needs_migration(self) -> bool:
        """Whether load synthesized a canonical value absent from native storage."""
        return self._needs_migration

    def load(self) -> dict:
        """Return canonical settings without host template metadata."""
        with self._lock:
            native = copy.deepcopy(dict(self.config))
            native_text = native.get("text_settings")
            self._needs_migration = (
                any(key in native for key in ALL_LEGACY_TEXT_KEYS)
                or not isinstance(native_text, Mapping)
                or set(native_text) != set(TEXT_DEFAULTS)
            )
            result = {key: native[key] for key in _SCALAR_KEYS if key in native}
            result["text_settings"] = migrate_text_settings(native)
            if "balance_providers" in native:
                records = []
                entries = native.get("balance_providers")
                if isinstance(entries, list):
                    for entry in entries:
                        if not isinstance(entry, Mapping):
                            continue
                        record = copy.deepcopy(dict(entry))
                        template_key = record.pop("__template_key", None)
                        kind = record.get("kind") or template_key
                        if kind not in BALANCE_KINDS:
                            continue
                        if template_key in BALANCE_KINDS and template_key != kind:
                            continue
                        record["kind"] = kind
                        records.append(record)
                result["balance_providers"] = records
            for key, template_key in _TEMPLATE_KINDS.items():
                if key not in native:
                    continue
                result[key] = []
                entries = native[key]
                if not isinstance(entries, list):
                    result[key] = copy.deepcopy(entries)
                    continue
                fields = (
                    {"id", "name", "daily_limit", "user_ids"}
                    if key == "user_groups"
                    else {"group_id", "daily_limit"}
                )
                for entry in entries:
                    if not isinstance(entry, Mapping):
                        continue
                    if entry.get("__template_key") not in {None, template_key}:
                        continue
                    result[key].append(
                        {
                            field: copy.deepcopy(entry[field])
                            for field in fields
                            if field in entry
                        }
                    )
            return result

    def save(self, canonical: Mapping[str, object]) -> None:
        """Save canonical settings while retaining unrelated host keys.

        Args:
            canonical: Fully validated canonical configuration.

        Raises:
            Exception: If AstrBot cannot persist its config.
        """
        with self._lock:
            before = copy.deepcopy(dict(self.config))
            migration_before = self._needs_migration
            updated = copy.deepcopy(before)
            schema = getattr(self.config, "schema", None)
            schema_keys = set(schema) if isinstance(schema, Mapping) else None
            for key in _SCALAR_KEYS:
                if key in canonical and (
                    schema_keys is None or key in schema_keys or key in updated
                    or key == "text_settings"
                ):
                    updated[key] = copy.deepcopy(canonical[key])

            for key, template_key in _TEMPLATE_KINDS.items():
                existing = updated.get(key)
                if not isinstance(existing, list):
                    existing = []
                retained = [
                    copy.deepcopy(entry)
                    for entry in existing
                    if not isinstance(entry, Mapping)
                    or entry.get("__template_key") not in {None, template_key}
                ]
                for entry in canonical[key]:
                    retained.append({"__template_key": template_key, **copy.deepcopy(entry)})
                updated[key] = retained

            existing_records = updated.get("balance_providers")
            if not isinstance(existing_records, list):
                existing_records = []
            retained_records = [
                copy.deepcopy(entry)
                for entry in existing_records
                if not isinstance(entry, Mapping)
                or (
                    entry.get("__template_key") not in BALANCE_KINDS
                    and entry.get("kind") not in BALANCE_KINDS
                )
            ]
            for record in canonical.get("balance_providers", []):
                kind = record["kind"]
                retained_records.append(
                    {"__template_key": kind, **copy.deepcopy(record)}
                )
            updated["balance_providers"] = retained_records

            for key in ALL_LEGACY_TEXT_KEYS:
                updated.pop(key, None)

            for key in _LEGACY_EMPTY_KEYS:
                if updated.get(key) == {}:
                    updated.pop(key)

            try:
                self.config.clear()
                self.config.update(updated)
                persist = getattr(self.config, "save_config", None)
                if callable(persist):
                    persist()
                self._needs_migration = False
            except BaseException:
                self.config.clear()
                self.config.update(before)
                self._needs_migration = migration_before
                raise
