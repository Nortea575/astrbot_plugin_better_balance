"""Balance adapters and local AstrBot provider discovery.

This module deliberately keeps provider credentials inside AstrBot. Automatic
discovery records only a provider ID; a key is resolved at query time.
"""

from __future__ import annotations

import asyncio
import base64
import copy
import hashlib
import hmac
import json
import re
import time
import unicodedata
import uuid
from collections.abc import Mapping
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from urllib.parse import quote, urlsplit


_RESERVED_ALIASES = {"所有", "当前", "用量"}
_KINDS = (
    "deepseek",
    "kimi",
    "openrouter",
    "newapi",
    "aliyun",
    "volcengine",
    "custom_openai",
    "custom_anthropic",
    "custom",
)
_CUSTOM_KINDS = {"custom_openai", "custom_anthropic", "custom"}
_SUPPORTED_FIELDS = ("余额", "币种", "赠送余额", "充值余额", "代金券余额", "现金余额")
_SENSITIVE_INPUTS = {"api_key", "access_key_secret"}


def _input(
    name,
    label,
    *,
    required=False,
    secret=False,
    hint="",
    kind="string",
    options=None,
    labels=None,
):
    field_type = "secret" if secret else kind
    result = {
        "key": name,
        "type": field_type,
        "label": label,
        "required": required,
        "hint": hint,
    }
    if options is not None:
        result["options"] = list(options)
    if labels is not None:
        result["labels"] = list(labels)
    return result


_CATALOG = [
    {
        "kind": "newapi",
        "label": "NewAPI",
        "settings": {
            "base_url": "",
            "api_key": "",
            "quota_scope": "account",
            "display_unit": "auto",
            "quota_per_unit": "",
            "usd_exchange_rate": "",
            "custom_currency_symbol": "",
            "custom_currency_exchange_rate": "",
        },
        "input_fields": [
            _input(
                "base_url",
                "NewAPI 站点地址",
                required=True,
                hint="可填站点根地址、反向代理子路径或末尾带 /v1 的模型 API 地址。",
            ),
            _input(
                "api_key",
                "NewAPI API Key",
                required=True,
                secret=True,
                hint="留空时尝试使用绑定 Provider 的凭证。",
            ),
            _input(
                "quota_scope",
                "查询范围",
                required=True,
                kind="select",
                options=["account", "token"],
                labels=["账户余额", "令牌余额"],
                hint="账户查询用户总额度；令牌查询当前 Key 的额度。",
            ),
            _input(
                "display_unit",
                "显示单位",
                required=True,
                kind="select",
                options=["auto", "CNY", "USD", "QUOTA"],
                labels=["自动读取站点", "人民币", "美元", "原始额度"],
                hint="自动模式读取站点显示设置；QUOTA 保留原始配额。",
            ),
            _input(
                "quota_per_unit",
                "每单位配额",
                hint="留空时读取站点设置；用于 USD、CNY 或自定义单位换算。",
            ),
            _input(
                "usd_exchange_rate",
                "美元兑人民币汇率",
                hint="留空时读取站点设置；CNY 换算需要此值。",
            ),
            _input(
                "custom_currency_symbol",
                "自定义币种符号",
                hint="留空时读取站点设置；例如“积分”。",
            ),
            _input(
                "custom_currency_exchange_rate",
                "自定义币种汇率",
                hint="留空时读取站点设置；自定义单位换算需要此值。",
            ),
        ],
        "template_variables": ["别名", "余额", "币种", "查询范围"],
    },
    {
        "kind": "deepseek",
        "label": "DeepSeek",
        "settings": {
            "url": "https://api.deepseek.com/user/balance",
            "api_key": "",
            "currency": "",
        },
        "input_fields": [
            _input("url", "余额查询地址", required=True),
            _input(
                "api_key",
                "DeepSeek API Key",
                required=True,
                secret=True,
                hint="自动扫描时通过 Provider ID 解析，不复制密钥。",
            ),
            _input("currency", "默认币种", hint="留空时使用返回的余额币种。"),
        ],
        "template_variables": ["别名", "余额", "币种", "赠送余额", "充值余额"],
    },
    {
        "kind": "kimi",
        "label": "Kimi（月之暗面）",
        "settings": {
            "url": "https://api.moonshot.cn/v1/users/me/balance",
            "api_key": "",
            "currency": "CNY",
        },
        "input_fields": [
            _input("url", "余额查询地址", required=True),
            _input(
                "api_key",
                "Kimi API Key",
                required=True,
                secret=True,
                hint="自动扫描时通过 Provider ID 解析，不复制密钥。",
            ),
            _input("currency", "币种"),
        ],
        "template_variables": ["别名", "余额", "币种", "代金券余额", "现金余额"],
    },
    {
        "kind": "openrouter",
        "label": "OpenRouter",
        "settings": {
            "url": "https://openrouter.ai/api/v1/credits",
            "api_key": "",
            "currency": "USD",
        },
        "input_fields": [
            _input("url", "额度查询地址", required=True),
            _input(
                "api_key",
                "Management Key",
                required=True,
                secret=True,
                hint="Credits 接口要求 Management Key；自动扫描的模型 API Key 不可代替。",
            ),
            _input("currency", "币种"),
        ],
        "template_variables": ["别名", "余额", "币种"],
    },
    {
        "kind": "aliyun",
        "label": "阿里云 BSS",
        "settings": {
            "access_key_id": "",
            "access_key_secret": "",
            "region": "cn-hangzhou",
            "currency": "CNY",
        },
        "input_fields": [
            _input("access_key_id", "AccessKey ID", required=True),
            _input("access_key_secret", "AccessKey Secret", required=True, secret=True),
            _input(
                "region",
                "区域",
                hint="BSS OpenAPI 使用 business.aliyuncs.com RPC endpoint。",
            ),
            _input("currency", "默认币种"),
        ],
        "template_variables": ["别名", "余额", "币种"],
    },
    {
        "kind": "volcengine",
        "label": "火山引擎",
        "settings": {
            "access_key_id": "",
            "access_key_secret": "",
            "region": "cn-beijing",
            "currency": "CNY",
        },
        "input_fields": [
            _input("access_key_id", "AccessKey ID", required=True),
            _input(
                "access_key_secret", "Secret Access Key", required=True, secret=True
            ),
            _input("region", "区域", required=True, hint="用于火山引擎 V4 签名。"),
            _input("currency", "默认币种"),
        ],
        "template_variables": ["别名", "余额", "币种"],
    },
]

_CUSTOM_DEFAULTS = {
    "url": "",
    "api_key": "",
    "method": "GET",
    "auth_mode": "bearer",
    "auth_header": "Authorization",
    "headers": {},
    "body": {},
    "fields": {field: "" for field in _SUPPORTED_FIELDS},
}
for _kind, _label in (
    ("custom_openai", "自定义（OpenAI 来源）"),
    ("custom_anthropic", "自定义（Anthropic 来源）"),
    ("custom", "自定义 HTTP"),
):
    _CATALOG.append(
        {
            "kind": _kind,
            "label": _label,
            "settings": copy.deepcopy(_CUSTOM_DEFAULTS),
            "input_fields": [
                _input(
                    "url",
                    "余额查询地址",
                    required=True,
                    hint="这是单独配置的余额 HTTP 接口，不会从模型协议推断。",
                ),
                _input(
                    "method",
                    "请求方法",
                    required=True,
                    kind="select",
                    options=["GET", "POST"],
                ),
                _input("api_key", "API Key", secret=True),
                _input(
                    "auth_mode",
                    "认证方式",
                    required=True,
                    kind="select",
                    options=["none", "bearer", "header"],
                ),
                _input("auth_header", "认证请求头", hint="auth_mode=header 时使用。"),
                _input("headers", "额外请求头", kind="json", hint="JSON 对象。"),
                _input(
                    "body",
                    "POST JSON 正文",
                    kind="json",
                    hint="JSON 对象；GET 请求不会发送正文。",
                ),
                _input(
                    "fields",
                    "响应字段映射",
                    required=True,
                    kind="json",
                    hint='JSON Pointer 映射，例如 {"余额":"/data/balance","币种":"/data/currency"}。',
                ),
            ],
            "template_variables": ["别名", "余额", "币种"],
        }
    )

_CATALOG_BY_KIND = {item["kind"]: item for item in _CATALOG}
for _item in _CATALOG:
    for _field in _item["input_fields"]:
        _field["default"] = copy.deepcopy(_item["settings"].get(_field["key"], ""))


class BalanceQueryError(Exception):
    """Sanitized, user-facing error with a stable category."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


def _text(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def _record_alias(value: object, index: int) -> str:
    alias = _text(value)
    if not alias:
        raise ValueError(f"balance_providers[{index}].alias 不能为空")
    if any(char.isspace() for char in alias):
        raise ValueError(f"balance_providers[{index}].alias 不能包含空白字符")
    if alias in _RESERVED_ALIASES:
        raise ValueError(f"balance_providers[{index}].alias 不能使用保留字")
    return alias


def _parse_decimal(value: object, label: str = "余额") -> str:
    if (
        isinstance(value, bool)
        or value is None
        or isinstance(value, (dict, list, tuple))
    ):
        raise BalanceQueryError("invalid_response", f"余额接口返回的{label}格式无效")
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        raise BalanceQueryError(
            "invalid_response", f"余额接口返回的{label}格式无效"
        ) from None
    if not number.is_finite():
        raise BalanceQueryError("invalid_response", f"余额接口返回的{label}格式无效")
    return format(number, "f")


def _currency(value: object, fallback: str = "") -> str:
    candidate = _text(value) or _text(fallback)
    if (
        not candidate
        or len(candidate) > 12
        or not re.fullmatch(r"[A-Za-z0-9_-]+", candidate)
    ):
        raise BalanceQueryError("invalid_response", "余额接口返回的币种格式无效")
    return candidate.upper()


def _read_pointer(document: object, pointer: str) -> object:
    """Resolve a JSON Pointer without evaluation or implicit path syntax."""
    if not isinstance(pointer, str) or (pointer != "" and not pointer.startswith("/")):
        raise BalanceQueryError("invalid_configuration", "JSON Pointer 必须以 / 开头")
    current = document
    if pointer == "":
        return current
    for raw_token in pointer[1:].split("/"):
        token_chars = []
        index = 0
        while index < len(raw_token):
            char = raw_token[index]
            if char == "~":
                if index + 1 >= len(raw_token) or raw_token[index + 1] not in "01":
                    raise BalanceQueryError(
                        "invalid_configuration", "JSON Pointer 转义格式无效"
                    )
                token_chars.append("~" if raw_token[index + 1] == "0" else "/")
                index += 2
            else:
                token_chars.append(char)
                index += 1
        token = "".join(token_chars)
        if isinstance(current, Mapping):
            if token not in current:
                raise BalanceQueryError("invalid_response", "余额接口缺少已映射的字段")
            current = current[token]
        elif isinstance(current, list):
            if not token.isdigit() or (len(token) > 1 and token.startswith("0")):
                raise BalanceQueryError(
                    "invalid_configuration", "JSON Pointer 数组下标无效"
                )
            try:
                current = current[int(token)]
            except IndexError:
                raise BalanceQueryError(
                    "invalid_response", "余额接口缺少已映射的字段"
                ) from None
        else:
            raise BalanceQueryError("invalid_response", "余额接口缺少已映射的字段")
    return current


def _aliyun_rfc3986(value: object) -> str:
    return quote(str(value), safe="-_.~", encoding="utf-8", errors="strict")


def _aliyun_signature(access_key_secret: str, params: Mapping[str, str]) -> str:
    """Alibaba Cloud RPC HMAC-SHA1 signature (RFC 3986 canonical query)."""
    canonical = "&".join(
        f"{_aliyun_rfc3986(key)}={_aliyun_rfc3986(value)}"
        for key, value in sorted(params.items())
        if key != "Signature"
    )
    string_to_sign = f"GET&%2F&{_aliyun_rfc3986(canonical)}"
    digest = hmac.new(
        (access_key_secret + "&").encode("utf-8"),
        string_to_sign.encode("utf-8"),
        hashlib.sha1,
    ).digest()
    return base64.b64encode(digest).decode("ascii")


def normalize_records(records: object) -> list[dict]:
    """Normalize balance records without requiring a service or context instance."""
    return BalanceService.normalize_records(records)


def _volcengine_signature(
    access_key_id: str,
    secret_access_key: str,
    region: str,
    date_text: str,
    host: str,
    query: Mapping[str, str],
) -> str:
    """Volcano Engine V4 HMAC-SHA256 signature for signed GET requests."""
    short_date = date_text[:8]
    canonical_query = "&".join(
        f"{quote(str(key), safe='-_.~')}={quote(str(value), safe='-_.~')}"
        for key, value in sorted(query.items())
    )
    signed_headers = "host;x-date"
    canonical_headers = f"host:{host}\nx-date:{date_text}\n"
    empty_sha256 = hashlib.sha256(b"").hexdigest()
    canonical_request = "\n".join(
        ("GET", "/", canonical_query, canonical_headers, signed_headers, empty_sha256)
    )
    scope = f"{short_date}/{region}/billing/request"
    string_to_sign = "\n".join(
        (
            "HMAC-SHA256",
            date_text,
            scope,
            hashlib.sha256(canonical_request.encode()).hexdigest(),
        )
    )
    key_date = hmac.new(
        secret_access_key.encode(), short_date.encode(), hashlib.sha256
    ).digest()
    key_region = hmac.new(key_date, region.encode(), hashlib.sha256).digest()
    key_service = hmac.new(key_region, b"billing", hashlib.sha256).digest()
    signing_key = hmac.new(key_service, b"request", hashlib.sha256).digest()
    signature = hmac.new(
        signing_key, string_to_sign.encode(), hashlib.sha256
    ).hexdigest()
    return (
        "HMAC-SHA256 Credential="
        f"{access_key_id}/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
    )


class BalanceService:
    """Query and discover provider balance records.

    Args:
        context: AstrBot context used to resolve loaded provider credentials.
        store: Plugin store exposing ``get_config()`` and optionally ``save_config()``.
        transport: Optional async HTTP test transport ``(method, url, **kwargs)``.
        timeout: Per-provider timeout in seconds.
        cache_ttl: Cache lifetime for successful and failed responses.
    """

    def __init__(
        self,
        context,
        store,
        transport=None,
        timeout: float = 8.0,
        cache_ttl: float = 30.0,
    ):
        self.context = context
        self.store = store
        self.transport = transport
        self.timeout = max(0.1, float(timeout))
        self.cache_ttl = max(0.0, float(cache_ttl))
        self._session = None
        self._cache: dict[tuple[str, str], tuple[float, dict]] = {}
        self._inflight: dict[tuple[str, str], asyncio.Task] = {}
        self._closed = False

    @staticmethod
    def catalog() -> list[dict]:
        """Return balance adapter defaults, editable input descriptors and variables."""
        return copy.deepcopy(_CATALOG)

    @staticmethod
    def template_mapping(kind: str) -> str:
        """Return the text_settings template key used to render this kind."""
        return "balance_custom" if kind in _CUSTOM_KINDS else f"balance_{kind}"

    @staticmethod
    def normalize_records(records: object) -> list[dict]:
        """Validate records while allowing incomplete adapter settings."""
        if not isinstance(records, list):
            raise ValueError("balance_providers 必须是数组")
        normalized = []
        ids = set()
        aliases = set()
        bound_provider_ids = set()
        for index, raw in enumerate(records):
            label = f"balance_providers[{index}]"
            if not isinstance(raw, Mapping):
                raise ValueError(f"{label} 必须是对象")
            allowed = {
                "id",
                "alias",
                "kind",
                "provider_ids",
                "credential_ref",
                "settings",
                "origin",
                "scan_status",
                "scan_note",
            }
            if set(raw) - allowed:
                raise ValueError(f"{label} 包含未知字段")
            kind = raw.get("kind")
            if kind not in _KINDS:
                raise ValueError(f"{label}.kind 不受支持")
            alias = _record_alias(raw.get("alias"), index)
            alias_key = alias.casefold()
            if alias_key in aliases:
                raise ValueError("余额记录 alias 必须唯一")
            aliases.add(alias_key)

            provider_ids_raw = raw.get("provider_ids", [])
            if not isinstance(provider_ids_raw, list):
                raise ValueError(f"{label}.provider_ids 必须是数组")
            provider_ids = []
            for item in provider_ids_raw:
                value = _text(item)
                if not value:
                    raise ValueError(f"{label}.provider_ids 只能包含非空字符串")
                if value not in provider_ids:
                    provider_ids.append(value)
                if value in bound_provider_ids:
                    raise ValueError("一个 AstrBot Provider ID 不能绑定到多条余额记录")
                bound_provider_ids.add(value)

            settings = raw.get("settings", {})
            if not isinstance(settings, Mapping):
                raise ValueError(f"{label}.settings 必须是对象")
            record_id = _text(raw.get("id"))
            if not record_id:
                seed = json.dumps(
                    {
                        "alias": alias,
                        "kind": kind,
                        "provider_ids": provider_ids,
                        "index": index,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
                record_id = "balance-" + hashlib.sha256(seed.encode()).hexdigest()[:16]
            if record_id in ids:
                raise ValueError("余额记录 id 必须唯一")
            ids.add(record_id)
            origin = raw.get("origin", "manual")
            scan_status = raw.get("scan_status", "configured")
            if not isinstance(origin, str) or origin not in {"scan", "manual"}:
                raise ValueError(f"{label}.origin 只能是 scan 或 manual")
            if not isinstance(scan_status, str) or scan_status not in {
                "configured",
                "needs_configuration",
                "unrecognized",
            }:
                raise ValueError(f"{label}.scan_status 无效")
            credential_ref = raw.get("credential_ref", "")
            if not isinstance(credential_ref, str):
                raise ValueError(f"{label}.credential_ref 必须是字符串")
            scan_note = raw.get("scan_note", "")
            if not isinstance(scan_note, str):
                raise ValueError(f"{label}.scan_note 必须是字符串")
            normalized.append(
                {
                    "id": record_id,
                    "alias": alias,
                    "kind": kind,
                    "provider_ids": provider_ids,
                    "credential_ref": credential_ref.strip(),
                    "settings": copy.deepcopy(dict(settings)),
                    "origin": origin,
                    "scan_status": scan_status,
                    "scan_note": scan_note,
                }
            )
        return normalized

    def scan(self, existing_records: object) -> dict:
        """Discover loaded providers locally; never makes provider network calls."""
        existing = self.normalize_records(existing_records)
        discovered = self._discover_providers()
        existing_by_provider = {}
        for record in existing:
            for provider_id in record["provider_ids"]:
                existing_by_provider.setdefault(provider_id, []).append(record)

        claimed = set(existing_by_provider)
        added = []
        updated = []
        needs_configuration = []
        result_records = copy.deepcopy(existing)
        result_index_by_id = {
            item["id"]: index for index, item in enumerate(result_records)
        }

        for item in discovered:
            provider_ids = item["provider_ids"]
            matches = []
            for provider_id in provider_ids:
                matches.extend(existing_by_provider.get(provider_id, []))
            matches = list({record["id"]: record for record in matches}.values())
            manual_matches = [
                record for record in matches if record["origin"] == "manual"
            ]
            manual_bound_ids = set()
            if manual_matches:
                manual_bound_ids = {
                    provider_id
                    for manual_record in manual_matches
                    for provider_id in manual_record["provider_ids"]
                }
                # A manual binding owns only its explicitly listed Provider IDs. If a
                # discovered shared source also has unbound models, keep those visible.
                provider_ids = [
                    provider_id
                    for provider_id in provider_ids
                    if provider_id not in manual_bound_ids
                ]
                claimed.update(manual_bound_ids)
                if not provider_ids:
                    continue
                item = copy.deepcopy(item)
                item["provider_ids"] = provider_ids
                if item["credential_ref"] not in provider_ids:
                    item["credential_ref"] = provider_ids[0]
                matches = []
                for provider_id in provider_ids:
                    matches.extend(existing_by_provider.get(provider_id, []))
                matches = list(
                    {
                        record["id"]: record
                        for record in matches
                        if record["origin"] != "manual"
                    }.values()
                )

            target = matches[0] if matches else None
            duplicate_scan_ids = {
                record["id"] for record in matches[1:] if record["origin"] == "scan"
            }
            if target is None:
                alias = self._unique_alias(item["alias"], result_records)
                target = {
                    "id": item["id"],
                    "alias": alias,
                    "kind": item["kind"],
                    "provider_ids": provider_ids,
                    "credential_ref": item["credential_ref"],
                    "settings": self._default_settings(item["kind"]),
                    "origin": "scan",
                    "scan_status": item["scan_status"],
                    "scan_note": item["scan_note"],
                }
                result_index_by_id[target["id"]] = len(result_records)
                result_records.append(target)
                added.append(copy.deepcopy(target))
            else:
                index = result_index_by_id[target["id"]]
                before = copy.deepcopy(result_records[index])
                current = result_records[index]
                # Keep user edits to alias/settings and stable id while refreshing provider links.
                current["kind"] = item["kind"]
                current["provider_ids"] = provider_ids
                if not current["credential_ref"]:
                    current["credential_ref"] = item["credential_ref"]
                current["scan_status"] = item["scan_status"]
                current["scan_note"] = item["scan_note"]
                if _text(current["settings"].get("api_key")) and current["kind"] in {
                    "deepseek",
                    "kimi",
                    "openrouter",
                }:
                    current["scan_status"] = "configured"
                    current["scan_note"] = ""
                if not current["settings"]:
                    current["settings"] = self._default_settings(current["kind"])
                if current != before:
                    updated.append(copy.deepcopy(current))
                target = current
            if duplicate_scan_ids:
                result_records = [
                    record
                    for record in result_records
                    if record["id"] not in duplicate_scan_ids
                ]
                for duplicate_id in duplicate_scan_ids:
                    result_index_by_id.pop(duplicate_id, None)
                result_index_by_id = {
                    record["id"]: index for index, record in enumerate(result_records)
                }
            claimed.update(provider_ids)
            if target["scan_status"] != "configured":
                needs_configuration.append(copy.deepcopy(target))

        # Keep stale scan records, marking provider mismatches instead of deleting them.
        seen_provider_ids = {
            provider_id for item in discovered for provider_id in item["provider_ids"]
        }
        for record in result_records:
            if record["origin"] != "scan" or not record["provider_ids"]:
                continue
            if record["id"] in {item["id"] for item in added}:
                continue
            if any(
                provider_id in seen_provider_ids
                for provider_id in record["provider_ids"]
            ):
                continue
            if (
                record["scan_status"] != "unrecognized"
                or record["scan_note"] != "未找到原 Provider；请重新绑定或手动检查。"
            ):
                record["scan_status"] = "unrecognized"
                record["scan_note"] = "未找到原 Provider；请重新绑定或手动检查。"
                if not any(item["id"] == record["id"] for item in updated):
                    updated.append(copy.deepcopy(record))
            needs_configuration.append(copy.deepcopy(record))

        # Existing unrecognized entries remain visible and actionable.
        for record in result_records:
            if record["scan_status"] != "configured" and not any(
                item["id"] == record["id"] for item in needs_configuration
            ):
                needs_configuration.append(copy.deepcopy(record))

        normalized = self.normalize_records(result_records)
        return {
            "records": normalized,
            "added": added,
            "updated": updated,
            "needs_configuration": needs_configuration,
        }

    def _discover_providers(self) -> list[dict]:
        getter = getattr(self.context, "get_all_providers", None)
        if not callable(getter):
            return []
        try:
            providers = getter()
        except Exception:
            return []
        if not isinstance(providers, (list, tuple)):
            return []
        raw_config = self._context_config()
        raw_providers, raw_sources = self._raw_provider_config(raw_config)

        candidates = []
        for provider in providers:
            meta_value = getattr(provider, "meta", None)
            try:
                meta = meta_value() if callable(meta_value) else meta_value
            except Exception:
                meta = None
            if meta is None:
                continue
            provider_id = _text(self._meta_field(meta, "id"))
            if not provider_id:
                continue
            provider_config = getattr(provider, "provider_config", {})
            if not isinstance(provider_config, Mapping):
                provider_config = {}
            provider_source_id = _text(provider_config.get("provider_source_id"))
            raw_instance = raw_providers.get(provider_id, {})
            raw_source = raw_sources.get(provider_source_id, {})
            effective_base = _text(provider_config.get("api_base"))
            # Raw instance key presence tells us whether account settings override the source.
            override = any(key in raw_instance for key in ("key", "api_base"))
            can_merge_source = bool(provider_source_id and raw_source and not override)
            if not effective_base and isinstance(raw_source, Mapping):
                effective_base = _text(raw_source.get("api_base"))
            host = self._host(effective_base)
            kind = self._kind_for_host(host)
            key_count = self._provider_key_count(provider_config)
            if kind is None:
                kind = {
                    "api.openai.com": "custom_openai",
                    "api.anthropic.com": "custom_anthropic",
                }.get(host, "custom")
                if kind in {"custom_openai", "custom_anthropic"}:
                    status = "needs_configuration"
                    note = "仅识别到模型 API 域名；余额接口需单独配置 URL 和 JSON Pointer 字段映射。"
                else:
                    status = "unrecognized"
                    note = (
                        "未识别的余额接口域名；需手动配置独立余额 HTTP 地址和字段映射。"
                    )
                    if not effective_base:
                        note = "Provider 未提供 API Base；需手动配置独立余额 HTTP 地址和字段映射。"
            elif kind == "openrouter":
                status = "needs_configuration"
                note = "OpenRouter 查询需要单独配置 Management Key；扫描到的模型 Key 不会用于此接口。"
            elif kind == "aliyun":
                status = "needs_configuration"
                note = "模型 Provider 仅用于识别阿里云账号；余额查询需另外配置 BSS AccessKey ID 和 Secret。"
            elif kind == "volcengine":
                status = "needs_configuration"
                note = "模型 Provider 仅用于识别火山引擎账号；余额查询需另外配置云 AccessKey ID 和 Secret。"
            elif key_count == 1:
                status = "configured"
                note = ""
            elif key_count > 1:
                status = "needs_configuration"
                note = "Provider 配置了多个 Key，无法确认余额账户；请手动选择或配置单一账户凭证。"
            else:
                status = "needs_configuration"
                note = "Provider 未配置单一 API Key；请检查余额查询凭证。"

            source_group = provider_source_id if can_merge_source else provider_id
            candidates.append(
                {
                    "provider_id": provider_id,
                    "kind": kind,
                    "status": status,
                    "note": note,
                    "source_group": source_group,
                    "provider_source_id": provider_source_id,
                    "model": _text(self._meta_field(meta, "model")),
                    "host": host,
                }
            )

        groups = {}
        for candidate in candidates:
            group_key = (candidate["kind"], candidate["source_group"])
            groups.setdefault(group_key, []).append(candidate)

        result = []
        for (kind, source_group), items in groups.items():
            # A source group can only be merged when every effective instance inherits
            # the account-related key and base from that source. Non-inheriting instances
            # use their provider ID as source_group and therefore stay separate.
            provider_ids = sorted(item["provider_id"] for item in items)
            if kind == "custom":
                group_status = "unrecognized"
                group_note = items[0]["note"]
            elif kind in {
                "openrouter",
                "aliyun",
                "volcengine",
                "custom_openai",
                "custom_anthropic",
            }:
                group_status = "needs_configuration"
                group_note = items[0]["note"]
            else:
                key_counts = {item["status"] for item in items}
                group_status = (
                    "configured"
                    if key_counts == {"configured"}
                    else "needs_configuration"
                )
                group_note = (
                    ""
                    if group_status == "configured"
                    else next(
                        (
                            item["note"]
                            for item in items
                            if item["status"] != "configured"
                        ),
                        "Provider 余额凭证需手动配置。",
                    )
                )
            label = _CATALOG_BY_KIND[kind]["label"]
            alias_prefix = re.sub(r"\s+", "", label.split("（")[0])
            slug = re.sub(r"\s+", "", source_group)
            slug = re.sub(r"[^\w-]+", "", slug, flags=re.UNICODE)[:20]
            alias = (
                f"{alias_prefix}-{slug}"
                if slug
                else f"{alias_prefix}-{provider_ids[0]}"
            )
            seed = f"{kind}:{source_group}"
            stable_id = "scan-" + hashlib.sha256(seed.encode()).hexdigest()[:16]
            credential_ref = provider_ids[0] if len(provider_ids) == 1 else ""
            if len(provider_ids) > 1:
                # Shared source credential resolution uses the first stable member ID.
                credential_ref = provider_ids[0]
            if kind in {"aliyun", "volcengine"}:
                # Their model providers do not carry the cloud-account credentials.
                credential_ref = ""
            result.append(
                {
                    "id": stable_id,
                    "alias": alias,
                    "kind": kind,
                    "provider_ids": provider_ids,
                    "credential_ref": credential_ref,
                    "scan_status": group_status,
                    "scan_note": group_note,
                }
            )
        return result

    def _context_config(self):
        getter = getattr(self.context, "get_config", None)
        if not callable(getter):
            return {}
        try:
            value = getter()
        except Exception:
            return {}
        return value if isinstance(value, Mapping) else {}

    @staticmethod
    def _raw_provider_config(config: Mapping) -> tuple[dict, dict]:
        def indexed(value):
            result = {}
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, Mapping):
                        item_id = _text(
                            item.get("id")
                            or item.get("provider_id")
                            or item.get("provider_source_id")
                        )
                        if item_id:
                            result[item_id] = item
            elif isinstance(value, Mapping):
                for item_id, item in value.items():
                    if isinstance(item, Mapping):
                        result[_text(item_id)] = item
            return result

        return indexed(config.get("provider", config.get("providers", []))), indexed(
            config.get("provider_sources", [])
        )

    @staticmethod
    def _host(url: str) -> str:
        try:
            return (urlsplit(url).hostname or "").lower().rstrip(".")
        except ValueError:
            return ""

    @staticmethod
    def _meta_field(meta, name: str):
        if isinstance(meta, Mapping):
            return meta.get(name)
        return getattr(meta, name, None)

    @staticmethod
    def _kind_for_host(host: str) -> str | None:
        return {
            "api.deepseek.com": "deepseek",
            "api.moonshot.cn": "kimi",
            "openrouter.ai": "openrouter",
            "dashscope.aliyuncs.com": "aliyun",
            "ark.cn-beijing.volces.com": "volcengine",
        }.get(host)

    @staticmethod
    def _provider_key_count(provider_config: Mapping) -> int:
        keys = provider_config.get("key")
        if isinstance(keys, list):
            return sum(
                1 for item in keys if isinstance(item, str) and bool(item.strip())
            )
        if isinstance(keys, str):
            return int(bool(keys.strip()))
        return 0

    @staticmethod
    def _default_settings(kind: str) -> dict:
        return copy.deepcopy(_CATALOG_BY_KIND[kind]["settings"])

    @staticmethod
    def _unique_alias(base: str, records: list[dict]) -> str:
        used = {item["alias"].casefold() for item in records}
        candidate = base
        suffix = 2
        while candidate.casefold() in used or candidate in _RESERVED_ALIASES:
            candidate = f"{base}-{suffix}"
            suffix += 1
        return candidate

    async def query_record(self, record: Mapping) -> dict:
        """Query one record with a short cache and per-record in-flight merge."""
        try:
            normalized = self.normalize_records([dict(record)])[0]
        except (TypeError, ValueError):
            return self._result_from_invalid(record)
        if self._closed:
            return self._failure(normalized, "unavailable", "余额服务已关闭。")
        fingerprint = self._fingerprint(normalized)
        cache_key = (normalized["id"], fingerprint)
        cached = self._cache.get(cache_key)
        now = time.monotonic()
        if cached and now - cached[0] < self.cache_ttl:
            result = copy.deepcopy(cached[1])
            result["cached"] = True
            return result

        task = self._inflight.get(cache_key)
        owner = task is None
        if owner:
            task = asyncio.create_task(self._run_query(normalized, cache_key))
            self._inflight[cache_key] = task
        try:
            result = copy.deepcopy(await asyncio.shield(task))
            if not owner:
                result["cached"] = True
            return result
        finally:
            if owner and self._inflight.get(cache_key) is task:
                self._inflight.pop(cache_key, None)

    async def query_all(self) -> list[dict]:
        """Query all configured records independently and preserve their order."""
        config = self._store_config()
        records = (
            config.get("balance_providers", []) if isinstance(config, Mapping) else []
        )
        try:
            canonical = self.normalize_records(records)
        except (TypeError, ValueError):
            return []
        return list(
            await asyncio.gather(*(self.query_record(item) for item in canonical))
        )

    async def _run_query(self, record: dict, cache_key: tuple[str, str]) -> dict:
        try:
            result = await asyncio.wait_for(
                self._query_uncached(record), timeout=self.timeout
            )
        except asyncio.TimeoutError:
            result = self._failure(record, "timeout", "余额查询超时。")
        except BalanceQueryError as error:
            result = self._failure(record, error.code, error.message)
        except Exception:
            result = self._failure(
                record, "network", "余额查询失败，请检查网络或接口设置。"
            )
        if self._is_current_record(record):
            self._cache[cache_key] = (time.monotonic(), copy.deepcopy(result))
        else:
            result = self._failure(
                record, "config_changed", "余额配置已更新，请重新查询。"
            )
        return result

    async def _query_uncached(self, record: dict) -> dict:
        if record["scan_status"] == "unrecognized" and not self._has_explicit_config(
            record
        ):
            raise BalanceQueryError(
                "unrecognized", "此 Provider 未识别余额接口，请先手动配置。"
            )
        if record[
            "scan_status"
        ] == "needs_configuration" and not self._has_explicit_config(record):
            if record["kind"] == "openrouter":
                raise BalanceQueryError(
                    "needs_configuration",
                    "OpenRouter 余额查询需要单独填写 Management Key。",
                )
            raise BalanceQueryError(
                "needs_configuration", "余额查询信息尚未配置，请补充凭证和接口设置。"
            )
        settings = record["settings"]
        kind = record["kind"]
        if kind in {"aliyun", "volcengine"}:
            rows = await self._query_cloud(kind, record)
        else:
            auth_mode = _text(settings.get("auth_mode", "bearer")).lower()
            requires_key = kind not in _CUSTOM_KINDS or auth_mode != "none"
            api_key = (
                ""
                if kind in _CUSTOM_KINDS and auth_mode == "none"
                else self._resolve_api_key(record, required=requires_key)
            )
            if kind == "openrouter" and not _text(settings.get("api_key")):
                raise BalanceQueryError(
                    "needs_configuration",
                    "OpenRouter 余额查询需要单独填写 Management Key。",
                )
            if kind == "newapi":
                rows = await self._query_newapi(record, api_key)
            elif kind in _CUSTOM_KINDS:
                rows = await self._query_custom(record, api_key)
            else:
                rows = await self._query_standard(kind, record, api_key)
        return {
            "id": record["id"],
            "alias": record["alias"],
            "kind": kind,
            "ok": True,
            "rows": rows,
            "error_code": "",
            "error": "",
            "queried_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "cached": False,
        }

    @staticmethod
    def _has_explicit_config(record: Mapping) -> bool:
        settings = record["settings"]
        if record["kind"] == "newapi":
            return bool(
                _text(settings.get("base_url"))
                and (
                    _text(settings.get("api_key"))
                    or _text(record.get("credential_ref"))
                )
            )
        if record["kind"] == "openrouter":
            return bool(_text(settings.get("api_key")))
        if record["kind"] in {"aliyun", "volcengine"}:
            return bool(
                _text(settings.get("access_key_id"))
                and _text(settings.get("access_key_secret"))
            )
        if record["kind"] in _CUSTOM_KINDS:
            fields = settings.get("fields")
            return bool(
                _text(settings.get("url"))
                and isinstance(fields, Mapping)
                and _text(fields.get("余额"))
            )
        return bool(
            _text(settings.get("api_key")) or _text(record.get("credential_ref"))
        )

    def _resolve_api_key(self, record: Mapping, *, required: bool) -> str:
        # Explicit settings take precedence, including a custom Management Key.
        explicit = _text(record["settings"].get("api_key"))
        if explicit:
            return explicit
        if record["kind"] == "openrouter":
            if required:
                raise BalanceQueryError(
                    "needs_configuration",
                    "OpenRouter 余额查询需要单独填写 Management Key。",
                )
            return ""
        provider_id = _text(record.get("credential_ref"))
        provider_ids = []
        if provider_id:
            provider_ids.append(provider_id)
        provider_ids.extend(
            candidate
            for candidate in record.get("provider_ids", [])
            if isinstance(candidate, str) and candidate not in provider_ids
        )
        for provider_id in provider_ids:
            provider = self._find_provider(provider_id)
            provider_config = (
                getattr(provider, "provider_config", {}) if provider is not None else {}
            )
            keys = (
                provider_config.get("key")
                if isinstance(provider_config, Mapping)
                else None
            )
            if isinstance(keys, str):
                keys = [keys]
            if isinstance(keys, list):
                values = [
                    item.strip()
                    for item in keys
                    if isinstance(item, str) and item.strip()
                ]
                if len(values) == 1:
                    return values[0]
                if len(values) > 1:
                    raise BalanceQueryError(
                        "needs_configuration",
                        "Provider 配置了多个 Key，无法安全选择余额账户。",
                    )
        if required:
            raise BalanceQueryError(
                "needs_configuration", "余额查询凭证尚未配置或无法解析。"
            )
        return ""

    def _find_provider(self, provider_id: str):
        getter = getattr(self.context, "get_all_providers", None)
        if not callable(getter):
            return None
        try:
            providers = getter()
        except Exception:
            return None
        for provider in providers if isinstance(providers, (list, tuple)) else []:
            meta_value = getattr(provider, "meta", None)
            try:
                meta = meta_value() if callable(meta_value) else meta_value
            except Exception:
                continue
            if meta is not None and _text(self._meta_field(meta, "id")) == provider_id:
                return provider
        return None

    async def _query_standard(
        self, kind: str, record: dict, api_key: str
    ) -> list[dict]:
        settings = record["settings"]
        url = _text(settings.get("url"))
        if not url:
            raise BalanceQueryError("needs_configuration", "余额查询地址尚未配置。")
        headers = {"Authorization": f"Bearer {api_key}"}
        payload = await self._http_json("GET", url, headers=headers)
        fallback_currency = _text(settings.get("currency"))
        if kind == "deepseek":
            entries = (
                payload.get("balance_infos") if isinstance(payload, Mapping) else None
            )
            if not isinstance(entries, list) or not entries:
                raise BalanceQueryError(
                    "invalid_response", "DeepSeek 返回的余额列表格式无效。"
                )
            rows = []
            for entry in entries:
                if not isinstance(entry, Mapping):
                    raise BalanceQueryError(
                        "invalid_response", "DeepSeek 返回的余额列表格式无效。"
                    )
                rows.append(
                    {
                        "余额": _parse_decimal(entry.get("total_balance")),
                        "币种": _currency(entry.get("currency"), fallback_currency),
                        "赠送余额": _parse_decimal(
                            entry.get("granted_balance"), "赠送余额"
                        ),
                        "充值余额": _parse_decimal(
                            entry.get("topped_up_balance"), "充值余额"
                        ),
                    }
                )
            return rows
        if kind == "kimi":
            if not isinstance(payload, Mapping):
                raise BalanceQueryError("invalid_response", "Kimi 返回的余额格式无效。")
            code = payload.get("code")
            if payload.get("status") is False or (
                code is not None and str(code) != "0"
            ):
                raise BalanceQueryError(
                    "provider_error", "Kimi 余额查询失败或凭证无效。"
                )
            data = payload.get("data")
            if not isinstance(data, Mapping):
                raise BalanceQueryError("invalid_response", "Kimi 返回的余额格式无效。")
            return [
                {
                    "余额": _parse_decimal(data.get("available_balance")),
                    "币种": _currency(fallback_currency or "CNY"),
                    "代金券余额": _parse_decimal(
                        data.get("voucher_balance"), "代金券余额"
                    ),
                    "现金余额": _parse_decimal(data.get("cash_balance"), "现金余额"),
                }
            ]
        if kind == "openrouter":
            data = payload.get("data") if isinstance(payload, Mapping) else None
            if not isinstance(data, Mapping):
                raise BalanceQueryError(
                    "invalid_response", "OpenRouter 返回的额度格式无效。"
                )
            total = Decimal(_parse_decimal(data.get("total_credits"), "总额度"))
            used = Decimal(_parse_decimal(data.get("total_usage"), "已用额度"))
            return [
                {
                    "余额": format(total - used, "f"),
                    "币种": _currency(fallback_currency or "USD"),
                }
            ]
        raise BalanceQueryError("invalid_configuration", "余额适配器配置无效。")

    @staticmethod
    def _newapi_urls(base_url: object) -> tuple[str, str]:
        """Validate a NewAPI site URL and derive its usage and public status endpoints."""
        value = _text(base_url)
        if not value or any(char.isspace() or ord(char) < 32 for char in value):
            raise BalanceQueryError(
                "needs_configuration", "请填写有效的 NewAPI 站点地址。"
            )
        if "?" in value or "#" in value:
            raise BalanceQueryError(
                "invalid_configuration", "NewAPI 站点地址不能包含查询参数或片段。"
            )
        try:
            parsed = urlsplit(value)
            # Accessing port validates malformed ports; credentials in the authority
            # are rejected so they can never be forwarded or included in endpoint URLs.
            _ = parsed.port
        except ValueError:
            raise BalanceQueryError(
                "invalid_configuration", "NewAPI 站点地址格式无效。"
            ) from None
        if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
            raise BalanceQueryError(
                "invalid_configuration",
                "NewAPI 站点地址必须使用 http(s) 且包含主机名。",
            )
        if parsed.username is not None or parsed.password is not None:
            raise BalanceQueryError(
                "invalid_configuration", "NewAPI 站点地址不能内嵌账号或凭证。"
            )
        if any(char.isspace() or ord(char) < 33 for char in parsed.hostname):
            raise BalanceQueryError(
                "invalid_configuration", "NewAPI 站点地址主机名格式无效。"
            )

        path = parsed.path.rstrip("/")
        lower_path = path.lower()
        for suffix in ("/api/usage/token", "/api/status", "/v1"):
            if lower_path.endswith(suffix):
                path = path[: -len(suffix)]
                break
        base = f"{parsed.scheme.lower()}://{parsed.netloc}{path.rstrip('/')}"
        return f"{base}/api/usage/token/", f"{base}/api/status"

    @staticmethod
    def _newapi_data(payload: object, *, source: str) -> Mapping:
        if not isinstance(payload, Mapping):
            raise BalanceQueryError(
                "invalid_response", f"NewAPI {source}接口返回格式无效。"
            )
        if "code" not in payload and "success" not in payload:
            raise BalanceQueryError(
                "invalid_response", f"NewAPI {source}接口返回格式无效。"
            )
        if "code" in payload and payload.get("code") is not True:
            raise BalanceQueryError(
                "provider_error", f"NewAPI {source}接口查询失败或权限不足。"
            )
        if "success" in payload and payload.get("success") is not True:
            raise BalanceQueryError(
                "provider_error", f"NewAPI {source}接口查询失败或权限不足。"
            )
        data = payload.get("data")
        if not isinstance(data, Mapping):
            raise BalanceQueryError(
                "invalid_response", f"NewAPI {source}接口返回格式无效。"
            )
        if source == "令牌用量" and data.get("object") != "token_usage":
            raise BalanceQueryError("invalid_response", "NewAPI 令牌用量响应类型无效。")
        return data

    @staticmethod
    def _newapi_metadata_number(value: object, label: str, *, manual: bool) -> Decimal:
        code = "invalid_configuration" if manual else "newapi_metadata"
        message = (
            f"NewAPI 的{label}必须是有限正数，请补全有效参数或改选 QUOTA 原始单位。"
        )
        if (
            isinstance(value, bool)
            or value is None
            or isinstance(value, (Mapping, list, tuple))
        ):
            raise BalanceQueryError(code, message)
        try:
            number = Decimal(str(value).strip())
        except (InvalidOperation, ValueError, TypeError):
            raise BalanceQueryError(code, message) from None
        if not number.is_finite() or number <= 0:
            raise BalanceQueryError(code, message)
        return number

    @staticmethod
    def _newapi_custom_symbol(value: object, *, manual: bool) -> str:
        code = "invalid_configuration" if manual else "newapi_metadata"
        message = (
            "NewAPI 自定义币种符号无效，请填写安全的纯文本符号或改选 QUOTA 原始单位。"
        )
        if not isinstance(value, str) or any(
            unicodedata.category(char) == "Cc" for char in value
        ):
            raise BalanceQueryError(code, message)
        symbol = _text(value)
        if not symbol or len(symbol) > 64:
            raise BalanceQueryError(code, message)
        return symbol

    async def _query_newapi(self, record: dict, api_key: str) -> list[dict]:
        settings = record["settings"]
        usage_url, status_url = self._newapi_urls(settings.get("base_url"))
        scope = _text(settings.get("quota_scope", "account")).lower() or "account"
        if scope not in {"account", "token"}:
            raise BalanceQueryError(
                "invalid_configuration", "NewAPI 查询范围只能是 account 或 token。"
            )
        unit_setting = _text(settings.get("display_unit", "auto")).upper() or "AUTO"
        if unit_setting not in {"AUTO", "CNY", "USD", "QUOTA"}:
            raise BalanceQueryError("invalid_configuration", "NewAPI 显示单位无效。")
        if not api_key:
            raise BalanceQueryError(
                "needs_configuration", "NewAPI 查询需要 API Key 或绑定 Provider 凭证。"
            )

        payload = await self._http_json(
            "GET", usage_url, headers={"Authorization": f"Bearer {api_key}"}
        )
        data = self._newapi_data(payload, source="令牌用量")
        if scope == "token":
            unlimited = data.get("unlimited_quota")
            if not isinstance(unlimited, bool):
                raise BalanceQueryError(
                    "invalid_response", "NewAPI 令牌不限额字段格式无效。"
                )
            if unlimited:
                return [{"余额": "不限额", "币种": "", "查询范围": "令牌"}]
        if scope == "account":
            user_quota = data.get("user_quota")
            if (
                not isinstance(user_quota, Mapping)
                or "total_available" not in user_quota
            ):
                raise BalanceQueryError(
                    "account_quota_unavailable",
                    "无法读取 NewAPI 账户总额度；当前 Key 可能无账户额度权限，或站点版本不支持该字段。请检查权限和版本。",
                )
            raw_value = user_quota.get("total_available")
        else:
            if "total_available" not in data:
                raise BalanceQueryError(
                    "invalid_response", "NewAPI 令牌响应缺少可用额度字段。"
                )
            raw_value = data.get("total_available")
        raw_quota = Decimal(_parse_decimal(raw_value, "可用额度"))

        if unit_setting == "QUOTA":
            return [
                {
                    "余额": format(raw_quota, "f"),
                    "币种": "QUOTA",
                    "查询范围": "账户" if scope == "account" else "令牌",
                }
            ]

        manual_values = {}
        for name in (
            "quota_per_unit",
            "usd_exchange_rate",
            "custom_currency_symbol",
            "custom_currency_exchange_rate",
        ):
            value = settings.get(name)
            manual_values[name] = (
                value
                if value is not None
                and not (isinstance(value, str) and not value.strip())
                else None
            )

        required_numbers = {
            "USD": ("quota_per_unit",),
            "CNY": ("quota_per_unit", "usd_exchange_rate"),
        }
        needs_status = unit_setting == "AUTO" or any(
            manual_values.get(name) is None
            for name in required_numbers.get(unit_setting, ())
        )
        status_data = {}
        if needs_status:
            try:
                status_payload = await self._http_json("GET", status_url)
                status_data = dict(self._newapi_data(status_payload, source="站点状态"))
            except asyncio.TimeoutError:
                raise BalanceQueryError(
                    "newapi_metadata",
                    "读取 NewAPI 站点显示配置超时，请补全单位换算参数或改选 QUOTA 原始单位。",
                ) from None
            except BalanceQueryError:
                raise BalanceQueryError(
                    "newapi_metadata",
                    "无法读取 NewAPI 站点显示配置，请补全单位换算参数或改选 QUOTA 原始单位。",
                ) from None

        if unit_setting == "AUTO":
            unit = _text(status_data.get("quota_display_type")).upper()
            if unit not in {"USD", "CNY", "TOKENS", "CUSTOM"}:
                raise BalanceQueryError(
                    "newapi_metadata",
                    "NewAPI 站点未提供有效的额度显示单位，请补全单位换算参数或改选 QUOTA 原始单位。",
                )
        else:
            unit = unit_setting

        query_scope = "账户" if scope == "account" else "令牌"
        if unit == "TOKENS":
            return [
                {
                    "余额": format(raw_quota, "f"),
                    "币种": "QUOTA",
                    "查询范围": query_scope,
                }
            ]

        def parameter(name: str, label: str) -> Decimal:
            value = manual_values.get(name)
            manual = value is not None
            if value is None:
                value = status_data.get(name)
            if value is None or (isinstance(value, str) and not value.strip()):
                raise BalanceQueryError(
                    "newapi_metadata",
                    "NewAPI 单位换算配置缺失，请补全对应参数或改选 QUOTA 原始单位。",
                )
            return self._newapi_metadata_number(value, label, manual=manual)

        try:
            if unit == "USD":
                amount = raw_quota / parameter("quota_per_unit", "每单位配额")
                currency = "USD"
            elif unit == "CNY":
                amount = (
                    raw_quota
                    / parameter("quota_per_unit", "每单位配额")
                    * parameter("usd_exchange_rate", "美元兑人民币汇率")
                )
                currency = "CNY"
            elif unit == "CUSTOM":
                amount = (
                    raw_quota
                    / parameter("quota_per_unit", "每单位配额")
                    * parameter("custom_currency_exchange_rate", "自定义币种汇率")
                )
                symbol = manual_values.get("custom_currency_symbol")
                manual_symbol = symbol is not None
                if symbol is None:
                    symbol = status_data.get("custom_currency_symbol")
                currency = self._newapi_custom_symbol(symbol, manual=manual_symbol)
            else:
                raise BalanceQueryError(
                    "newapi_metadata",
                    "NewAPI 额度显示单位无效，请补全单位参数或改选 QUOTA 原始单位。",
                )
            if not amount.is_finite():
                raise BalanceQueryError(
                    "newapi_metadata",
                    "NewAPI 单位换算结果无效，请检查配置参数或改选 QUOTA 原始单位。",
                )
        except (ArithmeticError, InvalidOperation):
            raise BalanceQueryError(
                "newapi_metadata",
                "NewAPI 单位换算失败，请检查配置参数或改选 QUOTA 原始单位。",
            ) from None
        return [
            {"余额": format(amount, "f"), "币种": currency, "查询范围": query_scope}
        ]

    async def _query_cloud(self, kind: str, record: dict) -> list[dict]:
        settings = record["settings"]
        access_key_id = _text(settings.get("access_key_id"))
        secret = _text(settings.get("access_key_secret"))
        if not access_key_id or not secret:
            raise BalanceQueryError(
                "needs_configuration", "云账户 AccessKey ID 和 Secret 需单独配置。"
            )
        url = (
            "https://business.aliyuncs.com/"
            if kind == "aliyun"
            else "https://billing.volcengineapi.com/"
        )
        fallback_currency = _text(settings.get("currency")) or "CNY"
        if kind == "aliyun":
            if self._host(url) != "business.aliyuncs.com":
                raise BalanceQueryError(
                    "invalid_configuration",
                    "阿里云 BSS 仅向官方 business.aliyuncs.com 地址发送凭证。",
                )
            params = {
                "Action": "QueryAccountBalance",
                "AccessKeyId": access_key_id,
                "Format": "JSON",
                "SignatureMethod": "HMAC-SHA1",
                "SignatureNonce": uuid.uuid4().hex,
                "SignatureVersion": "1.0",
                "Timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "Version": "2017-12-14",
            }
            params["Signature"] = _aliyun_signature(secret, params)
            payload = await self._http_json("GET", url, params=params)
            if not isinstance(payload, Mapping) or payload.get("Success") is False:
                raise BalanceQueryError(
                    "provider_error", "阿里云 BSS 余额查询失败或权限不足。"
                )
            data = payload.get("Data")
            if not isinstance(data, Mapping) or (
                "Code" in payload and str(payload.get("Code")) != "200"
            ):
                raise BalanceQueryError(
                    "provider_error", "阿里云 BSS 余额查询失败或权限不足。"
                )
            return [
                {
                    "余额": _parse_decimal(data.get("AvailableAmount")),
                    "币种": _currency(data.get("Currency"), fallback_currency),
                }
            ]

        region = _text(settings.get("region")) or "cn-beijing"
        if self._host(url) != "billing.volcengineapi.com":
            raise BalanceQueryError(
                "invalid_configuration",
                "火山引擎余额查询仅支持官方 billing.volcengineapi.com 地址。",
            )
        date_text = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        query = {"Action": "QueryBalanceAcct", "Version": "2022-01-01"}
        host = urlsplit(url).hostname or "billing.volcengineapi.com"
        authorization = _volcengine_signature(
            access_key_id, secret, region, date_text, host, query
        )
        payload = await self._http_json(
            "GET",
            url,
            params=query,
            headers={"X-Date": date_text, "Authorization": authorization},
        )
        data = payload.get("Result") if isinstance(payload, Mapping) else None
        if not isinstance(data, Mapping):
            raise BalanceQueryError(
                "provider_error", "火山引擎余额查询失败或权限不足。"
            )
        return [
            {
                "余额": _parse_decimal(data.get("AvailableBalance")),
                "币种": _currency(fallback_currency),
            }
        ]

    async def _query_custom(self, record: dict, api_key: str) -> list[dict]:
        settings = record["settings"]
        url = _text(settings.get("url"))
        method = _text(settings.get("method", "GET")).upper()
        auth_mode = _text(settings.get("auth_mode", "bearer")).lower()
        headers = settings.get("headers", {})
        body = settings.get("body", {})
        fields = settings.get("fields", {})
        if not url:
            raise BalanceQueryError(
                "needs_configuration", "自定义余额接口地址尚未配置。"
            )
        if method not in {"GET", "POST"}:
            raise BalanceQueryError(
                "invalid_configuration", "自定义请求方法仅支持 GET 或 POST。"
            )
        if auth_mode not in {"none", "bearer", "header"}:
            raise BalanceQueryError("invalid_configuration", "自定义认证方式无效。")
        if (
            not isinstance(headers, Mapping)
            or not isinstance(body, Mapping)
            or not isinstance(fields, Mapping)
        ):
            raise BalanceQueryError(
                "invalid_configuration",
                "自定义 headers、body 和 fields 必须为 JSON 对象。",
            )
        if "余额" not in fields or not _text(fields.get("余额")):
            raise BalanceQueryError(
                "needs_configuration", "请配置“余额”的 JSON Pointer。"
            )
        request_headers = {str(key): str(value) for key, value in headers.items()}
        if auth_mode == "bearer":
            if not api_key:
                raise BalanceQueryError(
                    "needs_configuration", "自定义余额接口需要 API Key。"
                )
            request_headers["Authorization"] = f"Bearer {api_key}"
        elif auth_mode == "header":
            if not api_key:
                raise BalanceQueryError(
                    "needs_configuration", "自定义余额接口需要 API Key。"
                )
            auth_header = _text(settings.get("auth_header")) or "Authorization"
            if not re.fullmatch(r"[A-Za-z0-9-]+", auth_header):
                raise BalanceQueryError("invalid_configuration", "认证请求头名称无效。")
            request_headers[auth_header] = api_key
        payload = await self._http_json(
            method,
            url,
            headers=request_headers,
            json_body=dict(body) if method == "POST" else None,
        )
        row = {}
        for field in _SUPPORTED_FIELDS:
            pointer = fields.get(field)
            if not _text(pointer):
                continue
            value = _read_pointer(payload, pointer)
            if field == "币种":
                row[field] = _currency(value)
            elif field == "余额" or field.endswith("余额"):
                row[field] = _parse_decimal(value, field)
            else:
                # Only known, explicitly mapped display values are exposed.
                row[field] = _parse_decimal(value, field)
        if "币种" not in row:
            row["币种"] = _currency(_text(settings.get("currency")) or "CNY")
        return [row]

    async def _http_json(self, method: str, url: str, **kwargs) -> object:
        try:
            if self.transport is not None:
                response = self.transport(method, url, **kwargs)
                if asyncio.iscoroutine(response):
                    response = await response
                status, payload = await self._unpack_response(response)
            else:
                if self._session is None:
                    import aiohttp

                    self._session = aiohttp.ClientSession(
                        timeout=aiohttp.ClientTimeout(total=self.timeout)
                    )
                request_kwargs = dict(kwargs)
                json_body = request_kwargs.pop("json_body", None)
                response = await self._session.request(
                    method,
                    url,
                    **request_kwargs,
                    json=json_body,
                )
                async with response:
                    status = int(response.status)
                    if status < 200 or status >= 300:
                        payload = None
                    else:
                        try:
                            payload = await response.json(content_type=None)
                        except Exception:
                            raise BalanceQueryError(
                                "invalid_response", "余额查询接口未返回有效 JSON。"
                            ) from None
        except BalanceQueryError:
            raise
        except asyncio.TimeoutError:
            raise
        except Exception:
            raise BalanceQueryError(
                "network", "无法连接余额查询接口，请检查网络和地址。"
            ) from None
        if status < 200 or status >= 300:
            if status in {401, 403}:
                raise BalanceQueryError(
                    "authentication", "余额查询认证失败或权限不足。"
                )
            if status == 404:
                raise BalanceQueryError("endpoint", "余额查询接口不存在，请检查地址。")
            raise BalanceQueryError("http", "余额查询接口返回 HTTP 错误。")
        if not isinstance(payload, (Mapping, list)):
            raise BalanceQueryError(
                "invalid_response", "余额查询接口未返回 JSON 对象或数组。"
            )
        return payload

    @staticmethod
    async def _unpack_response(response):
        if isinstance(response, tuple) and len(response) == 2:
            status, payload = response
            return int(status), payload
        if isinstance(response, Mapping) or isinstance(response, list):
            return 200, response
        status = int(getattr(response, "status", 200))
        json_method = getattr(response, "json", None)
        if callable(json_method):
            payload = json_method()
            if asyncio.iscoroutine(payload):
                payload = await payload
            return status, payload
        raise BalanceQueryError(
            "invalid_response", "测试 HTTP transport 返回格式无效。"
        )

    def _store_config(self):
        getter = getattr(self.store, "get_config", None)
        if not callable(getter):
            return {}
        try:
            return getter()
        except Exception:
            return {}

    def _is_current_record(self, record: Mapping) -> bool:
        config = self._store_config()
        records = (
            config.get("balance_providers") if isinstance(config, Mapping) else None
        )
        if not isinstance(records, list):
            return True
        for candidate in records:
            if isinstance(candidate, Mapping) and candidate.get("id") == record.get(
                "id"
            ):
                try:
                    normalized = self.normalize_records([dict(candidate)])[0]
                except (TypeError, ValueError):
                    return False
                return self._fingerprint(normalized) == self._fingerprint(record)
        return False

    @staticmethod
    def _fingerprint(record: Mapping) -> str:
        encoded = json.dumps(record, ensure_ascii=False, sort_keys=True, default=str)
        return hashlib.sha256(encoded.encode()).hexdigest()

    @staticmethod
    def _failure(record: Mapping, code: str, message: str) -> dict:
        return {
            "id": _text(record.get("id")),
            "alias": _text(record.get("alias")),
            "kind": _text(record.get("kind")),
            "ok": False,
            "rows": [],
            "error_code": code,
            "error": message,
            "queried_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "cached": False,
        }

    @classmethod
    def _result_from_invalid(cls, record: object) -> dict:
        mapping = record if isinstance(record, Mapping) else {}
        return cls._failure(mapping, "invalid_record", "余额记录配置格式无效。")

    async def close(self) -> None:
        """Close the shared HTTP session and stop pending requests."""
        if self._closed:
            return
        self._closed = True
        for task in list(self._inflight.values()):
            if not task.done():
                task.cancel()
        if self._inflight:
            await asyncio.gather(*self._inflight.values(), return_exceptions=True)
        self._inflight.clear()
        if self._session is not None:
            await self._session.close()
            self._session = None
        self._cache.clear()
