"""Canonical V3 configuration and V2 migration helpers."""

from __future__ import annotations

import copy
import re
import uuid
from collections.abc import Mapping

TEXT_DEFAULTS = {
    "quota_reply": "今日已用 {used} 次，总配额 {total} 次。",
    "unlimited_reply": "今日已用 {used} 次，个人额度不限。",
    "group_quota_reply": "本群今日已用 {used} 次，总配额 {total} 次。",
    "group_unlimited_reply": "本群今日已用 {used} 次，群额度不限。",
    "blacklist_reply": "当前会话已被禁止聊天，请联系管理员。",
    "exhausted_reply": "今日配额已用尽，请在下次重置后再试。",
    "music_quota_exhausted": "今日点歌额度已用尽：已用 {used}/{total}，剩余 {remaining}。",
    "music_shared_exhausted": "本次点歌需要 {cost} 个聊天额度（倍率 {multiplier}），当前剩余 {remaining}。",
    "music_blacklist": "当前会话已被禁止点歌。",
    "music_unavailable": "音乐额度联动暂不可用，请稍后再试。",
    "balance_deepseek": "{别名}：{余额} {币种}（赠送 {赠送余额}，充值 {充值余额}）",
    "balance_kimi": "{别名}：{余额} {币种}（代金券 {代金券余额}，现金 {现金余额}）",
    "balance_openrouter": "{别名}：{余额} {币种}",
    "balance_aliyun": "{别名}：{余额} {币种}",
    "balance_volcengine": "{别名}：{余额} {币种}",
    "balance_newapi": "{别名}（{查询范围}）：{余额} {币种}",
    "balance_custom": "{别名}：{余额} {币种}",
    "balance_all_failure": "{别名}：查询失败（{错误}）",
    "balance_failure": "{别名}：查询失败（{错误}）",
    "balance_not_found": "未找到供应商：{别名}。",
    "balance_unbound": "当前会话的供应商尚未配置余额查询。",
    "balance_empty": "尚未记录模型供应商。",
    "balance_usage_hint": "用法：{前缀}余额 所有／当前／别名／用量 1天|3天|1周",
    "usage_reply": "最近{时间}全局 Token 用量：{token}。",
    "usage_failure": "暂时无法读取全局 Token 用量。",
    "help_reply": "{命令列表}",
}

DEFAULT_CONFIG = {
    "schema_version": 2,
    "time_zone": "Asia/Shanghai",
    "reset_time": "00:00",
    "default_user_limit": 70,
    "default_group_limit": 200,
    "user_groups": [],
    "group_rules": [],
    "music_integration": {
        "enabled": False,
        "independent_quota": False,
        "multiplier": 1,
        "default_user_limit": 70,
        "default_group_limit": 200,
        "user_group_limits": [],
        "group_rules": [],
    },
    "balance_providers": [],
    "text_settings": TEXT_DEFAULTS,
}

LEGACY_TEXT_KEYS = tuple(TEXT_DEFAULTS.keys())[:6]
ALL_LEGACY_TEXT_KEYS = tuple(TEXT_DEFAULTS)
BALANCE_KINDS = {
    "deepseek",
    "kimi",
    "openrouter",
    "aliyun",
    "volcengine",
    "newapi",
    "custom_openai",
    "custom_anthropic",
    "custom",
}

_RESET_RE = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d$")
_QQ_RE = re.compile(r"^[0-9]+$")
_TEMPLATE_FIELDS = {
    "quota_reply": {"used", "total"},
    "unlimited_reply": {"used", "total"},
    "group_quota_reply": {"used", "total"},
    "group_unlimited_reply": {"used", "total"},
    "blacklist_reply": {"used", "total"},
    "exhausted_reply": {"used", "total"},
    "music_quota_exhausted": {"used", "total", "remaining", "cost", "multiplier"},
    "music_shared_exhausted": {"used", "total", "remaining", "cost", "multiplier"},
    "music_blacklist": {"used", "total", "remaining", "cost", "multiplier"},
    "music_unavailable": {"used", "total", "remaining", "cost", "multiplier"},
    "balance_deepseek": {"别名", "余额", "币种", "赠送余额", "充值余额"},
    "balance_kimi": {"别名", "余额", "币种", "代金券余额", "现金余额"},
    "balance_openrouter": {"别名", "余额", "币种"},
    "balance_aliyun": {"别名", "余额", "币种"},
    "balance_volcengine": {"别名", "余额", "币种"},
    "balance_newapi": {"别名", "余额", "币种", "查询范围"},
    "balance_custom": {"别名", "余额", "币种"},
    "balance_all_failure": {"别名", "错误"},
    "balance_failure": {"别名", "错误"},
    "balance_not_found": {"别名"},
    "balance_unbound": set(),
    "balance_empty": set(),
    "balance_usage_hint": {"前缀"},
    "usage_reply": {"时间", "token"},
    "usage_failure": set(),
    "help_reply": {"命令列表", "前缀"},
}


def default_config() -> dict:
    """Return a fresh copy of the V3 canonical defaults."""
    return copy.deepcopy(DEFAULT_CONFIG)


def migrate_text_settings(payload: Mapping[str, object]) -> dict:
    """Merge legacy root text values into the nested V3 text object.

    Existing non-default nested values take precedence over V2 root values.
    If the host inserted nested defaults before plugin startup, a customized
    legacy value wins over that default so migration does not lose user text.
    """
    nested_raw = payload.get("text_settings")
    nested = nested_raw if isinstance(nested_raw, Mapping) else {}
    output = {}
    for key, default in TEXT_DEFAULTS.items():
        nested_value = nested.get(key)
        legacy_value = payload.get(key)
        if nested_value is not None and nested_value != default:
            output[key] = copy.deepcopy(nested_value)
        elif key in payload:
            output[key] = copy.deepcopy(legacy_value)
        elif key in nested:
            output[key] = copy.deepcopy(nested_value)
        else:
            output[key] = copy.deepcopy(default)
    return output


def _validate_template(value: object, label: str, allowed: set[str]) -> str:
    if not isinstance(value, str):
        raise ValueError(f"text_settings.{label} 必须是字符串")
    index = 0
    while index < len(value):
        char = value[index]
        if char == "{":
            if index + 1 < len(value) and value[index + 1] == "{":
                index += 2
                continue
            end = value.find("}", index + 1)
            if end < 0 or value[index + 1 : end] not in allowed:
                expected = "、".join(f"{{{name}}}" for name in sorted(allowed))
                raise ValueError(
                    f"text_settings.{label} 只允许 {expected or '普通文本'} 和转义花括号"
                )
            index = end + 1
            continue
        if char == "}":
            if index + 1 < len(value) and value[index + 1] == "}":
                index += 2
                continue
            raise ValueError(f"text_settings.{label} 花括号格式无效")
        index += 1
    return value


def _normalize_balance_providers(value: object) -> list[dict]:
    """Normalize records using the balance adapter's canonical validator."""
    from .balance import normalize_records

    normalized = normalize_records(value)
    if not isinstance(normalized, list):
        raise ValueError("balance_providers 规范化结果无效")
    return normalized


def normalize_config(payload: object, base: Mapping[str, object]) -> dict:
    """Validate and migrate a canonical or legacy partial configuration."""
    if not isinstance(payload, Mapping):
        raise ValueError("配置必须是对象")
    allowed_keys = set(DEFAULT_CONFIG) | {
        "time_zone",
        "reset_time",
        "default_user_limit",
        "default_group_limit",
        "user_groups",
        "group_rules",
        "music_integration",
        *LEGACY_TEXT_KEYS,
    }
    unknown = set(payload) - allowed_keys
    if unknown:
        raise ValueError(f"未知配置项：{', '.join(sorted(str(x) for x in unknown))}")

    raw = copy.deepcopy(default_config())
    raw.update(copy.deepcopy(dict(base)))
    raw["text_settings"] = migrate_text_settings(raw)
    if "music_integration" in payload:
        music_payload = payload["music_integration"]
        if not isinstance(music_payload, Mapping):
            raise ValueError("music_integration 必须是对象")
        merged_music = copy.deepcopy(
            raw.get("music_integration", DEFAULT_CONFIG["music_integration"])
        )
        merged_music.update(copy.deepcopy(dict(music_payload)))
        raw["music_integration"] = merged_music
    raw.update(
        {
            key: copy.deepcopy(value)
            for key, value in payload.items()
            if key not in ALL_LEGACY_TEXT_KEYS
            and key not in {"text_settings", "music_integration"}
        }
    )
    nested_update = payload.get("text_settings")
    if nested_update is not None:
        if not isinstance(nested_update, Mapping):
            raise ValueError("text_settings 必须是对象")
        unknown_text = set(nested_update) - set(TEXT_DEFAULTS)
        if unknown_text:
            raise ValueError(
                f"text_settings 含未知字段：{', '.join(sorted(str(x) for x in unknown_text))}"
            )
        merged_text = copy.deepcopy(raw["text_settings"])
        merged_text.update(copy.deepcopy(dict(nested_update)))
        raw["text_settings"] = merged_text
    for key in LEGACY_TEXT_KEYS:
        if key in payload and (
            not isinstance(nested_update, Mapping) or key not in nested_update
        ):
            raw["text_settings"][key] = copy.deepcopy(payload[key])

    schema_version = raw.get("schema_version", 2)
    if (
        isinstance(schema_version, bool)
        or not isinstance(schema_version, int)
        or schema_version not in (1, 2)
    ):
        raise ValueError("schema_version 必须为 1 或 2")
    if raw.get("time_zone") != "Asia/Shanghai":
        raise ValueError("time_zone 仅支持 Asia/Shanghai")
    reset_time = raw.get("reset_time")
    if not isinstance(reset_time, str) or not _RESET_RE.fullmatch(reset_time):
        raise ValueError("reset_time 必须为 HH:mm 格式")

    limits = {}
    for key in ("default_user_limit", "default_group_limit"):
        value = raw.get(key)
        if isinstance(value, bool) or not isinstance(value, int) or value < -1:
            raise ValueError(f"{key} 必须是 -1、0 或正整数")
        limits[key] = value

    user_groups_raw = raw.get("user_groups")
    if not isinstance(user_groups_raw, list):
        raise ValueError("user_groups 必须是数组")
    user_groups = []
    group_ids = set()
    group_names = set()
    member_to_group = {}
    for index, item in enumerate(user_groups_raw):
        label = f"user_groups[{index}]"
        if not isinstance(item, Mapping):
            raise ValueError(f"{label} 必须是对象")
        if set(item) - {"id", "name", "daily_limit", "user_ids"} or not {
            "name",
            "daily_limit",
            "user_ids",
        }.issubset(item):
            raise ValueError(f"{label} 必须包含 name、daily_limit、user_ids")
        raw_group_id = item.get("id")
        group_id = (
            f"group-{uuid.uuid4().hex}"
            if raw_group_id is None
            or (isinstance(raw_group_id, str) and not raw_group_id.strip())
            else raw_group_id.strip()
            if isinstance(raw_group_id, str)
            else None
        )
        name = item.get("name")
        if not isinstance(group_id, str) or not group_id:
            raise ValueError(f"{label}.id 必须是非空字符串")
        if not isinstance(name, str) or not name.strip():
            raise ValueError(f"{label}.name 不能为空")
        name = name.strip()
        if group_id.casefold() in group_ids or name.casefold() in group_names:
            raise ValueError("user_groups 的 id 和 name 必须唯一")
        group_ids.add(group_id.casefold())
        group_names.add(name.casefold())
        daily_limit = item.get("daily_limit")
        if (
            isinstance(daily_limit, bool)
            or not isinstance(daily_limit, int)
            or daily_limit < -1
        ):
            raise ValueError(f"{label}.daily_limit 必须是 -1、0 或正整数")
        user_ids_raw = item.get("user_ids")
        if not isinstance(user_ids_raw, list):
            raise ValueError(f"{label}.user_ids 必须是数组")
        user_ids = []
        for member_index, member in enumerate(user_ids_raw):
            member_id = member.strip() if isinstance(member, str) else ""
            if not _QQ_RE.fullmatch(member_id):
                raise ValueError(
                    f"{label}.user_ids[{member_index}] 必须是 QQ 数字字符串"
                )
            if member_id in member_to_group:
                raise ValueError("一个用户只能属于一个 user_group")
            member_to_group[member_id] = group_id
            user_ids.append(member_id)
        user_groups.append(
            {
                "id": group_id,
                "name": name,
                "daily_limit": daily_limit,
                "user_ids": user_ids,
            }
        )

    group_rules_raw = raw.get("group_rules")
    if not isinstance(group_rules_raw, list):
        raise ValueError("group_rules 必须是数组")
    group_rules = []
    rule_ids = set()
    for index, item in enumerate(group_rules_raw):
        label = f"group_rules[{index}]"
        if not isinstance(item, Mapping) or set(item) != {"group_id", "daily_limit"}:
            raise ValueError(f"{label} 必须包含 group_id、daily_limit")
        group_id = item.get("group_id")
        group_id = group_id.strip() if isinstance(group_id, str) else ""
        daily_limit = item.get("daily_limit")
        if not isinstance(group_id, str) or not _QQ_RE.fullmatch(group_id):
            raise ValueError(f"{label}.group_id 必须是 QQ 数字字符串")
        if group_id in rule_ids:
            raise ValueError("group_rules 的 group_id 必须唯一")
        if (
            isinstance(daily_limit, bool)
            or not isinstance(daily_limit, int)
            or daily_limit < -1
        ):
            raise ValueError(f"{label}.daily_limit 必须是 -1、0 或正整数")
        rule_ids.add(group_id)
        group_rules.append({"group_id": group_id, "daily_limit": daily_limit})

    music_raw = raw.get("music_integration")
    music_keys = {
        "enabled",
        "independent_quota",
        "multiplier",
        "default_user_limit",
        "default_group_limit",
        "user_group_limits",
        "group_rules",
    }
    if not isinstance(music_raw, Mapping) or set(music_raw) - music_keys:
        raise ValueError("music_integration 配置字段无效")
    music = copy.deepcopy(DEFAULT_CONFIG["music_integration"])
    music.update(copy.deepcopy(dict(music_raw)))
    for key in ("enabled", "independent_quota"):
        if not isinstance(music.get(key), bool):
            raise ValueError(f"music_integration.{key} 必须是布尔值")
    multiplier = music.get("multiplier")
    if (
        isinstance(multiplier, bool)
        or not isinstance(multiplier, int)
        or multiplier < 1
    ):
        raise ValueError("music_integration.multiplier 必须是正整数")
    music_limits = {}
    for key in ("default_user_limit", "default_group_limit"):
        value = music.get(key)
        if isinstance(value, bool) or not isinstance(value, int) or value < -1:
            raise ValueError(f"music_integration.{key} 必须是 -1、0 或正整数")
        music_limits[key] = value

    user_group_limits_raw = music.get("user_group_limits")
    if not isinstance(user_group_limits_raw, list):
        raise ValueError("music_integration.user_group_limits 必须是数组")
    known_user_group_ids = {item["id"] for item in user_groups}
    user_group_limits = []
    configured_user_groups = set()
    for index, item in enumerate(user_group_limits_raw):
        label = f"music_integration.user_group_limits[{index}]"
        if not isinstance(item, Mapping) or set(item) != {
            "user_group_id",
            "daily_limit",
        }:
            raise ValueError(f"{label} 必须包含 user_group_id、daily_limit")
        user_group_id = item.get("user_group_id")
        daily_limit = item.get("daily_limit")
        if (
            not isinstance(user_group_id, str)
            or user_group_id not in known_user_group_ids
        ):
            raise ValueError(f"{label}.user_group_id 必须引用已有用户组")
        if user_group_id in configured_user_groups:
            raise ValueError(
                "music_integration.user_group_limits 的 user_group_id 必须唯一"
            )
        if (
            isinstance(daily_limit, bool)
            or not isinstance(daily_limit, int)
            or daily_limit < -1
        ):
            raise ValueError(f"{label}.daily_limit 必须是 -1、0 或正整数")
        configured_user_groups.add(user_group_id)
        user_group_limits.append(
            {"user_group_id": user_group_id, "daily_limit": daily_limit}
        )

    music_group_rules_raw = music.get("group_rules")
    if not isinstance(music_group_rules_raw, list):
        raise ValueError("music_integration.group_rules 必须是数组")
    music_group_rules = []
    music_rule_ids = set()
    for index, item in enumerate(music_group_rules_raw):
        label = f"music_integration.group_rules[{index}]"
        if not isinstance(item, Mapping) or set(item) != {"group_id", "daily_limit"}:
            raise ValueError(f"{label} 必须包含 group_id、daily_limit")
        group_id = item.get("group_id")
        group_id = group_id.strip() if isinstance(group_id, str) else ""
        daily_limit = item.get("daily_limit")
        if not _QQ_RE.fullmatch(group_id):
            raise ValueError(f"{label}.group_id 必须是 QQ 数字字符串")
        if group_id in music_rule_ids:
            raise ValueError("music_integration.group_rules 的 group_id 必须唯一")
        if (
            isinstance(daily_limit, bool)
            or not isinstance(daily_limit, int)
            or daily_limit < -1
        ):
            raise ValueError(f"{label}.daily_limit 必须是 -1、0 或正整数")
        music_rule_ids.add(group_id)
        music_group_rules.append({"group_id": group_id, "daily_limit": daily_limit})
    music = {
        **{key: music[key] for key in ("enabled", "independent_quota", "multiplier")},
        **music_limits,
        "user_group_limits": user_group_limits,
        "group_rules": music_group_rules,
    }

    text_raw = raw.get("text_settings")
    if not isinstance(text_raw, Mapping):
        raise ValueError("text_settings 必须是对象")
    text_settings = {}
    for key, default in TEXT_DEFAULTS.items():
        value = text_raw.get(key, default)
        text_settings[key] = _validate_template(value, key, _TEMPLATE_FIELDS[key])

    return {
        "schema_version": 2,
        "time_zone": "Asia/Shanghai",
        "reset_time": reset_time,
        **limits,
        "user_groups": user_groups,
        "group_rules": group_rules,
        "music_integration": music,
        "balance_providers": _normalize_balance_providers(
            raw.get("balance_providers", [])
        ),
        "text_settings": text_settings,
    }
