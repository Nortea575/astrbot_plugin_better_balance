"""Runtime quota adapters for AstrBot and the supported chat plugins."""

from __future__ import annotations

import contextvars
import inspect
import os
import sys
import uuid
from dataclasses import dataclass, field
from functools import wraps
from typing import Any, Callable

from .quota import Reservation, Scope, Store


@dataclass
class _EventState:
    event: Any
    scope: Scope
    reservation: Reservation | None = None
    status: str = "new"
    denial_notified: bool = False
    sends_wrapped: bool = False
    send_originals: dict[str, tuple[Any, bool]] = field(default_factory=dict)
    direct_workflow: Any = None


@dataclass
class _DirectWorkflow:
    context: Any
    scope: Scope
    reservation: Reservation
    events: list[Any] = field(default_factory=list)


class QuotaCompatibility:
    """Reserve quota before generation and settle it after delivery.

    The adapter patches only the AstrBot workflow stages and plugin entry
    points that require accounting. It never wraps Provider instances.
    """

    STATE_ATTR = "_better_balance_quota_state"

    def __init__(self, store: Store) -> None:
        self.store = store
        self._active = True
        self._events: dict[int, _EventState] = {}
        self._patches: list[tuple[type, str, Any, Any, bool]] = []
        self._installed: dict[str, bool] = {}
        self._discovered: dict[str, bool] = {}
        self._context_sends: dict[int, tuple[Any, Any, Any, int]] = {}
        self._active_direct_reservation: contextvars.ContextVar[
            _DirectWorkflow | None
        ] = contextvars.ContextVar(
            f"better_balance_direct_send_{id(self)}", default=None
        )
        self._active_proactive_events: contextvars.ContextVar[list[Any] | None] = (
            contextvars.ContextVar(f"better_balance_proactive_events_{id(self)}", default=None)
        )

    @staticmethod
    def scope_from_event(event: Any) -> Scope | None:
        """Resolve an event to a platform-isolated private or group scope."""
        platform_id = ""
        for name in ("get_platform_id",):
            getter = getattr(event, name, None)
            if callable(getter):
                try:
                    platform_id = str(getter() or "").strip()
                except Exception:
                    platform_id = ""
                if platform_id:
                    break

        umo = str(getattr(event, "unified_msg_origin", "") or "")
        parts = umo.split(":")
        if not platform_id and len(parts) >= 3:
            platform_id = parts[0].strip()
        if not platform_id:
            return None

        private = False
        is_private = getattr(event, "is_private_chat", None)
        if callable(is_private):
            try:
                private = bool(is_private())
            except Exception:
                private = False
        elif is_private is not None:
            private = bool(is_private)

        group_id = ""
        get_group_id = getattr(event, "get_group_id", None)
        if callable(get_group_id):
            try:
                group_id = str(get_group_id() or "").strip()
            except Exception:
                group_id = ""

        if not private and len(parts) >= 3:
            message_type = parts[1].casefold()
            if "group" in message_type:
                group_id = group_id or parts[2].strip()
            elif any(tag in message_type for tag in ("friend", "private", "direct")):
                private = True

        if private:
            target_id = parts[2].strip() if len(parts) >= 3 else ""
            if not target_id:
                get_sender_id = getattr(event, "get_sender_id", None)
                try:
                    target_id = (
                        str(get_sender_id() or "").strip()
                        if callable(get_sender_id)
                        else ""
                    )
                except Exception:
                    target_id = ""
                if not target_id:
                    target_id = str(getattr(event, "sender_id", "") or "").strip()
            return Scope(platform_id, "private", target_id) if target_id else None

        if group_id:
            return Scope(platform_id, "group", group_id)
        return None

    @staticmethod
    def _is_model_result(result: Any) -> bool:
        """Check the sent result object itself, excluding tool/plugin messages."""
        if result is None:
            return False
        for name in ("is_model_result", "is_llm_result"):
            marker = getattr(result, name, None)
            if callable(marker):
                try:
                    if marker():
                        return True
                except Exception:
                    pass
        content_type = getattr(result, "result_content_type", None)
        if content_type is None:
            return False
        name = str(getattr(content_type, "name", content_type)).casefold()
        return any(tag in name for tag in ("model", "llm", "stream"))

    def _event_state(self, event: Any) -> _EventState | None:
        state = self._events.get(id(event))
        if state is not None and state.event is event:
            return state
        value = getattr(event, self.STATE_ATTR, None)
        if isinstance(value, _EventState) and value.event is event:
            self._events[id(event)] = value
            return value
        return None

    def _store_event_state(self, state: _EventState) -> None:
        self._events[id(state.event)] = state
        try:
            setattr(state.event, self.STATE_ATTR, state)
        except Exception:
            pass

    def reserve_event(self, event: Any) -> Reservation | None:
        """Reserve one slot for this message's next LLM reply round."""
        if not self._active:
            return None
        scope = self.scope_from_event(event)
        if scope is None:
            return None
        state = self._event_state(event)
        if (
            state is not None
            and state.status in {"pending", "committed"}
            and state.reservation
        ):
            return state.reservation
        if state is not None and state.status == "denied":
            return None
        direct = self._active_direct_reservation.get()
        if direct is not None and direct.scope == scope:
            state = _EventState(
                event=event,
                scope=scope,
                reservation=direct.reservation,
                status="pending",
                direct_workflow=direct,
            )
            direct.events.append(event)
            self._store_event_state(state)
            self._wrap_event_sends(event)
            return direct.reservation
        state = _EventState(event=event, scope=scope)
        reservation = self.store.reserve(scope, f"event:{uuid.uuid4().hex}")
        if reservation is None:
            state.status = "denied"
            self._store_event_state(state)
            return None
        state.reservation = reservation
        state.status = "pending"
        self._store_event_state(state)
        self._wrap_event_sends(event)
        return reservation

    def reserve_scope(self, scope: Scope, request_id: str) -> Reservation | None:
        """Reserve directly for a scheduled plugin workflow without an event."""
        if not self._active:
            return None
        return self.store.reserve(scope, request_id)

    def commit_event(self, event: Any) -> bool:
        state = self._event_state(event)
        if not state or state.status != "pending" or not state.reservation:
            return False
        succeeded = self.store.commit(state.reservation.token)
        if succeeded:
            state.status = "committed"
        return succeeded

    def release_event(self, event: Any) -> bool:
        state = self._event_state(event)
        if not state or state.status != "pending" or not state.reservation:
            return False
        succeeded = self.store.release(state.reservation.token)
        state.status = "released"
        state.reservation = None
        return succeeded

    def commit_reservation(self, reservation: Reservation) -> bool:
        if not self._active:
            return False
        succeeded = self.store.commit(reservation.token)
        direct = self._active_direct_reservation.get()
        if succeeded and direct and direct.reservation.token == reservation.token:
            for event in direct.events:
                state = self._event_state(event)
                if state and state.reservation and state.reservation.token == reservation.token:
                    state.status = "committed"
        return succeeded

    def release_reservation(self, reservation: Reservation | None) -> bool:
        if not self._active or reservation is None:
            return False
        return self.store.release(reservation.token)

    def _wrap_event_sends(self, event: Any) -> None:
        state = self._event_state(event)
        if state is None or getattr(state, "sends_wrapped", False):
            return
        originals: dict[str, tuple[Any, bool]] = {}

        def had_instance_value(name: str) -> bool:
            try:
                return name in vars(event)
            except TypeError:
                return True

        send = getattr(event, "send", None)
        if callable(send):
            originals["send"] = (send, had_instance_value("send"))

            @wraps(send)
            async def tracked_send(message: Any, *args: Any, **kwargs: Any) -> Any:
                result = await send(message, *args, **kwargs)
                if (
                    self._active
                    and result is not False
                    and self._is_model_result(message)
                ):
                    self.commit_event(event)
                return result

            try:
                setattr(event, "send", tracked_send)
            except Exception:
                originals.pop("send", None)

        send_streaming = getattr(event, "send_streaming", None)
        if callable(send_streaming):
            originals["send_streaming"] = (
                send_streaming,
                had_instance_value("send_streaming"),
            )

            @wraps(send_streaming)
            async def tracked_streaming(generator: Any, *args: Any, **kwargs: Any) -> Any:
                delivered = False

                async def watched_stream():
                    nonlocal delivered
                    async for chunk in generator:
                        chunk_type = str(getattr(chunk, "type", "")).casefold()
                        chunk_chain = getattr(chunk, "chain", None)
                        if (
                            chunk_chain
                            and chunk_type
                            not in {"break", "reasoning", "tool_call", "agent_stats"}
                        ):
                            delivered = True
                        yield chunk

                result = await send_streaming(watched_stream(), *args, **kwargs)
                if (
                    self._active
                    and result is not False
                    and delivered
                    and self._current_result_is_model(event)
                ):
                    self.commit_event(event)
                return result

            try:
                setattr(event, "send_streaming", tracked_streaming)
            except Exception:
                originals.pop("send_streaming", None)

        state.send_originals = originals
        state.sends_wrapped = True

    def _restore_event_sends(self, event: Any) -> None:
        state = self._event_state(event)
        if state is None:
            return
        for name, (original, had_instance_value) in state.send_originals.items():
            current = getattr(event, name, None)
            if getattr(current, "__wrapped__", None) is original or current is original:
                try:
                    if had_instance_value:
                        setattr(event, name, original)
                    else:
                        delattr(event, name)
                except Exception:
                    pass
        state.send_originals = {}
        state.sends_wrapped = False

    def _current_result_is_model(self, event: Any) -> bool:
        getter = getattr(event, "get_result", None)
        if not callable(getter):
            return False
        try:
            return self._is_model_result(getter())
        except Exception:
            return False

    def settle_event_workflow(self, event: Any) -> None:
        """Release unfinished reservations and restore per-event send wrappers."""
        self.release_event(event)
        self._restore_event_sends(event)
        state = self._event_state(event)
        if state and state.status in {"committed", "released", "denied"}:
            self._events.pop(id(event), None)
            try:
                delattr(event, self.STATE_ATTR)
            except Exception:
                pass

    def _denial_text(self, scope: Scope) -> str:
        snapshot = self.store.snapshot(scope)
        key = (
            "blacklist_reply"
            if snapshot["mode"] == "blacklist"
            else "exhausted_reply"
        )
        text_settings = self.store.get_config().get("text_settings", {})
        template = str(
            text_settings.get(key, "")
            if isinstance(text_settings, dict)
            else ""
        )
        try:
            return template.format_map(
                {"used": snapshot["used"], "total": snapshot["total"]}
            )
        except (KeyError, ValueError):
            return template

    @staticmethod
    def _is_private(event: Any, scope: Scope | None = None) -> bool:
        if scope is not None:
            return scope.kind == "private"
        resolved = QuotaCompatibility.scope_from_event(event)
        return bool(resolved and resolved.kind == "private")

    @staticmethod
    def _is_group_mention(event: Any) -> bool:
        self_id = ""
        getter = getattr(event, "get_self_id", None)
        if callable(getter):
            try:
                self_id = str(getter() or "")
            except Exception:
                pass
        if not self_id:
            self_id = str(getattr(event, "self_id", "") or "")
        message_obj = getattr(event, "message_obj", None)
        components = getattr(message_obj, "message", None) or []
        for component in components:
            raw_type = getattr(component, "type", "")
            comp_type = str(getattr(raw_type, "value", raw_type)).casefold()
            target = getattr(component, "qq", getattr(component, "target", None))
            component_name = type(component).__name__.casefold()
            if (
                (comp_type == "at" or component_name == "at")
                and self_id
                and str(target) == self_id
            ):
                return True
        if not self_id:
            marker = getattr(event, "is_at", None)
            if callable(marker):
                try:
                    return bool(marker())
                except Exception:
                    return False
            return marker is True
        return False

    @staticmethod
    def is_system_command(event: Any) -> bool:
        """Identify commands from AstrBot's activated handler metadata."""
        getter = getattr(event, "get_extra", None)
        if not callable(getter):
            return False
        try:
            activated_handlers = getter("activated_handlers") or []
        except Exception:
            return False
        for handler in activated_handlers:
            for event_filter in getattr(handler, "event_filters", ()) or ():
                filter_type = event_filter if isinstance(event_filter, type) else type(event_filter)
                if any(
                    base.__name__ in {"CommandFilter", "CommandGroupFilter"}
                    for base in getattr(filter_type, "__mro__", (filter_type,))
                ):
                    return True
        return False

    async def preflight_event(self, event: Any) -> bool:
        """Reserve before message handlers can call direct provider methods."""
        if self.is_system_command(event):
            return True
        if self.scope_from_event(event) is None:
            stop = getattr(event, "stop_event", None)
            if callable(stop):
                stop()
            return False
        if self.reserve_event(event) is not None:
            return True
        await self.deny_event(event)
        return False

    async def deny_event(self, event: Any) -> None:
        """Stop this event without a provider call and apply the notice policy."""
        state = self._event_state(event)
        scope = state.scope if state else self.scope_from_event(event)
        if scope is None:
            stop = getattr(event, "stop_event", None)
            if callable(stop):
                stop()
            return
        background = False
        getter = getattr(event, "get_extra", None)
        if callable(getter):
            try:
                background = bool(getter("_better_balance_background"))
            except Exception:
                pass
        should_notify = not background and (
            scope.kind == "private" or self._is_group_mention(event)
        )
        if should_notify and not (state and state.denial_notified):
            text = self._denial_text(scope)
            sender = getattr(event, "send", None)
            plain_result = getattr(event, "plain_result", None)
            if callable(sender) and callable(plain_result):
                try:
                    await sender(plain_result(text))
                except Exception:
                    pass
            if state:
                state.denial_notified = True
        stop = getattr(event, "stop_event", None)
        if callable(stop):
            stop()

    def _install_context_send(self, context: Any) -> bool:
        key = id(context)
        existing = self._context_sends.get(key)
        if existing and existing[0] is context:
            self._context_sends[key] = (context, existing[1], existing[2], existing[3] + 1)
            return True
        original = getattr(context, "send_message", None)
        if not callable(original):
            return False

        @wraps(original)
        async def tracked_send(*args: Any, **kwargs: Any) -> Any:
            result = await original(*args, **kwargs)
            active = self._active_direct_reservation.get()
            if self._active and active and id(active.context) == key and result:
                self.commit_reservation(active.reservation)
            return result

        try:
            setattr(context, "send_message", tracked_send)
        except Exception:
            return False
        self._context_sends[key] = (context, original, tracked_send, 1)
        return True

    def _release_context_send(self, context: Any) -> None:
        key = id(context)
        record = self._context_sends.get(key)
        if not record or record[0] is not context:
            return
        count = record[3] - 1
        if count > 0:
            self._context_sends[key] = (record[0], record[1], record[2], count)
            return
        if getattr(context, "send_message", None) is record[2]:
            try:
                setattr(context, "send_message", record[1])
            except Exception:
                pass
        self._context_sends.pop(key, None)

    def _patch_descriptor(self, owner: type, name: str, builder: Callable[[Any], Any]) -> bool:
        try:
            descriptor = inspect.getattr_static(owner, name)
        except AttributeError:
            return False
        function = descriptor.__func__ if isinstance(descriptor, (staticmethod, classmethod)) else descriptor
        if getattr(function, "_better_balance_wrapper", False):
            return False
        wrapped = builder(function)
        setattr(wrapped, "_better_balance_wrapper", True)
        if isinstance(descriptor, staticmethod):
            replacement = staticmethod(wrapped)
        elif isinstance(descriptor, classmethod):
            replacement = classmethod(wrapped)
        else:
            replacement = wrapped
        setattr(owner, name, replacement)
        self._patches.append(
            (owner, name, replacement, descriptor, name in owner.__dict__)
        )
        return True

    def _patch_agent_stage(self, owner: type) -> None:
        def build(original: Any) -> Any:
            if inspect.isasyncgenfunction(original):

                @wraps(original)
                async def wrapped(instance: Any, event: Any, *args: Any, **kwargs: Any):
                    try:
                        async for item in original(instance, event, *args, **kwargs):
                            yield item
                    except BaseException:
                        self.release_event(event)
                        raise
                    finally:
                        if not self._current_result_is_model(event):
                            self.release_event(event)

                return wrapped
            return original

        self._patch_descriptor(owner, "process", build)

    def _patch_process_stage(self, owner: type) -> None:
        """Release preflight slots when a message never yields a model result."""

        def build(original: Any) -> Any:
            if inspect.isasyncgenfunction(original):

                @wraps(original)
                async def wrapped(instance: Any, event: Any, *args: Any, **kwargs: Any):
                    try:
                        async for item in original(instance, event, *args, **kwargs):
                            yield item
                    except BaseException:
                        self.release_event(event)
                        raise
                    finally:
                        if not self._current_result_is_model(event):
                            self.settle_event_workflow(event)

                return wrapped
            return original

        self._patch_descriptor(owner, "process", build)

    def _patch_respond_stage(self, owner: type) -> None:
        def build(original: Any) -> Any:
            if inspect.iscoroutinefunction(original):

                @wraps(original)
                async def wrapped(instance: Any, event: Any, *args: Any, **kwargs: Any):
                    try:
                        return await original(instance, event, *args, **kwargs)
                    finally:
                        self.settle_event_workflow(event)

                return wrapped
            return original

        self._patch_descriptor(owner, "process", build)

    def _patch_proactive_generation(self, owner: type) -> None:
        def build(original: Any) -> Any:
            @wraps(original)
            async def wrapped(instance: Any, *args: Any, **kwargs: Any):
                try:
                    bound = inspect.signature(original).bind_partial(
                        instance, *args, **kwargs
                    )
                    event = bound.arguments.get("event")
                except (TypeError, ValueError):
                    event = kwargs.get("event")
                if event is not None:
                    active_events = self._active_proactive_events.get()
                    if active_events is not None and event not in active_events:
                        active_events.append(event)
                    setter = getattr(event, "set_extra", None)
                    if callable(setter):
                        try:
                            setter("_better_balance_background", True)
                        except Exception:
                            pass
                try:
                    result = await original(instance, *args, **kwargs)
                except BaseException:
                    if event is not None:
                        self.settle_event_workflow(event)
                    raise
                response = result[0] if isinstance(result, tuple) and result else result
                if response is None and event is not None:
                    self.settle_event_workflow(event)
                return result

            return wrapped

        self._patch_descriptor(owner, "_generate_llm_response", build)

    def _patch_proactive_sender(self, owner: type) -> None:
        def build(original: Any) -> Any:
            @wraps(original)
            async def wrapped(instance: Any, *args: Any, **kwargs: Any):
                try:
                    bound = inspect.signature(original).bind_partial(
                        instance, *args, **kwargs
                    )
                    event = bound.arguments.get("event")
                except (TypeError, ValueError):
                    event = kwargs.get("event")
                try:
                    sent = await original(instance, *args, **kwargs)
                except BaseException:
                    if event is not None:
                        self.settle_event_workflow(event)
                    raise
                if event is not None:
                    if sent:
                        self.commit_event(event)
                    self.settle_event_workflow(event)
                return sent

            return wrapped

        self._patch_descriptor(owner, "_send_proactive_message", build)

    def _patch_proactive_chat_flow(self, owner: type) -> None:
        def build(original: Any) -> Any:
            @wraps(original)
            async def wrapped(instance: Any, *args: Any, **kwargs: Any):
                events: list[Any] = []
                token = self._active_proactive_events.set(events)
                try:
                    return await original(instance, *args, **kwargs)
                finally:
                    for event in events:
                        self.settle_event_workflow(event)
                    self._active_proactive_events.reset(token)

            return wrapped

        self._patch_descriptor(owner, "check_and_chat", build)

    def _patch_decision_ai(self, owner: type, name: str, decision_result: bool) -> None:
        def build(original: Any) -> Any:
            @wraps(original)
            async def wrapped(*args: Any, **kwargs: Any):
                event = kwargs.get("event")
                if event is None:
                    event = next(
                        (value for value in args if self.scope_from_event(value)), None
                    )
                if event is None:
                    return False if decision_result else ""
                reservation = self.reserve_event(event)
                if reservation is None:
                    await self.deny_event(event)
                    return False if decision_result else ""
                try:
                    result = await original(*args, **kwargs)
                except BaseException:
                    self.release_event(event)
                    raise
                if decision_result and not result:
                    self.release_event(event)
                elif not decision_result and not result:
                    self.release_event(event)
                return result

            return wrapped

        self._patch_descriptor(owner, name, build)

    def _patch_group_proactive_manager(self, owner: type) -> None:
        def build(original: Any) -> Any:
            @wraps(original)
            async def wrapped(cls: type, *args: Any, **kwargs: Any):
                try:
                    values = inspect.signature(original).bind_partial(
                        cls, *args, **kwargs
                    ).arguments
                except (TypeError, ValueError):
                    return None
                context = values.get("context")
                platform_id = str(values.get("platform_id") or "").strip()
                chat_id = str(values.get("chat_id") or "").strip()
                if context is None or not platform_id or not chat_id:
                    return None
                kind = "private" if values.get("is_private") else "group"
                scope = Scope(platform_id, kind, chat_id)
                reservation = self.reserve_scope(scope, f"chatplus-proactive:{uuid.uuid4().hex}")
                if reservation is None:
                    return None
                if not self._install_context_send(context):
                    self.release_reservation(reservation)
                    return None
                workflow = _DirectWorkflow(context, scope, reservation)
                token = self._active_direct_reservation.set(workflow)
                try:
                    await original(cls, *args, **kwargs)
                except BaseException:
                    self.release_reservation(reservation)
                    raise
                finally:
                    for event in workflow.events:
                        self.settle_event_workflow(event)
                    self._active_direct_reservation.reset(token)
                    self.release_reservation(reservation)
                    self._release_context_send(context)
                return None

            return wrapped

        self._patch_descriptor(owner, "_process_proactive_chat_simplified", build)

    def install(self) -> None:
        """Install narrow, reload-safe wrappers for the detected runtime modules."""
        if not self._active:
            return
        self._installed = {
            "incoming_message_preflight": True,
            "chatplus_media_provider_gate": True,
            "core_process_stage": False,
            "core_agent_request": False,
            "core_respond_stage": False,
            "proactive_chat_llm": False,
            "proactive_chat_sender": False,
            "proactive_chat_flow": False,
            "group_chat_decision_ai": False,
            "group_chat_proactive_queue": False,
            "private_chat_proactive_queue": False,
        }
        self._discovered = {name: value for name, value in self._installed.items()}
        self._discovered["incoming_message_preflight"] = True
        self._discovered["chatplus_media_provider_gate"] = True
        agent_module = sys.modules.get("astrbot.core.pipeline.process_stage.method.agent_request")
        agent_class = getattr(agent_module, "AgentRequestSubStage", None)
        if isinstance(agent_class, type):
            self._discovered["core_agent_request"] = True
            self._patch_agent_stage(agent_class)
            self._installed["core_agent_request"] = self._is_patched(agent_class, "process")
        process_module = sys.modules.get("astrbot.core.pipeline.process_stage.stage")
        process_class = getattr(process_module, "ProcessStage", None)
        if isinstance(process_class, type):
            self._discovered["core_process_stage"] = True
            self._patch_process_stage(process_class)
            self._installed["core_process_stage"] = self._is_patched(process_class, "process")
        respond_module = sys.modules.get("astrbot.core.pipeline.respond.stage")
        respond_class = getattr(respond_module, "RespondStage", None)
        if isinstance(respond_class, type):
            self._discovered["core_respond_stage"] = True
            self._patch_respond_stage(respond_class)
            self._installed["core_respond_stage"] = self._is_patched(respond_class, "process")

        for module in tuple(sys.modules.values()):
            filename = str(getattr(module, "__file__", "")).replace("\\", "/").casefold()
            if "/astrbot_plugin_proactive_chat/core/llm_adapter.py" in filename:
                mixin = getattr(module, "LlmMixin", None)
                if isinstance(mixin, type):
                    self._discovered["proactive_chat_llm"] = True
                    self._patch_proactive_generation(mixin)
                    self._installed["proactive_chat_llm"] = self._is_patched(
                        mixin, "_generate_llm_response"
                    )
            elif "/astrbot_plugin_proactive_chat/core/message_sender.py" in filename:
                mixin = getattr(module, "SenderMixin", None)
                if isinstance(mixin, type):
                    self._discovered["proactive_chat_sender"] = True
                    self._patch_proactive_sender(mixin)
                    self._installed["proactive_chat_sender"] = self._is_patched(
                        mixin, "_send_proactive_message"
                    )
            elif "/astrbot_plugin_proactive_chat/core/chat_flow.py" in filename:
                mixin = getattr(module, "ProactiveCoreMixin", None)
                if isinstance(mixin, type):
                    self._discovered["proactive_chat_flow"] = True
                    self._patch_proactive_chat_flow(mixin)
                    self._installed["proactive_chat_flow"] = self._is_patched(
                        mixin, "check_and_chat"
                    )
            elif "/astrbot_plugin_group_chat_plus/utils/decision_ai.py" in filename:
                decision_ai = getattr(module, "DecisionAI", None)
                if isinstance(decision_ai, type):
                    self._discovered["group_chat_decision_ai"] = True
                    self._patch_decision_ai(decision_ai, "should_reply", True)
                    self._patch_decision_ai(decision_ai, "call_decision_ai", False)
                    self._installed["group_chat_decision_ai"] = self._is_patched(
                        decision_ai, "should_reply"
                    ) and self._is_patched(decision_ai, "call_decision_ai")
            elif "/astrbot_plugin_group_chat_plus/utils/proactive_chat_manager.py" in filename:
                manager = getattr(module, "ProactiveChatManager", None)
                if isinstance(manager, type):
                    self._discovered["group_chat_proactive_queue"] = True
                    self._patch_group_proactive_manager(manager)
                    self._installed["group_chat_proactive_queue"] = self._is_patched(
                        manager, "_process_proactive_chat_simplified"
                    )
            else:
                manager = getattr(module, "ProactiveChatManager", None)
                manager_module = inspect.getmodule(manager) if isinstance(manager, type) else None
                module_path = os.path.normpath(filename).replace("\\", "/")
                source_path = os.path.normpath(
                    str(getattr(manager_module, "__file__", ""))
                ).replace("\\", "/").casefold()
                private_manager_suffix = (
                    "/astrbot_plugin_group_chat_plus/private_chat/"
                    "private_chat_utils/private_chat_proactive_chat_manager.py"
                )
                private_package_suffix = (
                    "/astrbot_plugin_group_chat_plus/private_chat/"
                    "private_chat_utils/__init__.py"
                )
                private_export = (
                    module_path.endswith(private_package_suffix)
                    and source_path.endswith(private_manager_suffix)
                )
                private_source = (
                    module_path.casefold().endswith(private_manager_suffix)
                    and source_path.endswith(private_manager_suffix)
                )
                if isinstance(manager, type) and (private_source or private_export):
                    self._discovered["private_chat_proactive_queue"] = True
                    self._patch_group_proactive_manager(manager)
                    self._installed["private_chat_proactive_queue"] = self._is_patched(
                        manager, "_process_proactive_chat_simplified"
                    )

    def _is_patched(self, owner: type, name: str) -> bool:
        try:
            descriptor = inspect.getattr_static(owner, name)
        except AttributeError:
            return False
        function = descriptor.__func__ if isinstance(descriptor, (staticmethod, classmethod)) else descriptor
        return bool(getattr(function, "_better_balance_wrapper", False))

    def status(self) -> dict[str, Any]:
        """Return a compact runtime compatibility status for the management UI."""
        pending = sum(1 for state in self._events.values() if state.status == "pending")
        adapter_states = {
            name: (
                "installed"
                if installed
                else "patch_failed"
                if self._discovered.get(name, False)
                else "not_loaded"
            )
            for name, installed in self._installed.items()
        }
        return {
            "active": self._active,
            "patched_methods": len(self._patches),
            "pending_reservations": pending,
            "adapters": dict(self._installed),
            "adapter_states": adapter_states,
        }

    def close(self) -> None:
        """Release local reservations and restore exact original method objects."""
        if not self._active:
            return
        self._active = False
        for state in list(self._events.values()):
            if state.status == "pending" and state.reservation:
                self.store.release(state.reservation.token)
            self._restore_event_sends(state.event)
            try:
                delattr(state.event, self.STATE_ATTR)
            except Exception:
                pass
        self._events.clear()
        for _context, original, wrapper, _count in list(self._context_sends.values()):
            if _context is not None and getattr(_context, "send_message", None) is wrapper:
                try:
                    setattr(_context, "send_message", original)
                except Exception:
                    pass
        self._context_sends.clear()
        for owner, name, replacement, original, had_own in reversed(self._patches):
            try:
                if inspect.getattr_static(owner, name) is replacement:
                    if had_own:
                        setattr(owner, name, original)
                    else:
                        delattr(owner, name)
            except Exception:
                pass
        self._patches.clear()

