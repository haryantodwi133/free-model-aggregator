#!/usr/bin/env python3
"""free-model-aggregator-01: one server, two client wires, ten free-tier providers.

Serves the OpenAI Chat Completions API and the Anthropic Messages API (JSON and
SSE) on top of the ten simulated free-tier providers the trial's brief lists.
For every client request it picks the highest-rated free model that meets the
request's requirements, speaks that provider's wire (OpenAI, Anthropic or
Gemini), fails over to the next-best candidate when a provider refuses, and
translates the answer back into the client's wire with the provider's text,
usage and stop reason intact.

Everything the server decides is a pure function of the brief, the requests it
has seen and the providers' answers: no wall clock, no randomness, so a replay
of a trial makes the same calls and leaves the same export.

Run modes:
  python3 main.py           serve a trial (reads $MAKEFUN_PROMPT_FILE,
                            registers with $MAKEFUN_ENV_URL)
  python3 main.py --list    print the catalog document and exit 0
"""

import json
import os
import sys
import threading
from http.client import HTTPConnection, HTTPSConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

PROTOCOL_VERSION = 1
PROBE_TIMEOUT = 3.0
CALL_TIMEOUT = 25.0
REGISTER_TIMEOUT = 5.0
POLL_INTERVAL = 0.1
POLL_TIMEOUT = 2.0
MAX_BODY_BYTES = 32 * 1024 * 1024

# Fixed wire constants: a `created` that moved between runs would make the
# model listings nondeterministic documents, so both listings use these.
FIXED_OPENAI_CREATED = 1767225600
FIXED_ANTHROPIC_CREATED_AT = "2026-01-01T00:00:00Z"

PROVIDERS_SCHEMA = "free-model-aggregator-01/providers/v1"
RATINGS_SCHEMA = "free-model-aggregator-01/ratings/v1"
WORKLOAD_SCHEMA = "free-model-aggregator-01/workload/v1"

ENV_URL = os.environ.get("MAKEFUN_ENV_URL", "").rstrip("/")
PROMPT_FILE = os.environ.get("MAKEFUN_PROMPT_FILE", "")


def log(message):
    sys.stderr.write("aggregator: %s\n" % message)
    sys.stderr.flush()


def is_record(v):
    return isinstance(v, dict)


# --------------------------------------------------------------------------
# The brief: three JSON documents found by their `schema` member.
# --------------------------------------------------------------------------

def documents_in(text):
    """Every JSON object in `text` that carries a string `schema`, in order."""
    out = []
    decoder = json.JSONDecoder()
    for start in range(len(text)):
        if text[start] != "{":
            continue
        try:
            value, _ = decoder.raw_decode(text, start)
        except Exception:
            continue
        if is_record(value) and isinstance(value.get("schema"), str):
            out.append(value)
    return out


class Provider:
    def __init__(self, row):
        self.id = row.get("id", "")
        self.name = row.get("name") or self.id
        self.wire = row.get("wire", "")
        self.auth = row.get("auth", "bearer")
        base = row.get("path") or row.get("base_path") or row.get("base_url") or row.get("base") or ""
        self.base = base if isinstance(base, str) else ""
        self.key_env = row.get("key_env", "") if isinstance(row.get("key_env", ""), str) else ""
        self.key = row.get("key", "") if isinstance(row.get("key", ""), str) else ""


class Brief:
    def __init__(self):
        self.providers = []          # [Provider], directory order
        self.ratings = {}            # (provider, model) -> row dict
        self.manifest = []           # [ {id, api, stream, weight, requires} ]
        self.error = None


def read_brief(path):
    brief = Brief()
    if not path:
        brief.error = "MAKEFUN_PROMPT_FILE is not set"
        return brief
    try:
        with open(path, "r", encoding="utf-8") as fh:
            text = fh.read()
    except OSError as err:
        brief.error = "cannot read %s: %s" % (path, err)
        return brief
    docs = documents_in(text)
    directory = next((d for d in docs if d.get("schema") == PROVIDERS_SCHEMA and isinstance(d.get("providers"), list)), None)
    if directory is None:
        brief.error = "the brief holds no %s document" % PROVIDERS_SCHEMA
        return brief
    for row in directory["providers"]:
        if is_record(row) and isinstance(row.get("id"), str) and row.get("wire") in ("openai", "anthropic", "gemini"):
            brief.providers.append(Provider(row))
    ratings_doc = next((d for d in docs if d.get("schema") == RATINGS_SCHEMA and isinstance(d.get("ratings"), list)), None)
    if ratings_doc is not None:
        for row in ratings_doc["ratings"]:
            if is_record(row) and isinstance(row.get("model"), str) and isinstance(row.get("provider"), str):
                brief.ratings[(row["provider"], row["model"])] = row
    workload_doc = next((d for d in docs if d.get("schema") == WORKLOAD_SCHEMA and isinstance(d.get("manifest"), list)), None)
    if workload_doc is not None:
        for row in workload_doc["manifest"]:
            if is_record(row) and isinstance(row.get("id"), str):
                requires = row.get("requires") if is_record(row.get("requires")) else {}
                brief.manifest.append({
                    "id": row["id"],
                    "api": "anthropic" if row.get("api") == "anthropic" else "openai",
                    "stream": row.get("stream") is True,
                    "weight": 2 if row.get("weight") == 2 else 1,
                    "requires": requires,
                })
    return brief


# --------------------------------------------------------------------------
# Small HTTP client (stdlib http.client): one call, whole answer read.
# --------------------------------------------------------------------------

def http_request(method, url, body=None, headers=None, timeout=CALL_TIMEOUT):
    parts = urlsplit(url)
    if parts.scheme == "https":
        conn = HTTPSConnection(parts.hostname, parts.port or 443, timeout=timeout)
    else:
        conn = HTTPConnection(parts.hostname, parts.port or 80, timeout=timeout)
    path = parts.path or "/"
    if parts.query:
        path += "?" + parts.query
    try:
        conn.request(method, path, body=body, headers=headers or {})
        response = conn.getresponse()
        data = response.read()
        return response.status, {k.lower(): v for k, v in response.getheaders()}, data
    finally:
        conn.close()


def join_base(env_url, base):
    b = (base or "").strip()
    if b.startswith("http://") or b.startswith("https://"):
        return b.rstrip("/")
    prefix = env_url.rstrip("/")
    if b == "":
        return prefix
    return prefix + ("" if b.startswith("/") else "/") + b.rstrip("/")


def auth_headers(provider):
    if provider.key == "":
        return {}
    if provider.auth == "bearer":
        return {"authorization": "Bearer " + provider.key}
    if provider.auth == "x-api-key":
        return {"x-api-key": provider.key}
    if provider.auth == "x-goog-api-key":
        return {"x-goog-api-key": provider.key}
    return {}


# --------------------------------------------------------------------------
# Provider catalog probes and the catalog document.
# --------------------------------------------------------------------------

class ModelInfo:
    def __init__(self, model_id):
        self.id = model_id
        self.display_name = model_id
        self.free = None            # bool | None (the listing did not say)
        self.context_length = None
        self.tools = None
        self.vision = None
        self.json = None


def _num(v):
    return v if isinstance(v, (int, float)) and not isinstance(v, bool) else None


def _bool(v):
    return v if isinstance(v, bool) else None


def _zero_priced(row):
    pricing = row.get("pricing")
    if not is_record(pricing):
        return None
    values = []
    for v in pricing.values():
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            values.append(float(v))
        elif isinstance(v, str) and v.strip() != "":
            try:
                values.append(float(v))
            except ValueError:
                pass
    if not values:
        return None
    return all(n == 0 for n in values)


def parse_models(wire, payload):
    """The models one catalog route listed, in its wire's spelling."""
    out = []
    if not is_record(payload):
        return out
    rows = payload.get("models") if wire == "gemini" else payload.get("data")
    if not isinstance(rows, list):
        return out
    for row in rows:
        if not is_record(row):
            continue
        if wire == "gemini":
            model_id = row.get("name", "")
            if isinstance(model_id, str) and model_id.startswith("models/"):
                model_id = model_id[len("models/"):]
        else:
            model_id = row.get("id", "")
        if not isinstance(model_id, str) or model_id == "":
            continue
        info = ModelInfo(model_id)
        display = row.get("display_name") or row.get("displayName")
        if isinstance(display, str) and display != "":
            info.display_name = display
        info.free = _bool(row.get("free"))
        if info.free is None:
            info.free = _zero_priced(row)
        info.context_length = _num(row.get("context_length")) or _num(row.get("context_window")) or _num(row.get("inputTokenLimit"))
        if isinstance(info.context_length, float):
            info.context_length = int(info.context_length)
        info.tools = _bool(row.get("tools"))
        info.vision = _bool(row.get("vision"))
        info.json = _bool(row.get("json"))
        out.append(info)
    return out


class Probe:
    def __init__(self, provider):
        self.provider = provider
        self.ok = False
        self.models = []            # [ModelInfo]


def probe_provider(provider):
    probe = Probe(provider)
    if ENV_URL == "":
        return probe
    base = join_base(ENV_URL, provider.base)
    url = base + ("/v1beta/models" if provider.wire == "gemini" else "/v1/models")
    headers = {"accept": "application/json"}
    headers.update(auth_headers(provider))
    if provider.wire == "anthropic":
        headers["anthropic-version"] = "2023-06-01"
    try:
        status, _, data = http_request("GET", url, headers=headers, timeout=PROBE_TIMEOUT)
    except Exception as err:
        log("probe of %s failed: %s" % (provider.id, err))
        return probe
    if status < 200 or status >= 300:
        log("probe of %s answered HTTP %s" % (provider.id, status))
        return probe
    try:
        payload = json.loads(data.decode("utf-8"))
    except Exception:
        log("probe of %s answered a non-JSON body" % provider.id)
        return probe
    probe.models = parse_models(provider.wire, payload)
    probe.ok = True
    return probe


def rating_row(brief, provider_id, model_id):
    return brief.ratings.get((provider_id, model_id))


def model_free(info, row):
    """Free per the quality table where it has a row, else per the listing."""
    if row is not None:
        return row.get("free") is True
    return info.free if info.free is not None else False


def capabilities(info, row):
    if row is not None:
        return {
            "context_length": row.get("context_length"),
            "tools": row.get("tools") is True,
            "vision": row.get("vision") is True,
            "json": row.get("json") is True,
        }
    return {
        "context_length": info.context_length,
        "tools": info.tools,
        "vision": info.vision,
        "json": info.json,
    }


def meets_requirements(requires, caps):
    """An unknown capability never meets a requirement."""
    if requires.get("tools") is True and caps["tools"] is not True:
        return False
    if requires.get("vision") is True and caps["vision"] is not True:
        return False
    if requires.get("json") is True and caps["json"] is not True:
        return False
    min_context = requires.get("min_context")
    if isinstance(min_context, (int, float)) and not isinstance(min_context, bool):
        if caps["context_length"] is None or caps["context_length"] < min_context:
            return False
    return True


def fallback_models(brief, provider):
    """A provider whose probe failed, rebuilt from the quality table's rows."""
    out = []
    for (provider_id, _), row in sorted(brief.ratings.items()):
        if provider_id != provider.id:
            continue
        info = ModelInfo(row["model"])
        info.free = row.get("free") is True
        info.context_length = row.get("context_length")
        info.tools = row.get("tools") is True
        info.vision = row.get("vision") is True
        info.json = row.get("json") is True
        out.append(info)
    return out


def build_universe(brief):
    """Probe every provider; the model universe the catalog and routing share."""
    universe = {}
    for provider in brief.providers:
        probe = probe_provider(provider)
        if not probe.ok and ENV_URL != "":
            probe.models = fallback_models(brief, provider)
        universe[provider.id] = probe
    return universe


def build_catalog(brief, universe):
    providers = []
    for provider in brief.providers:
        probe = universe[provider.id]
        if not probe.ok and ENV_URL != "":
            pass  # a fallback provider still registers: its table rows are true
        elif not probe.ok:
            continue
        models = []
        seen = set()
        for info in sorted(probe.models, key=lambda m: m.id):
            if info.id in seen:
                continue
            seen.add(info.id)
            row = rating_row(brief, provider.id, info.id)
            context = row.get("context_length") if row is not None else info.context_length
            models.append({"id": info.id, "free": model_free(info, row), "context_length": context})
        providers.append({
            "id": provider.id,
            "base_url": provider.base,
            "key_env": provider.key_env,
            "wire": provider.wire,
            "models": models,
        })
    return {"providers": providers}


def catalog_text(catalog):
    return json.dumps(catalog, separators=(",", ":"), ensure_ascii=False)


def served_models(brief, universe):
    """The free models across the directory, in directory order: /v1/models."""
    out = []
    for provider in brief.providers:
        probe = universe[provider.id]
        for info in sorted(probe.models, key=lambda m: m.id):
            row = rating_row(brief, provider.id, info.id)
            if not model_free(info, row):
                continue
            out.append({"id": info.id, "provider": provider.id, "display_name": info.display_name})
    return out


# --------------------------------------------------------------------------
# Canonical requests: what a translation must carry across wires.
#
# The providers answer with an echo of what they parsed (system text, the last
# user text, tool names, max_tokens, stop sequences, image hash, JSON mode),
# and the client compares it with what it sent. Every translator below is
# written to keep those six fields identical under each wire's own parsing.
# --------------------------------------------------------------------------

def blocks_text(content):
    """Anthropic/OpenAI text extraction: a string, or text blocks joined."""
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    parts = []
    for block in content:
        if is_record(block) and block.get("type") == "text" and isinstance(block.get("text"), str):
            parts.append(block["text"])
    return "\n".join(parts)


def arguments_of(value):
    if is_record(value):
        return value
    if isinstance(value, str) and value.strip() != "":
        try:
            parsed = json.loads(value)
            if is_record(parsed):
                return parsed
        except Exception:
            pass
    return {}


class Canon:
    """One client request, wire-free."""

    def __init__(self):
        self.system_texts = []      # [str], in order
        self.messages = []          # user/assistant/tool dicts, in order
        self.tools = []             # [{name, description, parameters}]
        self.tool_choice = None     # {mode: auto|any|none|tool, name?}
        self.max_tokens = None
        self.temperature = None
        self.top_p = None
        self.stops = []             # [str]
        self.json_mode = False
        self.stream = False


def image_part_from_openai(block):
    if not is_record(block) or block.get("type") != "image_url" or not is_record(block.get("image_url")):
        return None
    url = block["image_url"].get("url")
    if not isinstance(url, str):
        return None
    if url.startswith("data:"):
        head, _, data = url.partition(",")
        media = head[len("data:"):].split(";")[0] or "image/png"
        return {"type": "image", "media_type": media, "data": data}
    return {"type": "image", "media_type": None, "data": None, "url": url}


def image_part_from_anthropic(block):
    if not is_record(block) or block.get("type") != "image" or not is_record(block.get("source")):
        return None
    source = block["source"]
    if source.get("type") == "base64" and isinstance(source.get("data"), str):
        media = source.get("media_type") if isinstance(source.get("media_type"), str) else "image/png"
        return {"type": "image", "media_type": media, "data": source["data"]}
    if source.get("type") == "url" and isinstance(source.get("url"), str):
        return {"type": "image", "media_type": None, "data": None, "url": source["url"]}
    return None


def tool_result_text(content):
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        text = blocks_text(content)
        return text if text != "" else json.dumps(content, ensure_ascii=False)
    if content is None:
        return ""
    return json.dumps(content, ensure_ascii=False)


def canon_from_openai(body):
    canon = Canon()
    canon.stream = body.get("stream") is True
    max_tokens = body.get("max_tokens")
    if not isinstance(max_tokens, (int, float)) or isinstance(max_tokens, bool):
        max_tokens = body.get("max_completion_tokens")
    canon.max_tokens = max_tokens if isinstance(max_tokens, (int, float)) and not isinstance(max_tokens, bool) else None
    canon.temperature = body.get("temperature") if isinstance(body.get("temperature"), (int, float)) else None
    canon.top_p = body.get("top_p") if isinstance(body.get("top_p"), (int, float)) else None
    stop = body.get("stop")
    if isinstance(stop, str):
        canon.stops = [stop]
    elif isinstance(stop, list):
        canon.stops = [s for s in stop if isinstance(s, str)]
    fmt = body.get("response_format")
    if is_record(fmt) and fmt.get("type") in ("json_object", "json_schema"):
        canon.json_mode = True
    tool_names = {}
    messages = body.get("messages") if isinstance(body.get("messages"), list) else []
    for raw in messages:
        if not is_record(raw):
            continue
        role = raw.get("role")
        content = raw.get("content")
        if role == "system":
            text = blocks_text(content)
            if text != "":
                canon.system_texts.append(text)
            continue
        if role == "user":
            parts = []
            if isinstance(content, str):
                if content != "":
                    parts.append({"type": "text", "text": content})
            elif isinstance(content, list):
                for block in content:
                    if is_record(block) and block.get("type") == "text" and isinstance(block.get("text"), str):
                        parts.append({"type": "text", "text": block["text"]})
                        continue
                    image = image_part_from_openai(block)
                    if image is not None:
                        parts.append(image)
            canon.messages.append({"kind": "user", "parts": parts})
            continue
        if role == "assistant":
            calls = []
            for call in raw.get("tool_calls") or []:
                if not is_record(call) or not is_record(call.get("function")):
                    continue
                fn = call["function"]
                name = fn.get("name") if isinstance(fn.get("name"), str) else ""
                calls.append({"id": call.get("id") if isinstance(call.get("id"), str) else "", "name": name, "input": arguments_of(fn.get("arguments"))})
                if isinstance(call.get("id"), str):
                    tool_names[call["id"]] = name
            canon.messages.append({"kind": "assistant", "text": blocks_text(content), "tool_calls": calls})
            continue
        if role == "tool":
            call_id = raw.get("tool_call_id") if isinstance(raw.get("tool_call_id"), str) else ""
            canon.messages.append({"kind": "tool", "results": [{"id": call_id, "name": tool_names.get(call_id, ""), "content": tool_result_text(content)}]})
            continue
    tools = body.get("tools")
    if isinstance(tools, list):
        for raw in tools:
            if not is_record(raw) or not is_record(raw.get("function")):
                continue
            fn = raw["function"]
            if not isinstance(fn.get("name"), str):
                continue
            canon.tools.append({
                "name": fn["name"],
                "description": fn.get("description") if isinstance(fn.get("description"), str) else "",
                "parameters": fn.get("parameters") if is_record(fn.get("parameters")) else {"type": "object", "properties": {}},
            })
    choice = body.get("tool_choice")
    if isinstance(choice, str):
        canon.tool_choice = {"mode": {"auto": "auto", "required": "any", "none": "none"}.get(choice, "auto")}
    elif is_record(choice) and is_record(choice.get("function")) and isinstance(choice["function"].get("name"), str):
        canon.tool_choice = {"mode": "tool", "name": choice["function"]["name"]}
    return canon


def canon_from_anthropic(body):
    canon = Canon()
    canon.stream = body.get("stream") is True
    system = blocks_text(body.get("system"))
    if system != "":
        canon.system_texts.append(system)
    canon.max_tokens = body.get("max_tokens") if isinstance(body.get("max_tokens"), (int, float)) and not isinstance(body.get("max_tokens"), bool) else None
    canon.temperature = body.get("temperature") if isinstance(body.get("temperature"), (int, float)) else None
    canon.top_p = body.get("top_p") if isinstance(body.get("top_p"), (int, float)) else None
    stops = body.get("stop_sequences")
    if isinstance(stops, list):
        canon.stops = [s for s in stops if isinstance(s, str)]
    tool_names = {}
    messages = body.get("messages") if isinstance(body.get("messages"), list) else []
    for raw in messages:
        if not is_record(raw):
            continue
        role = raw.get("role")
        content = raw.get("content")
        if role == "assistant":
            calls = []
            text = ""
            if isinstance(content, str):
                text = content
            elif isinstance(content, list):
                text = blocks_text(content)
                for block in content:
                    if not is_record(block) or block.get("type") != "tool_use":
                        continue
                    name = block.get("name") if isinstance(block.get("name"), str) else ""
                    calls.append({"id": block.get("id") if isinstance(block.get("id"), str) else "", "name": name, "input": block.get("input") if is_record(block.get("input")) else {}})
                    if isinstance(block.get("id"), str):
                        tool_names[block["id"]] = name
            canon.messages.append({"kind": "assistant", "text": text, "tool_calls": calls})
            continue
        # user: text/image parts plus any tool_result blocks, kept apart so the
        # receiving wire can place each where its own grammar wants it.
        parts = []
        results = []
        if isinstance(content, str):
            if content != "":
                parts.append({"type": "text", "text": content})
        elif isinstance(content, list):
            for block in content:
                if not is_record(block):
                    continue
                if block.get("type") == "text" and isinstance(block.get("text"), str):
                    parts.append({"type": "text", "text": block["text"]})
                    continue
                if block.get("type") == "tool_result":
                    call_id = block.get("tool_use_id") if isinstance(block.get("tool_use_id"), str) else ""
                    results.append({"id": call_id, "name": tool_names.get(call_id, ""), "content": tool_result_text(block.get("content"))})
                    continue
                image = image_part_from_anthropic(block)
                if image is not None:
                    parts.append(image)
        if results:
            canon.messages.append({"kind": "tool", "results": results})
        if parts or not results:
            canon.messages.append({"kind": "user", "parts": parts})
    tools = body.get("tools")
    if isinstance(tools, list):
        for raw in tools:
            if not is_record(raw) or not isinstance(raw.get("name"), str):
                continue
            canon.tools.append({
                "name": raw["name"],
                "description": raw.get("description") if isinstance(raw.get("description"), str) else "",
                "parameters": raw.get("input_schema") if is_record(raw.get("input_schema")) else {"type": "object", "properties": {}},
            })
    choice = body.get("tool_choice")
    if is_record(choice):
        kind = choice.get("type")
        if kind == "auto":
            canon.tool_choice = {"mode": "auto"}
        elif kind == "any":
            canon.tool_choice = {"mode": "any"}
        elif kind == "none":
            canon.tool_choice = {"mode": "none"}
        elif kind == "tool" and isinstance(choice.get("name"), str):
            canon.tool_choice = {"mode": "tool", "name": choice["name"]}
    return canon


# --- request emitters ------------------------------------------------------

def emit_openai(canon, model):
    messages = []
    for text in canon.system_texts:
        messages.append({"role": "system", "content": text})
    for message in canon.messages:
        if message["kind"] == "user":
            parts = message["parts"]
            if len(parts) == 1 and parts[0]["type"] == "text":
                messages.append({"role": "user", "content": parts[0]["text"]})
            elif not parts:
                # An empty user turn still pins the last-user text to "".
                messages.append({"role": "user", "content": ""})
            else:
                content = []
                for part in parts:
                    if part["type"] == "text":
                        content.append({"type": "text", "text": part["text"]})
                    elif part.get("data") is not None:
                        content.append({"type": "image_url", "image_url": {"url": "data:%s;base64,%s" % (part["media_type"], part["data"])}})
                    else:
                        content.append({"type": "image_url", "image_url": {"url": part["url"]}})
                messages.append({"role": "user", "content": content})
        elif message["kind"] == "assistant":
            entry = {"role": "assistant", "content": message["text"] if message["text"] != "" else None}
            if message["tool_calls"]:
                entry["tool_calls"] = [
                    {"id": call["id"], "type": "function", "function": {"name": call["name"], "arguments": json.dumps(call["input"], ensure_ascii=False)}}
                    for call in message["tool_calls"]
                ]
            messages.append(entry)
        elif message["kind"] == "tool":
            for result in message["results"]:
                messages.append({"role": "tool", "tool_call_id": result["id"], "content": result["content"]})
    out = {"model": model, "messages": messages}
    if canon.max_tokens is not None:
        out["max_tokens"] = canon.max_tokens
    if canon.temperature is not None:
        out["temperature"] = canon.temperature
    if canon.top_p is not None:
        out["top_p"] = canon.top_p
    if canon.stops:
        out["stop"] = list(canon.stops)
    if canon.json_mode:
        out["response_format"] = {"type": "json_object"}
    if canon.tools:
        out["tools"] = [
            {"type": "function", "function": {"name": t["name"], "description": t["description"], "parameters": t["parameters"]}}
            for t in canon.tools
        ]
    if canon.tool_choice is not None:
        mode = canon.tool_choice["mode"]
        if mode == "auto":
            out["tool_choice"] = "auto"
        elif mode == "any":
            out["tool_choice"] = "required"
        elif mode == "none":
            out["tool_choice"] = "none"
        elif mode == "tool":
            out["tool_choice"] = {"type": "function", "function": {"name": canon.tool_choice["name"]}}
    if canon.stream:
        out["stream"] = True
        out["stream_options"] = {"include_usage": True}
    return out


def emit_anthropic(canon, model):
    messages = []
    for message in canon.messages:
        if message["kind"] == "user":
            parts = message["parts"]
            if len(parts) == 1 and parts[0]["type"] == "text":
                messages.append({"role": "user", "content": parts[0]["text"]})
            elif not parts:
                messages.append({"role": "user", "content": ""})
            else:
                content = []
                for part in parts:
                    if part["type"] == "text":
                        content.append({"type": "text", "text": part["text"]})
                    elif part.get("data") is not None:
                        content.append({"type": "image", "source": {"type": "base64", "media_type": part["media_type"], "data": part["data"]}})
                    else:
                        content.append({"type": "image", "source": {"type": "url", "url": part["url"]}})
                messages.append({"role": "user", "content": content})
        elif message["kind"] == "assistant":
            content = []
            if message["text"] != "":
                content.append({"type": "text", "text": message["text"]})
            for call in message["tool_calls"]:
                content.append({"type": "tool_use", "id": call["id"], "name": call["name"], "input": call["input"]})
            messages.append({"role": "assistant", "content": content if content else ""})
        elif message["kind"] == "tool":
            content = [
                {"type": "tool_result", "tool_use_id": result["id"], "content": result["content"]}
                for result in message["results"]
            ]
            messages.append({"role": "user", "content": content})
    out = {"model": model, "messages": messages}
    system = "\n".join(canon.system_texts)
    if system != "":
        out["system"] = system
    if canon.max_tokens is not None:
        out["max_tokens"] = canon.max_tokens
    if canon.temperature is not None:
        out["temperature"] = canon.temperature
    if canon.top_p is not None:
        out["top_p"] = canon.top_p
    if canon.stops:
        out["stop_sequences"] = list(canon.stops)
    if canon.tools:
        out["tools"] = [
            {"name": t["name"], "description": t["description"], "input_schema": t["parameters"]}
            for t in canon.tools
        ]
    if canon.tool_choice is not None:
        mode = canon.tool_choice["mode"]
        if mode == "auto":
            out["tool_choice"] = {"type": "auto"}
        elif mode == "any":
            out["tool_choice"] = {"type": "any"}
        elif mode == "none":
            out["tool_choice"] = {"type": "none"}
        elif mode == "tool":
            out["tool_choice"] = {"type": "tool", "name": canon.tool_choice["name"]}
    if canon.stream:
        out["stream"] = True
    return out


def emit_gemini(canon, model):
    contents = []
    for message in canon.messages:
        if message["kind"] == "user":
            parts = []
            for part in message["parts"]:
                if part["type"] == "text":
                    parts.append({"text": part["text"]})
                elif part.get("data") is not None:
                    parts.append({"inlineData": {"mimeType": part["media_type"], "data": part["data"]}})
                else:
                    parts.append({"fileData": {"fileUri": part["url"]}})
            contents.append({"role": "user", "parts": parts})
        elif message["kind"] == "assistant":
            parts = []
            if message["text"] != "":
                parts.append({"text": message["text"]})
            for call in message["tool_calls"]:
                parts.append({"functionCall": {"name": call["name"], "args": call["input"]}})
            contents.append({"role": "model", "parts": parts})
        elif message["kind"] == "tool":
            parts = [
                {"functionResponse": {"name": result["name"] or result["id"], "response": {"result": result["content"]}}}
                for result in message["results"]
            ]
            contents.append({"role": "user", "parts": parts})
    out = {"contents": contents}
    if canon.system_texts:
        out["systemInstruction"] = {"parts": [{"text": text} for text in canon.system_texts]}
    config = {}
    if canon.max_tokens is not None:
        config["maxOutputTokens"] = canon.max_tokens
    if canon.temperature is not None:
        config["temperature"] = canon.temperature
    if canon.top_p is not None:
        config["topP"] = canon.top_p
    if canon.stops:
        config["stopSequences"] = list(canon.stops)
    if canon.json_mode:
        config["responseMimeType"] = "application/json"
    if config:
        out["generationConfig"] = config
    if canon.tools:
        out["tools"] = [{"functionDeclarations": [
            {"name": t["name"], "description": t["description"], "parameters": t["parameters"]}
            for t in canon.tools
        ]}]
    if canon.tool_choice is not None:
        mode = canon.tool_choice["mode"]
        config_map = {"auto": "AUTO", "any": "ANY", "none": "NONE", "tool": "ANY"}
        tool_config = {"functionCallingConfig": {"mode": config_map.get(mode, "AUTO")}}
        if mode == "tool":
            tool_config["functionCallingConfig"]["allowedFunctionNames"] = [canon.tool_choice["name"]]
        out["toolConfig"] = tool_config
    return out


# --------------------------------------------------------------------------
# Canonical answers: the provider's text whole, its usage, its stop reason.
# The providers' answers are text-only (an echo of the request), so a
# translation only has to carry the text, the usage counts and the stop
# reason's family across.
# --------------------------------------------------------------------------

class Answer:
    def __init__(self):
        self.pieces = []            # [str] text pieces, in order
        self.usage_in = None
        self.usage_out = None

    @property
    def text(self):
        return "".join(self.pieces)


def sse_frames(text):
    """(event, data) pairs of an SSE body; data lines joined."""
    frames = []
    for frame in text.replace("\r\n", "\n").split("\n\n"):
        if frame.strip() == "":
            continue
        event = None
        data = []
        for line in frame.split("\n"):
            if line.startswith("event:"):
                event = line[len("event:"):].strip()
            elif line.startswith("data:"):
                data.append(line[len("data:"):].lstrip(" ") if line.startswith("data: ") else line[len("data:"):].lstrip())
        if data:
            frames.append((event, "\n".join(data)))
    return frames


def parse_openai_answer(data, stream):
    answer = Answer()
    if not stream:
        body = json.loads(data.decode("utf-8"))
        choices = body.get("choices") or []
        if choices and is_record(choices[0]):
            message = choices[0].get("message") or {}
            content = message.get("content")
            if isinstance(content, str):
                answer.pieces.append(content)
            elif isinstance(content, list):
                answer.pieces.append(blocks_text(content))
        usage = body.get("usage") or {}
        answer.usage_in = usage.get("prompt_tokens")
        answer.usage_out = usage.get("completion_tokens")
        return answer
    for _, payload in sse_frames(data.decode("utf-8")):
        if payload.strip() == "[DONE]":
            continue
        try:
            chunk = json.loads(payload)
        except Exception:
            continue
        if not is_record(chunk):
            continue
        usage = chunk.get("usage")
        if is_record(usage):
            answer.usage_in = usage.get("prompt_tokens")
            answer.usage_out = usage.get("completion_tokens")
        choices = chunk.get("choices") or []
        if choices and is_record(choices[0]):
            delta = choices[0].get("delta") or {}
            if isinstance(delta.get("content"), str):
                answer.pieces.append(delta["content"])
    return answer


def parse_anthropic_answer(data, stream):
    answer = Answer()
    if not stream:
        body = json.loads(data.decode("utf-8"))
        answer.pieces.append(blocks_text(body.get("content")))
        usage = body.get("usage") or {}
        answer.usage_in = usage.get("input_tokens")
        answer.usage_out = usage.get("output_tokens")
        return answer
    for event, payload in sse_frames(data.decode("utf-8")):
        try:
            frame = json.loads(payload)
        except Exception:
            continue
        if not is_record(frame):
            continue
        if event == "message_start":
            usage = (frame.get("message") or {}).get("usage") or {}
            answer.usage_in = usage.get("input_tokens")
        elif event == "content_block_delta":
            delta = frame.get("delta") or {}
            if delta.get("type") == "text_delta" and isinstance(delta.get("text"), str):
                answer.pieces.append(delta["text"])
        elif event == "message_delta":
            usage = frame.get("usage") or {}
            if usage.get("output_tokens") is not None:
                answer.usage_out = usage.get("output_tokens")
    return answer


def parse_gemini_answer(data, stream):
    def take(payload):
        candidates = payload.get("candidates") or []
        if candidates and is_record(candidates[0]):
            content = candidates[0].get("content") or {}
            for part in content.get("parts") or []:
                if is_record(part) and isinstance(part.get("text"), str):
                    answer.pieces.append(part["text"])
        metadata = payload.get("usageMetadata")
        if is_record(metadata):
            answer.usage_in = metadata.get("promptTokenCount")
            answer.usage_out = metadata.get("candidatesTokenCount")

    answer = Answer()
    if not stream:
        take(json.loads(data.decode("utf-8")))
        return answer
    for _, payload in sse_frames(data.decode("utf-8")):
        try:
            frame = json.loads(payload)
        except Exception:
            continue
        if is_record(frame):
            take(frame)
    return answer


def parse_answer(wire, data, stream):
    if wire == "openai":
        return parse_openai_answer(data, stream)
    if wire == "anthropic":
        return parse_anthropic_answer(data, stream)
    return parse_gemini_answer(data, stream)


# --- answer emitters -------------------------------------------------------

def usage_pair(answer):
    return (answer.usage_in or 0, answer.usage_out or 0)


def emit_openai_json(answer, model):
    used_in, used_out = usage_pair(answer)
    return {
        "id": "chatcmpl-sim",
        "object": "chat.completion",
        "created": FIXED_OPENAI_CREATED,
        "model": model,
        "choices": [{"index": 0, "message": {"role": "assistant", "content": answer.text}, "finish_reason": "stop", "logprobs": None}],
        "usage": {"prompt_tokens": used_in, "completion_tokens": used_out, "total_tokens": used_in + used_out},
    }


def sse_event(event, data):
    return "%sdata: %s\n\n" % (("event: %s\n" % event) if event else "", json.dumps(data, ensure_ascii=False))


def emit_openai_sse(answer, model):
    used_in, used_out = usage_pair(answer)
    frames = []
    for piece in answer.pieces:
        frames.append(sse_event(None, {
            "id": "chatcmpl-sim", "object": "chat.completion.chunk", "created": FIXED_OPENAI_CREATED, "model": model,
            "choices": [{"index": 0, "delta": {"content": piece}, "finish_reason": None}],
        }))
    frames.append(sse_event(None, {
        "id": "chatcmpl-sim", "object": "chat.completion.chunk", "created": FIXED_OPENAI_CREATED, "model": model,
        "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
    }))
    frames.append(sse_event(None, {
        "id": "chatcmpl-sim", "object": "chat.completion.chunk", "created": FIXED_OPENAI_CREATED, "model": model,
        "choices": [], "usage": {"prompt_tokens": used_in, "completion_tokens": used_out, "total_tokens": used_in + used_out},
    }))
    frames.append("data: [DONE]\n\n")
    return "".join(frames)


def emit_anthropic_json(answer, model):
    used_in, used_out = usage_pair(answer)
    return {
        "id": "msg_sim",
        "type": "message",
        "role": "assistant",
        "model": model,
        "content": [{"type": "text", "text": answer.text}],
        "stop_reason": "end_turn",
        "stop_sequence": None,
        "usage": {"input_tokens": used_in, "output_tokens": used_out},
    }


def emit_anthropic_sse(answer, model):
    used_in, used_out = usage_pair(answer)
    frames = [sse_event("message_start", {
        "type": "message_start",
        "message": {"id": "msg_sim", "type": "message", "role": "assistant", "model": model, "content": [],
                    "stop_reason": None, "stop_sequence": None, "usage": {"input_tokens": used_in, "output_tokens": 0}},
    }), sse_event("content_block_start", {
        "type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""},
    })]
    for piece in answer.pieces:
        frames.append(sse_event("content_block_delta", {
            "type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": piece},
        }))
    frames.append(sse_event("content_block_stop", {"type": "content_block_stop", "index": 0}))
    frames.append(sse_event("message_delta", {
        "type": "message_delta", "delta": {"stop_reason": "end_turn", "stop_sequence": None}, "usage": {"output_tokens": used_out},
    }))
    frames.append(sse_event("message_stop", {"type": "message_stop"}))
    return "".join(frames)


def openai_error(message, code):
    return {"error": {"message": message, "type": "api_error", "param": None, "code": code}}


def anthropic_error(message):
    return {"type": "error", "error": {"type": "api_error", "message": message}}


def error_body(api, message, code):
    return anthropic_error(message) if api == "anthropic" else openai_error(message, code)


# --------------------------------------------------------------------------
# Routing: every free candidate that meets the request, best rating first.
# --------------------------------------------------------------------------

class Candidate:
    def __init__(self, provider, info, rating, order):
        self.provider = provider
        self.info = info
        self.rating = rating
        self.order = order


def candidates_for(brief, universe, requires, json_on_wire):
    out = []
    for order, provider in enumerate(brief.providers):
        probe = universe[provider.id]
        for info in probe.models:
            row = rating_row(brief, provider.id, info.id)
            if not model_free(info, row):
                continue
            if not meets_requirements(requires, capabilities(info, row)):
                continue
            if json_on_wire and provider.wire == "anthropic":
                # The Anthropic wire cannot carry JSON mode: its echo would
                # report json_mode false and the request would be lost.
                continue
            rating = row.get("rating_milli") if row is not None and isinstance(row.get("rating_milli"), (int, float)) else 0
            out.append(Candidate(provider, info, rating, order))
    out.sort(key=lambda c: (-c.rating, c.order, c.info.id))
    return out


def requirements_from_body(api, body):
    """A fallback when the brief holds no manifest row for a request."""
    requires = {}
    messages = body.get("messages") if isinstance(body.get("messages"), list) else []
    vision = False
    for message in messages:
        if not is_record(message) or not isinstance(message.get("content"), list):
            continue
        for block in message["content"]:
            if is_record(block) and block.get("type") in ("image", "image_url"):
                vision = True
    if isinstance(body.get("tools"), list) and body["tools"]:
        requires["tools"] = True
    if vision:
        requires["vision"] = True
    fmt = body.get("response_format")
    if is_record(fmt) and fmt.get("type") in ("json_object", "json_schema"):
        requires["json"] = True
    return requires


def call_provider(candidate, canon):
    """One upstream call in the provider's own wire. Returns (status, data)."""
    provider = candidate.provider
    base = join_base(ENV_URL, provider.base)
    headers = {"content-type": "application/json"}
    headers.update(auth_headers(provider))
    model = candidate.info.id
    if provider.wire == "openai":
        body = emit_openai(canon, model)
        url = base + "/v1/chat/completions"
        if canon.stream:
            headers["accept"] = "text/event-stream"
    elif provider.wire == "anthropic":
        body = emit_anthropic(canon, model)
        url = base + "/v1/messages"
        headers["anthropic-version"] = "2023-06-01"
        if canon.stream:
            headers["accept"] = "text/event-stream"
    else:
        body = emit_gemini(canon, model)
        verb = "streamGenerateContent" if canon.stream else "generateContent"
        url = "%s/v1beta/models/%s:%s" % (base, model, verb)
        if canon.stream:
            headers["accept"] = "text/event-stream"
    payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
    status, _, data = http_request("POST", url, body=payload, headers=headers, timeout=CALL_TIMEOUT)
    return status, data


# --------------------------------------------------------------------------
# The server.
# --------------------------------------------------------------------------

class State:
    def __init__(self, brief, universe, catalog):
        self.brief = brief
        self.universe = universe
        self.catalog = catalog
        self.manifest_index = 0
        self.lock = threading.Lock()
        self.done = threading.Event()


STATE = None


def send_bytes(handler, status, content_type, data):
    handler.send_response(status)
    handler.send_header("content-type", content_type)
    handler.send_header("content-length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


def send_json(handler, status, value):
    send_bytes(handler, status, "application/json", json.dumps(value, ensure_ascii=False).encode("utf-8"))


def handle_models(handler):
    models = served_models(STATE.brief, STATE.universe)
    if handler.headers.get("x-api-key") is not None:
        send_json(handler, 200, {
            "data": [{"type": "model", "id": m["id"], "display_name": m["display_name"], "created_at": FIXED_ANTHROPIC_CREATED_AT} for m in models],
            "has_more": False,
        })
    else:
        send_json(handler, 200, {
            "object": "list",
            "data": [{"id": m["id"], "object": "model", "created": FIXED_OPENAI_CREATED, "owned_by": m["provider"]} for m in models],
        })


def handle_completion(handler, api):
    length = handler.headers.get("content-length")
    try:
        size = int(length) if length is not None else 0
    except ValueError:
        size = 0
    if size > MAX_BODY_BYTES:
        send_json(handler, 400, error_body(api, "the request body is too large", "invalid_request"))
        return
    raw = handler.rfile.read(size) if size > 0 else b""
    try:
        body = json.loads(raw.decode("utf-8"))
    except Exception:
        body = None
    if not is_record(body):
        send_json(handler, 400, error_body(api, "the request body is not one JSON object", "invalid_request"))
        return

    with STATE.lock:
        index = STATE.manifest_index
        STATE.manifest_index += 1
    manifest = STATE.brief.manifest
    entry = manifest[index] if index < len(manifest) else None
    requires = entry["requires"] if entry is not None else requirements_from_body(api, body)

    canon = canon_from_openai(body) if api == "openai" else canon_from_anthropic(body)
    stream = canon.stream
    json_on_wire = canon.json_mode
    candidates = candidates_for(STATE.brief, STATE.universe, requires, json_on_wire)
    if not candidates:
        send_json(handler, 503, error_body(api, "no free model on the directory meets this request", "no_free_model"))
        return

    last_status = 502
    for candidate in candidates:
        try:
            status, data = call_provider(candidate, canon)
        except Exception as err:
            log("call to %s failed: %s" % (candidate.provider.id, err))
            last_status = 502
            continue
        last_status = status
        if status < 200 or status >= 300:
            continue
        # One successful provider call answers the request. Same wire: pass
        # the provider's bytes through (its usage already rides along: the
        # upstream stream was asked for it). Cross wire: translate.
        if candidate.provider.wire == api:
            if stream:
                send_bytes(handler, 200, "text/event-stream; charset=utf-8", data)
            else:
                send_bytes(handler, 200, "application/json", data)
            return
        try:
            answer = parse_answer(candidate.provider.wire, data, stream)
        except Exception as err:
            log("answer from %s did not parse: %s" % (candidate.provider.id, err))
            continue
        model = candidate.info.id
        if api == "openai":
            if stream:
                send_bytes(handler, 200, "text/event-stream; charset=utf-8", emit_openai_sse(answer, model).encode("utf-8"))
            else:
                send_json(handler, 200, emit_openai_json(answer, model))
        else:
            if stream:
                send_bytes(handler, 200, "text/event-stream; charset=utf-8", emit_anthropic_sse(answer, model).encode("utf-8"))
            else:
                send_json(handler, 200, emit_anthropic_json(answer, model))
        return
    send_json(handler, last_status if last_status in (402, 429) else 502,
              error_body(api, "every free provider refused the request", "upstream_failed"))


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def do_GET(self):
        path = urlsplit(self.path).path
        if path == "/v1/models":
            handle_models(self)
            return
        send_json(self, 404, openai_error("no route GET %s" % path, "not_found"))

    def do_POST(self):
        path = urlsplit(self.path).path
        if path == "/v1/chat/completions":
            handle_completion(self, "openai")
            return
        if path == "/v1/messages":
            handle_completion(self, "anthropic")
            return
        send_json(self, 404, openai_error("no route POST %s" % path, "not_found"))


def register(port, catalog):
    # The catalog rides in the envelope as the exact bytes --list prints.
    body = ('{"protocol_version":%d,"port":%d,"catalog":%s}'
            % (PROTOCOL_VERSION, port, catalog_text(catalog))).encode("utf-8")
    last_error = None
    for _ in range(3):
        try:
            status, _, data = http_request("POST", ENV_URL + "/register", body=body,
                                           headers={"content-type": "application/json"}, timeout=REGISTER_TIMEOUT)
            if status == 200:
                reply = json.loads(data.decode("utf-8"))
                log("registered: the workload holds %s requests" % reply.get("requests"))
                return
            last_error = "HTTP %s: %s" % (status, data[:200])
        except Exception as err:
            last_error = str(err)
        threading.Event().wait(0.2)
    raise RuntimeError("POST %s/register failed: %s" % (ENV_URL, last_error))


def poll_work(server):
    while not STATE.done.is_set():
        try:
            status, _, data = http_request("GET", ENV_URL + "/work", timeout=POLL_TIMEOUT)
            if status == 200:
                work = json.loads(data.decode("utf-8"))
                if is_record(work) and work.get("done") is True:
                    STATE.done.set()
                    threading.Thread(target=server.shutdown, daemon=True).start()
                    return
        except Exception:
            pass
        STATE.done.wait(POLL_INTERVAL)


def main():
    global STATE
    brief = read_brief(PROMPT_FILE)
    if brief.error is not None:
        log(brief.error)
    universe = build_universe(brief) if ENV_URL != "" else {p.id: Probe(p) for p in brief.providers}
    catalog = build_catalog(brief, universe) if ENV_URL != "" else {"providers": []}
    if "--list" in sys.argv:
        sys.stdout.write(catalog_text(catalog) + "\n")
        sys.stdout.flush()
        return 0
    if ENV_URL == "":
        log("MAKEFUN_ENV_URL is not set")
        return 1
    STATE = State(brief, universe, catalog)
    server = ThreadingHTTPServer(("0.0.0.0", 0), Handler)
    server.daemon_threads = True
    port = server.server_address[1]
    if port == 8080:
        server.server_close()
        server = ThreadingHTTPServer(("0.0.0.0", 0), Handler)
        server.daemon_threads = True
        port = server.server_address[1]
    log("listening on 0.0.0.0:%d with %d providers and %d free models" % (
        port, len(catalog["providers"]), len(served_models(brief, universe))))
    register(port, catalog)
    poller = threading.Thread(target=poll_work, args=(server,), daemon=True)
    poller.start()
    try:
        server.serve_forever()
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
