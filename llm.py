"""Minimal OpenRouter client (stdlib only) with an on-disk response cache.

Config (environment or a .env file next to this script):
    OPENROUTER_API_KEY   required
    EXTRACT_MODEL        cheap model for per-document extraction
    REASON_MODEL         stronger model for diagrams, verification and Q&A
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import time
import urllib.error
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
CACHE = HERE / ".cache" / "llm"
API = "https://openrouter.ai/api/v1/chat/completions"


def _load_env():
    env = HERE / ".env"
    if env.exists():
        for line in env.read_text().splitlines():
            m = re.match(r"\s*([A-Z_][A-Z0-9_]*)\s*=\s*(.*?)\s*$", line)
            if m and m.group(1) not in os.environ:
                os.environ[m.group(1)] = m.group(2).strip("\"'")


_load_env()
EXTRACT_MODEL = os.environ.get("EXTRACT_MODEL", "deepseek/deepseek-v4.1-flash")
REASON_MODEL = os.environ.get("REASON_MODEL", "deepseek/deepseek-v4.1-flash")


def available() -> bool:
    return bool(os.environ.get("OPENROUTER_API_KEY"))


class LLMError(RuntimeError):
    pass


def chat(messages: list[dict], model: str, *, tools: list | None = None, json_mode: bool = False,
         max_tokens: int = 4000, cache: bool = True) -> dict:
    """Returns the assistant message dict. Identical requests are served from cache."""
    body = {"model": model, "messages": messages, "max_tokens": max_tokens, "temperature": 0}
    if tools:
        body["tools"] = tools
    if json_mode:
        body["response_format"] = {"type": "json_object"}
    raw = json.dumps(body, sort_keys=True)
    key = hashlib.sha256(raw.encode()).hexdigest()
    path = CACHE / f"{key}.json"
    if cache and path.exists():
        return json.loads(path.read_text())
    if not available():
        raise LLMError("OPENROUTER_API_KEY is not set (env or .env)")
    msg, finish = _send(raw)
    if (_empty(msg) or finish == "length") and max_tokens < 32000:
        # Reasoning models (e.g. DeepSeek) can spend the budget on hidden reasoning and return an
        # empty or cut-off answer. Retry once with more room and less reasoning.
        retry = dict(body, max_tokens=min(max_tokens * 4, 32000), reasoning={"effort": "low"})
        msg, finish = _send(json.dumps(retry, sort_keys=True))
    if _empty(msg):
        raise LLMError(f"model returned an empty answer (reasoning used {len(msg.get('reasoning') or '')} chars)")
    if cache and finish != "length":  # never cache empty/cut-off answers: a rerun would replay the failure
        CACHE.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(msg))
    return msg


def _empty(msg: dict) -> bool:
    return not (msg.get("content") or "").strip() and not msg.get("tool_calls")


def _send(raw: str) -> dict:
    req = urllib.request.Request(API, data=raw.encode(), headers={
        "Authorization": f"Bearer {os.environ['OPENROUTER_API_KEY']}",
        "Content-Type": "application/json",
        "X-Title": "attt-context-service",
    })
    for attempt in range(4):
        try:
            with urllib.request.urlopen(req, timeout=180) as r:
                data = json.loads(r.read())
            break
        except urllib.error.HTTPError as e:
            detail = e.read().decode(errors="replace")[:300]
            if e.code in (429, 500, 502, 503, 504) and attempt < 3:
                time.sleep(2 ** attempt * 2)
                continue
            raise LLMError(f"OpenRouter HTTP {e.code}: {detail}") from None
        except (urllib.error.URLError, TimeoutError) as e:
            if attempt < 3:
                time.sleep(2 ** attempt * 2)
                continue
            raise LLMError(f"OpenRouter unreachable: {e}") from None
    if "error" in data:
        raise LLMError(f"OpenRouter error: {data['error']}")
    choice = data["choices"][0]
    return choice["message"], choice.get("finish_reason") or choice.get("native_finish_reason")


def chat_json(system: str, user: str | list, model: str, max_tokens: int = 4000) -> dict:
    """Ask for a JSON object and parse it (tolerates ```json fences)."""
    msg = chat([{"role": "system", "content": system}, {"role": "user", "content": user}],
               model, json_mode=True, max_tokens=max_tokens)
    text = msg.get("content") or ""
    m = re.search(r"\{.*\}", text, re.S)
    if not m:
        raise LLMError(f"model returned no JSON: {text[:200]}")
    return json.loads(m.group(0))


def image_part(path: Path) -> dict:
    mime = "image/png" if path.suffix.lower() == ".png" else "image/jpeg"
    data = base64.b64encode(path.read_bytes()).decode()
    return {"type": "image_url", "image_url": {"url": f"data:{mime};base64,{data}"}}
