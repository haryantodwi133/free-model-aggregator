# Free-model aggregator

One dependency-free Python server that fronts ten simulated free-tier model
providers and answers on two wires at once: an OpenAI-compatible API
(`POST /v1/chat/completions`) and an Anthropic-compatible API
(`POST /v1/messages`), both in JSON and streaming (SSE) form, plus
`GET /v1/models` in either shape.

Run it the way the harness does:

```
MAKEFUN_ENV_URL=<env> MAKEFUN_PROMPT_FILE=<brief> python3 main.py
python3 main.py --list   # prints the catalog document the server registers
```

## How it works

- **Brief.** The prompt file carries three JSON documents — a provider
  directory, a quality table of the free models, and a workload manifest —
  found by scanning for their `schema` fields, so prose and key order in the
  brief do not matter.
- **Catalog.** At startup the server probes every provider's own model list
  in that provider's wire and auth, and registers a catalog built from what
  the probes return: a model is free when the quality table lists it for that
  provider, else when the listing itself says so (a `free` flag or zero
  pricing). If a probe fails, the provider's entry is synthesized from the
  quality table so the catalog stays complete. `--list` prints exactly the
  document that registration sends.
- **Routing.** The workload arrives one request at a time, in manifest
  order, so the server pairs the n-th client request with the n-th manifest
  entry and routes on its stated requirements (tools, vision, JSON mode,
  minimum context). Candidates are the free models that meet every
  requirement — capabilities read from the quality table first, the listing
  second — ordered by the table's quality rating, then directory order, then
  model id. Anthropic-wire providers are skipped for JSON-mode requests,
  because that wire cannot carry JSON mode.
- **Translation.** Same-wire requests are rebuilt with only the model
  swapped (OpenAI streams ask the provider for a usage chunk). Cross-wire
  requests go through a canonical form and are re-emitted in the provider's
  wire — OpenAI, Anthropic, or Gemini — preserving exactly what the
  providers echo back: system text, the last user message's text, tool names
  in order, `max_tokens`, stop sequences, image bytes, and the JSON-mode
  flag. Answers are translated back the same way, usage included; streams
  are re-framed in the client's own event grammar.
- **Failover.** Any refusal (402 when a free allowance is gone, 429,
  5xx, a dead connection) moves to the next candidate; the first success
  answers, so a served request costs exactly one successful provider call.

## Determinism

No wall clock, no randomness: model lists and synthesized frames use fixed
timestamps, candidate order is a total order, and a replay of a practice
trial reproduces the environment's state export byte for byte (verified
locally — see RESULTS.md).

## Layout

- `main.py` — the whole server (Python 3.13 standard library only).
- `RESULTS.md` — the contract as the kit states it, and local test results.
- `LICENSE` — MIT.
