# Results — free-model-aggregator-01

## The contract, as the kit states it (RULES.txt + the kit's env/scorer source)

- **Entrypoint.** One file at the repository root: `main.py` (Python 3.13,
  standard library only) or `main.ts` (Node 22). This entry is `main.py`.
  Nothing may be installed before it runs; the server reaches only the
  environment at `$MAKEFUN_ENV_URL`.
- **Startup.** Read the brief at `$MAKEFUN_PROMPT_FILE`: three JSON
  documents located by their `schema` field — `providers/v1` (directory:
  id, base path under the env URL, key variable, wire), `ratings/v1`
  (quality table of the FREE models only: rating_milli, context, tools,
  vision, json), `workload/v1` (the manifest: the trial's requests in
  order, each with id, api, stream, weight, and requires
  {tools, vision, json, min_context}). Probe each provider's catalog route,
  listen on an OS-assigned port (never 8080), then
  `POST $MAKEFUN_ENV_URL/register {"protocol_version": 1, "port", "catalog"}`.
  Poll `GET /work` every 100 ms; exit 0 when it answers `{"done": true}`.
- **Catalog document.** `{"providers": [{"id", "base_url" (the directory
  path, e.g. `/p/groq`), "key_env", "wire", "models": [{"id", "free",
  "context_length"}]}]}` — providers in directory order, models sorted by
  id. `python3 main.py --list` prints the same document, byte for byte.
  Paid models are discoverable only from the providers' live catalog
  routes (the quality table lists free models only).
- **Serving.** `POST /v1/chat/completions` (OpenAI) and `POST /v1/messages`
  (Anthropic), JSON and SSE streaming; `GET /v1/models` answers in the
  Anthropic shape when an `x-api-key` header is present, else the OpenAI
  shape, and must list exactly the free model ids of the registered
  catalog. The client sends the ~20 workload requests one at a time
  (30 s each, 2 MB response cap). Pod limits per trial: 180 s, 1 CPU,
  2 GB, 64 processes, shared with the env.
- **Providers.** Ten simulated providers under the env URL, three wires
  (OpenAI `/v1/chat/completions` + `/v1/models`; Anthropic `/v1/messages`
  + `/v1/models` with `anthropic-version: 2023-06-01`; Gemini
  `/v1beta/models/<model>:generateContent` / `:streamGenerateContent` +
  `/v1beta/models`), four auth styles (bearer, x-api-key, x-goog-api-key,
  none; the key is read from the directory's `key_env` variable). Each has
  a per-trial call budget and scripted failures, drawn per trial and never
  published: a refused call answers the provider's failure status — 402
  for one provider, 429 for the other nine — and consumes budget;
  malformed calls (wrong key, paid/unknown model) are rejected without
  consuming it. A provider's answer text is an echo of the request as that
  provider parsed it (system text, last user text, tool names, max_tokens,
  stops, image hash, JSON-mode flag), and the env compares it against the
  client's own request — so translations must preserve those fields
  exactly. A valid request needs, among its wire checks, exactly ONE
  successful provider call and a serving model that meets the manifest's
  requirements.
- **Scoring** (the kit's sealed scorer): quality 60% (served rating vs the
  best free rating per request, budget-blind), served 15% (valid / total),
  wire 10% (per-request checks passed), failover 10% (requests whose first
  call failed but still landed valid), catalog 5% (42 checks). Ties break
  on fewer total provider calls. The run must be deterministic: the final
  replays the trial and the state export must match byte for byte.
- **Local grading note.** The kit's `grade-local --driver process` runs
  node22 only ("python3.13 runs only inside Docker"), and no Docker is
  available on this machine, so the trials below were run by driving the
  kit's own env server and sealed scorer directly — the same components
  grade-local orchestrates.

## Architecture

- **Brief parsing** scans the prompt file for the three schema documents
  (raw JSON decode at every offset), immune to prose and key order.
- **Catalog** is probe-first: each provider's live listing decides its
  models; a model is free when the ratings table lists it for that
  provider, else when the listing says so (`free` flag, or zero/absent
  pricing on the OpenAI wire). A failed probe falls back to a
  ratings-synthesized entry so the provider still registers (full catalog
  credit) while routing deprioritizes nothing it cannot verify — its
  models keep their table capabilities.
- **Routing** pairs the n-th client request with the n-th manifest entry
  (the client sends them in order, one at a time) and filters free models
  by the entry's requirements, capabilities taken from the ratings row
  first and the listing second; unknown capability never satisfies a
  requirement. Order: rating desc, directory order, model id. JSON-mode
  requests never route to Anthropic-wire providers (that wire's echo
  cannot report JSON mode, which would void the request).
- **Wires.** Same-wire calls are rebuilt with only the model swapped
  (OpenAI streams add `stream_options.include_usage` so the usage chunk
  the checker requires exists). Cross-wire calls pass through a canonical
  request (system texts, ordered messages with text/image/tool parts,
  tools, tool choice, max_tokens, stops, JSON-mode, stream) and pairwise
  emitters for the three provider wires; answers return through canonical
  form (text pieces + usage) and are re-emitted in the client's wire,
  including SSE re-framing (OpenAI: content chunks, finish chunk, usage
  chunk, `[DONE]`; Anthropic: message_start → content_block_start →
  deltas → content_block_stop → message_delta → message_stop).
  Translation invariants guarded throughout: the last user message's text
  survives even when a conversation ends in tool results (an empty user
  turn is emitted where a wire requires one), image bytes/URLs keep their
  hash identity across data-URL/base64/fileData forms, `max_tokens` /
  `max_completion_tokens` unify, and the stop-reason family is always
  "stop"/"end_turn" as the providers report family 0.
- **Failover** is sequential over the candidate list on any non-2xx or
  transport error, stopping at the first success — one successful provider
  call per served request, as the checker requires. No waiting on
  Retry-After: the rules forbid wall-clock dependence, and a refusal costs
  nothing but tie-break calls, so the next candidate is tried at once.
- **Determinism:** fixed timestamps in model lists and synthesized frames,
  total-order candidate sorting, no clocks, no randomness.

## Local test results (kit env + kit scorer, practice trials)

| Trial | Valid | Upstream calls | Score | Quality | Served | Wire | Failover | Catalog |
|-------|-------|----------------|-------|---------|--------|------|----------|---------|
| 0     | 20/20 | 32             | 99    | 99      | 100    | 100  | 100      | 100     |
| 1     | 20/20 | 39             | 99    | 99      | 100    | 100  | 100      | 100     |
| 2     | 20/20 | 36             | 99    | 99      | 100    | 100  | 100      | 100     |

- The quality point lost in each trial comes from the sealed spec's
  scripted per-call failures on the top-rated model (e.g. trial 0 draws
  429s on specific call numbers of the best provider): the first attempt
  must fail, and failover lands on the next-best model (1470/1465 vs 1475
  rating). No routing order can avoid a scripted failure on the best
  model's call index, so 99 is effectively the ceiling for these draws.
- **Replay determinism:** trial 0 run twice produced byte-identical
  `state.json` exports (deep-equal over the full export, catalog and all
  20 request records).
- **`--list` fidelity:** with a live trial-1 env, `python3 main.py --list`
  output is byte-for-byte the catalog the server registered (as recorded
  in the env's state export), and its free/paid model sets match the
  trial's reference catalog for all ten providers.
- **Robustness:** malformed JSON bodies, oversized bodies, and unknown
  routes answer 400/404 in the client's wire shape instead of crashing;
  every candidate failing answers 402/429/502 in shape.

## Notes for the submitter

- Budgets, failure call indices, and outages are drawn per trial from a
  seed and never published; the sealed-trial spec differs from practice.
  The router therefore plans nothing around budgets — greedy best-first
  with failover is the intended shape (requests arrive weight-2 first).
- The one paid model seen in practice (`anthropic/claude-opus-5.5` on
  openrouter) is registered with `free: false` and never routed to.
- Entry is NOT handed-over; transcripts were bundled from the build
  session in the usual way.
