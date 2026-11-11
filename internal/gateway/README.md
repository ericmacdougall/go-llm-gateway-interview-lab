# Gateway request path

The reference uses Go HTTP with mTLS established by the listening process. A verified leaf with one exact configured SPIFFE URI SAN resolves to a client, department, app and role. The handler rejects caller-supplied identity override headers and restricts each request to the resolved client's configuration. Certificate issuance, OIDC delegation and SPIFFE federation remain deployment responsibilities.

| Decision | Reason and trade-off | Implementation and verification |
| --- | --- | --- |
| Authenticate before decoding inference inputs | An unauthenticated client must never trigger provider work. Strict SAN matching requires managing a trusted CA and identity map. | `principal` requires a verified leaf, one URI SAN and exact configuration match. The real TLS tests include no certificate, an untrusted certificate, a foreign workload and an override attempt. |
| Evaluate all route eligibility before admission | A provider failure cannot make a prohibited route eligible. Ordered configuration is simple and explainable but does not optimize cost dynamically. | `eligible` intersects client environment, explicit model choice, capabilities and CEL. Static policy compiles at startup and uses a bounded evaluation budget. Duplicate canonical origin/model pairs are rejected so aliases of the same route cannot consume another attempt. Tests assert that excluded region and environment providers receive zero requests. |
| Reserve and commit dispatch before provider I/O | A transport error does not prove the provider incurred no usage. Durably recording dispatch can leave an uncertain hold if the process dies just before sending. | `Reserve` and `MarkDispatch` precede each network attempt. The dispatch failure test observes zero provider calls. Every candidate takes its own hold under the overall deadline and attempt cap. |
| Normalize explicit outcome codes | Capabilities and known availability failures can use permitted alternatives. An explicit safety refusal and unrecognized response are terminal. Strict validation excludes incomplete vendor responses. | `invoke` checks matching attempt IDs, complete usage counters, and permitted status/code combinations. Redirects are not followed. Tests exercise safety, unknown evidence, unavailable provider, deadline and redirect controls. |
| Compute costs using integer rates | Integer units avoid floating-point drift. The estimate uses UTF-8 bytes of roles and text plus the output cap as a conservative lab approximation rather than a vendor tokenizer. | `cost` bounds rates and usage counters. Settlement uses final usage and the attempted route's recorded prices. A request cannot reuse an uncertain attempt's hold. |
| Use a read-only reconciliation adapter | A timeout must never cause the reconciler to replay inference. A stable attempt ID allows the lab provider to correlate even when the provider ID reply was lost. Real providers without equivalent lookup need manual disposition. | `Lookup` requires a configured matching provider origin, model and account, validates both IDs where available, and makes GET /usage calls only. Test captures usage after a timed-out inference and verifies that the inference call count stays at one. |
| Finish text accounting before JSON success; stop fallback after a stream write | JSON success follows settlement. Real vendor SSE delivers deltas before final accounting, so response commitment prevents switching providers after partial output. | The fixture protocol renders a bounded settled completion as SSE. The OpenAI, Anthropic and Gemini adapters deliver actual upstream text deltas, consume terminal usage and settle afterward. A partial stream, explicit refusal or caller disconnect preserves unresolved usage, and emitted bytes prevent fallback. |
| Audit request-level denials without logging prompts | Missing audit availability must not bypass authorization. A single denial records the active rule and version but does not reveal every excluded candidate. | `reject` persists a structured denial and sends a generic result. The ledger owns financial and attempt event persistence. |

The default fixture upstream protocol is a deterministic local test contract. POST /infer accepts an attempt ID, model, messages and output cap. GET /usage reads historical usage by stable attempt ID and optional provider request ID. GET /calls returns fixture counters. The public completion shape supports text messages, an optional exact model, a bounded token count, capability hints and SSE output. The fixture protocol buffers a settled result, while supported vendor protocols forward upstream deltas before terminal accounting. It does not claim drop-in support for arbitrary OpenAI, Anthropic or Google SDK requests.

Provider endpoints are configuration inputs, not user inputs. TLS verification is required by the default client for HTTPS, redirects are disabled, and cleartext requires an explicit loopback IP fixture flag. Region and environment fields describe an approved deployment; the gateway does not establish the cloud's physical location or provider retention controls. A production installation still needs trusted route inventory, controlled egress and suitable provider contracts.

Attempts remain traceable after request cancellation. The handler makes a bounded outcome-write attempt with a fresh accounting deadline, then the scheduled worker can reconcile a durable dispatch if that write failed. When a provider's final amount is unknown, the reservation stays held.


## Provider protocols and credentials

A route's `protocol` selects `openai`, `openrouter`, `anthropic`, `gemini`, or the default `fixture`. Real-provider routes name an `auth_env` variable. The constructor reads its value into unexported runtime state and never stores it in route metadata, audit records or returned errors. Configure provider origins through trusted deployment configuration. The server disables redirects, verifies HTTPS certificates, and sends authentication only to that configured origin.

| Protocol | Request and stream method | Normalization |
| --- | --- | --- |
| OpenAI-compatible | POST /v1/chat/completions with text messages, max_completion_tokens, one choice and store=false. Stream mode requests include_usage. | Text content and explicit refusal/content_filter, stop/length finish, prompt_tokens and completion_tokens. Tool calls are terminal unsupported content. |
| OpenRouter | POST /api/v1/chat/completions with Bearer authentication, max_tokens, and optional stream=true. | OpenAI-compatible text/usage semantics. The last usage event may repeat one empty delta and finish_reason, which is accepted before DONE. |
| Anthropic | POST /v1/messages with anthropic-version 2023-06-01, x-api-key, top-level system text, max_tokens and user/assistant conversation. | Text blocks, end_turn/max_tokens/stop_sequence, explicit refusal. Streaming consumes message_start, text block deltas, cumulative message_delta usage and message_stop. Extended thinking blocks, tools, beta server fallback and unknown event kinds fail conservatively. |
| Gemini | POST /v1beta/models/{model}:generateContent or :streamGenerateContent?alt=sse with x-goog-api-key, systemInstruction and role-mapped contents. | Text parts, promptFeedback blocking and safety finish reasons, STOP/MAX_TOKENS, responseId and usageMetadata. Hidden thought text is omitted; billed thought counts are retained in output units. |

The public request remains the deliberately bounded OpenAI-like text endpoint. Multimodal inputs, tool calls, vendor-native inbound schemas, Responses API and arbitrary SDK features need their own adapters and contract tests. Model capability metadata must be populated for the exact models and deployment in use.

Configured accounting uses one flat input-unit rate and one output-unit rate. OpenAI completion usage includes reasoning categories in the provider total. Anthropic cache creation/read input categories are added to ordinary input counts. Gemini prompt plus tool-use prompt counts are input units, and candidates plus thought counts are output units. Cache discounts, service tiers and provider-specific billing categories can change invoices. This reference records deterministic configured-unit usage and does not assert exact invoice equivalence.

Real-provider aggregate reporting does not provide a generic way to recover one unknown request's exact bill. The generic usage lookup therefore leaves real-provider unknown usage pending for audited manual evidence. It never fabricates zero usage or replays inference. The fixture provides an exact per-attempt usage endpoint specifically for deterministic recovery tests.

A route can set `requires_approval`. CEL and client-environment restrictions remain prerequisites. Before each protected provider attempt, the approval service's `GrantID` selects a current durable grant for the authenticated principal and exact model. `Store.MarkDispatch` then rechecks that exact grant, scope, state and fresh expiry inside the dispatch transaction, retaining a shared grant-row lock through commit. A missing checker denies protected routes. This is a model-access grant and cannot widen region, environment or budget limits.

The SSE parser bounds event size and total bytes. It forwards only accepted text deltas with write deadlines and flushing. Final accounting runs after the stream has completed or failed. On truncated usage, malformed events or consumer disconnect, the durable reservation remains recoverable. Once a delta is flushed, a later error is sent in the SSE stream with no provider fallback. Content already delivered cannot be retracted if the provider later emits a refusal.

Protocol references were consulted November 10, 2026. Tests use local, protocol-faithful HTTP fixtures and real mTLS ingress, with barriers proving that the caller receives a delta before the provider is allowed to send final usage.

- [OpenAI Chat API reference](https://developers.openai.com/api/reference/resources/chat)
- [Anthropic Messages reference](https://platform.claude.com/docs/en/api/messages)
- [Anthropic streaming events](https://platform.claude.com/docs/en/build-with-claude/streaming)
- [Gemini generate-content reference](https://aistudio.google.com/api-reference/generate-content)


OpenRouter authentication uses the configured bearer credential and an optional static X-OpenRouter-Title attribution. The request uses max_tokens and the router's /api/v1/chat/completions path. Token usage is normalized with the OpenAI-compatible parser; the router's final usage chunk can repeat an empty choice and terminal finish reason. A test barrier covers this documented final-chunk shape. The gateway does not infer the router's downstream serving region from its public origin; sovereignty-restricted clients must have independently approved routing guarantees before using an aggregator.

OpenRouter protocol references checked November 10, 2026.

- [OpenRouter chat completions](https://openrouter.ai/docs/api/api-reference/chat/create-a-chat-completion)
- [OpenRouter streaming and final usage chunk](https://openrouter.ai/docs/api_reference/streaming)
- [OpenRouter authentication](https://openrouter.ai/docs/api_reference/authentication)
