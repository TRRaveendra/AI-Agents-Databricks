# Calling the Databricks coding backend

## Why not Genie Code

Genie Code (Databricks' AI coding/data assistant embedded in notebooks, the
SQL editor, jobs, and the file editor) is **UI-only** — there is currently
no REST/SDK endpoint to invoke it headlessly from an orchestrator or a job.
Do not design any agent around calling "Genie Code via API"; it will not
work today. Re-check Databricks' Genie Code docs periodically in case this
changes.

## What to use instead: Foundation Model API

Every code/SQL/doc-generation step in this pipeline should call a **Model
Serving Foundation Model API** endpoint (pay-per-token, OpenAI-compatible
chat completions) or a deployed Mosaic AI Agent Framework endpoint. This is
the real agent-callable "coding agent" surface in Databricks.

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

def call_coding_backend(system_prompt: str, user_prompt: str,
                         endpoint: str = "databricks-claude-sonnet-4-5",
                         max_tokens: int = 4096) -> str:
    resp = w.serving_endpoints.query(
        name=endpoint,
        messages=[
            {{"role": "system", "content": system_prompt}},
            {{"role": "user", "content": user_prompt}},
        ],
        max_tokens=max_tokens,
        temperature=0.2,
    )
    return resp.choices[0].message.content
```

Pick per-agent model strength deliberately: smaller/cheaper models for
extraction & validation agents (1, 2, 13), the strongest available coding
model for generation/review agents (6, 7, 8, 9, 10), mid-tier for the rest.
Wrap every call in `@mlflow.trace` and log tokens + latency to
`state.agent_runs`.

## Where Genie Agents (Genie Spaces) DOES fit

The Genie Agents Conversation API *is* a real, supported headless API — but
it answers natural-language **data questions**, it does not write general
production code. Use it as a *tool* inside agents 3, 4, 8, and 15 (marked
above) for ad-hoc profiling / business-metric questions, not as the backend
for code generation.

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

def ask_genie(space_id: str, question: str, conversation_id: str | None = None):
    if conversation_id is None:
        result = w.genie.start_conversation_and_wait(space_id=space_id, content=question)
    else:
        result = w.genie.create_message_and_wait(
            space_id=space_id, conversation_id=conversation_id, content=question
        )
    return result  # contains conversation_id, generated SQL, and result data — record both for citations
```

Auth: OAuth M2M (service principal) for anything running unattended in a
job/agent; PAT only for local testing. Requires `CAN USE` on the SQL
warehouse backing the Genie Space, plus access to the underlying UC tables.

## Guardrails for every call

- Never pass secrets/PII into prompts; reference by name and let the
  backend/tool resolve values via `dbutils.secrets` or UC grants.
- Log every call (prompt, response, tokens, latency, ticket id) to
  `state.agent_runs` for audit.
- Respect a per-agent max-iteration / token budget from pipeline state to
  avoid runaway loops.
