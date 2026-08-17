# Output contract — Agent 12 Testing Agent

The orchestrator (LangGraph supervisor) validates this agent's return value
against the JSON schema below before handing off to the next node. Adapt
field names to match `agents/prompts.py` if that file already exists in the
target project — this is the reference shape, not a hard-coded schema file.

```json
{
  "agent": "12_testing-agent",
  "ticket_id": "string",
  "status": "complete | needs_clarification | failed",
  "artifact_path": "/Volumes/{catalog}/state/artifacts/{ticket}/12_testing-agent/...",
  "summary": "2-4 sentence summary of what was produced, for downstream agents' context window",
  "clarification_questions": ["only present when status == needs_clarification"],
  "citations": ["source table / doc / prior-agent-artifact this output is grounded in"],
  "tool_calls": [
    {"tool": "foundation_model_api | genie_agents_conversation_api | uc_metadata | sql_warehouse | jobs_api", "detail": "string"}
  ],
  "tokens": {"input": 0, "output": 0},
  "duration_ms": 0
}
```

Rules:
- `status: "failed"` is only for unrecoverable errors (backend unreachable,
  permission denied) — everything else resolves to `complete` or
  `needs_clarification`.
- `citations` must reference something retrievable (a UC table, a prior
  artifact path, a Genie conversation_id) — never a made-up source.
- Keep `summary` short; downstream agents receive this + the artifact path,
  not the full artifact, to control context size.
