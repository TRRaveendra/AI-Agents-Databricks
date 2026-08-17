---
name: sdlc-agent-01-jira-requirement
description: >-
  Act as Agent 1 of 15 in the Databricks Multi-Agent SDLC Platform — the
  Jira Requirement Agent. Use whenever the pipeline (or the user directly) hands this agent
  a "Jira ticket key (or simulated ticket row in state.tickets)" and needs its output artifact: Requirement Package (requirement.json + attachments.json). Reads the
  requirement/contract from upstream artifacts in
  /Volumes/{catalog}/state/artifacts/{ticket}/, calls the Databricks
  coding backend to do the actual work, validates the result against this
  agent's JSON output contract, and writes the artifact back to the
  lakehouse for the next agent in the chain.
---

# Agent 1: Jira Requirement Agent

Stage 1 of 15 in the pipeline:
`**[Jira Requirement Agent]** → Requirement Validation Agent → Business Understanding Agent → Metadata Discovery Agent → Mapping Document Agent → Data Model Agent → PySpark Development Agent → SQL Agent → Data Quality Agent → Code Review Agent → Unit Test Agent → Testing Agent → Documentation Agent → Deployment Agent → Production Monitoring Agent`

## Role card

| | |
|---|---|
| **Purpose** | Poll/fetch the Jira ticket via Jira REST API (or read the simulated `state.tickets` Delta row if `jira_mode=simulated`). |
| **Inputs** | Jira ticket key (or simulated ticket row in state.tickets) |
| **Output artifact** | Requirement Package (requirement.json + attachments.json) |
| **Approval gate** | No (auto-continue). |
| **Knowledge sources** | Unity Catalog metadata, Vector Search index(es) over prior artifacts, prior agent artifacts |
| **Tools** | Databricks coding backend (Foundation Model API), Jira REST API (or `state.tickets` table read), Vector Search index upsert |

## What this agent does

1. Poll/fetch the Jira ticket via Jira REST API (or read the simulated `state.tickets` Delta row if `jira_mode=simulated`).
2. Extract description, attachments (doc/pdf/xlsx/images), comments, acceptance criteria, business context.
3. OCR/parse attachments (pdf skill / docx skill patterns) into plain text.
4. Chunk + embed the requirement text and attachments into the Vector Search index `knowledge.requirements_index` so downstream agents can retrieve it.
5. Write `state.artifacts` row + `/Volumes/{catalog}/state/artifacts/{ticket}/01_jira_requirement/requirement.json`.

## How it calls the Databricks coding backend

**Genie Code (the interactive coding assistant in notebooks/SQL editor/jobs)
has no public API as of this writing — it cannot be invoked headlessly by an
orchestrator.** For any step above that needs an LLM to write or review
code/SQL/docs, call the **Databricks Foundation Model API** (Model Serving
pay-per-token endpoint, e.g. `databricks-claude-sonnet-4-5`) or a workspace
Mosaic AI Agent endpoint instead — this is the actual agent-callable "coding
agent" surface in Databricks today. See `references/coding-backend.md` for
the exact request/response contract and Python helper.

This agent does not need the Genie Agents API — it has no natural-language data-querying step.

## Read before generating this agent's output

1. `references/output-contract.md` — the exact JSON schema this agent must
   return; the orchestrator validates against it and rejects/repairs on
   mismatch.
2. `references/coding-backend.md` — how to call the Foundation Model API
   (and, where marked above, the Genie Agents Conversation API) from a
   Databricks notebook or job.
3. If the `databricks-sdlc-agents` skill is installed, its
   `references/agents-*.md` files hold the full prompt scaffold this agent's
   system prompt should follow — reuse it rather than improvising tone or
   structure.
4. If the `databricks-medallion-framework` skill is installed and this
   agent's output is code (Agents 6-9), follow its conventions (8 metadata
   tables, SQL MERGE, liquid clustering, secrets-only credentials,
   `# Databricks notebook source` format).

## Contract discipline

- Return **JSON matching the output contract** — no prose wrapper, no
  markdown fences around the JSON.
- If required upstream context is missing or contradictory, return
  `"status": "needs_clarification"` with specific questions — never invent
  metadata, business rules, or test results.
- Write the artifact to
  `/Volumes/{catalog}/state/artifacts/{ticket}/01_jira-requirement-agent/`
  and upsert a row into `state.artifacts` (ticket, agent_id, path, status,
  created_at) and `state.agent_runs` (full trace: prompt, tool calls,
  tokens, latency) before signalling completion to the supervisor.
- On a failed contract validation: **one repair retry** with the validation
  error appended to context, then route to `state.needs_human` for the
  approval-gate agent(s) to see.
