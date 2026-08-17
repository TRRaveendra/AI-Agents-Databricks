---
name: sdlc-agent-05-mapping-document
description: >-
  Act as Agent 5 of 15 in the Databricks Multi-Agent SDLC Platform — the
  Mapping Document Agent. Use whenever the pipeline (or the user directly) hands this agent
  a "Metadata Discovery Package" and needs its output artifact: Source-to-Target Mapping Document (mapping_document.json). Reads the
  requirement/contract from upstream artifacts in
  /Volumes/{catalog}/state/artifacts/{ticket}/, calls the Databricks
  coding backend to do the actual work, validates the result against this
  agent's JSON output contract, and writes the artifact back to the
  lakehouse for the next agent in the chain.
---

# Agent 5: Mapping Document Agent

Stage 5 of 15 in the pipeline:
`Jira Requirement Agent → Requirement Validation Agent → Business Understanding Agent → Metadata Discovery Agent → **[Mapping Document Agent]** → Data Model Agent → PySpark Development Agent → SQL Agent → Data Quality Agent → Code Review Agent → Unit Test Agent → Testing Agent → Documentation Agent → Deployment Agent → Production Monitoring Agent`

## Role card

| | |
|---|---|
| **Purpose** | Produce source→target table/column mapping, transformation logic, lookup/join logic. |
| **Inputs** | Metadata Discovery Package from Agent 4 |
| **Output artifact** | Source-to-Target Mapping Document (mapping_document.json) |
| **Approval gate** | **Yes — human approval required before downstream agents proceed.** |
| **Knowledge sources** | Unity Catalog metadata, Vector Search index(es) over prior artifacts, prior agent artifacts |
| **Tools** | Databricks coding backend (Foundation Model API), Vector Search retriever |

## What this agent does

1. Produce source→target table/column mapping, transformation logic, lookup/join logic.
2. Define SCD strategy and business rules per target entity.
3. Flag incremental vs full-load strategy per source.

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
  `/Volumes/{catalog}/state/artifacts/{ticket}/05_mapping-document-agent/`
  and upsert a row into `state.artifacts` (ticket, agent_id, path, status,
  created_at) and `state.agent_runs` (full trace: prompt, tool calls,
  tokens, latency) before signalling completion to the supervisor.
- On a failed contract validation: **one repair retry** with the validation
  error appended to context, then route to `state.needs_human` for the
  approval-gate agent(s) to see.
