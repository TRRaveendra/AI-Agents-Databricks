# Notes on Informatica PowerCenter & IDMC Export Structure

## PowerCenter

Typical export is a single or multi-object XML file containing:

- `<SOURCE>` / `<TARGET>` definitions
- `<MAPPING>` with `<TRANSFORMATION>` elements (SOURCE QUALIFIER, EXPRESSION, FILTER, JOINER, LOOKUP, AGGREGATOR, ROUTER, UPDATE STRATEGY, etc.)
- `<INSTANCE>` and `<CONNECTOR>` describing the data flow graph
- `<SESSION>` with connection references, parameter files, pre/post SQL
- `<WORKFLOW>` / `<WORKLET>` with task dependencies

Key XPath-style locations (conceptual):

- Transformations: `//MAPPING/TRANSFORMATION`
- Ports & expressions: `//TRANSFORMATION/TRANSFORMFIELD` + expression attributes
- Connectors: `//MAPPING/CONNECTOR`
- Lookup SQL overrides / conditions: inside LOOKUP transformation attributes
- Session connections: `//SESSION/SESSIONEXTENSION`

## IDMC / IICS (Cloud)

Exports are often JSON or a different XML shape (mapping configuration, task flows). The logical objects are similar (Source, Target, Expression, Filter, Joiner, Lookup, etc.) but attribute names differ.

When only a high-level description is available (no XML), ask the user for:

1. List of transformations in execution order
2. Key expressions (especially IIF / DECODE / lookups)
3. Source and target definitions (type, filter, SQL override)
4. Update Strategy logic and target load type (insert/update/delete/upsert)
5. Session-level parameters and scheduling

## Recommended Agent Behavior

1. If full XML is provided, parse into a graph of transformations and emit one `run_` function per mapping.
2. If only a summary is provided, generate the function from the described logic and clearly mark assumptions.
3. Always surface unconnected lookups, sequence generators, and multi-target update strategies for human review.
4. Prefer generating both a pure Python/PySpark module and a thin Databricks notebook wrapper that calls it.
