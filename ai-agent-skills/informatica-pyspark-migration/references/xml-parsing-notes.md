# Parsing PowerCenter XML and IDMC Exports

Read this before touching an export file. Never eyeball a mapping XML larger
than a couple of hundred lines — parse it into a component graph first, then
work from the graph.

## Contents

1. [What a PowerCenter export looks like](#1-what-a-powercenter-export-looks-like)
2. [Parsing safely](#2-parsing-safely)
3. [The element hierarchy](#3-the-element-hierarchy)
4. [Building the dataflow graph](#4-building-the-dataflow-graph)
5. [Where each transformation hides its logic](#5-where-each-transformation-hides-its-logic)
6. [Sessions and workflows in the XML](#6-sessions-and-workflows-in-the-xml)
7. [A working extractor](#7-a-working-extractor)
8. [What the XML does not tell you](#8-what-the-xml-does-not-tell-you)
9. [IDMC / Cloud Data Integration exports](#9-idmc--cloud-data-integration-exports)

---

## 1. What a PowerCenter export looks like

An export from Repository Manager or `pmrep ObjectExport` is a single XML file:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE POWERMART SYSTEM "powrmart.dtd">
<POWERMART CREATION_DATE="..." REPOSITORY_VERSION="...">
  <REPOSITORY NAME="REP_DEV" VERSION="186" CODEPAGE="UTF-8" DATABASETYPE="Oracle">
    <FOLDER NAME="SALES_DW" GROUP="" OWNER="dev" ...>
      <SOURCE NAME="CUSTOMER" DBDNAME="ORA_SRC" .../>
      <TARGET NAME="DIM_CUSTOMER" .../>
      <MAPPING NAME="m_CUSTOMER_DIM_LOAD" ISVALID="YES">...</MAPPING>
      <SESSION NAME="s_m_CUSTOMER_DIM_LOAD" MAPPINGNAME="m_CUSTOMER_DIM_LOAD">...</SESSION>
      <WORKFLOW NAME="wf_DAILY_SALES">...</WORKFLOW>
    </FOLDER>
  </REPOSITORY>
</POWERMART>
```

Practical notes: exports are usually UTF-8 but can carry a repository codepage
that is not; the `SHORTCUT` element means the object lives in a shared folder
and its definition is elsewhere; a "mapping only" export omits sessions and
workflows, so if you were given one, ask for the workflow export too before
promising an orchestration conversion.

## 2. Parsing safely

The `<!DOCTYPE ... SYSTEM "powrmart.dtd">` declaration is an external entity
reference. Parse with entity resolution and network access disabled — an export
file is untrusted input, and a DTD fetch will either hang or leak.

```python
from lxml import etree

parser = etree.XMLParser(
    resolve_entities=False,   # no entity expansion
    no_network=True,          # never fetch the DTD
    load_dtd=False,
    huge_tree=False,
    recover=True,             # tolerate the minor malformations exports contain
)
tree = etree.parse(path, parser)
root = tree.getroot()
```

If `lxml` is unavailable, `xml.etree.ElementTree` does not resolve external
entities by default and is acceptable; `defusedxml` is better still. For exports
over a few hundred MB (whole-folder exports of mature repositories reach this),
use `etree.iterparse` on `MAPPING` / `SESSION` / `WORKFLOW` elements and clear
each after processing, rather than loading the whole tree.

## 3. The element hierarchy

Inside `<MAPPING>`:

| Element | Meaning |
|---|---|
| `<TRANSFORMATION>` | the *definition* — type, ports, expressions |
| `<INSTANCE>` | the *placement* on the canvas; references a TRANSFORMATION by name |
| `<CONNECTOR>` | one port-to-port link: `FROMINSTANCE`, `FROMFIELD`, `TOINSTANCE`, `TOFIELD` |
| `<TARGETLOADORDER>` | the order target instances are loaded |
| `<MAPPINGVARIABLE>` | `$$` variables with datatype, aggregation (MAX/MIN/COUNT), initial value |
| `<ERPINFO>`, `<METADATAEXTENSION>` | usually ignorable |

Inside `<TRANSFORMATION>`:

| Element | Meaning |
|---|---|
| `<TRANSFORMFIELD>` | a port: `NAME`, `PORTTYPE`, `DATATYPE`, `PRECISION`, `SCALE`, `EXPRESSION`, `DEFAULTVALUE` |
| `<TABLEATTRIBUTE>` | the transformation's properties, as `NAME`/`VALUE` pairs |
| `<GROUP>` | Router/Union groups, with the group condition in `EXPRESSION` |
| `<FIELDATTRIBUTE>` | per-port properties (sort key, sort direction, lookup condition role) |

`PORTTYPE` values you will see: `INPUT`, `OUTPUT`, `INPUT/OUTPUT`,
`VARIABLE`, `LOCAL VARIABLE`, `LOOKUP`, `RETURN`. The `VARIABLE` ports are
evaluated in declaration order and are where row-carrying logic lives — read
them in document order, never alphabetically.

The `TYPE` attribute on `<TRANSFORMATION>` uses these strings (exact spelling
matters when you filter on them):

```
Source Qualifier, Expression, Filter, Router, Joiner, Aggregator, Sorter,
Rank, Lookup Procedure, Sequence, Update Strategy, Normalizer,
Union Transformation, Stored Procedure, Custom Transformation,
External Procedure, Transaction Control, SQL, Java, XML Parser, XML Generator,
Application Source Qualifier, MQ Source Qualifier
```

Note `Lookup Procedure` (not "Lookup") and `Sequence` (not "Sequence Generator").

## 4. Building the dataflow graph

`<CONNECTOR>` elements are the ground truth for the dataflow. Build a directed
graph keyed on instance name, then topologically sort it:

```python
edges = [(c.get("FROMINSTANCE"), c.get("TOINSTANCE"))
         for c in mapping.findall("CONNECTOR")]
```

Deduplicate — there is one CONNECTOR per *port*, so two instances linked by
twelve ports produce twelve edges. Keep the port-level detail separately; you
need it to know which columns actually flow forward, and therefore which columns
to `select` at each step. Ports that exist in a transformation but are never
connected onward are dead and should not appear in the generated code.

The topological order gives you the sequence of steps to emit. Where the graph
branches (Router, or one instance feeding two downstream instances), that is
where a `cache()` may be justified.

## 5. Where each transformation hides its logic

| Transformation | Where to look |
|---|---|
| Source Qualifier | `TABLEATTRIBUTE` `Sql Query`, `Source Filter`, `User Defined Join`, `Number Of Sorted Ports`, `Select Distinct` |
| Expression | each `TRANSFORMFIELD`'s `EXPRESSION` attribute, in document order |
| Filter | `TABLEATTRIBUTE` `Filter Condition` |
| Router | `<GROUP>` elements; each has `NAME` and `EXPRESSION`; DEFAULT group is implicit |
| Joiner | `TABLEATTRIBUTE` `Join Condition` and `Join Type`; master ports carry a `FIELDATTRIBUTE` marking the master group |
| Lookup | `TABLEATTRIBUTE` `Lookup Sql Override`, `Lookup condition`, `Lookup table name`, `Lookup policy on multiple match`, `Lookup caching enabled`, `Dynamic lookup cache`, `Recache from lookup source` |
| Aggregator | `TRANSFORMFIELD` with `EXPRESSION` for aggregates; group-by ports carry `GROUPBY` in `FIELDATTRIBUTE`/`PORTTYPE` context; `TABLEATTRIBUTE` `Sorted Input`, `Is Incremental Aggregation` |
| Sorter | `FIELDATTRIBUTE` `Sort Key` / `Sort Direction` per port; `TABLEATTRIBUTE` `Distinct` |
| Rank | `TABLEATTRIBUTE` `Top/Bottom`, `Number of Ranks`; the rank port has `FIELDATTRIBUTE` marking it |
| Sequence | `TABLEATTRIBUTE` `Start Value`, `Increment By`, `End Value`, `Cycle`, `Reset`, `Number of Cached Values` |
| Update Strategy | `TABLEATTRIBUTE` `Update Strategy Expression` |
| Normalizer | `TRANSFORMFIELD` `OCCURS`, plus generated `GCID_`/`GK_` ports |
| Stored Procedure | `TABLEATTRIBUTE` `Stored Procedure Name`, `Call Text`, `Stored Procedure Type` |
| SQL / Java | `TABLEATTRIBUTE` holding the query or the Java snippet — extract it verbatim |

Expressions arrive XML-escaped (`&lt;`, `&gt;`, `&amp;`, `&#10;` for newlines).
Unescape before parsing them, or your function-mapping pass will miss operators.

## 6. Sessions and workflows in the XML

```xml
<SESSION NAME="s_m_CUSTOMER_DIM_LOAD" MAPPINGNAME="m_CUSTOMER_DIM_LOAD">
  <ATTRIBUTE NAME="Treat source rows as" VALUE="Data driven"/>
  <ATTRIBUTE NAME="Enable high precision" VALUE="YES"/>
  <SESSIONEXTENSION SINSTANCENAME="DIM_CUSTOMER" TYPE="WRITER" ...>
    <CONNECTIONREFERENCE CONNECTIONNAME="ORA_TGT" CONNECTIONSUBTYPE="Oracle"/>
    <ATTRIBUTE NAME="Truncate target table option" VALUE="NO"/>
    <ATTRIBUTE NAME="Pre SQL" VALUE="DELETE FROM stg WHERE ..."/>
  </SESSIONEXTENSION>
</SESSION>

<WORKFLOW NAME="wf_DAILY_SALES">
  <TASK NAME="s_m_STG_ORDERS" TYPE="Session" .../>
  <TASK NAME="dec_CHECK_ROWS"  TYPE="Decision" .../>
  <TASKINSTANCE NAME="s_m_STG_ORDERS" TASKNAME="s_m_STG_ORDERS" TASKTYPE="Session"/>
  <WORKFLOWLINK FROMTASK="Start" TOTASK="s_m_STG_ORDERS" CONDITION=""/>
  <WORKFLOWLINK FROMTASK="s_m_STG_ORDERS" TOTASK="s_m_FACT_SALES"
                CONDITION="$s_m_STG_ORDERS.Status = SUCCEEDED"/>
  <SCHEDULER NAME="sc_DAILY" .../>
</WORKFLOW>
```

`<WORKFLOWLINK>` gives you the job DAG; the `CONDITION` attribute is what becomes
`run_if` or a `condition_task` (see orchestration §4). `TASK TYPE` values include
`Session`, `Command`, `Decision`, `Assignment`, `Timer`, `Event-Wait`,
`Event-Raise`, `Email`, `Control`, `Start`, `Worklet`.

Session-level `<ATTRIBUTE>` elements are where the properties that change your
*generated code* live — `Treat source rows as` and `Enable high precision` in
particular. Always extract them; a mapping-only conversion that ignores them can
be wrong in ways no amount of code review will reveal.

## 7. A working extractor

```python
from lxml import etree
from collections import defaultdict

PARSER = etree.XMLParser(resolve_entities=False, no_network=True,
                         load_dtd=False, recover=True)

def extract_mappings(path):
    root = etree.parse(path, PARSER).getroot()
    out = []
    for folder in root.iter("FOLDER"):
        for m in folder.findall("MAPPING"):
            out.append(_mapping_summary(folder.get("NAME"), m))
    return out

def _mapping_summary(folder, m):
    transforms = []
    for t in m.findall("TRANSFORMATION"):
        transforms.append({
            "name":  t.get("NAME"),
            "type":  t.get("TYPE"),
            "props": {a.get("NAME"): a.get("VALUE")
                      for a in t.findall("TABLEATTRIBUTE")},
            "ports": [{
                "name":     f.get("NAME"),
                "porttype": f.get("PORTTYPE"),
                "datatype": f.get("DATATYPE"),
                "precision": f.get("PRECISION"),
                "scale":    f.get("SCALE"),
                "expr":     f.get("EXPRESSION"),
                "default":  f.get("DEFAULTVALUE"),
            } for f in t.findall("TRANSFORMFIELD")],
            "groups": [{"name": g.get("NAME"), "expr": g.get("EXPRESSION")}
                       for g in t.findall("GROUP")],
        })

    instances = [{"name": i.get("NAME"),
                  "type": i.get("TYPE"),                     # SOURCE / TARGET / TRANSFORMATION
                  "transformation": i.get("TRANSFORMATION_NAME"),
                  "transformation_type": i.get("TRANSFORMATION_TYPE")}
                 for i in m.findall("INSTANCE")]

    edges, port_edges = set(), defaultdict(list)
    for c in m.findall("CONNECTOR"):
        edges.add((c.get("FROMINSTANCE"), c.get("TOINSTANCE")))
        port_edges[(c.get("FROMINSTANCE"), c.get("TOINSTANCE"))].append(
            (c.get("FROMFIELD"), c.get("TOFIELD")))

    return {
        "folder": folder,
        "mapping": m.get("NAME"),
        "valid": m.get("ISVALID"),
        "variables": [{"name": v.get("NAME"), "datatype": v.get("DATATYPE"),
                       "aggfunc": v.get("AGGFUNCTION"), "default": v.get("DEFAULTVALUE")}
                      for v in m.findall("MAPPINGVARIABLE")],
        "transformations": transforms,
        "instances": instances,
        "edges": sorted(edges),
        "port_edges": {f"{k[0]}->{k[1]}": v for k, v in port_edges.items()},
    }
```

Use this to produce the inventory table from SKILL.md Step 2 before writing any
PySpark. For a folder-level export, run it across all mappings first and emit a
complexity summary — transformation counts by type per mapping, with the
high-risk types called out. That summary is the migration plan.

## 8. What the XML does not tell you

Be explicit with the user about these gaps rather than filling them with
assumptions:

- **Actual data volumes and skew.** Nothing in the export indicates whether a
  table has a thousand rows or a billion, which drives every broadcast and
  partitioning decision.
- **Whether the mapping is still used.** Repositories are full of mappings that
  have not run in years. Check the workflow schedules and run history before
  converting anything.
- **Parameter file contents.** Referenced by name; the file itself is separate
  and often holds the connection and schema names you need.
- **Shortcut targets.** A `SHORTCUT` element points at a shared folder object
  whose definition is in another export.
- **Stored procedure, Java, and Custom transformation bodies.** Only the name and
  call text are in the XML.
- **Source/target physical DDL beyond declared ports.** Constraints, indexes and
  actual nullability live in the database.
- **Why anything is the way it is.** The XML has no comments worth reading. When
  a transformation looks pointless, ask before deleting it — it is occasionally
  load-bearing.

## 9. IDMC / Cloud Data Integration exports

IDMC (Informatica Intelligent Cloud Services) exports are ZIP archives of JSON
assets, one file per object, obtained from the UI or the REST v3 export API.
The structure has changed across releases, so **inspect before assuming**:

```python
import zipfile, json
with zipfile.ZipFile(path) as z:
    names = z.namelist()
    print(names[:40])
    manifest = next((n for n in names if "manifest" in n.lower()), None)
    sample = json.loads(z.read(names[1]))
    print(json.dumps(sample, indent=2)[:4000])
```

What you are looking for, whatever the exact key names in the version you have:

- The **asset type** of each JSON (mapping / mapping task / taskflow /
  connection / parameter set) — usually in a `type` or `@type` field.
- The **transformation list** inside a mapping asset, with each node's type and
  its properties. IDMC node types largely mirror PowerCenter names, so the
  transformation-patterns reference applies directly.
- The **links** between nodes, which serve the same role as `CONNECTOR`.
- **Expression strings**, which use the same expression language as PowerCenter,
  so expression-function-map applies unchanged. This is the main reason an IDMC
  migration is not a separate skill.
- **Taskflows**, which map to Lakeflow Jobs the way workflows do, with a richer
  set of control constructs (parallel paths, decisions, jump, wait) that convert
  to the same `depends_on` / `run_if` / `condition_task` vocabulary.

Differences from PowerCenter worth checking for:

- **Advanced (Spark-based) mappings** in CDI-Elastic already ran on Spark. Their
  semantics may already differ from classic PowerCenter behaviour — verify null
  handling against the Advanced mapping's documented behaviour rather than
  assuming the PowerCenter rules in the function map.
- **Hierarchical/structured data** transformations (JSON, XML, hierarchy
  parser/builder) have no PowerCenter analogue; convert them with Spark's native
  nested-type functions (`from_json`, struct/array operations).
- **Cloud connectors** (Salesforce, NetSuite, Workday, SAP) map to Lakeflow
  Connect ingestion, Partner Connect, or a third-party connector — this is an
  ingestion-architecture decision, not a transformation conversion, and belongs
  in the plan as a separate work item.
- **Mapping in Advanced Mode / SQL ELT** pushes processing to the warehouse.
  Those mappings are often the easiest to convert, since the pushdown SQL is
  visible and close to what Databricks SQL will run.

If the user only has access to the IDMC UI and not an export, the mapping's
"View Details" / documentation export gives the node list and expressions, which
is enough to work from. Ask for it rather than working from screenshots.
