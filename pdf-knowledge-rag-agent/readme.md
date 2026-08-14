# 📚 PDF RAG with Databricks Vector Search

![PDF RAG with Databricks Vector Search](https://github.com/TRRaveendra/AI-Agents-Databricks/blob/main/images/pdf-rag-agent-architecture.png?raw=true)

A complete **Retrieval-Augmented Generation (RAG)** reference implementation for building a PDF question-answering pipeline using **Databricks Vector Search, Delta Lake, Unity Catalog, managed embeddings, and an LLM**.

The notebook demonstrates the complete flow:

```text
PDF Documents
      │
      ▼
PDF Text Extraction
      │
      ▼
Intelligent Chunking
      │
      ▼
Delta Lake Source Table
      │
      ▼
Databricks Vector Search
      │
      ├── Semantic Search
      ├── Hybrid Search
      └── Metadata Filtering
      │
      ▼
Relevant Context
      │
      ▼
LLM
      │
      ▼
Grounded Answer + Sources
```

---

## 🎯 What This Project Demonstrates

This implementation covers the major building blocks required for a Databricks-based PDF RAG solution:

* 📄 PDF document ingestion
* ✂️ Multiple text chunking strategies
* 🗃️ Delta Lake source table
* 🔑 Primary-key based Vector Search indexing
* 🔄 Change Data Feed
* 🧠 Databricks managed embeddings
* 🔎 Semantic vector search
* 🔀 Hybrid semantic + keyword search
* 🎯 Metadata-filtered retrieval
* 🤖 LLM-based answer generation
* 📚 Source-aware responses
* 🔄 Triggered and continuous index synchronization
* 📊 Index and source-table monitoring
* 💰 Performance and cost optimization guidance
* 🧹 Operational cleanup procedures

The notebook describes the overall architecture as:

```text
PDF Files
   ↓
Parse & Chunk
   ↓
Delta Table
   ↓
Vector Index
   ↓
Query
   ↓
Retrieve Context
   ↓
LLM
   ↓
Grounded Response
```

---

## 🏗️ Architecture

```text
                    ┌─────────────────────┐
                    │     PDF Documents   │
                    │   Unity Catalog     │
                    │       Volume        │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │    PDF Extraction   │
                    │       pypdf         │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │   Text Chunking     │
                    │                     │
                    │ • Semantic          │
                    │ • Fixed-size        │
                    │ • Paragraph         │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │     Delta Table     │
                    │                     │
                    │ chunk_id            │
                    │ file_name           │
                    │ page_number         │
                    │ chunk_text          │
                    │ metadata            │
                    └──────────┬──────────┘
                               │
                               │ Delta Sync
                               ▼
                    ┌─────────────────────┐
                    │ Databricks Vector   │
                    │       Search        │
                    │                     │
                    │ Managed Embeddings  │
                    │ GTE Large EN        │
                    └──────────┬──────────┘
                               │
                 ┌─────────────┼─────────────┐
                 ▼             ▼             ▼
           Semantic        Hybrid        Filtered
            Search         Search         Search
                 │             │             │
                 └─────────────┼─────────────┘
                               ▼
                    ┌─────────────────────┐
                    │ Retrieved Context   │
                    └──────────┬──────────┘
                               ▼
                    ┌─────────────────────┐
                    │         LLM         │
                    │ Grounded Generation │
                    └──────────┬──────────┘
                               ▼
                    ┌─────────────────────┐
                    │ Answer + Sources    │
                    └─────────────────────┘
```

---

## 🛠️ Technology Stack

| Component        | Technology                    |
| ---------------- | ----------------------------- |
| Platform         | Databricks                    |
| Governance       | Unity Catalog                 |
| Storage          | Delta Lake                    |
| Document Storage | Unity Catalog Volume          |
| PDF Parser       | `pypdf`                       |
| Vector Database  | Databricks Vector Search      |
| Embeddings       | `databricks-gte-large-en`     |
| Search           | Semantic / Hybrid / Filtered  |
| SDK              | Databricks SDK                |
| LLM              | Databricks Model Serving      |
| Processing       | PySpark                       |
| RAG Pattern      | Retrieve → Augment → Generate |

---

## ⚙️ Configuration

The main configuration parameters include:

```python
CATALOG = "workspace"
SCHEMA = "default"

ENDPOINT_NAME = "pdf_search_endpoint"
ENDPOINT_TYPE = "STANDARD"

INDEX_NAME = f"{CATALOG}.{SCHEMA}.pdf_vector_index"
SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.pdf_source_table"

VOLUME_PATH = f"/Volumes/{CATALOG}/default/pdf_documents"

EMBEDDING_MODEL = "databricks-gte-large-en"
EMBEDDING_DIMENSION = 1024

CHUNK_SIZE = 1000
CHUNK_OVERLAP = 200

PIPELINE_TYPE = "TRIGGERED"
```

These values are defined centrally in the notebook and are intended to be adapted to the target Databricks environment.

---

## 📄 PDF Ingestion

PDF files are read from a Unity Catalog Volume.

The notebook uses `pypdf` for extraction and processes documents page by page. Each extracted page is converted into one or more chunks before being written to the Delta source table.

Example storage location:

```text
/Volumes/<catalog>/<schema>/pdf_documents/
```

Example:

```text
pdf_documents/
├── document-01.pdf
├── document-02.pdf
├── security-guide.pdf
└── mlops-guide.pdf
```

> **Note:** Text extraction is dependent on the PDF containing extractable text. Scanned/image-only PDFs require an OCR stage that is not implemented in this notebook.

---

## ✂️ Chunking Strategies

Three chunking strategies are provided:

### 1. Semantic Chunking

Sentence-aware chunking with overlap.

Recommended for:

* Articles
* Reports
* Technical documentation
* General business documents

### 2. Fixed-size Chunking

Predictable character-based chunking.

Useful for:

* Code
* Structured text
* Documents where consistent chunk sizes are preferred

### 3. Paragraph Chunking

Paragraph-aware chunking.

Useful for:

* Well-structured documents
* Documents with clear paragraph boundaries

The default configuration uses:

```python
CHUNK_SIZE = 1000
CHUNK_OVERLAP = 200
```

The notebook provides all three implementations.

---

## 🗃️ Delta Source Table

The Delta source table stores:

```text
chunk_id
file_name
file_path
page_number
chunk_text
chunk_index
chunk_length
chunking_strategy
created_at
updated_at
has_images
has_tables
```

The design uses:

* `chunk_id` as the primary key
* Change Data Feed
* File-based clustering
* Delta auto optimization

These properties support the Delta Sync Vector Search architecture.

---

## 🔎 Search Capabilities

### Semantic Search

Semantic search uses vector similarity to retrieve conceptually relevant chunks.

Best suited for:

```text
"What are the security best practices?"
```

The notebook demonstrates semantic search through both SQL and the Databricks Python SDK.

---

### Hybrid Search

Hybrid search combines:

```text
Semantic Similarity
        +
Keyword Matching
        ↓
Better Retrieval
```

It is particularly useful for:

* Technical terminology
* Product IDs
* Error codes
* SKUs
* Exact phrases
* Domain-specific vocabulary

The notebook uses:

```python
query_type="HYBRID"
```

for hybrid retrieval.

---

### Metadata-Filtered Search

Search can be scoped using metadata such as:

```text
file_name
page_number
has_tables
has_images
chunk_length
```

Example:

```json
{
  "file_name": "document.pdf"
}
```

The notebook also demonstrates endpoint-specific filter syntax for Standard and Storage-Optimized configurations.

---

## 🤖 End-to-End RAG

The final RAG workflow follows:

```text
User Question
     │
     ▼
Vector Search
     │
     ▼
Top-K Chunks
     │
     ▼
Context Construction
     │
     ▼
Grounded Prompt
     │
     ▼
LLM
     │
     ▼
Answer
     │
     ▼
Source References
```

The notebook uses a grounding instruction that tells the model to answer using only the retrieved context and indicate when the context is insufficient.

The generated response is accompanied by source metadata including:

* Source file
* Page number
* Relevance score
* Retrieved content preview

---

## 🔄 Vector Index Synchronization

Two synchronization modes are supported:

### TRIGGERED

```text
Source Table
     │
     ▼
Manual Sync
     │
     ▼
Vector Index
```

Useful for:

* Batch ingestion
* Controlled updates
* Lower operational cost

Manual synchronization:

```python
w.vector_search_indexes.sync_index(
    index_name=INDEX_NAME
)
```

### CONTINUOUS

```text
Source Table Changes
        │
        ▼
Automatic Sync
        │
        ▼
Vector Index
```

Useful for frequently changing document collections.

The notebook explicitly documents the operational difference between these modes.

---

## 📊 Monitoring

The implementation provides operational checks for:

* Endpoint status
* Index readiness
* Indexed row count
* Source table row count
* Synchronization percentage
* Index configuration
* Document statistics
* Chunk statistics

Example monitoring metrics:

```text
Total Files
Total Chunks
Average Chunk Length
Minimum Chunk Length
Maximum Chunk Length
Indexed Rows
Source Rows
Last Updated
```

It also provides a sample SQL pattern for building a monitoring dashboard.

---

## ⚡ Performance Optimization

The notebook recommends tuning:

### Endpoint

```text
STANDARD
    ↓
Low latency / real-time workloads

STORAGE_OPTIMIZED
    ↓
Large-scale / cost-sensitive workloads
```

### Chunk Size

```text
500–800 chars
    → More precise retrieval

1000–1500 chars
    → More contextual retrieval
```

### Retrieval

Avoid over-fetching:

```python
num_results=5
```

and request only the columns required by the application.

These optimization recommendations are documented in the notebook's advanced operations section.

---

## 💰 Cost Optimization

Recommended practices include:

* Select the appropriate Vector Search endpoint type.
* Avoid unnecessarily large indexes.
* Request only required result columns.
* Avoid excessive `num_results`.
* Remove obsolete documents.
* Use triggered synchronization when real-time synchronization is unnecessary.
* Optimize the underlying Delta table.

The notebook specifically distinguishes the operational trade-off between triggered and continuous synchronization.

---

## ➕ Incremental PDF Updates

For incremental ingestion:

```text
New PDF
   │
   ▼
Unity Catalog Volume
   │
   ▼
Parse + Chunk
   │
   ▼
APPEND to Delta Table
   │
   ▼
Vector Index Sync
```

For a triggered pipeline:

```python
w.vector_search_indexes.sync_index(
    index_name=INDEX_NAME
)
```

For continuous synchronization, the Vector Search index updates automatically after source-table changes.

---

## 🔐 Security Considerations

Before using this pattern in production:

* Use Unity Catalog permissions.
* Restrict access to source volumes.
* Apply appropriate catalog/schema/table privileges.
* Protect model-serving permissions.
* Avoid hard-coded credentials.
* Parameterize configuration.
* Validate user-provided filters.
* Apply document-level authorization where required.
* Avoid returning unauthorized document content.
* Add audit logging for sensitive workloads.

The notebook already includes permission checks and troubleshooting guidance around schema, catalog, endpoint, and model access.

---

## ⚠️ Verification Notes

This repository should currently be treated as a **production-oriented reference implementation**, not a drop-in production application.

### Issue 1 — Paragraph Chunking

The paragraph chunker accepts:

```python
chunk_text_paragraphs(
    text,
    max_chunk_size=1000
)
```

while the ingestion logic passes both:

```python
chunk_size=CHUNK_SIZE
overlap=CHUNK_OVERLAP
```

to the selected function.

Therefore, selecting:

```python
CHUNKING_STRATEGY = "paragraph"
```

requires a small interface correction.

---

### Issue 2 — Source Table Overwrite

The notebook creates a source table with:

```text
PRIMARY KEY
CHANGE DATA FEED
TABLE PROPERTIES
```

but the ingestion section subsequently uses:

```python
df.write.mode("overwrite").saveAsTable(SOURCE_TABLE)
```

For a production implementation, ingestion should preserve the table contract and use an appropriate `INSERT`, `MERGE`, or controlled `APPEND` strategy rather than recreating/replacing the table definition.

---

### Issue 3 — Multi-Query / Reranking

The introductory description refers to advanced RAG patterns including multi-query retrieval and reranking.

The implementation reviewed here demonstrates:

* Semantic search
* Python SDK search
* Hybrid search
* Metadata filtering
* Context augmentation
* LLM generation

A dedicated **multi-query expansion + reranking implementation is not present** in the current file and should not be advertised as implemented until those components are added.

---

### Issue 4 — OCR

The current ingestion uses:

```python
pypdf.PdfReader(...)
page.extract_text()
```

Therefore, scanned PDFs and image-only documents require an additional OCR/document-intelligence stage.

---

### Issue 5 — LLM Endpoint Configuration

The LLM serving endpoint is currently specified directly in the notebook.

For reusable deployment, move it into configuration:

```python
LLM_ENDPOINT = "<your-model-serving-endpoint>"
```

---

## 📁 Suggested Repository Structure

```text
pdf-rag-vector-search/
│
├── README.md
│
├── notebooks/
│   └── PDF RAG with Vector Search.py
│
├── references/
│   ├── chunking.md
│   ├── vector-search.md
│   └── rag-patterns.md
│
├── config/
│   └── config.example.yaml
│
├── tests/
│   ├── test_chunking.py
│   └── test_retrieval.py
│
└── docs/
    └── architecture.md
```

---

## 🚀 Quick Start

### 1. Create a Unity Catalog Volume

```sql
CREATE VOLUME IF NOT EXISTS
<catalog>.<schema>.pdf_documents;
```

### 2. Upload PDFs

Upload PDF files to:

```text
/Volumes/<catalog>/<schema>/pdf_documents/
```

### 3. Configure the Notebook

Update:

```python
CATALOG
SCHEMA
ENDPOINT_NAME
ENDPOINT_TYPE
VOLUME_PATH
EMBEDDING_MODEL
CHUNK_SIZE
CHUNK_OVERLAP
PIPELINE_TYPE
```

### 4. Run the Notebook

Execute the sections in order:

```text
1. Configuration
2. Chunking Utilities
3. Vector Search Endpoint
4. Source Table
5. PDF Ingestion
6. Vector Index
7. Verification
8. Semantic Search
9. Python SDK Search
10. Hybrid Search
11. Filtered Search
12. End-to-End RAG
13. Maintenance
14. Advanced Operations
```

---

## 📌 Notebook Section Map

| Section | Capability                   |
| ------- | ---------------------------- |
| 1       | Configuration & Environment  |
| 2       | Chunking Utilities           |
| 3       | Vector Search Endpoint       |
| 4       | Delta Source Table           |
| 5       | PDF Ingestion                |
| 6       | Delta Sync Vector Index      |
| 7       | Verification                 |
| 8       | Semantic Search — SQL        |
| 9       | Semantic Search — Python SDK |
| 10      | Hybrid Search                |
| 11      | Metadata Filtering           |
| 12      | End-to-End RAG               |
| 13      | Maintenance & Monitoring     |
| 14      | Advanced Operations          |

---

## 🎓 Learning Outcomes

After working through this project, you should understand how to:

* Build a PDF ingestion pipeline on Databricks.
* Chunk documents for RAG.
* Store document chunks in Delta Lake.
* Create a Databricks Vector Search index.
* Use managed embeddings.
* Perform semantic retrieval.
* Perform hybrid retrieval.
* Apply metadata filters.
* Synchronize Delta tables with Vector Search.
* Build a grounded LLM prompt.
* Generate source-aware RAG responses.
* Monitor index health.
* Optimize retrieval performance and cost.

---

## 🧭 Production Roadmap

Recommended next enhancements:

```text
Current Reference Implementation
            │
            ▼
     ┌─────────────────┐
     │ OCR Support     │
     └────────┬────────┘
              ▼
     ┌─────────────────┐
     │ Incremental     │
     │ Ingestion       │
     └────────┬────────┘
              ▼
     ┌─────────────────┐
     │ Multi-Query     │
     │ Retrieval       │
     └────────┬────────┘
              ▼
     ┌─────────────────┐
     │ Reranking       │
     └────────┬────────┘
              ▼
     ┌─────────────────┐
     │ Evaluation      │
     │ + Ground Truth  │
     └────────┬────────┘
              ▼
     ┌─────────────────┐
     │ Observability   │
     │ + Governance    │
     └────────┬────────┘
              ▼
     ┌─────────────────┐
     │ Production RAG  │
     └─────────────────┘
```

---

## 📄 Source

Implementation reviewed from:

`PDF RAG with Vector Search.py`

The source notebook identifies itself as a complete end-to-end PDF RAG implementation using Databricks Vector Search and documents its core architecture and features.

---

## ⭐ Summary

This project provides a practical foundation for:

```text
PDF
 ↓
Extract
 ↓
Chunk
 ↓
Delta Lake
 ↓
Vector Search
 ↓
Retrieve
 ↓
Augment
 ↓
LLM
 ↓
Grounded Answer
```

It is particularly useful as a **Databricks RAG learning project, reference architecture, and starting point for an enterprise PDF knowledge assistant**.

**Build the retrieval layer correctly first — then add intelligence, evaluation, governance, and agentic capabilities on top.**
