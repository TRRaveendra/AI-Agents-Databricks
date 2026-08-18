
# 🚀 Databricks Multi-Modal RAG System
## Powered by Microsoft Markitdown & Vector Search

---

### 📋 Project Overview

This is a **production-ready, multi-modal Retrieval-Augmented Generation (RAG) system** built on Databricks. It supports multiple document types and provides semantic search capabilities for intelligent question-answering systems.

#### Key Features:

* **Multi-Modal Support**: PDFs, Markdown, and Images
* **Serverless Compatible**: Runs on Databricks Serverless compute
* **Delta Lake Storage**: ACID transactions with Change Data Feed
* **Vector Search**: Managed embeddings (1024-dimensional)
* **RAG Agent**: Pure Python retrieval pipeline
* **Production Ready**: End-to-end document processing

---

### 🎯 What Can It Do?

✅ **Document Ingestion**
- Automatically download documents from URLs
- Store in Unity Catalog Volumes
- Support PDF, Markdown, and Image formats

✅ **Text Extraction**
- Microsoft Markitdown for PDF conversion
- OCR-ready architecture for images
- Preserves document structure

✅ **Intelligent Chunking**
- Configurable chunk size (default: 1000 characters)
- Overlap for context preservation (default: 200 characters)
- Sentence boundary detection

✅ **Vector Search**
- Databricks managed embeddings
- Delta Sync for automatic updates
- Low-latency semantic search

✅ **RAG Pipeline**
- Retrieve relevant context
- Format prompts for LLMs
- Ready for DBRX, OpenAI, or any LLM

---

### 📊 Sample Results

**Documents Processed:** 9 (6 PDFs + 1 Markdown + 3 Images)

**Total Chunks:** 605
- PDFs: 581 chunks (96%)
- Markdown: 21 chunks (3.5%)
- Images: 3 chunks (0.5%)

**Embedding Model:** `databricks-gte-large-en`

**Query Latency:** ~200-500ms

**Retrieval Accuracy:** 0.60+ similarity scores

---

### 🏗️ System Architecture

```
┌─────────────────────────────────────┐
│   Document Sources                  │
│  (PDFs, Images, Markdown)           │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│   Unity Catalog Volume              │
│   /Volumes/main/default/...         │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│   Microsoft Markitdown              │
│   (Text Extraction)                 │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│   Chunking Pipeline                 │
│   (1000 chars, 200 overlap)         │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│   Delta Lake Table                  │
│   (CDF + Primary Key)               │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│   Vector Search Index               │
│   (databricks-gte-large-en)         │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│   RAG Agent                         │
│   (Retrieve + Generate)             │
└─────────────────────────────────────┘
```



## 🚀 Quick Start Guide

### Step 1: Configuration

Set up your Unity Catalog resources:

```python
CATALOG = "main"                    # Your catalog name
SCHEMA = "default"                  # Your schema name
VOLUME_NAME = "rag_documents"       # Volume for documents
TABLE_NAME = "document_chunks"      # Table for chunks
VECTOR_SEARCH_ENDPOINT = "rag_endpoint"  # Endpoint name
```

---

### Step 2: Install Dependencies

All dependencies installed in a **single cell** to minimize Python restarts:

```python
%pip install 'markitdown[all]' \
    databricks-vectorsearch \
    databricks-sdk \
    langchain \
    langchain-community \
    'mlflow[databricks]' \
    requests \
    beautifulsoup4 \
    --quiet

dbutils.library.restartPython()
```

**Packages:**
- `markitdown[all]`: Document conversion (PDF, images, Excel)
- `databricks-vectorsearch`: Vector Search SDK
- `databricks-sdk`: Databricks platform SDK
- `langchain`: RAG framework
- `mlflow[databricks]`: Model tracking and serving

---

### Step 3: Create Unity Catalog Resources

```python
# Create catalog, schema, and volume
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME_NAME}")

# Create directories
dbutils.fs.mkdirs(f"{VOLUME_PATH}/raw")
dbutils.fs.mkdirs(f"{VOLUME_PATH}/markdown")
```

---

### Step 4: Add Documents

Two methods to add documents:

**Method 1: Download from URLs**
```python
import requests

documents = [
    {
        "name": "paper.pdf",
        "url": "https://example.com/paper.pdf",
        "type": "pdf"
    }
]

for doc in documents:
    response = requests.get(doc['url'])
    content_b64 = base64.b64encode(response.content).decode('utf-8')
    dbutils.fs.put(f"{RAW_DOCS_PATH}/{doc['name']}.b64", content_b64)
```

**Method 2: Upload Local Files**
```python
dbutils.fs.cp(
    "file:/Workspace/path/to/document.pdf",
    f"{RAW_DOCS_PATH}/document.pdf.b64"
)
```

---

### Step 5: Process Documents

```python
from markitdown import MarkItDown

md_converter = MarkItDown()

# Convert to markdown
for doc in documents:
    result = md_converter.convert(local_path)
    markdown_content = result.text_content
    
    # Save markdown
    dbutils.fs.put(
        f"{MARKDOWN_PATH}/{doc['name']}.md",
        markdown_content
    )
```

---

### Step 6: Create Vector Search Index

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecResponse,
    EmbeddingSourceColumn
)

w = WorkspaceClient()

# Create endpoint
w.vector_search_endpoints.create_endpoint(
    name=VECTOR_SEARCH_ENDPOINT,
    endpoint_type="STANDARD"
)

# Create index
w.vector_search_indexes.create_index(
    name=INDEX_NAME,
    endpoint_name=VECTOR_SEARCH_ENDPOINT,
    primary_key="chunk_id",
    index_type="DELTA_SYNC",
    delta_sync_index_spec=DeltaSyncVectorIndexSpecResponse(
        source_table=table_full_name,
        embedding_source_columns=[
            EmbeddingSourceColumn(
                name="content",
                embedding_model_endpoint_name="databricks-gte-large-en"
            )
        ]
    )
)
```

---

### Step 7: Query the System

```python
# Initialize RAG agent
rag_agent = RAGAgent()

# Ask questions
result = rag_agent.query(
    "What is the transformer architecture?",
    num_results=5
)

# View results
for doc in result['context_docs']:
    print(f"{doc['doc_id']}: {doc['content'][:200]}...")
```



## 🖼️ Image Support

### Current Implementation: Synthetic Samples

The system includes **3 synthetic test images** with predefined text:

| Image | Content | Size |
|-------|---------|------|
| `databricks_overview.png` | Platform features (Unity Catalog, Delta Lake, MLflow) | 404 chars |
| `ml_workflow.png` | 6-step ML workflow with best practices | 467 chars |
| `data_architecture.png` | Bronze/Silver/Gold lakehouse layers | 364 chars |

#### Why Synthetic?

**Serverless Limitations:**
- ❌ **EasyOCR/PaddleOCR**: Crash Python kernel (deep learning models too large)
- ❌ **Tesseract**: Requires system-level installation (`apt-get` not available)
- ✅ **Solution**: Cloud OCR APIs or Standard/GPU clusters

**What This Proves:**
- RAG pipeline architecture handles multi-modal documents
- Text extraction method is abstracted
- Production deployment needs ~10 lines of code change

---

### Production OCR Options

#### Option 1: Azure Document Intelligence (✅ Recommended)

**Works on Serverless**

```python
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential

client = DocumentAnalysisClient(
    endpoint="https://your-resource.cognitiveservices.azure.com/",
    credential=AzureKeyCredential("your-api-key")
)

with open(image_path, "rb") as f:
    result = client.begin_analyze_document("prebuilt-read", f).result()
    text = " ".join([
        line.content 
        for page in result.pages 
        for line in page.lines
    ])
```

**Features:**
- High accuracy OCR
- Table detection
- Layout analysis
- Multi-language support

---

#### Option 2: Google Cloud Vision API

**Works on Serverless**

```python
from google.cloud import vision

client = vision.ImageAnnotatorClient()

with open(image_path, 'rb') as f:
    image = vision.Image(content=f.read())
    response = client.text_detection(image=image)
    text = response.text_annotations[0].description
```

**Features:**
- Fast processing
- Handwriting detection
- 50+ languages

---

#### Option 3: AWS Textract

**Works on Serverless**

```python
import boto3

textract = boto3.client('textract')

with open(image_path, 'rb') as f:
    response = textract.detect_document_text(
        Document={'Bytes': f.read()}
    )
    text = " ".join([
        block['Text'] 
        for block in response['Blocks'] 
        if block['BlockType'] == 'LINE'
    ])
```

**Features:**
- Form extraction
- Table detection
- Native AWS integration

---

#### Option 4: Tesseract (Standard Cluster)

**Requires Standard/GPU Cluster**

**Cluster Init Script:**
```bash
#!/bin/bash
apt-get update
apt-get install -y tesseract-ocr libtesseract-dev
```

**Python Code:**
```python
import pytesseract
from PIL import Image

text = pytesseract.image_to_string(Image.open(image_path))
```

**Features:**
- Free and open-source
- 100+ languages
- Offline processing

---

### Migration Path: Synthetic → Production

**Current (Synthetic):**
```python
synthetic_image_text = {
    "databricks_overview.png": "DATABRICKS PLATFORM OVERVIEW...",
    "ml_workflow.png": "MACHINE LEARNING WORKFLOW...",
    "data_architecture.png": "LAKEHOUSE DATA ARCHITECTURE..."
}

if doc['name'] in synthetic_image_text:
    text = synthetic_image_text[doc['name']]
```

**Production (Azure DI):**
```python
from azure.ai.formrecognizer import DocumentAnalysisClient

client = DocumentAnalysisClient(endpoint=..., credential=...)

with open(temp_file_path, "rb") as f:
    result = client.begin_analyze_document("prebuilt-read", f).result()
    text = " ".join([line.content for page in result.pages for line in page.lines])
```

**That's it!** Everything else stays the same.

---

### Image Content Examples

**databricks_overview.png:**
```
DATABRICKS PLATFORM OVERVIEW

• Unity Catalog: Unified governance for data and AI
• Delta Lake: ACID transactions and time travel
• MLflow: Complete ML lifecycle management
• Vector Search: Similarity search at scale
• Lakehouse: Combines data warehouse and data lake

Key Features:
- Unified analytics platform
- Built-in security and governance
- Support for SQL, Python, R, Scala
- Auto-scaling compute clusters
```

**ml_workflow.png:**
```
MACHINE LEARNING WORKFLOW

1. Data Ingestion: Collect and store raw data
2. Feature Engineering: Transform and prepare features
3. Model Training: Train models with MLflow tracking
4. Model Evaluation: Validate model performance
5. Model Deployment: Deploy to production endpoints
6. Monitoring: Track model drift and performance

Best Practices:
- Version control your data and models
- Use feature stores for reusability
- Implement A/B testing for model comparison
```

**data_architecture.png:**
```
LAKEHOUSE DATA ARCHITECTURE

Bronze Layer (Raw Data):
- Ingested from multiple sources
- Original format preserved
- Append-only for audit trail

Silver Layer (Cleaned Data):
- Validated and deduplicated
- Standardized schema
- Business rules applied

Gold Layer (Aggregated Data):
- Business-level aggregations
- Ready for analytics and ML
- Optimized for queries
```



## 🤖 LLM Integration

### RAG Agent Architecture

```python
class RAGAgent:
    """Simple RAG agent using Databricks Vector Search"""
    
    def __init__(self, model_endpoint="databricks-dbrx-instruct"):
        self.w = WorkspaceClient()
        self.model_endpoint = model_endpoint
        
    def retrieve(self, query, num_results=5):
        """Retrieve relevant documents from Vector Search"""
        results = self.w.vector_search_indexes.query_index(
            index_name=INDEX_NAME,
            columns=["chunk_id", "doc_id", "content", "description", "doc_type"],
            query_text=query,
            num_results=num_results
        )
        return documents
    
    def generate_answer(self, query, context_docs):
        """Generate answer using retrieved context"""
        context = "\n\n".join([
            f"Document: {doc['doc_id']}\n{doc['content']}"
            for doc in context_docs
        ])
        
        prompt = f"""You are a helpful AI assistant.

Context:
{context}

Question: {query}

Answer: Provide a detailed answer based on the context above."""
        
        return {"query": query, "context_docs": context_docs, "prompt": prompt}
    
    def query(self, question, num_results=5):
        """Complete RAG pipeline: retrieve + generate"""
        docs = self.retrieve(question, num_results)
        result = self.generate_answer(question, docs)
        return result
```

---

### Databricks Foundation Models (DBRX)

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Get RAG results
result = rag_agent.query("What is the transformer architecture?")

# Generate answer with DBRX
response = w.serving_endpoints.query(
    name="databricks-dbrx-instruct",
    inputs={"prompt": result['prompt']}
)

answer = response.predictions[0]
print(answer)
```

**DBRX Features:**
- 132B parameter mixture-of-experts model
- Optimized for instruction following
- Low latency on Databricks
- No API key required

---

### OpenAI Integration

```python
import openai

openai.api_key = dbutils.secrets.get("openai", "api-key")

result = rag_agent.query("Explain Unity Catalog")

response = openai.ChatCompletion.create(
    model="gpt-4",
    messages=[
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": result['prompt']}
    ]
)

answer = response.choices[0].message.content
print(answer)
```

---

### Anthropic Claude Integration

```python
import anthropic

client = anthropic.Anthropic(
    api_key=dbutils.secrets.get("anthropic", "api-key")
)

result = rag_agent.query("What are the key features of Delta Lake?")

message = client.messages.create(
    model="claude-3-opus-20240229",
    max_tokens=1024,
    messages=[
        {"role": "user", "content": result['prompt']}
    ]
)

answer = message.content[0].text
print(answer)
```

---

### Sample Query Flow

**1. User asks a question:**
```python
question = "What is the transformer architecture?"
```

**2. Retrieve relevant context:**
```python
results = w.vector_search_indexes.query_index(
    index_name="main.default.document_index",
    query_text=question,
    num_results=5
)
```

**3. Retrieved documents:**
```
Document 1: attention_paper.pdf (score: 0.6205)
  "The Transformer is the first transduction model relying 
   entirely on self-attention to compute representations..."

Document 2: attention_paper.pdf (score: 0.6007)
  "The Transformer follows this overall architecture using 
   stacked self-attention and point-wise fully connected layers..."

Document 3: attention_paper.pdf (score: 0.5910)
  "We propose the Transformer, a model architecture eschewing 
   recurrence and instead relying entirely on an attention mechanism..."
```

**4. Generate prompt:**
```python
prompt = f"""You are a helpful AI assistant.

Context:
Document: attention_paper.pdf
The Transformer is the first transduction model relying entirely on self-attention...

Document: attention_paper.pdf
The Transformer follows this overall architecture using stacked self-attention...

Question: {question}

Answer: Provide a detailed answer based on the context above.
"""
```

**5. Send to LLM:**
```python
response = w.serving_endpoints.query(
    name="databricks-dbrx-instruct",
    inputs={"prompt": prompt}
)
```

**6. Return answer:**
```
"The Transformer is a novel neural network architecture that relies 
entirely on self-attention mechanisms, eschewing recurrence and 
convolutions. It uses stacked self-attention and point-wise fully 
connected layers to compute representations of input and output 
sequences. This architecture allows for more parallelization and 
can reach state-of-the-art results in translation quality."
```

---

### Prompt Templates

**Basic Q&A:**
```python
prompt = f"""Context:
{context}

Question: {query}

Answer:"""
```

**Step-by-step reasoning:**
```python
prompt = f"""Context:
{context}

Question: {query}

Let's think step by step:
1."""
```

**With citations:**
```python
prompt = f"""Context:
{context}

Question: {query}

Provide an answer with citations to specific documents."""
```

**Multi-turn conversation:**
```python
prompt = f"""Context:
{context}

Conversation history:
{history}

Current question: {query}

Answer:"""
```
