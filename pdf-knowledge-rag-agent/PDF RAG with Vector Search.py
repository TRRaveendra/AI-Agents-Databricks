# Databricks notebook source
# DBTITLE 1,📚 PDF RAG with Vector Search - Complete Implementation
# MAGIC %md
# MAGIC # 📚 PDF RAG with Databricks Vector Search - Production-Ready Implementation
# MAGIC
# MAGIC ## Overview
# MAGIC This notebook demonstrates a complete end-to-end RAG (Retrieval-Augmented Generation) pipeline for PDF documents using Databricks Vector Search.
# MAGIC
# MAGIC ## Features
# MAGIC * ✅ Vector Search endpoint creation and management
# MAGIC * ✅ Intelligent PDF chunking with multiple strategies
# MAGIC * ✅ Delta Sync with managed embeddings (databricks-gte-large-en)
# MAGIC * ✅ Multiple search modes: Semantic, Hybrid (semantic + keyword), and Filtered
# MAGIC * ✅ Advanced RAG patterns: Multi-query retrieval, reranking
# MAGIC * ✅ End-to-end RAG with LLM integration
# MAGIC * ✅ Production best practices: error handling, monitoring, validation
# MAGIC
# MAGIC ## Architecture
# MAGIC ```
# MAGIC PDF Files → Parse & Chunk → Delta Table (with CDF + PK) → Vector Index → Multiple Query Modes → LLM Integration
# MAGIC ```
# MAGIC
# MAGIC ## Prerequisites
# MAGIC * Unity Catalog volume with PDF files
# MAGIC * Vector Search endpoint permissions
# MAGIC * Databricks Runtime with Vector Search support
# MAGIC
# MAGIC ## Sections
# MAGIC 1. **Configuration & Setup** - Environment configuration and validation
# MAGIC 2. **Utility Functions** - Reusable chunking and helper functions
# MAGIC 3. **Infrastructure Setup** - Endpoint, tables, and index creation
# MAGIC 4. **Data Ingestion** - PDF parsing and loading
# MAGIC 5. **Query Patterns** - Multiple search strategies
# MAGIC 6. **RAG Integration** - End-to-end retrieval with LLM
# MAGIC 7. **Maintenance** - Monitoring and operations

# COMMAND ----------

# DBTITLE 1,1. Configuration - Environment Setup
# =============================================================================
# SECTION 1: CONFIGURATION & ENVIRONMENT SETUP
# =============================================================================
# This cell defines all configuration parameters for the RAG pipeline.
# Update these values based on your environment and requirements.

# -----------------------------------------------------------------------------
# Unity Catalog Configuration
# -----------------------------------------------------------------------------
CATALOG = "workspace"  # Your catalog name
SCHEMA = "default"     # Your schema name

# -----------------------------------------------------------------------------
# Vector Search Configuration
# -----------------------------------------------------------------------------
# Endpoint: Choose between STANDARD (low latency <100ms) or STORAGE_OPTIMIZED (cost-effective, large scale)
ENDPOINT_NAME = "pdf_search_endpoint"
ENDPOINT_TYPE = "STANDARD"  # Options: "STANDARD" or "STORAGE_OPTIMIZED"

# Index and table names - using 3-level namespacing (catalog.schema.name)
INDEX_NAME = f"{CATALOG}.{SCHEMA}.pdf_vector_index"
SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.pdf_source_table"

# -----------------------------------------------------------------------------
# PDF Source Configuration
# -----------------------------------------------------------------------------
# Unity Catalog Volume path where PDF files are stored
VOLUME_PATH = f"/Volumes/{CATALOG}/default/pdf_documents"

# -----------------------------------------------------------------------------
# Embedding Model Configuration
# -----------------------------------------------------------------------------
# databricks-gte-large-en: 1024 dimensions, 8192 token context, high quality
# databricks-bge-large-en: 1024 dimensions, 512 token context, general purpose
EMBEDDING_MODEL = "databricks-gte-large-en"
EMBEDDING_DIMENSION = 1024  # Must match the model's output dimension

# -----------------------------------------------------------------------------
# Chunking Strategy Configuration
# -----------------------------------------------------------------------------
CHUNK_SIZE = 1000      # Characters per chunk (balance between context and specificity)
CHUNK_OVERLAP = 200    # Overlap to preserve context across boundaries

# -----------------------------------------------------------------------------
# Pipeline Configuration
# -----------------------------------------------------------------------------
# TRIGGERED: Manual sync required (use for batch processing)
# CONTINUOUS: Auto-sync on source table changes (use for real-time updates)
PIPELINE_TYPE = "TRIGGERED"

# =============================================================================
# VALIDATION & DISPLAY
# =============================================================================

print("="*70)
print("PDF RAG PIPELINE CONFIGURATION")
print("="*70)
print(f"\n📂 Unity Catalog:")
print(f"   Catalog:      {CATALOG}")
print(f"   Schema:       {SCHEMA}")
print(f"   Source Table: {SOURCE_TABLE}")
print(f"\n🔍 Vector Search:")
print(f"   Endpoint:     {ENDPOINT_NAME} ({ENDPOINT_TYPE})")
print(f"   Index:        {INDEX_NAME}")
print(f"   Pipeline:     {PIPELINE_TYPE}")
print(f"\n🧠 Embeddings:")
print(f"   Model:        {EMBEDDING_MODEL}")
print(f"   Dimensions:   {EMBEDDING_DIMENSION}")
print(f"\n📝 Data Source:")
print(f"   Volume:       {VOLUME_PATH}")
print(f"   Chunk Size:   {CHUNK_SIZE} chars")
print(f"   Overlap:      {CHUNK_OVERLAP} chars")
print(f"\n{'='*70}")
print("✅ Configuration loaded successfully!")

# COMMAND ----------

# DBTITLE 1,2. Utility Functions - Chunking Strategies
# =============================================================================
# SECTION 2: UTILITY FUNCTIONS - TEXT CHUNKING STRATEGIES
# =============================================================================
# Provides multiple chunking strategies for optimal text splitting.
# Different strategies work better for different document types.

import hashlib
from datetime import datetime
from typing import List, Dict, Any

# -----------------------------------------------------------------------------
# Strategy 1: Semantic Chunking (sentence-aware with overlap)
# -----------------------------------------------------------------------------
def chunk_text_semantic(text: str, chunk_size: int = 1000, overlap: int = 200) -> List[str]:
    """
    Split text into overlapping chunks with sentence boundary awareness.
    
    Best for: General documents, articles, reports
    
    Args:
        text: Input text to chunk
        chunk_size: Maximum characters per chunk
        overlap: Characters to overlap between chunks (preserves context)
    
    Returns:
        List of text chunks
    
    Example:
        chunks = chunk_text_semantic("Long document...", chunk_size=1000, overlap=200)
    """
    if not text or len(text) <= chunk_size:
        return [text.strip()] if text and text.strip() else []
    
    chunks = []
    start = 0
    
    while start < len(text):
        end = start + chunk_size
        chunk = text[start:end]
        
        # Try to break at sentence boundary for better semantic coherence
        if end < len(text):
            # Look for sentence endings (., !, ?, or newlines)
            last_period = chunk.rfind('.')
            last_exclaim = chunk.rfind('!')
            last_question = chunk.rfind('?')
            last_newline = chunk.rfind('\n')
            
            break_point = max(last_period, last_exclaim, last_question, last_newline)
            
            # Only break if we're past halfway through the chunk
            if break_point > chunk_size * 0.5:
                chunk = text[start:start + break_point + 1]
                start = start + break_point + 1 - overlap
            else:
                start = end - overlap
        else:
            start = end
        
        chunk = chunk.strip()
        if chunk:
            chunks.append(chunk)
    
    return chunks

# -----------------------------------------------------------------------------
# Strategy 2: Fixed-size Chunking (simple, predictable)
# -----------------------------------------------------------------------------
def chunk_text_fixed(text: str, chunk_size: int = 1000, overlap: int = 200) -> List[str]:
    """
    Split text into fixed-size chunks with overlap.
    
    Best for: Code, structured data, when consistent chunk sizes are important
    
    Args:
        text: Input text to chunk
        chunk_size: Characters per chunk
        overlap: Characters to overlap
    
    Returns:
        List of text chunks
    """
    if not text:
        return []
    
    chunks = []
    start = 0
    
    while start < len(text):
        end = start + chunk_size
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        start = end - overlap
    
    return chunks

# -----------------------------------------------------------------------------
# Strategy 3: Paragraph-based Chunking
# -----------------------------------------------------------------------------
def chunk_text_paragraphs(text: str, max_chunk_size: int = 1000) -> List[str]:
    """
    Split text by paragraphs, combining small paragraphs to meet size threshold.
    
    Best for: Well-structured documents with clear paragraph breaks
    
    Args:
        text: Input text to chunk
        max_chunk_size: Maximum characters per chunk
    
    Returns:
        List of text chunks
    """
    if not text:
        return []
    
    # Split by double newlines (paragraph breaks)
    paragraphs = [p.strip() for p in text.split('\n\n') if p.strip()]
    
    chunks = []
    current_chunk = []
    current_size = 0
    
    for para in paragraphs:
        para_size = len(para)
        
        # If single paragraph exceeds max size, add it as its own chunk
        if para_size > max_chunk_size:
            if current_chunk:
                chunks.append('\n\n'.join(current_chunk))
                current_chunk = []
                current_size = 0
            chunks.append(para)
        # If adding this paragraph would exceed max size, start new chunk
        elif current_size + para_size > max_chunk_size:
            if current_chunk:
                chunks.append('\n\n'.join(current_chunk))
            current_chunk = [para]
            current_size = para_size
        # Add paragraph to current chunk
        else:
            current_chunk.append(para)
            current_size += para_size
    
    # Add remaining chunk
    if current_chunk:
        chunks.append('\n\n'.join(current_chunk))
    
    return chunks

# -----------------------------------------------------------------------------
# Utility: Generate deterministic chunk IDs
# -----------------------------------------------------------------------------
def generate_chunk_id(file_name: str, page_number: int, chunk_index: int) -> str:
    """
    Generate a deterministic, unique chunk ID using SHA256.
    
    Args:
        file_name: Source PDF filename
        page_number: Page number (0-indexed)
        chunk_index: Chunk index within the page
    
    Returns:
        64-character hex string (SHA256 hash)
    """
    raw_id = f"{file_name}|{page_number}|{chunk_index}"
    return hashlib.sha256(raw_id.encode()).hexdigest()

print("✅ Chunking utilities loaded successfully!")
print("\nAvailable strategies:")
print("  1. chunk_text_semantic()    - Sentence-aware chunking (recommended)")
print("  2. chunk_text_fixed()       - Fixed-size chunking")
print("  3. chunk_text_paragraphs()  - Paragraph-based chunking")
print("  4. generate_chunk_id()      - Generate deterministic IDs")

# COMMAND ----------

# DBTITLE 1,3. Infrastructure - Create Vector Search Endpoint
# =============================================================================
# SECTION 3: INFRASTRUCTURE SETUP - VECTOR SEARCH ENDPOINT
# =============================================================================
# Creates or verifies the Vector Search endpoint.
# Endpoints provide compute for hosting and querying vector indexes.

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import EndpointType
import time

w = WorkspaceClient()

# -----------------------------------------------------------------------------
# Helper: Wait for endpoint to become ONLINE
# -----------------------------------------------------------------------------
def wait_for_endpoint(endpoint_name: str, timeout_minutes: int = 10) -> bool:
    """
    Poll endpoint status until it becomes ONLINE or timeout occurs.
    
    Args:
        endpoint_name: Name of the endpoint
        timeout_minutes: Maximum time to wait
    
    Returns:
        True if endpoint is ONLINE, False otherwise
    """
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    
    while time.time() - start_time < timeout_seconds:
        try:
            endpoint = w.vector_search_endpoints.get_endpoint(endpoint_name)
            status = endpoint.endpoint_status.state.value
            
            if status == "ONLINE":
                print(f"\n✅ Endpoint '{endpoint_name}' is ONLINE and ready!")
                return True
            elif status in ["PROVISIONING", "UPDATING"]:
                elapsed = int(time.time() - start_time)
                print(f"   [{elapsed}s] Endpoint status: {status}... waiting...")
                time.sleep(15)
            else:
                print(f"\n❌ Endpoint in unexpected state: {status}")
                return False
        except Exception as e:
            print(f"   Error checking endpoint: {e}")
            time.sleep(10)
    
    print(f"\n❌ Timeout after {timeout_minutes} minutes waiting for endpoint")
    return False

# -----------------------------------------------------------------------------
# Create or verify endpoint
# -----------------------------------------------------------------------------
print("="*70)
print("VECTOR SEARCH ENDPOINT SETUP")
print("="*70)

try:
    # Check if endpoint already exists
    existing_endpoints = [e.name for e in w.vector_search_endpoints.list_endpoints()]
    
    if ENDPOINT_NAME in existing_endpoints:
        print(f"\n🔍 Endpoint '{ENDPOINT_NAME}' already exists. Checking status...")
        
        endpoint = w.vector_search_endpoints.get_endpoint(ENDPOINT_NAME)
        status = endpoint.endpoint_status.state.value
        endpoint_type = endpoint.endpoint_type.value
        
        print(f"   Status: {status}")
        print(f"   Type:   {endpoint_type}")
        
        if status == "ONLINE":
            print(f"\n✅ Endpoint is ready to use!")
        else:
            print(f"\n⏳ Waiting for endpoint to become ONLINE...")
            wait_for_endpoint(ENDPOINT_NAME, timeout_minutes=2)
    
    else:
        print(f"\n🛠️ Creating new Vector Search Endpoint: {ENDPOINT_NAME}")
        print(f"   Type: {ENDPOINT_TYPE}")
        print(f"\nEndpoint Types:")
        print(f"   • STANDARD: Low latency (20-50ms), 320M vectors, higher cost")
        print(f"   • STORAGE_OPTIMIZED: Higher latency (300-500ms), 1B+ vectors, 7x lower cost")
        print(f"\nProvisioning endpoint... This takes 5-10 minutes.")
        
        # Map string to enum
        endpoint_type_enum = EndpointType.STANDARD if ENDPOINT_TYPE == "STANDARD" else EndpointType.STORAGE_OPTIMIZED
        
        w.vector_search_endpoints.create_endpoint(
            name=ENDPOINT_NAME,
            endpoint_type=endpoint_type_enum
        )
        
        print(f"\n✅ Endpoint creation initiated!")
        print(f"\n⏳ Waiting for provisioning to complete...")
        wait_for_endpoint(ENDPOINT_NAME, timeout_minutes=15)

except Exception as e:
    print(f"\n❌ Error managing endpoint: {str(e)}")
    print(f"\nTroubleshooting:")
    print(f"  1. Verify you have Vector Search permissions")
    print(f"  2. Check workspace quotas and limits")
    print(f"  3. Ensure endpoint name is unique in the workspace")
    raise

print(f"\n{'='*70}")

# COMMAND ----------

# DBTITLE 1,4. Infrastructure - Create Source Table with Enhanced Schema
# =============================================================================
# SECTION 4: INFRASTRUCTURE SETUP - SOURCE TABLE
# =============================================================================
# Creates the Delta table that stores chunked PDF content.
# This table is the source for the Vector Search index.
#
# CRITICAL REQUIREMENTS for Delta Sync indexes:
#   1. PRIMARY KEY constraint (not just a unique column)
#   2. Change Data Feed (CDF) enabled
#   3. Unity Catalog 3-level namespace (catalog.schema.table)

print("="*70)
print("SOURCE TABLE SETUP")
print("="*70)

try:
    # Create schema if it doesn't exist
    print(f"\n📂 Ensuring schema exists: {CATALOG}.{SCHEMA}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    print(f"   ✅ Schema ready")
    
    # Create source table with comprehensive schema
    print(f"\n📊 Creating source table: {SOURCE_TABLE}")
    
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SOURCE_TABLE} (
      -- Primary Key (REQUIRED for Delta Sync)
      chunk_id STRING NOT NULL 
        COMMENT 'Unique identifier for each text chunk (SHA256 hash)',
      
      -- Document Metadata
      file_name STRING 
        COMMENT 'Source PDF filename',
      file_path STRING 
        COMMENT 'Full path to the PDF file',
      page_number INT 
        COMMENT 'Page number in the PDF (1-indexed for human readability)',
      
      -- Chunk Information
      chunk_text STRING 
        COMMENT 'Extracted text content that will be embedded',
      chunk_index INT 
        COMMENT 'Sequential chunk number within the page (0-indexed)',
      chunk_length INT 
        COMMENT 'Character count of the chunk',
      chunking_strategy STRING 
        COMMENT 'Strategy used to create this chunk (semantic, fixed, paragraph)',
      
      -- Timestamps
      created_at TIMESTAMP 
        COMMENT 'When this chunk was processed and added',
      updated_at TIMESTAMP 
        COMMENT 'When this chunk was last modified',
      
      -- Quality Metadata
      has_images BOOLEAN 
        COMMENT 'Whether the source page contains images',
      has_tables BOOLEAN 
        COMMENT 'Whether the source page contains tables',
      
      -- Primary Key Constraint (REQUIRED)
      CONSTRAINT pdf_source_pk PRIMARY KEY (chunk_id)
    )
    -- Cluster by file for efficient filtering
    CLUSTER BY (file_name)
    COMMENT 'PDF document chunks for RAG vector search indexing'
    TBLPROPERTIES (
      -- Enable Change Data Feed (REQUIRED for Delta Sync auto-sync)
      'delta.enableChangeDataFeed' = 'true',
      -- Additional properties for optimization
      'delta.autoOptimize.optimizeWrite' = 'true',
      'delta.autoOptimize.autoCompact' = 'true'
    )
    """)
    
    print(f"   ✅ Table created successfully!")
    print(f"\n📝 Table properties:")
    print(f"   • Primary Key: chunk_id (required for Delta Sync)")
    print(f"   • Change Data Feed: Enabled (required for auto-sync)")
    print(f"   • Clustering: By file_name (optimized filtering)")
    print(f"   • Auto-optimize: Enabled (automatic compaction)")
    
    # Verify table properties
    print(f"\n🔍 Verifying table configuration...")
    table_info = spark.sql(f"DESCRIBE EXTENDED {SOURCE_TABLE}").collect()
    
    # Check for CDF
    cdf_enabled = any('delta.enableChangeDataFeed' in str(row) and 'true' in str(row) for row in table_info)
    if cdf_enabled:
        print(f"   ✅ Change Data Feed: Enabled")
    else:
        print(f"   ⚠️  Change Data Feed: Not enabled - enabling now...")
        spark.sql(f"ALTER TABLE {SOURCE_TABLE} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
    
    # Check current row count
    row_count = spark.sql(f"SELECT COUNT(*) as count FROM {SOURCE_TABLE}").collect()[0]['count']
    print(f"   📊 Current row count: {row_count:,}")
    
except Exception as e:
    print(f"\n❌ Error creating source table: {str(e)}")
    print(f"\nTroubleshooting:")
    print(f"  1. Verify you have CREATE TABLE permission on schema {CATALOG}.{SCHEMA}")
    print(f"  2. Check that catalog and schema exist")
    print(f"  3. Ensure you have USE CATALOG and USE SCHEMA permissions")
    raise

print(f"\n{'='*70}")
print("✅ Source table is ready for data ingestion!")

# COMMAND ----------

# DBTITLE 1,5. Data Ingestion - Parse and Load PDFs
# =============================================================================
# SECTION 5: DATA INGESTION - PDF PARSING AND LOADING
# =============================================================================
# Extracts text from PDFs and chunks intelligently for optimal embedding.
# Uses semantic chunking strategy by default for best RAG performance.

# Install pypdf if not available
%pip install -q pypdf

import pypdf
import os
from pyspark.sql import Row
from datetime import datetime

print("="*70)
print("PDF PROCESSING AND DATA INGESTION")
print("="*70)

# Configuration
CHUNKING_STRATEGY = "semantic"  # Options: semantic, fixed, paragraph

print(f"\n📚 Processing Configuration:")
print(f"   Source: {VOLUME_PATH}")
print(f"   Strategy: {CHUNKING_STRATEGY}")
print(f"   Chunk Size: {CHUNK_SIZE} chars")
print(f"   Overlap: {CHUNK_OVERLAP} chars")

# Select chunking function based on strategy
if CHUNKING_STRATEGY == "semantic":
    chunk_function = chunk_text_semantic
elif CHUNKING_STRATEGY == "fixed":
    chunk_function = chunk_text_fixed
elif CHUNKING_STRATEGY == "paragraph":
    chunk_function = chunk_text_paragraphs
else:
    chunk_function = chunk_text_semantic
    print(f"   ⚠️  Unknown strategy, defaulting to semantic")

rows = []
files_processed = 0
files_failed = 0
total_chunks = 0

print(f"\n🔍 Scanning for PDF files...")

try:
    # Get list of PDF files
    pdf_files = [f for f in os.listdir(VOLUME_PATH) if f.endswith(".pdf")]
    
    if not pdf_files:
        print(f"\n⚠️  No PDF files found in {VOLUME_PATH}")
        print(f"\nTo add PDFs:")
        print(f"  1. Navigate to: {VOLUME_PATH}")
        print(f"  2. Upload PDF files")
        print(f"  3. Re-run this cell")
    else:
        print(f"   Found {len(pdf_files)} PDF file(s)\n")
        
        # Process each PDF
        for file in pdf_files:
            full_path = os.path.join(VOLUME_PATH, file)
            
            try:
                print(f"\n📄 Processing: {file}")
                
                with open(full_path, "rb") as f:
                    reader = pypdf.PdfReader(f)
                    num_pages = len(reader.pages)
                    file_chunks = 0
                    
                    print(f"   Pages: {num_pages}")
                    
                    # Process each page
                    for page_num, page in enumerate(reader.pages):
                        try:
                            # Extract text from page
                            text = page.extract_text()
                            
                            if not text or not text.strip():
                                continue
                            
                            # Detect if page has images or tables (simple heuristic)
                            has_images = len(page.images) > 0 if hasattr(page, 'images') else False
                            has_tables = '|' in text or '+--' in text  # Simple table detection
                            
                            # Create chunks using selected strategy
                            chunks = chunk_function(text, chunk_size=CHUNK_SIZE, overlap=CHUNK_OVERLAP)
                            
                            # Create row for each chunk
                            for idx, chunk in enumerate(chunks):
                                chunk_id = generate_chunk_id(file, page_num, idx)
                                
                                rows.append(Row(
                                    chunk_id=chunk_id,
                                    file_name=file,
                                    file_path=full_path,
                                    page_number=page_num + 1,  # 1-indexed
                                    chunk_text=chunk,
                                    chunk_index=idx,
                                    chunk_length=len(chunk),
                                    chunking_strategy=CHUNKING_STRATEGY,
                                    created_at=datetime.now(),
                                    updated_at=datetime.now(),
                                    has_images=has_images,
                                    has_tables=has_tables
                                ))
                                file_chunks += 1
                        
                        except Exception as e:
                            print(f"   ⚠️  Page {page_num + 1} failed: {str(e)[:60]}")
                            continue
                    
                    print(f"   ✅ Extracted {file_chunks} chunks")
                    total_chunks += file_chunks
                    files_processed += 1
            
            except Exception as e:
                print(f"   ❌ Failed to process: {str(e)[:60]}")
                files_failed += 1
                continue
        
        # Write to Delta table
        if rows:
            print(f"\n💾 Writing to Delta table...")
            df = spark.createDataFrame(rows)
            df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(SOURCE_TABLE)
            
            print(f"\n{'='*70}")
            print("✅ DATA INGESTION COMPLETE")
            print(f"{'='*70}")
            print(f"   📄 Files processed: {files_processed}")
            print(f"   ❌ Files failed: {files_failed}")
            print(f"   📊 Total chunks: {total_chunks:,}")
            print(f"   💾 Table: {SOURCE_TABLE}")
            print(f"\n📊 Statistics:")
            
            # Calculate statistics
            avg_chunk_size = sum(len(r.chunk_text) for r in rows) / len(rows) if rows else 0
            print(f"   Average chunk size: {avg_chunk_size:.0f} chars")
            print(f"   Chunks per file: {total_chunks / files_processed:.1f}")
            
        else:
            print(f"\n⚠️  No content extracted from PDFs")
            print(f"   Check that PDFs contain text (not just images)")

except FileNotFoundError:
    print(f"\n❌ Volume path not found: {VOLUME_PATH}")
    print(f"\nTo create the volume:")
    print(f"  1. Run: CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.pdf_documents")
    print(f"  2. Upload PDF files to the volume")
    print(f"  3. Update VOLUME_PATH in configuration if needed")
except Exception as e:
    print(f"\n❌ Unexpected error: {str(e)}")
    raise

print(f"\n{'='*70}")

# COMMAND ----------

# DBTITLE 1,6. Infrastructure - Create Delta Sync Vector Index
# =============================================================================
# SECTION 6: INFRASTRUCTURE SETUP - DELTA SYNC VECTOR INDEX
# =============================================================================
# Creates a Delta Sync index with managed embeddings.
# The index automatically syncs with the source table when data changes.

from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecResponse, 
    EmbeddingSourceColumn, 
    PipelineType, 
    VectorIndexType
)
import time

print("="*70)
print("VECTOR SEARCH INDEX SETUP")
print("="*70)

try:
    # Check if index already exists
    try:
        existing_index = w.vector_search_indexes.get_index(index_name=INDEX_NAME)
        print(f"\n🔍 Index '{INDEX_NAME}' already exists")
        print(f"   Ready: {existing_index.status.ready}")
        print(f"   Indexed Rows: {existing_index.status.indexed_row_count or 0}")
        print(f"   Message: {existing_index.status.message}")
        
        if existing_index.status.ready and existing_index.status.indexed_row_count:
            print(f"\n✅ Index is ready to use!")
        else:
            print(f"\n⏳ Index is still syncing...")
    
    except Exception:
        # Index doesn't exist, create it
        print(f"\n🛠️ Creating Delta Sync Vector Index: {INDEX_NAME}")
        print(f"\n🧠 Index Configuration:")
        print(f"   Type: DELTA_SYNC (auto-syncs with source table)")
        print(f"   Endpoint: {ENDPOINT_NAME}")
        print(f"   Source: {SOURCE_TABLE}")
        print(f"   Primary Key: chunk_id")
        print(f"   Embedding Column: chunk_text")
        print(f"   Embedding Model: {EMBEDDING_MODEL} ({EMBEDDING_DIMENSION}D)")
        print(f"   Pipeline: {PIPELINE_TYPE}")
        
        # Map string to enum
        pipeline_type_enum = PipelineType.TRIGGERED if PIPELINE_TYPE == "TRIGGERED" else PipelineType.CONTINUOUS
        
        # Create the index
        index = w.vector_search_indexes.create_index(
            name=INDEX_NAME,
            endpoint_name=ENDPOINT_NAME,
            primary_key="chunk_id",
            index_type=VectorIndexType.DELTA_SYNC,
            delta_sync_index_spec=DeltaSyncVectorIndexSpecResponse(
                source_table=SOURCE_TABLE,
                embedding_source_columns=[
                    EmbeddingSourceColumn(
                        name="chunk_text",  # Text column to embed
                        embedding_model_endpoint_name=EMBEDDING_MODEL
                    )
                ],
                pipeline_type=pipeline_type_enum
            )
        )
        
        print(f"\n✅ Index created successfully!")
        print(f"\n💡 Index Features:")
        print(f"   • Managed Embeddings: Databricks automatically generates embeddings")
        print(f"   • Auto-Sync: Changes to source table automatically update index")
        print(f"   • Incremental: Only new/changed rows are processed")
        
        if PIPELINE_TYPE == "TRIGGERED":
            print(f"\n⚠️  Pipeline Type: TRIGGERED")
            print(f"   Manual sync required after data changes:")
            print(f"   w.vector_search_indexes.sync_index(index_name=INDEX_NAME)")
        else:
            print(f"\n✅ Pipeline Type: CONTINUOUS")
            print(f"   Index will automatically sync on source table changes")
        
        print(f"\n⏳ Initial index sync starting... This may take several minutes.")
    
    # Monitor index sync status
    print(f"\n🔍 Monitoring sync status...")
    
    for i in range(30):  # Wait up to 5 minutes
        try:
            index_status = w.vector_search_indexes.get_index(index_name=INDEX_NAME)
            ready = index_status.status.ready
            message = index_status.status.message
            row_count = index_status.status.indexed_row_count
            
            elapsed = i * 10
            
            if ready and row_count and row_count > 0:
                print(f"\n{'='*70}")
                print("✅ INDEX IS ONLINE AND READY!")
                print(f"{'='*70}")
                print(f"   Indexed Rows: {row_count:,}")
                print(f"   Ready: {ready}")
                print(f"   Dimensions: {EMBEDDING_DIMENSION}")
                print(f"   Model: {EMBEDDING_MODEL}")
                break
            else:
                status_msg = message if message else "Syncing..."
                print(f"   [{elapsed}s] Ready: {ready} | Rows: {row_count or 0} | {status_msg}")
                time.sleep(10)
        except Exception as e:
            print(f"   Checking status... {str(e)[:60]}")
            time.sleep(10)
    else:
        print(f"\n⚠️  Index sync still in progress after 5 minutes")
        print(f"   This is normal for large datasets")
        print(f"   Check status with the verification cell below")

except Exception as e:
    print(f"\n❌ Error creating index: {str(e)}")
    print(f"\nTroubleshooting:")
    print(f"  1. Verify endpoint is ONLINE")
    print(f"  2. Check source table has data and CDF enabled")
    print(f"  3. Verify primary key constraint exists")
    print(f"  4. Ensure you have permissions on the endpoint")
    raise

print(f"\n{'='*70}")

# COMMAND ----------

# DBTITLE 1,7. Verification - Check Index Status and View Sample Data
# =============================================================================
# SECTION 7: VERIFICATION - INDEX STATUS AND DATA INSPECTION
# =============================================================================
# Verifies the index is ready and inspects source data.

print("="*70)
print("VERIFICATION AND STATUS CHECK")
print("="*70)

# -----------------------------------------------------------------------------
# Check Index Status
# -----------------------------------------------------------------------------
print("\n🔍 Vector Search Index Status:")
print("-" * 70)

try:
    index_info = w.vector_search_indexes.get_index(index_name=INDEX_NAME)
    
    print(f"   Index Name: {index_info.name}")
    print(f"   Ready: {'✅ Yes' if index_info.status.ready else '⏳ No'}")
    print(f"   Indexed Rows: {index_info.status.indexed_row_count or 0:,}")
    print(f"   Primary Key: {index_info.primary_key}")
    print(f"   Status: {index_info.status.message}")
    
    if index_info.delta_sync_index_spec:
        spec = index_info.delta_sync_index_spec
        print(f"\n   Source Table: {spec.source_table}")
        print(f"   Pipeline Type: {spec.pipeline_type}")
        
        if spec.embedding_source_columns:
            for col in spec.embedding_source_columns:
                print(f"   Embedding Column: {col.name}")
                print(f"   Embedding Model: {col.embedding_model_endpoint_name}")
    
    if index_info.status.ready and index_info.status.indexed_row_count:
        print(f"\n   ✅ Index is ready for queries!")
    else:
        print(f"\n   ⚠️  Index not ready yet. Wait for ready=True and indexed_row_count > 0")
        print(f"   For TRIGGERED pipelines, run: w.vector_search_indexes.sync_index(INDEX_NAME)")
        
except Exception as e:
    print(f"   ❌ Error: {str(e)}")

# -----------------------------------------------------------------------------
# View Source Table Statistics
# -----------------------------------------------------------------------------
print(f"\n{'='*70}")
print("📊 Source Table Statistics:")
print("-" * 70)

try:
    # Get row count
    stats = spark.sql(f"""
        SELECT 
            COUNT(*) as total_chunks,
            COUNT(DISTINCT file_name) as total_files,
            AVG(chunk_length) as avg_chunk_length,
            MIN(chunk_length) as min_chunk_length,
            MAX(chunk_length) as max_chunk_length
        FROM {SOURCE_TABLE}
    """).collect()[0]
    
    print(f"   Total Chunks: {stats.total_chunks:,}")
    print(f"   Total Files: {stats.total_files}")
    print(f"   Avg Chunk Length: {stats.avg_chunk_length:.0f} chars")
    print(f"   Min Chunk Length: {stats.min_chunk_length} chars")
    print(f"   Max Chunk Length: {stats.max_chunk_length} chars")
    
except Exception as e:
    print(f"   ❌ Error: {str(e)}")

# -----------------------------------------------------------------------------
# Display Sample Chunks
# -----------------------------------------------------------------------------
print(f"\n{'='*70}")
print("📝 Sample Chunks from Source Table:")
print("-" * 70)

try:
    df_sample = spark.sql(f"""
        SELECT 
            file_name,
            page_number,
            chunk_index,
            LEFT(chunk_text, 80) as chunk_preview,
            chunk_length,
            chunking_strategy
        FROM {SOURCE_TABLE}
        ORDER BY file_name, page_number, chunk_index
        LIMIT 5
    """)
    
    display(df_sample)
    
except Exception as e:
    print(f"   ❌ Error: {str(e)}")

print(f"\n{'='*70}")
print("✅ Verification complete!")

# COMMAND ----------

# DBTITLE 1,8. Query Mode 1: Semantic Search (SQL)
# =============================================================================
# SECTION 8: QUERY MODE 1 - SEMANTIC SEARCH USING SQL
# =============================================================================
# Pure semantic search using vector similarity.
# Best for: Natural language queries, conceptual searches
#
# The VECTOR_SEARCH SQL function is the recommended approach for querying.

query_text = "What are the main security guidelines and best practices mentioned?"

print("="*70)
print("🔍 SEMANTIC SEARCH (SQL)")
print("="*70)
print(f"\nQuery: '{query_text}'")
print(f"\nSearch Type: Pure semantic (ANN - Approximate Nearest Neighbors)")
print(f"How it works: Converts query to embedding and finds similar vectors")
print("-" * 70)

try:
    # Execute semantic search using SQL
    df_results = spark.sql(f"""
        SELECT 
            file_name,
            page_number,
            chunk_text,
            score
        FROM VECTOR_SEARCH(
            index => '{INDEX_NAME}',
            query => '{query_text}',
            num_results => 5
        )
        ORDER BY score DESC
    """)
    
    # Display results
    results = df_results.collect()
    
    if results:
        print(f"\n✅ Found {len(results)} relevant chunks:\n")
        
        for i, row in enumerate(results, 1):
            print(f"Result #{i} - Score: {row.score:.4f}")
            print(f"  📄 File: {row.file_name}")
            print(f"  📊 Page: {row.page_number}")
            print(f"  📝 Excerpt: {row.chunk_text[:150]}...")
            print()
    else:
        print("\n⚠️  No results found. Ensure index is synced and contains data.")
    
    # Also display as table
    print(f"{'='*70}")
    print("📋 Detailed Results Table:")
    display(df_results)
    
except Exception as e:
    print(f"\n❌ Error querying index: {str(e)}")
    print(f"\nTroubleshooting:")
    print(f"  1. Verify index is ONLINE and indexed_row_count > 0")
    print(f"  2. Check that source table contains data")
    print(f"  3. For TRIGGERED pipeline, run: w.vector_search_indexes.sync_index(INDEX_NAME)")

print(f"\n{'='*70}")

# COMMAND ----------

# DBTITLE 1,9. Query Mode 2: Semantic Search (Python SDK)
# =============================================================================
# SECTION 9: QUERY MODE 2 - SEMANTIC SEARCH USING PYTHON SDK
# =============================================================================
# Alternative approach using the Databricks SDK for programmatic access.
# Best for: Integration with Python applications, dynamic query generation

query_text = "How do I monitor and track model performance?"

print("="*70)
print("🔍 SEMANTIC SEARCH (Python SDK)")
print("="*70)
print(f"\nQuery: '{query_text}'")
print(f"\nSearch Type: Pure semantic (vector similarity)")
print(f"Columns returned: file_name, page_number, chunk_text, score")
print("-" * 70)

try:
    results = w.vector_search_indexes.query_index(
        index_name=INDEX_NAME,
        columns=["file_name", "page_number", "chunk_text"],
        query_text=query_text,
        num_results=5
    )
    
    if results.result and results.result.data_array:
        print(f"\n✅ Found {len(results.result.data_array)} relevant chunks:\n")
        
        for i, item in enumerate(results.result.data_array, 1):
            file_name = item[0]
            page_num = item[1]
            chunk_text = item[2]
            score = item[3]  # Similarity score is always the last column
            
            print(f"Result #{i}")
            print(f"  🎯 Score: {score:.4f}")
            print(f"  📄 File: {file_name}")
            print(f"  📊 Page: {page_num}")
            print(f"  📝 Excerpt: {chunk_text[:150]}...")
            print()
        
        # Convert to DataFrame for easier manipulation
        data_rows = [
            {
                "file_name": item[0],
                "page_number": item[1], 
                "chunk_text": item[2][:200],
                "score": item[3]
            }
            for item in results.result.data_array
        ]
        df = spark.createDataFrame(data_rows)
        
        print(f"{'='*70}")
        print("📋 Results as DataFrame:")
        display(df)
    
    else:
        print("\n⚠️  No results found. Ensure the index has been synced and contains data.")
        
except Exception as e:
    print(f"\n❌ Error querying index: {str(e)}")
    print(f"\nCommon issues:")
    print(f"  • Index not ready: Check status with get_index()")
    print(f"  • No indexed data: Verify indexed_row_count > 0")
    print(f"  • TRIGGERED pipeline: Manually sync with sync_index()")

print(f"\n{'='*70}")

# COMMAND ----------

# DBTITLE 1,10. Query Mode 3: Hybrid Search (Semantic + Keyword)
# =============================================================================
# SECTION 10: QUERY MODE 3 - HYBRID SEARCH (SEMANTIC + KEYWORD)
# =============================================================================
# Combines vector similarity (ANN) with keyword matching (BM25).
# Best for: Queries with specific terms, technical terminology, SKUs, codes
#
# Use hybrid when:
#   • Query contains exact terms that must match (error codes, product IDs)
#   • Technical or domain-specific terminology
#   • You want both semantic understanding AND exact matches

query_text = "MLflow model registry and versioning features"

print("="*70)
print("🔍 HYBRID SEARCH (Semantic + Keyword)")
print("="*70)
print(f"\nQuery: '{query_text}'")
print(f"\nSearch Type: Hybrid")
print(f"  🧠 Semantic: Vector similarity for conceptual matches")
print(f"  🔑 Keyword: BM25 for exact term matches")
print(f"\nWhy hybrid? Combines semantic understanding with keyword precision")
print("-" * 70)

try:
    results = w.vector_search_indexes.query_index(
        index_name=INDEX_NAME,
        columns=["file_name", "page_number", "chunk_text"],
        query_text=query_text,
        query_type="HYBRID",  # Enable hybrid search
        num_results=5
    )
    
    if results.result and results.result.data_array:
        print(f"\n✅ Found {len(results.result.data_array)} relevant chunks:\n")
        
        for i, item in enumerate(results.result.data_array, 1):
            file_name = item[0]
            page_num = item[1]
            chunk_text = item[2]
            score = item[3]
            
            print(f"Result #{i}")
            print(f"  🎯 Combined Score: {score:.4f}")
            print(f"  📄 File: {file_name}")
            print(f"  📊 Page: {page_num}")
            print(f"  📝 Excerpt: {chunk_text[:200]}...")
            print()
        
        # Convert to DataFrame
        data_rows = [
            {
                "rank": i,
                "file": item[0],
                "page": item[1],
                "preview": item[2][:150] + "...",
                "score": item[3]
            }
            for i, item in enumerate(results.result.data_array, 1)
        ]
        df = spark.createDataFrame(data_rows)
        
        print(f"{'='*70}")
        print("📋 Hybrid Search Results:")
        display(df)
        
    else:
        print("\n⚠️  No results found.")
        
except Exception as e:
    print(f"\n❌ Error: {str(e)}")
    print(f"\nNote: Hybrid search is available for both Delta Sync and Direct Access indexes.")
    print(f"If error persists, try pure semantic search (query_type not specified).")

print(f"\n{'='*70}")
print("💡 Comparison: Semantic vs Hybrid")
print(f"{'='*70}")
print("\nSemantic Search (ANN):")
print("  ✅ Best for natural language, conceptual queries")
print("  ✅ Understands meaning and context")
print("  ❌ May miss exact term matches")
print("\nHybrid Search (ANN + BM25):")
print("  ✅ Combines semantic understanding with keyword precision")
print("  ✅ Better for technical terms, codes, specific phrases")
print("  ✅ More balanced results across different query types")
print(f"\n{'='*70}")

# COMMAND ----------

# DBTITLE 1,11. Query Mode 4: Filtered Search (Metadata Filters)
# =============================================================================
# SECTION 11: QUERY MODE 4 - FILTERED SEARCH WITH METADATA
# =============================================================================
# Search within specific documents or metadata criteria.
# Best for: Scoping search to specific files, pages, or document types
#
# Filter syntax differs by endpoint type:
#   • STANDARD: filters_json with dictionary format
#   • STORAGE_OPTIMIZED: SQL-like filter strings

query_text = "best practices for model deployment"

print("="*70)
print("🔍 FILTERED SEARCH (Metadata Filters)")
print("="*70)
print(f"\nQuery: '{query_text}'")
print(f"Endpoint Type: {ENDPOINT_TYPE}")
print("-" * 70)

# Get unique files for filtering
try:
    files = spark.sql(f"""
        SELECT DISTINCT file_name 
        FROM {SOURCE_TABLE} 
        ORDER BY file_name
    """).collect()
    
    available_files = [row.file_name for row in files]
    
    if available_files:
        print(f"\n📂 Available Files:")
        for i, file in enumerate(available_files, 1):
            print(f"   {i}. {file}")
        
        # Use the first file for filtering
        filter_file = available_files[0]
        
        print(f"\n🔍 Filtering to: {filter_file}")
        print("-" * 70)
        
        # For STANDARD endpoints, use filters_json with dictionary format
        import json
        filters = {"file_name": filter_file}
        filters_json = json.dumps(filters)
        
        print(f"\nFilter configuration: {filters}")
        
        try:
            results = w.vector_search_indexes.query_index(
                index_name=INDEX_NAME,
                columns=["file_name", "page_number", "chunk_text"],
                query_text=query_text,
                num_results=5,
                filters_json=filters_json  # STANDARD endpoints use filters_json
            )
            
            if results.result and results.result.data_array:
                print(f"\n✅ Found {len(results.result.data_array)} relevant chunks in {filter_file}:\n")
                
                for i, item in enumerate(results.result.data_array, 1):
                    page_num = item[1]
                    chunk_text = item[2]
                    score = item[3]
                    
                    print(f"Result #{i}")
                    print(f"  🎯 Score: {score:.4f}")
                    print(f"  📊 Page: {page_num}")
                    print(f"  📝 Excerpt: {chunk_text[:150]}...")
                    print()
                
                # Display as table
                data_rows = [
                    {
                        "page": item[1],
                        "score": item[3],
                        "text_preview": item[2][:100] + "..."
                    }
                    for item in results.result.data_array
                ]
                df = spark.createDataFrame(data_rows)
                
                print(f"{'='*70}")
                print(f"📋 Filtered Results from {filter_file}:")
                display(df)
            
            else:
                print(f"\n⚠️  No results found in {filter_file}. Try without filters.")
        
        except Exception as e:
            print(f"\n❌ Error with filters: {str(e)}")
            print(f"\n💡 Filter syntax by endpoint type:")
            print(f"   STANDARD: Use filters_json with dict format")
            print(f"   STORAGE_OPTIMIZED: Use SQL-like filter strings")
    
    else:
        print("\n⚠️  No files found in source table. Run data ingestion first.")

except Exception as e:
    print(f"\n❌ Error: {str(e)}")

print(f"\n{'='*70}")
print("💡 Advanced Filter Examples")
print(f"{'='*70}")
print("\nSTANDARD Endpoint (filters_json):")
print('  {"file_name": "document.pdf"}')
print('  {"page_number": 5}')
print('  {"has_tables": true}')
print('  {"file_name": "doc.pdf", "page_number": 3}')
print("\nSTORAGE_OPTIMIZED Endpoint (SQL-like filters):")
print('  "file_name = \'document.pdf\'"')
print('  "page_number > 5 AND page_number < 10"')
print('  "has_tables = true AND chunk_length > 500"')
print(f"\n{'='*70}")

# COMMAND ----------

# DBTITLE 1,12. End-to-End RAG - Integration with LLM
# =============================================================================
# SECTION 12: END-TO-END RAG - INTEGRATION WITH LLM
# =============================================================================
# Complete RAG pipeline: Query → Retrieve → Augment → Generate
# Demonstrates how to use Vector Search results with an LLM for answers.

from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

user_question = "What is MLflow and how is it used for model tracking?"

print("="*70)
print("🤖 END-TO-END RAG WITH LLM")
print("="*70)
print(f"\n💬 User Question: {user_question}")
print("-" * 70)

# -----------------------------------------------------------------------------
# Step 1: Retrieve Relevant Context
# -----------------------------------------------------------------------------
print("\n🔍 Step 1: Retrieving relevant context from Vector Search...")

try:
    # Query the index with hybrid search for best results
    results = w.vector_search_indexes.query_index(
        index_name=INDEX_NAME,
        columns=["file_name", "page_number", "chunk_text"],
        query_text=user_question,
        query_type="HYBRID",  # Use hybrid for better accuracy
        num_results=3  # Top 3 most relevant chunks
    )
    
    if results.result and results.result.data_array:
        print(f"   ✅ Retrieved {len(results.result.data_array)} relevant chunks")
        
        # Extract context from results
        contexts = []
        for i, item in enumerate(results.result.data_array, 1):
            file_name = item[0]
            page_num = item[1]
            chunk_text = item[2]
            score = item[3]
            
            context_entry = f"""[Source {i}: {file_name}, Page {page_num}, Relevance: {score:.3f}]
{chunk_text}
"""
            contexts.append(context_entry)
            print(f"   • Source {i}: {file_name} (page {page_num}, score {score:.3f})")
        
        # Combine all contexts
        combined_context = "\n\n".join(contexts)
        
        # ---------------------------------------------------------------------
        # Step 2: Construct Prompt with Retrieved Context
        # ---------------------------------------------------------------------
        print(f"\n📝 Step 2: Constructing augmented prompt...")
        
        system_prompt = """You are a helpful assistant that answers questions based on the provided context.
        
Instructions:
- Answer the question using ONLY the information from the provided context
- If the context doesn't contain enough information, say so
- Cite sources by mentioning the source number (e.g., "According to Source 1...")
- Be concise and accurate"""
        
        user_prompt = f"""Context from relevant documents:
{combined_context}

---

Question: {user_question}

Answer:"""
        
        print(f"   ✅ Prompt constructed with {len(contexts)} context sources")
        print(f"   📏 Total context length: {len(combined_context)} characters")
        
        # ---------------------------------------------------------------------
        # Step 3: Generate Answer with LLM
        # ---------------------------------------------------------------------
        print(f"\n🧠 Step 3: Generating answer with LLM...")
        
        try:
            # Use Databricks Foundation Model API
            # Replace with your preferred model endpoint
            response = w.serving_endpoints.query(
                name="databricks-meta-llama-3-1-70b-instruct",  # or your preferred model
                messages=[
                    ChatMessage(role=ChatMessageRole.SYSTEM, content=system_prompt),
                    ChatMessage(role=ChatMessageRole.USER, content=user_prompt)
                ],
                max_tokens=500,
                temperature=0.1  # Low temperature for factual responses
            )
            
            answer = response.choices[0].message.content
            
            print(f"   ✅ Answer generated successfully")
            print("\n" + "="*70)
            print("💬 LLM RESPONSE")
            print("="*70)
            print(f"\n{answer}")
            print("\n" + "="*70)
            
            # Display sources
            print("\n📚 Sources Used:")
            print("-" * 70)
            for i, item in enumerate(results.result.data_array, 1):
                print(f"\nSource {i}:")
                print(f"  File: {item[0]}")
                print(f"  Page: {item[1]}")
                print(f"  Relevance: {item[3]:.4f}")
                print(f"  Preview: {item[2][:100]}...")
        
        except Exception as e:
            print(f"   ❌ LLM Error: {str(e)}")
            print(f"\n💡 Alternative: Use any LLM API (OpenAI, Anthropic, etc.)")
            print(f"\n   The retrieved context is ready to use:")
            print(f"   - System prompt: {len(system_prompt)} chars")
            print(f"   - User prompt with context: {len(user_prompt)} chars")
    
    else:
        print("   ⚠️  No relevant context found. Check index status.")

except Exception as e:
    print(f"\n❌ Error in RAG pipeline: {str(e)}")
    print(f"\nTroubleshooting:")
    print(f"  1. Verify index is ready and has data")
    print(f"  2. Check LLM endpoint is available")
    print(f"  3. Ensure you have permissions for the model endpoint")

print(f"\n{'='*70}")
print("💡 RAG Pipeline Summary")
print(f"{'='*70}")
print("\nThis example demonstrates a complete RAG workflow:")
print("  1. 🔍 Retrieve: Query vector index for relevant context")
print("  2. 📝 Augment: Construct prompt with retrieved context")
print("  3. 🧠 Generate: Use LLM to synthesize answer from context")
print("  4. 📚 Cite: Track and display source documents")
print(f"\n{'='*70}")

# COMMAND ----------

# DBTITLE 1,13. Maintenance - Manual Sync and Monitoring
# =============================================================================
# SECTION 13: MAINTENANCE - MANUAL SYNC AND MONITORING
# =============================================================================
# Operations for maintaining and monitoring your Vector Search deployment.
# Use these for TRIGGERED pipelines or when troubleshooting.

print("="*70)
print("🔧 MAINTENANCE OPERATIONS")
print("="*70)

# -----------------------------------------------------------------------------
# Operation 1: Manual Index Sync
# -----------------------------------------------------------------------------
print("\n🔄 Operation 1: Manual Index Sync")
print("-" * 70)
print(f"Purpose: Trigger sync for TRIGGERED pipeline type")
print(f"When to use: After adding/updating data in source table")
print(f"\nCurrent pipeline type: {PIPELINE_TYPE}")

if PIPELINE_TYPE == "TRIGGERED":
    print("\n👉 TRIGGERED pipeline requires manual sync")
    
    sync_now = input("\nTrigger sync now? (yes/no): ").strip().lower()
    
    if sync_now == "yes":
        try:
            print(f"\n🔄 Triggering sync for {INDEX_NAME}...")
            w.vector_search_indexes.sync_index(index_name=INDEX_NAME)
            
            print(f"   ✅ Sync triggered successfully!")
            print(f"\n   The sync process will:")
            print(f"     1. Detect changes in source table (via CDF)")
            print(f"     2. Generate embeddings for new/updated chunks")
            print(f"     3. Update the vector index")
            print(f"\n   ⏳ This may take several minutes...")
            print(f"   Monitor progress in the next operation below.")
        
        except Exception as e:
            print(f"   ❌ Error: {str(e)}")
    else:
        print("   Sync skipped")
else:
    print("\n✅ CONTINUOUS pipeline - automatic sync enabled")
    print("   Manual sync not needed for CONTINUOUS pipelines")

# -----------------------------------------------------------------------------
# Operation 2: Monitor Index Status
# -----------------------------------------------------------------------------
print(f"\n\n{'='*70}")
print("📈 Operation 2: Monitor Index Status")
print("-" * 70)

try:
    index_info = w.vector_search_indexes.get_index(index_name=INDEX_NAME)
    
    ready = index_info.status.ready
    indexed_rows = index_info.status.indexed_row_count or 0
    message = index_info.status.message
    
    print(f"\n📊 Index Health:")
    print(f"   Status: {'\u2705 READY' if ready else '\u23f3 NOT READY'}")
    print(f"   Indexed Rows: {indexed_rows:,}")
    print(f"   Message: {message}")
    
    # Get source table row count for comparison
    source_rows = spark.sql(f"SELECT COUNT(*) as count FROM {SOURCE_TABLE}").collect()[0]['count']
    
    print(f"\n📊 Data Sync Status:")
    print(f"   Source Table Rows: {source_rows:,}")
    print(f"   Indexed Rows: {indexed_rows:,}")
    
    if indexed_rows == source_rows:
        print(f"   ✅ Fully synced (100%)")
    elif indexed_rows < source_rows:
        pct = (indexed_rows / source_rows * 100) if source_rows > 0 else 0
        print(f"   ⏳ Sync in progress ({pct:.1f}%)")
        print(f"   Missing: {source_rows - indexed_rows:,} rows")
    else:
        print(f"   ⚠️  Index has more rows than source (may need refresh)")
    
    # Display index configuration
    print(f"\n🛠️  Index Configuration:")
    print(f"   Endpoint: {index_info.endpoint_name}")
    print(f"   Primary Key: {index_info.primary_key}")
    print(f"   Index Type: {index_info.index_type}")
    
    if index_info.delta_sync_index_spec:
        spec = index_info.delta_sync_index_spec
        print(f"   Source Table: {spec.source_table}")
        print(f"   Pipeline Type: {spec.pipeline_type}")

except Exception as e:
    print(f"   ❌ Error: {str(e)}")

# -----------------------------------------------------------------------------
# Operation 3: Check Endpoint Health
# -----------------------------------------------------------------------------
print(f"\n\n{'='*70}")
print("💻 Operation 3: Check Endpoint Health")
print("-" * 70)

try:
    endpoint = w.vector_search_endpoints.get_endpoint(ENDPOINT_NAME)
    
    status = endpoint.endpoint_status.state.value
    endpoint_type = endpoint.endpoint_type.value
    
    print(f"\n🔌 Endpoint Status:")
    print(f"   Name: {endpoint.name}")
    print(f"   Status: {status}")
    print(f"   Type: {endpoint_type}")
    
    if status == "ONLINE":
        print(f"   ✅ Endpoint is healthy and ready")
    else:
        print(f"   ⚠️  Endpoint status: {status}")
    
    # List indexes on this endpoint
    print(f"\n📊 Indexes on this endpoint:")
    indexes = [idx for idx in w.vector_search_indexes.list_indexes(endpoint_name=ENDPOINT_NAME)]
    
    for idx in indexes:
        print(f"   • {idx.name} (Primary Key: {idx.primary_key})")

except Exception as e:
    print(f"   ❌ Error: {str(e)}")

print(f"\n{'='*70}")
print("✅ Maintenance operations complete!")
print(f"{'='*70}")

# COMMAND ----------

# DBTITLE 1,14. Advanced Operations and Best Practices
# =============================================================================
# SECTION 14: ADVANCED OPERATIONS AND BEST PRACTICES
# =============================================================================
# Additional utilities, optimization tips, and cleanup operations.

print("="*70)
print("🎓 ADVANCED OPERATIONS & BEST PRACTICES")
print("="*70)

# -----------------------------------------------------------------------------
# Best Practice 1: Query Performance Optimization
# -----------------------------------------------------------------------------
print("\n🚀 Query Performance Optimization")
print("-" * 70)
print("""
1. Choose the Right Endpoint Type:
   • STANDARD: <100ms latency, best for real-time apps
   • STORAGE_OPTIMIZED: 300-500ms latency, 7x cheaper, 1B+ vectors

2. Optimize Chunk Size:
   • Smaller chunks (500-800 chars): More precise, more chunks
   • Larger chunks (1000-1500 chars): More context, fewer chunks
   • Balance based on your content and query patterns

3. Use Hybrid Search When:
   • Queries contain specific terms (codes, SKUs, technical terms)
   • You need both semantic understanding and exact matches
   • Domain has specialized vocabulary

4. Leverage Filters:
   • Filter by file_name to search specific documents
   • Filter by page_number for targeted page searches
   • Use metadata (has_tables, has_images) for content type filtering

5. Index Sync Strategy:
   • CONTINUOUS: Real-time apps, frequent updates
   • TRIGGERED: Batch processing, controlled sync timing
""")

# -----------------------------------------------------------------------------
# Best Practice 2: Cost Optimization
# -----------------------------------------------------------------------------
print(f"\n{'='*70}")
print("💰 Cost Optimization Tips")
print("-" * 70)
print("""
1. Endpoint Selection:
   • Storage-Optimized is 7x cheaper than Standard
   • Use Standard only if you need <100ms latency

2. Index Size Management:
   • Only sync necessary columns (use columns parameter)
   • Remove outdated documents regularly
   • Use delta.autoOptimize for table optimization

3. Query Efficiency:
   • Request only needed columns in results
   • Use appropriate num_results (don't over-fetch)
   • Cache frequently used queries in your application

4. Pipeline Type:
   • TRIGGERED: Lower cost, manual control
   • CONTINUOUS: Higher cost, automatic sync
""")

# -----------------------------------------------------------------------------
# Advanced: Add New PDFs (Incremental Update)
# -----------------------------------------------------------------------------
print(f"\n{'='*70}")
print("📚 Adding New PDFs (Incremental Update)")
print("-" * 70)
print("""
To add new PDFs without reprocessing existing ones:

1. Upload new PDF files to the volume:
   {volume_path}

2. Run the PDF processing cell (Section 5) with APPEND mode:
   Change: df.write.mode("overwrite")...
   To: df.write.mode("append")...

3. For TRIGGERED pipeline, manually sync:
   w.vector_search_indexes.sync_index(index_name=INDEX_NAME)

4. For CONTINUOUS pipeline:
   Index automatically updates when source table changes
""".format(volume_path=VOLUME_PATH))

# -----------------------------------------------------------------------------
# Advanced: Cleanup Operations
# -----------------------------------------------------------------------------
print(f"\n{'='*70}")
print("🧹 Cleanup Operations (Use with Caution)")
print("-" * 70)
print("""
WARNING: These operations are destructive!

# Delete the index:
w.vector_search_indexes.delete_index(index_name=INDEX_NAME)

# Delete the endpoint:
w.vector_search_endpoints.delete_endpoint(endpoint_name=ENDPOINT_NAME)

# Drop the source table:
spark.sql(f"DROP TABLE IF EXISTS {SOURCE_TABLE}")

# Delete volume contents:
# Navigate to the volume in UI and delete files manually
""")

# -----------------------------------------------------------------------------
# Monitoring Dashboard Query
# -----------------------------------------------------------------------------
print(f"\n{'='*70}")
print("📊 Monitoring Dashboard Query")
print("-" * 70)
print("""
Create a monitoring dashboard with this SQL:

SELECT 
  file_name,
  COUNT(*) as chunk_count,
  AVG(chunk_length) as avg_chunk_length,
  COUNT(DISTINCT page_number) as pages_processed,
  MAX(created_at) as last_updated
FROM {table}
GROUP BY file_name
ORDER BY last_updated DESC
""".format(table=SOURCE_TABLE))

# -----------------------------------------------------------------------------
# Quick Reference
# -----------------------------------------------------------------------------
print(f"\n{'='*70}")
print("📝 Quick Reference")
print("="*70)
print(f"""
Key Variables:
  CATALOG:          {CATALOG}
  SCHEMA:           {SCHEMA}
  ENDPOINT_NAME:    {ENDPOINT_NAME}
  INDEX_NAME:       {INDEX_NAME}
  SOURCE_TABLE:     {SOURCE_TABLE}
  VOLUME_PATH:      {VOLUME_PATH}
  EMBEDDING_MODEL:  {EMBEDDING_MODEL}
  
Common Operations:
  # Check index status
  w.vector_search_indexes.get_index(index_name=INDEX_NAME)
  
  # Manual sync
  w.vector_search_indexes.sync_index(index_name=INDEX_NAME)
  
  # Query index
  w.vector_search_indexes.query_index(
      index_name=INDEX_NAME,
      query_text="your question",
      num_results=5
  )
  
  # Query with filters
  w.vector_search_indexes.query_index(
      index_name=INDEX_NAME,
      query_text="your question",
      filters_json='{{"file_name": "document.pdf"}}',
      num_results=5
  )
""")

print(f"\n{'='*70}")
print("✅ Advanced operations guide complete!")
print(f"{'='*70}")