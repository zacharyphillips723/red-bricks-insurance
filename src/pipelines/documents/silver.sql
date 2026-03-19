-- =============================================================================
-- Red Bricks Insurance — Documents Domain: Silver Layer
-- =============================================================================
-- Cleansed document metadata with validated dates and IDs. Also produces a
-- chunked table for vector search embedding. Chunks are ~500 tokens with
-- 50-token overlap, approximated as character-level splits (1 token ~ 4 chars).
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Silver: Case Notes (cleansed document metadata)
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_case_notes (
  CONSTRAINT valid_document_id
    EXPECT (document_id IS NOT NULL)
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_member_id
    EXPECT (member_id IS NOT NULL AND member_id RLIKE '^MBR[0-9]+$')
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_created_date
    EXPECT (created_date IS NOT NULL AND CAST(created_date AS DATE) IS NOT NULL)
    ON VIOLATION DROP ROW,

  CONSTRAINT has_text_content
    EXPECT (full_text IS NOT NULL AND LENGTH(full_text) > 10)
    ON VIOLATION DROP ROW,

  -- Soft expectations
  CONSTRAINT valid_document_type
    EXPECT (document_type IN ('case_note', 'call_transcript', 'claims_summary'))
)
COMMENT 'Cleansed document metadata with validated member_id, dates, and text content. Covers case notes, call transcripts, and claims summaries.'
TBLPROPERTIES (
  'quality' = 'silver',
  'domain'  = 'documents'
)
AS
SELECT
  document_id,
  member_id,
  document_type,
  title,
  CAST(created_date AS DATE)   AS created_date,
  author,
  full_text,
  file_name,
  LENGTH(full_text)            AS text_length,
  source_file,
  ingestion_timestamp
FROM STREAM bronze_document_metadata;

-- ---------------------------------------------------------------------------
-- Silver: Case Notes Chunks (for vector search)
-- ---------------------------------------------------------------------------
-- Splits each document's full_text into ~500-token chunks with 50-token overlap.
-- Token approximation: 1 token ~ 4 characters → 500 tokens ~ 2000 chars,
-- 50 tokens ~ 200 chars overlap.
-- Uses POSEXPLODE + SEQUENCE to create sliding-window chunks.
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_case_notes_chunks
COMMENT 'Chunked document text for vector search embedding. Each row is a ~500-token chunk with 50-token overlap from the parent document.'
TBLPROPERTIES (
  'quality' = 'silver',
  'domain'  = 'documents',
  'delta.enableChangeDataFeed' = 'true'
)
AS
SELECT
  CONCAT(d.document_id, '_chunk_', LPAD(CAST(t.pos AS STRING), 3, '0')) AS chunk_id,
  d.document_id,
  d.member_id,
  d.document_type,
  d.title,
  d.created_date,
  d.author,
  SUBSTRING(d.full_text, t.pos * 1800 + 1, 2000) AS chunk_text,
  t.pos AS chunk_index,
  t.pos * 1800 AS start_pos,
  LENGTH(SUBSTRING(d.full_text, t.pos * 1800 + 1, 2000)) AS chunk_length
FROM STREAM silver_case_notes d
LATERAL VIEW POSEXPLODE(SEQUENCE(0, CAST(CEIL(LENGTH(d.full_text) / 1800.0) - 1 AS INT))) t AS pos, val
WHERE d.full_text IS NOT NULL
  AND LENGTH(d.full_text) > 0
  AND t.pos * 1800 < LENGTH(d.full_text);
