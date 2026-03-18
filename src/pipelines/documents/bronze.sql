-- =============================================================================
-- Red Bricks Insurance — Documents Domain: Bronze Layer
-- =============================================================================
-- Raw ingestion of document metadata (case notes, call transcripts, claims
-- summaries) from JSON files in the source volume. The metadata includes the
-- full extracted text content so downstream processing does not depend on
-- Document AI / PDF parsing.
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE bronze_document_metadata
COMMENT 'Raw document metadata ingested from JSON files. Contains document_id, member_id, document_type, full_text, and provenance fields.'
TBLPROPERTIES (
  'quality' = 'bronze',
  'domain'  = 'documents',
  'pipelines.autoOptimize.zOrderCols' = 'document_id,member_id'
)
AS
SELECT
  document_id,
  member_id,
  document_type,
  title,
  created_date,
  author,
  full_text,
  file_name,
  _metadata.file_path   AS source_file,
  current_timestamp()   AS ingestion_timestamp
FROM STREAM read_files(
  '${source_volume}/documents/metadata/',
  format => 'json'
);
