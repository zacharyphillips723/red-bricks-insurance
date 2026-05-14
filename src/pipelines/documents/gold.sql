-- =============================================================================
-- Red Bricks Insurance — Documents Domain: Gold Layer
-- =============================================================================
-- Aggregated document analytics: volume trends, type distributions, author
-- productivity, and member documentation coverage. These metrics support
-- the governance and care management story — showing document completeness,
-- identifying members with sparse records, and tracking documentation velocity.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Gold: Document Volume by Type and Month
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_document_volume
COMMENT 'Monthly document volume by type. Tracks documentation velocity and identifies trends in clinical note production.'
TBLPROPERTIES (
  'quality' = 'gold',
  'domain'  = 'documents'
)
AS
SELECT
  DATE_TRUNC('month', created_date)                    AS doc_month,
  document_type,
  COUNT(*)                                             AS document_count,
  COUNT(DISTINCT member_id)                            AS unique_members,
  ROUND(AVG(text_length), 0)                           AS avg_text_length,
  ROUND(MIN(text_length), 0)                           AS min_text_length,
  ROUND(MAX(text_length), 0)                           AS max_text_length
FROM LIVE.silver_case_notes
GROUP BY
  DATE_TRUNC('month', created_date),
  document_type;

-- ---------------------------------------------------------------------------
-- Gold: Member Documentation Coverage
-- ---------------------------------------------------------------------------
-- Identifies members with sparse or no documentation — a care management
-- risk indicator. Members with fewer than 2 documents may lack adequate
-- clinical records for outreach preparation.
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_member_document_coverage
COMMENT 'Per-member document counts and recency. Surfaces members with sparse records for care management attention.'
TBLPROPERTIES (
  'quality' = 'gold',
  'domain'  = 'documents'
)
AS
SELECT
  member_id,
  COUNT(*)                                             AS total_documents,
  COUNT(CASE WHEN document_type = 'case_note' THEN 1 END)       AS case_note_count,
  COUNT(CASE WHEN document_type = 'call_transcript' THEN 1 END) AS call_transcript_count,
  COUNT(CASE WHEN document_type = 'claims_summary' THEN 1 END)  AS claims_summary_count,
  MAX(created_date)                                    AS most_recent_doc_date,
  MIN(created_date)                                    AS earliest_doc_date,
  DATEDIFF(CURRENT_DATE(), MAX(created_date))          AS days_since_last_doc,
  ROUND(AVG(text_length), 0)                           AS avg_doc_length,
  CASE
    WHEN COUNT(*) = 0 THEN 'No Records'
    WHEN COUNT(*) = 1 THEN 'Sparse'
    WHEN COUNT(*) <= 3 THEN 'Adequate'
    ELSE 'Well-Documented'
  END                                                  AS documentation_tier
FROM LIVE.silver_case_notes
GROUP BY member_id;

-- ---------------------------------------------------------------------------
-- Gold: Author Productivity
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_author_productivity
COMMENT 'Author-level productivity metrics for clinical documentation. Tracks volume, type mix, and average document length per author.'
TBLPROPERTIES (
  'quality' = 'gold',
  'domain'  = 'documents'
)
AS
SELECT
  author,
  COUNT(*)                                             AS total_documents,
  COUNT(DISTINCT member_id)                            AS unique_members,
  COUNT(CASE WHEN document_type = 'case_note' THEN 1 END)       AS case_notes,
  COUNT(CASE WHEN document_type = 'call_transcript' THEN 1 END) AS call_transcripts,
  COUNT(CASE WHEN document_type = 'claims_summary' THEN 1 END)  AS claims_summaries,
  ROUND(AVG(text_length), 0)                           AS avg_doc_length,
  MIN(created_date)                                    AS first_doc_date,
  MAX(created_date)                                    AS last_doc_date
FROM LIVE.silver_case_notes
GROUP BY author;

-- ---------------------------------------------------------------------------
-- Gold: Document Summary Statistics
-- ---------------------------------------------------------------------------
-- Overall corpus statistics for dashboards and Genie space queries.
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_document_summary
COMMENT 'Corpus-level document statistics: total counts, type breakdown, coverage rates, and chunk metrics for vector search.'
TBLPROPERTIES (
  'quality' = 'gold',
  'domain'  = 'documents'
)
AS
SELECT
  COUNT(DISTINCT d.document_id)                        AS total_documents,
  COUNT(DISTINCT d.member_id)                          AS total_members_with_docs,
  COUNT(CASE WHEN d.document_type = 'case_note' THEN 1 END)       AS case_note_count,
  COUNT(CASE WHEN d.document_type = 'call_transcript' THEN 1 END) AS call_transcript_count,
  COUNT(CASE WHEN d.document_type = 'claims_summary' THEN 1 END)  AS claims_summary_count,
  ROUND(AVG(d.text_length), 0)                         AS avg_document_length,
  SUM(d.text_length)                                   AS total_text_bytes,
  (SELECT COUNT(*) FROM LIVE.silver_case_notes_chunks) AS total_chunks,
  ROUND(
    (SELECT AVG(chunk_length) FROM LIVE.silver_case_notes_chunks), 0
  )                                                    AS avg_chunk_length
FROM LIVE.silver_case_notes d;
