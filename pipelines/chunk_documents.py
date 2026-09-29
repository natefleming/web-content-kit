"""
Spark Declarative Pipeline: turn the Markdown pages in a Unity Catalog Volume into
search-ready chunks for Vector Search.

This file is NOT run directly. ``notebooks/02_index_volume_to_vector_search`` creates a
pipeline that points at this file, installs the ``web_content_kit`` package, and passes
these settings through the pipeline ``configuration``:

    web_content.source_path          /Volumes/<catalog>/<schema>/<volume>
    web_content.chunk_size_tokens    target chunk size (approximate tokens)
    web_content.chunk_overlap_tokens overlap between neighbouring chunks

Tables produced (in the pipeline's catalog and schema):

    documents        one row per Markdown file version picked up from the volume
    document_chunks  latest chunks for every page. Change Data Feed is on so a Vector
                     Search Delta-Sync index can follow it.

The chunking rules live in ``src/web_content_kit/chunking.py``.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from web_content_kit.chunking import Chunk, chunk_markdown
from web_content_kit.documents import parse_front_matter

SOURCE_PATH: str = spark.conf.get("web_content.source_path")  # noqa: F821 - provided by SDP
CHUNK_SIZE_TOKENS: int = int(spark.conf.get("web_content.chunk_size_tokens", "512"))  # noqa: F821
CHUNK_OVERLAP_TOKENS: int = int(spark.conf.get("web_content.chunk_overlap_tokens", "64"))  # noqa: F821

DOCUMENT_SCHEMA = T.StructType(
    [
        T.StructField("title", T.StringType()),
        T.StructField("source_url", T.StringType()),
        T.StructField("crawled_at", T.StringType()),
        T.StructField("markdown", T.StringType()),
    ]
)

CHUNKS_SCHEMA = T.ArrayType(
    T.StructType(
        [
            T.StructField("chunk_index", T.IntegerType()),
            T.StructField("section", T.StringType()),
            T.StructField("chunk_text", T.StringType()),
            T.StructField("token_estimate", T.IntegerType()),
        ]
    )
)


@F.udf(returnType=DOCUMENT_SCHEMA)
def read_document_udf(text: str | None) -> dict[str, str | None]:
    """Split a page file into its front-matter values and Markdown body."""
    metadata, body = parse_front_matter(text or "")
    return {
        "title": metadata.get("title"),
        "source_url": metadata.get("source_url"),
        "crawled_at": metadata.get("crawled_at"),
        "markdown": body,
    }


@F.udf(returnType=CHUNKS_SCHEMA)
def chunk_document_udf(title: str | None, markdown: str | None) -> list[Chunk]:
    """Split one page into search-ready chunks."""
    return chunk_markdown(title, markdown, CHUNK_SIZE_TOKENS, CHUNK_OVERLAP_TOKENS)


@dp.table(
    name="documents",
    comment="One row per Markdown file version found in the web content volume.",
    cluster_by=["source_url"],
)
@dp.expect_or_drop("has_content", "markdown IS NOT NULL AND length(trim(markdown)) > 0")
def documents() -> DataFrame:
    """Incrementally pick up new and changed Markdown files with Auto Loader."""
    raw: DataFrame = (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "text")
        .option("wholetext", "true")  # one row per file, not per line
        .option("pathGlobFilter", "*.md")
        .option("cloudFiles.allowOverwrites", "true")  # re-crawled files are read again
        .load(SOURCE_PATH)
        .select(
            F.col("value").alias("raw_text"),
            F.col("_metadata.file_path").alias("file_path"),
            F.col("_metadata.file_modification_time").alias("modified_at"),
        )
    )
    return raw.withColumn("doc", read_document_udf("raw_text")).select(
        F.sha2("file_path", 256).alias("doc_id"),
        "file_path",
        F.coalesce("doc.source_url", "file_path").alias("source_url"),
        F.coalesce("doc.title", F.regexp_extract("file_path", r"([^/]+)\.md$", 1)).alias("title"),
        F.col("doc.markdown").alias("markdown"),
        F.to_timestamp("doc.crawled_at").alias("crawled_at"),
        "modified_at",
    )


@dp.temporary_view(name="document_chunks_staged")
def document_chunks_staged() -> DataFrame:
    """Explode each new page into chunks (not saved; feeds the table below)."""
    docs = spark.readStream.table("documents")  # noqa: F821
    return docs.withColumn("chunk", F.explode(chunk_document_udf("title", "markdown"))).select(
        F.sha2(F.concat_ws("|", "file_path", F.col("chunk.chunk_index").cast("string")), 256).alias("chunk_id"),
        "doc_id",
        "source_url",
        "title",
        F.col("chunk.section").alias("section"),
        F.col("chunk.chunk_index").alias("chunk_index"),
        F.col("chunk.chunk_text").alias("chunk_text"),
        F.col("chunk.token_estimate").alias("token_estimate"),
        "file_path",
        "crawled_at",
        "modified_at",
    )


dp.create_streaming_table(
    name="document_chunks",
    comment="Latest search-ready chunk for every page section. Source for the Vector Search index.",
    table_properties={"delta.enableChangeDataFeed": "true"},
)

# Keep only the newest version of each chunk: a re-crawled page updates its chunks in
# place instead of adding duplicates.
dp.create_auto_cdc_flow(
    target="document_chunks",
    source="document_chunks_staged",
    keys=["chunk_id"],
    sequence_by="modified_at",
    stored_as_scd_type=1,
)
