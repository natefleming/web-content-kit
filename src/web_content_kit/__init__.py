"""Collect web pages into a Unity Catalog Volume and index them for Vector Search.

Modules:
    settings   validate widget / job parameter values
    logs       readable log output for notebooks
    documents  the Markdown page file format (front matter + body)
    crawler    step 1: crawl websites and save clean Markdown pages
    chunking   step 2: split pages into search-ready chunks
    indexing   step 2: manage the chunking pipeline and the Vector Search index
"""

__version__ = "0.1.0"
