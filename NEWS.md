# snstransparencia 0.1.0

Initial CRAN-preparation release.

## Features

- Added `SNSClient`, an R6 client for the Portuguese SNS Transparencia API.
- Added dataset catalogue helpers:
  - `list_datasets()`
  - `list_all_datasets()`
  - `search_datasets()`
- Added local catalogue cache helpers:
  - `cache_catalogue()`
  - `load_catalogue()`
  - `search_cached_catalogue()`
  - `list_cached_catalogues()`
  - `clear_catalogue_cache()`
- Added dataset metadata and discovery helpers:
  - `get_metadata()`
  - `get_dataset_info()`
  - `get_fields()`
  - `list_export_formats()`
  - `get_facets()`
- Added data access helpers:
  - `query_records()`
  - `download_data()`
  - `load_data()`
  - `get_data()`
  - `is_updated()`
  - `list_cached()`
  - `clear_cache()`
- Added support for selected columns, ODSQL filters, facet refinements,
  date bounds, ordering, grouping, and export limits.
- Added loading support for CSV, JSON, JSONL, and Parquet exports. Parquet
  loading requires the optional `arrow` package.
- Added metadata-backed validation for common field inputs in queries,
  downloads, and facet discovery.
- Added package-level convenience functions, including `sns_client()`,
  `sns_list_datasets()`, `sns_query_records()`, and `sns_get_data()`.

## Documentation

- Added a README with copy-paste examples using the `substancias` dataset.
- Added offline/cache workflow documentation.
- Added validation and raw ODSQL guidance.
- Added documentation for the package-level convenience functions.
