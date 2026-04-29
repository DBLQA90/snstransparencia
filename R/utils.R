# Internal helpers shared by SNSClient methods.

`%||%` <- function(x, y) {
  if (is.null(x) || length(x) == 0) y else x
}

compact_list <- function(x) {
  x[!vapply(x, is.null, logical(1))]
}

normalize_query_params <- function(params) {
  params <- compact_list(params)
  params[sort(names(params))]
}

collapse_api_expr <- function(x) {
  if (is.null(x)) return(NULL)
  if (!is.character(x)) {
    rlang::abort("API expressions must be character vectors.",
                 class = "snstransparencia_error")
  }
  if (length(x) == 0) return(NULL)
  paste(x, collapse = ",")
}

quote_odsql_text <- function(x) {
  if (!is.character(x) || length(x) != 1 || is.na(x) || !nzchar(x)) {
    rlang::abort("ODSQL text searches must be non-empty character strings.",
                 class = "snstransparencia_error")
  }
  as.character(jsonlite::toJSON(x, auto_unbox = TRUE))
}

validate_logical_flag <- function(value, arg) {
  if (!is.logical(value) || length(value) != 1 || is.na(value)) {
    rlang::abort(paste0("`", arg, "` must be TRUE or FALSE."),
                 class = "snstransparencia_error")
  }
  invisible(TRUE)
}

is_field_name <- function(x) {
  is.character(x) & grepl("^[A-Za-z_][A-Za-z0-9_]*$", x)
}

format_facet_filters <- function(x, arg = "filters") {
  if (is.null(x)) return(NULL)

  if (is.list(x) && !is.null(names(x))) {
    if (any(names(x) == "")) {
      rlang::abort(paste0("All `", arg, "` entries must be named."),
                   class = "snstransparencia_error")
    }
    values <- unlist(Map(function(name, value) {
      paste0(name, ":", as.character(value))
    }, names(x), x), use.names = FALSE)
    return(values)
  }

  if (!is.character(x)) {
    rlang::abort(paste0("`", arg, "` must be a named list or character vector."),
                 class = "snstransparencia_error")
  }

  x
}

format_facet_specs <- function(facets) {
  if (is.null(facets)) return(NULL)
  if (!is.character(facets)) {
    rlang::abort("`facets` must be a character vector.",
                 class = "snstransparencia_error")
  }
  if (length(facets) == 0) return(NULL)

  vapply(facets, function(x) {
    if (grepl("^facet\\(", x)) x else sprintf('facet(name="%s")', x)
  }, character(1), USE.NAMES = FALSE)
}

build_date_where <- function(date_field = NULL, date_from = NULL, date_to = NULL) {
  if (is.null(date_from) && is.null(date_to)) return(NULL)
  if (is.null(date_field) || !is.character(date_field) || length(date_field) != 1) {
    rlang::abort("`date_field` is required when `date_from` or `date_to` is supplied.",
                 class = "snstransparencia_error")
  }

  clauses <- character()
  if (!is.null(date_from)) {
    clauses <- c(clauses, sprintf("%s >= date'%s'", date_field, as.character(date_from)))
  }
  if (!is.null(date_to)) {
    clauses <- c(clauses, sprintf("%s <= date'%s'", date_field, as.character(date_to)))
  }
  paste(clauses, collapse = " AND ")
}

combine_where <- function(where = NULL, date_where = NULL) {
  if (is.null(where)) return(date_where)
  if (is.null(date_where)) return(where)
  paste0("(", where, ") AND (", date_where, ")")
}

extract_metadata_value <- function(metadata, path) {
  value <- metadata
  for (part in path) {
    if (is.null(value[[part]])) return(NULL)
    value <- value[[part]]
  }
  value
}

metadata_modified <- function(metadata) {
  extract_metadata_value(metadata, c("metas", "default", "modified"))
}

metadata_data_processed <- function(metadata) {
  extract_metadata_value(metadata, c("metas", "default", "data_processed"))
}

metadata_records_count <- function(metadata) {
  extract_metadata_value(metadata, c("metas", "default", "records_count"))
}

metadata_title <- function(metadata) {
  extract_metadata_value(metadata, c("metas", "default", "title"))
}

metadata_publisher <- function(metadata) {
  extract_metadata_value(metadata, c("metas", "default", "publisher"))
}

metadata_description <- function(metadata) {
  extract_metadata_value(metadata, c("metas", "default", "description"))
}

metadata_theme <- function(metadata) {
  extract_metadata_value(metadata, c("metas", "default", "theme"))
}

metadata_keyword <- function(metadata) {
  extract_metadata_value(metadata, c("metas", "default", "keyword"))
}

metadata_dcat_value <- function(metadata, key) {
  extract_metadata_value(metadata, c("metas", "dcat", key))
}

collapse_metadata_value <- function(x) {
  if (is.null(x) || length(x) == 0) return(NA_character_)
  paste(as.character(unlist(x, use.names = FALSE)), collapse = ", ")
}

strip_html_text <- function(x) {
  x <- collapse_metadata_value(x)
  if (is.na(x)) return(x)
  x <- gsub("<[^>]+>", " ", x)
  x <- gsub("&nbsp;|&#160;", " ", x)
  x <- gsub("\\s+", " ", x)
  trimws(x)
}

metadata_info_df <- function(metadata) {
  data.frame(
    dataset_id = metadata$dataset_id %||% NA_character_,
    title = collapse_metadata_value(metadata_title(metadata)),
    description = strip_html_text(metadata_description(metadata)),
    publisher = collapse_metadata_value(metadata_publisher(metadata)),
    records_count = metadata_records_count(metadata) %||% NA_integer_,
    modified = collapse_metadata_value(metadata_modified(metadata)),
    data_processed = collapse_metadata_value(metadata_data_processed(metadata)),
    theme = collapse_metadata_value(metadata_theme(metadata)),
    keyword = collapse_metadata_value(metadata_keyword(metadata)),
    creator = collapse_metadata_value(metadata_dcat_value(metadata, "creator")),
    contributor = collapse_metadata_value(metadata_dcat_value(metadata, "contributor")),
    accrual_periodicity = collapse_metadata_value(metadata_dcat_value(metadata, "accrualperiodicity")),
    temporal = collapse_metadata_value(metadata_dcat_value(metadata, "temporal")),
    spatial = collapse_metadata_value(metadata_dcat_value(metadata, "spatial")),
    stringsAsFactors = FALSE
  )
}

parse_api_time <- function(x) {
  if (is.null(x) || length(x) == 0 || is.na(x)) return(as.POSIXct(NA))
  x <- sub("Z$", "", x)
  x <- sub("([+-][0-9]{2}):?([0-9]{2})$", "", x)
  as.POSIXct(x, format = "%Y-%m-%dT%H:%M:%OS", tz = "UTC")
}

is_later_api_time <- function(live, cached) {
  live_time <- parse_api_time(live)
  cached_time <- parse_api_time(cached)

  if (!is.na(live_time) && !is.na(cached_time)) {
    return(live_time > cached_time)
  }

  !identical(live, cached)
}

is_scalar_atomic <- function(x) {
  (is.atomic(x) || inherits(x, "Date") || inherits(x, "POSIXt")) && length(x) <= 1
}

records_to_df <- function(records) {
  if (is.null(records) || length(records) == 0) {
    return(data.frame(stringsAsFactors = FALSE))
  }

  col_names <- unique(unlist(lapply(records, names), use.names = FALSE))
  cols <- lapply(col_names, function(nm) {
    values <- lapply(records, function(rec) rec[[nm]] %||% NA)
    if (all(vapply(values, is_scalar_atomic, logical(1)))) {
      return(unlist(values, use.names = FALSE))
    }
    I(values)
  })

  out <- as.data.frame(cols, stringsAsFactors = FALSE, optional = TRUE)
  names(out) <- col_names
  out
}

bind_data_frames <- function(rows) {
  rows <- Filter(function(x) is.data.frame(x) && nrow(x) > 0, rows)
  if (length(rows) == 0) return(data.frame(stringsAsFactors = FALSE))

  col_names <- unique(unlist(lapply(rows, names), use.names = FALSE))
  rows <- lapply(rows, function(x) {
    missing <- setdiff(col_names, names(x))
    for (name in missing) x[[name]] <- NA
    x[, col_names, drop = FALSE]
  })

  out <- do.call(rbind, rows)
  row.names(out) <- NULL
  out
}

metadata_fields <- function(metadata) {
  fields <- records_to_df(metadata$fields)
  if (!"name" %in% names(fields)) {
    return(data.frame(name = character(), type = character(),
                      stringsAsFactors = FALSE))
  }
  fields
}

metadata_field_names <- function(metadata) {
  fields <- metadata_fields(metadata)
  as.character(fields$name)
}

metadata_field_type <- function(metadata, field) {
  fields <- metadata_fields(metadata)
  if (!all(c("name", "type") %in% names(fields))) return(NA_character_)
  match <- fields$type[fields$name == field]
  if (length(match) == 0) NA_character_ else as.character(match[[1]])
}

split_api_expressions <- function(x) {
  if (is.null(x) || length(x) == 0) return(character())
  trimws(unlist(strsplit(x, ",", fixed = TRUE), use.names = FALSE))
}

field_names_from_select <- function(x) {
  exprs <- split_api_expressions(x)
  exprs <- exprs[nzchar(exprs)]
  if (length(exprs) == 0) return(character())

  fields <- character()
  for (expr in exprs) {
    if (is_field_name(expr)) {
      fields <- c(fields, expr)
      next
    }

    alias_match <- grepl(
      "^[A-Za-z_][A-Za-z0-9_]*[[:space:]]+[Aa][Ss][[:space:]]+[A-Za-z_][A-Za-z0-9_]*$",
      expr
    )
    if (alias_match) {
      fields <- c(fields, sub("[[:space:]]+[Aa][Ss][[:space:]].*$", "", expr))
    }
  }
  unique(fields)
}

field_names_from_facet_filters <- function(x) {
  if (is.null(x) || length(x) == 0) return(character())

  if (is.list(x) && !is.null(names(x))) {
    names <- names(x)
    return(unique(names[nzchar(names) & is_field_name(names)]))
  }

  if (!is.character(x)) return(character())

  fields <- vapply(x, function(item) {
    if (!grepl(":", item, fixed = TRUE)) return(NA_character_)
    trimws(sub(":.*$", "", item))
  }, character(1), USE.NAMES = FALSE)
  fields <- fields[!is.na(fields) & nzchar(fields) & is_field_name(fields)]
  unique(fields)
}

field_names_from_facet_specs <- function(facets) {
  if (is.null(facets) || length(facets) == 0) return(character())
  if (!is.character(facets)) return(character())

  fields <- vapply(facets, function(item) {
    item <- trimws(item)
    if (is_field_name(item)) return(item)

    match <- regexec('^facet\\(name="([^"]+)"\\)', item)
    pieces <- regmatches(item, match)[[1]]
    if (length(pieces) >= 2 && is_field_name(pieces[[2]])) {
      return(pieces[[2]])
    }
    NA_character_
  }, character(1), USE.NAMES = FALSE)

  unique(fields[!is.na(fields)])
}

format_field_list <- function(fields, max = 10) {
  fields <- fields[!is.na(fields)]
  if (length(fields) == 0) return("<none>")
  suffix <- if (length(fields) > max) ", ..." else ""
  paste0(paste(utils::head(fields, max), collapse = ", "), suffix)
}

validate_fields_exist <- function(metadata, dataset_id, fields, arg) {
  fields <- unique(fields[!is.na(fields) & nzchar(fields)])
  if (length(fields) == 0) return(invisible(TRUE))

  available <- metadata_field_names(metadata)
  missing <- setdiff(fields, available)
  if (length(missing) > 0) {
    rlang::abort(c(
      paste0("Unknown field in `", arg, "` for dataset '", dataset_id, "'."),
      x = paste(missing, collapse = ", "),
      i = paste0("Available fields include: ", format_field_list(available))
    ), class = "snstransparencia_error")
  }

  invisible(TRUE)
}

validate_date_field <- function(metadata, dataset_id, date_field) {
  if (is.null(date_field)) return(invisible(TRUE))
  validate_fields_exist(metadata, dataset_id, date_field, "date_field")

  type <- tolower(metadata_field_type(metadata, date_field))
  if (!is.na(type) && !type %in% c("date", "datetime")) {
    rlang::abort(c(
      paste0("`date_field` must be a date or datetime field for dataset '",
             dataset_id, "'."),
      x = paste0("Field '", date_field, "' has type '", type, "'.")
    ), class = "snstransparencia_error")
  }

  invisible(TRUE)
}

validate_dataset_query <- function(metadata, dataset_id, columns = NULL,
                                   select = NULL, date_field = NULL,
                                   filters = NULL, refine = NULL,
                                   exclude = NULL, facets = NULL) {
  validate_fields_exist(
    metadata, dataset_id,
    field_names_from_select(columns %||% select),
    "columns"
  )
  validate_date_field(metadata, dataset_id, date_field)
  validate_fields_exist(
    metadata, dataset_id,
    c(field_names_from_facet_filters(filters),
      field_names_from_facet_filters(refine)),
    "filters/refine"
  )
  validate_fields_exist(
    metadata, dataset_id,
    field_names_from_facet_filters(exclude),
    "exclude"
  )
  validate_fields_exist(
    metadata, dataset_id,
    field_names_from_facet_specs(facets),
    "facets"
  )
  invisible(TRUE)
}

validate_dataset_id <- function(dataset_id) {
  if (!is.character(dataset_id) || length(dataset_id) != 1 ||
      is.na(dataset_id) || !nzchar(dataset_id)) {
    rlang::abort("`dataset_id` must be a non-empty character string.",
                 class = "snstransparencia_error")
  }
  invisible(dataset_id)
}

validate_export_format <- function(format) {
  match.arg(format, c("csv", "json", "jsonl", "parquet"))
}

facets_to_df <- function(facets) {
  empty <- data.frame(
    facet = character(),
    name = character(),
    value = character(),
    count = integer(),
    state = character(),
    stringsAsFactors = FALSE
  )
  if (is.null(facets) || length(facets) == 0) return(empty)

  rows <- lapply(facets, function(facet_obj) {
    values <- facet_obj$facets
    if (is.null(values) || length(values) == 0) return(NULL)
    out <- records_to_df(values)
    if (nrow(out) == 0) return(NULL)
    out$facet <- facet_obj$name %||% NA_character_
    front <- c("facet", "name", "value", "count", "state")
    out[, c(intersect(front, names(out)), setdiff(names(out), front)), drop = FALSE]
  })

  rows <- Filter(Negate(is.null), rows)
  if (length(rows) == 0) return(empty)
  out <- do.call(rbind, rows)
  row.names(out) <- NULL
  out
}
