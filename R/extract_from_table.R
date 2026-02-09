#' Extract subset of data by patient IDs
#'
#' @param table_name The name of the table to extract from (e.g., "Observation").
#' @param table_path The path to the Parquet file containing the table data.
#' @param id_list A vector of patient IDs to extract.
#' @param output_path The path where the extracted subset of data will be saved in Parquet format.
#' @return None. The function writes the extracted data to the specified output path.
#'
#' @import data.table
#' @importFrom glue glue
#'
#' @export
extract_from_table <- function(table_name, table_path, id_list, output_path, select_cols = NULL) {
  # Record start time
  start_time <- Sys.time()
  message(glue::glue("[{start_time}] Starting extraction from {table_name}."))

  # Process table_path to handle wildcards
  message("Processing table path...")
  table_path <- process_table_path(table_path)
  read_func <- get_read_function(table_path)

  # Convert id_list to character vector if it's not already
  message("Processing patient IDs...")
  id_dt <- process_id_list(id_list)

  # Connect to DuckDB
  message("Connecting to DuckDB...")
  conn <- duckdb::dbConnect(duckdb::duckdb(), bigint = "integer64")
  on.exit(
    {
      duckdb::dbDisconnect(conn, shutdown = TRUE)
    },
    add = TRUE
  )

  # Get the schema for the specified table
  message("Retrieving table schema...")
  schema <- check_schema(table_name)

  # Check if the specified columns exist in the Parquet file
  message("Processing select columns...")
  cols <- process_select_cols(read_func, table_path, schema, select_cols, conn)
  select_clause <- build_cast_sql(schema, cols)


  # Create the SQL query to extract the subset of data based on patient IDs
  query <- glue::glue("
    COPY (
      SELECT {select_clause}
      FROM {read_func}('{table_path}') AS cprd
      INNER JOIN id_list_dt AS pat
      ON CAST(cprd.patid AS BIGINT) = CAST(pat.pat_id AS BIGINT)
    ) TO '{output_path}' (FORMAT 'parquet', COMPRESSION 'ZSTD')
  ")

  message(glue::glue("Executing extraction for {length(id_list)} patient IDs..."))

  # Enable progress bar for the query execution
  DBI::dbExecute(conn, "PRAGMA enable_progress_bar")

  # Register the id_list data.table as a temporary table in DuckDB
  duckdb::duckdb_register(conn, "id_list_dt", id_dt)
  # Execute the query to extract the subset of data and write to Parquet
  message(glue::glue("[{Sys.time()}] Executing extraction..."))
  DBI::dbExecute(conn, query)

  # Disconnect from the database
  # duckdb::dbDisconnect(conn, shutdown = TRUE)

  # Calculate and display execution time
  end_time <- Sys.time()
  elapsed_time <- difftime(end_time, start_time, units = "secs")
  message(glue::glue("[{end_time}] Extraction completed in {round(elapsed_time/60, 2)} minutes."))
}

#' Helper function to process table path (handle wildcards)
#' @noRd
process_table_path <- function(table_path) {
  # Check if path contains wildcards
  if (grepl("\\*\\.(txt|csv|parquet)$", table_path)) {
    # Extract directory and pattern
    dir_path <- dirname(table_path)
    file_pattern <- basename(table_path)

    # Get all matching files
    files <- list.files(dir_path,
      pattern = gsub("\\*", ".*", file_pattern),
      full.names = TRUE
    )

    if (length(files) == 0) {
      stop(glue::glue("No files found matching pattern: {table_path}"))
    }

    message(glue::glue("Found {length(files)} file(s) matching pattern"))

    # Return the wildcard path as-is
    return(table_path)
  } else {
    # Single file path
    if (!file.exists(table_path)) {
      stop(glue::glue("File not found: {table_path}"))
    }
    return(table_path)
  }
}

#' Helper function to get read function based on file extension
#' @noRd
get_read_function <- function(table_path) {
  # Extract file extension
  if (grepl("\\*\\.(txt|csv)$", table_path) || grepl("\\.(txt|csv)$", table_path)) {
    return("read_csv_auto")
  } else if (grepl("\\*\\.parquet$", table_path) || grepl("\\.parquet$", table_path)) {
    return("read_parquet")
  } else {
    stop(glue::glue("Unsupported file format in path: {table_path}"))
  }
}


#' Helper functions for extract_from_table
#' @noRd
process_id_list <- function(id_list) {
  if (!is.character(id_list)) {
    id_list <- as.character(id_list)
  }
  # Convert id_list to data.table
  id_dt <- data.table::data.table(pat_id = id_list)
  # Unique IDs only
  id_dt <- unique(id_dt, by = "pat_id")
  # Compare number of unique IDs with original list
  if (nrow(id_dt) < length(id_list)) {
    warning(glue::glue("Number of unique unique IDs ({nrow(id_dt)}) is less than the length of the original list ({length(id_list)}). Duplicates have been removed."))
  }

  id_dt
}

#' Helper function to check for table schema
#' @noRd
check_schema <- function(table_name) {
  # Check if the specified table has a defined schema
  table_name_lower <- tolower(table_name)
  if (!table_name_lower %in% names(default_cprd_schemas)) {
    warning(glue::glue("Schema not found for table '{table_name}'. Proceeding without type casting."))
    schema <- NULL
  } else {
    schema <- default_cprd_schemas[[table_name_lower]]
  }
  schema
}

#' Helper function to process select columns
#' @noRd
process_select_cols <- function(read_func, table_path, schema, select_cols, conn) {
  # Get the column names from the database file
  existing_cols_query <- glue::glue("
    SELECT *
    FROM {read_func}('{table_path}')
    LIMIT 0
  ")
  db_col_names <- names(DBI::dbGetQuery(conn, existing_cols_query))

  # Check if the specified columns exist in the database file
  if (is.null(select_cols)) {
    # If select_cols is NULL, use all columns from schema (if available) or all columns from file
    if (!is.null(schema)) {
      # Schema is not null. Filter to only include columns that exist in the database file
      select_cols <- intersect(names(schema), db_col_names)
      message(glue::glue("Select columns from schema that exist in database file: {paste(select_cols, collapse = ', ')}"))
    } else {
      # Schema is null. Use all columns from the database file
      select_cols <- db_col_names
    }
  } else {
    # Select columns was specified. Check for missing columns in the database file
    missing_cols <- setdiff(select_cols, db_col_names)
    if (length(missing_cols) > 0) {
      stop(glue::glue("Column(s) not found in database file: {paste(missing_cols, collapse = ', ')}"))
    }
  }

  select_cols
}

#' Helper function to build SQL select clause with type casting based on schema
#' @noRd
build_cast_sql <- function(schema, select_cols) {
  # Build select clause with type casting based on schema
  if (!is.null(schema)) {
    select_parts <- sapply(select_cols, function(col) {
      if (col %in% names(schema)) {
        col_type <- schema[[col]]
        # Return
        glue::glue('CAST(cprd."{col}" AS {col_type}) AS "{col}"')
      } else {
        # Return
        glue::glue('cprd."{col}"')
      }
    })
    select_clause <- DBI::SQL(glue::glue_collapse(select_parts, sep = ", "))
  } else {
    cols_with_prefix <- paste0("cprd.", '"', select_cols, '"')
    select_clause <- DBI::SQL(glue::glue_collapse(cols_with_prefix, sep = ", "))
  }
  # Return clause
  select_clause
}
