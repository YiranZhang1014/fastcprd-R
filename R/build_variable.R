#' Load and preprocess events
#'
#' @param path Path to CSV file
#' @param date_cols Character vector of date column names
#' @param val_col Name of value column (default: "value")
#' @return data.table with patid, event_date, and value columns
#'
#' @noRd
preprocess_events <- function(data, date_cols, val_col = "value") {
  # Convert date columns
  data[, (date_cols) := lapply(.SD, as.IDate), .SDcols = date_cols] # , format = "%d/%m/%Y"
  data[, patid := bit64::as.integer64(patid)]

  # Handle event_date
  if (length(date_cols) == 1) {
    setnames(data, date_cols[1], "event_date")
  } else {
    data[, event_date := fcoalesce(get(date_cols[1]), get(date_cols[2]))]
    data <- data[!is.na(event_date)]
  }

  # Handle value column
  if (!val_col %in% names(data)) {
    data[, (val_col) := NA]
  }

  # Select columns
  data <- data[, .(patid, event_date, value = get(val_col))]
  return(data)
}


#' Merge events from multiple sources
#'
#' @param var_dir Directory containing event files
#' @param var_name Variable name
#' @return Combined data.table of events
#'
#' @noRd
merge_events_old <- function(var_dir, var_name) {
  event_list <- list()

  medical_path <- file.path(var_dir, paste0(var_name, "_medical.csv"))
  icd10_path <- file.path(var_dir, paste0(var_name, "_icd10.csv"))

  if (file.exists(medical_path)) {
    message(paste(medical_path, "exists."))
    event_list[[length(event_list) + 1]] <-
      load_and_preprocess_events(medical_path, "obsdate", "value")
  }

  if (file.exists(icd10_path)) {
    message(paste(icd10_path, "exists."))
    event_list[[length(event_list) + 1]] <-
      load_and_preprocess_events(icd10_path, c("epistart", "epiend"), "value")
  }

  if (length(event_list) == 0) {
    stop(paste("No event data found for", var_name, ". Please check the paths."))
  }

  return(rbindlist(event_list))
}

merge_events <- function(obs_path, med_code_list) {
  event_list <- list()

  # Check file existence
  if (file.exists(obs_path)) {
    message(paste(obs_path, "exists. Starting extraction..."))
    res_dt <- extract_from_table(
      table_name = "observation",
      table_path = obs_path,
      col_name = "medcodeid",
      value_list = med_code_list,
      select_cols = c("patid", "medcodeid", "obsdate", "value")
    )

    message(paste("Extraction completed. Processing data..."))
    dt <- preprocess_events(data = res_dt, date_cols = "obsdate", val_col = "value")

    # Add to list
    event_list[[length(event_list) + 1]] <- dt
  }

  # Check if any events were loaded
  if (length(event_list) == 0) {
    stop(paste("No event data found for", var_name, ". Please check the paths."))
  }

  return(rbindlist(event_list))
}


#' Add binary variable
#'
#' @param data Main data.table
#' @param obs_path Path to observation data file
#' @param var_name Name of the variable to add
#' @param med_code_list List of medical codes to extract
#' @param start_col Start date column name
#' @param end_col End date column name
#' @param start_offset_days Days to offset start (default: 0)
#' @param end_offset_days Days to offset end (default: 0)
#' @param unique_col Unique identifier column (default: "pregid")
#' @return data.table with added binary variable
#'
#' @export
add_binary_variable <- function(
  data, obs_path, var_name, med_code_list,
  start_col, start_offset_days = 0,
  end_col, end_offset_days = 0,
  unique_col = "pregid"
) {
  dt <- copy(data)
  events_df <- merge_events(obs_path = obs_path, med_code_list = med_code_list)

  # Remove existing variable if present
  if (var_name %in% names(dt)) {
    dt[, (var_name) := NULL]
  }

  # Add time window
  dt[, `:=`(
    window_start = get(start_col) + start_offset_days,
    window_end = get(end_col) + end_offset_days
  )]

  # Left join to event data
  merged <- merge(dt, events_df, by = "patid", all.x = TRUE, allow.cartesian = TRUE)

  # Mark if event occurred within time window
  merged[, (var_name) := as.integer(
    !is.na(event_date) & # Only mark if event_date is not NA
      event_date >= window_start &
      event_date <= window_end
    # & event_date <= get(end_col)
  )]

  # Aggregate by unique_col
  agg <- merged[, .(var_value = {
    vals <- get(var_name)
    if (all(is.na(vals))) { # If all values are NA, return 0
      0L
    } else {
      max(vals, na.rm = TRUE)
    }
  }), by = unique_col]

  setnames(agg, "var_value", var_name)

  # Join back to original data
  final_dt <- merge(dt, agg, by = unique_col, all.x = TRUE)

  # 修改：确保缺失值填充为 0
  final_dt[is.na(get(var_name)) | is.infinite(get(var_name)), (var_name) := 0L]

  # Remove temporary columns
  final_dt[, c("window_start", "window_end") := NULL]

  return(final_dt)
}

#' Add continuous variable
#'
#' @param data Main data.table
#' @param obs_path Path to observation data file
#' @param var_name Name of the variable to add
#' @param med_code_list List of medical codes to extract
#' @param start_col Start date column name
#' @param end_col End date column name
#' @param start_offset_days Days to offset start (default: 0)
#' @param end_offset_days Days to offset end (default: 0)
#' @param unique_col Unique identifier column (default: "pregid")
#' @param value_col Name of the value column in event data (default: "value")
#' @param exclude_previous_records Whether to exclude events that fall within previous records (default: FALSE)
#' @param previous_start_col Start column for previous records (default: "pregstart")
#' @param previous_end_col End column for previous records (default: "pregend")
#' @return data.table with added continuous variable
#'
#' @export
add_continuous_variable <- function(
  data, obs_path, var_name, med_code_list,
  start_col, start_offset_days = 0,
  end_col, end_offset_days = 0,
  unique_col = "pregid",
  value_col = "value",
  exclude_previous_records = FALSE,
  previous_start_col = "pregstart",
  previous_end_col = "pregend"
) {
  dt <- copy(data)
  events_df <- merge_events(obs_path = obs_path, med_code_list = med_code_list)

  # --- Check value_col
  if (!value_col %in% names(events_df)) {
    stop(glue::glue("Error: Column '{value_col}' not found in event data. Available columns: {paste(names(events_df), collapse=', ')}"))
  }

  # Remove existing variable if present
  if (var_name %in% names(dt)) {
    dt[, (var_name) := NULL]
  }

  # Add time window
  dt[, `:=`(
    window_start = get(start_col) + start_offset_days,
    window_end = get(end_col) + end_offset_days
  )]

  # Left join to event data
  merged <- merge(dt, events_df, by = "patid", all.x = TRUE, allow.cartesian = TRUE)

  if (exclude_previous_records) {
    # Building the "previous records" table
    prev_dt <- dt[, .(patid,
      prev_start = get(previous_start_col),
      prev_end = get(previous_end_col)
    )]

    # Merge to identify events that fall within previous records
    tmp_dt <- merge(merged, prev_dt,
      by = "patid", all.x = FALSE, allow.cartesian = TRUE
    )
    # Identify events that fall within previous records
    removing_events <- tmp_dt[
      event_date >= prev_start &
        event_date <= prev_end &
        prev_start < get(start_col)
    ]
    # Remove these events from merged
    merged <- merged[!removing_events, on = .(patid, event_date)]
  }

  # Aggregate by unique_col
  agg <- merged[
    # Only consider records where event_date is not NA and value_col is not NA,
    # and within the time window
    !is.na(event_date) &
      !is.na(get(value_col)) & # Ensure value_col is not NA
      event_date >= window_start &
      event_date <= window_end,

    # Select the value corresponding to the most recent event_date within the window
    .(var_value = {
      vals <- get(value_col)
      dates <- event_date
      # which.max will return the index of the maximum date
      # This corresponds to the "most recent value"
      vals[which.max(dates)]
    }),
    by = unique_col
  ]

  setnames(agg, "var_value", var_name)

  # Join back to original data
  final_dt <- merge(dt, agg, by = unique_col, all.x = TRUE)

  # Remove temporary columns
  final_dt[, c("window_start", "window_end") := NULL]

  return(final_dt)
}


#' Add previous outcome indicator
#'
#' @param df data.table
#' @param outcome_col Column indicating outcome (0/1 or TRUE/FALSE)
#' @param new_col_name Name for new column
#' @param time_col Time column for ordering (default: "pregstart")
#' @return data.table with added previous outcome column
#'
#' @export
add_previous_outcome <- function(
  data, outcome_col, new_col_name,
  time_col = "pregstart"
) {
  # --- Check if outcome_col exists
  if (!outcome_col %in% names(data)) {
    stop(glue::glue("Error: Column '{outcome_col}' not found in data. Available columns: {paste(names(data), collapse=', ')}"))
  }

  # Remove existing variable if present
  if (new_col_name %in% names(data)) {
    data[, (new_col_name) := NULL]
  }

  # Sort by patient and time
  setorderv(data, cols = c("patid", time_col))

  # Set column name
  if (is.null(new_col_name)) {
    new_col_name <- paste0("pre_", outcome_col)
  }

  # Calculate cumulative maximum and shift
  data[, (new_col_name) := shift(cummax(get(outcome_col)), n = 1, fill = 0),
    by = patid
  ]

  return(data)
}
