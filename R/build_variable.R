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


#' Add binary variable (based on continuous variable logic with optional last event date)
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
#' @param exclude_previous_records Whether to exclude events that fall within previous records (default: FALSE)
#' @param previous_start_col Start column for previous records (default: "pregstart")
#' @param previous_end_col End column for previous records (default: "pregend")
#' @param keep_date Logical, whether to keep the date of the last event (default: TRUE)
#' @return data.table with added binary variable and optional date column
#'
#' @export
add_binary_variable <- function(
  data, obs_path, var_name, med_code_list,
  start_col, start_offset_days = 0,
  end_col, end_offset_days = 0,
  unique_col = "pregid",
  exclude_previous_records = FALSE,
  previous_start_col = "pregstart",
  previous_end_col = "pregend",
  keep_date = TRUE
) {
  dt <- copy(data)
  events_df <- merge_events(obs_path = obs_path, med_code_list = med_code_list)

  date_col_name <- paste0(var_name, "_date")

  # Remove existing variable if present
  if (var_name %in% names(dt)) {
    dt[, (var_name) := NULL]
  }
  if (keep_date && date_col_name %in% names(dt)) {
    dt[, (date_col_name) := NULL]
  }

  # Add time window
  dt[, `:=`(
    window_start = get(start_col) + start_offset_days,
    window_end = get(end_col) + end_offset_days
  )]

  # Left join to event data
  merged <- merge(dt, events_df, by = "patid", all.x = TRUE, allow.cartesian = TRUE)

  # Exclude previous records logic
  if (exclude_previous_records) {
    prev_dt <- dt[, .(patid,
      prev_start = get(previous_start_col),
      prev_end = get(previous_end_col)
    )]

    tmp_dt <- merge(merged, prev_dt,
      by = "patid", all.x = FALSE, allow.cartesian = TRUE
    )

    removing_events <- tmp_dt[
      event_date >= prev_start &
        event_date <= prev_end &
        prev_start < get(start_col)
    ]
    merged <- merged[!removing_events, on = .(patid, event_date)]
  }

  # Aggregate by unique_col
  agg <- merged[
    !is.na(event_date) &
      event_date >= window_start &
      event_date <= window_end,
    .(
      var_value = 1L, # Set to 1L if any event exists in the window
      var_date = max(event_date, na.rm = TRUE) # Extract the date of the last event in the window
    ),
    by = unique_col
  ]

  # Rename and merge back
  setnames(agg, "var_value", var_name)
  if (keep_date) {
    setnames(agg, "var_date", date_col_name)
  } else {
    agg[, var_date := NULL]
  }

  final_dt <- merge(dt, agg, by = unique_col, all.x = TRUE)

  # Fill missing binary variable with 0L
  final_dt[is.na(get(var_name)), (var_name) := 0L]

  # Clean up temporary window columns
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


#' Core internal function to add variables
#'
#' @noRd
.add_variable_core <- function(
  data, obs_path, var_name, med_code_list,
  start_col, start_offset_days, end_col, end_offset_days,
  unique_col, exclude_previous_records, previous_start_col, previous_end_col,
  keep_date, var_type = c("binary", "continuous"), value_col = NULL
) {
  var_type <- match.arg(var_type)
  dt <- copy(data)

  # Load and preprocess events
  # Check `med_code_list` is not empty and integer64 type
  if (length(med_code_list) == 0) {
    stop("Error: med_code_list cannot be empty.")
  }
  if (!bit64::is.integer64(med_code_list)) {
    med_code_list <- bit64::as.integer64(med_code_list)
  }
  events_df <- merge_events(obs_path = obs_path, med_code_list = med_code_list)

  if (var_type == "continuous" && !value_col %in% names(events_df)) {
    stop(glue::glue("Error: Column '{value_col}' not found in event data. Available columns: {paste(names(events_df), collapse=', ')}"))
  }

  date_col_name <- paste0(var_name, "_date")

  # Clean up existing variable and date column if they exist
  if (var_name %in% names(dt)) {
    dt[, (var_name) := NULL]
  }
  if (keep_date && date_col_name %in% names(dt)) {
    dt[, (date_col_name) := NULL]
  }

  # Time window calculation
  dt[, `:=`(
    window_start = get(start_col) + start_offset_days,
    window_end = get(end_col) + end_offset_days
  )]

  # Left join with event data
  merged <- merge(dt, events_df, by = "patid", all.x = TRUE, allow.cartesian = TRUE)

  if (exclude_previous_records) {
    prev_dt <- dt[, .(patid,
      prev_start = get(previous_start_col),
      prev_end = get(previous_end_col)
    )]

    tmp_dt <- merge(merged, prev_dt, by = "patid", all.x = FALSE, allow.cartesian = TRUE)

    removing_events <- tmp_dt[
      event_date >= prev_start &
        event_date <= prev_end &
        prev_start < get(start_col)
    ]
    merged <- merged[!removing_events, on = .(patid, event_date)]
  }

  # Aggregate by unique_col
  if (var_type == "binary") {
    agg <- merged[
      !is.na(event_date) &
        event_date >= window_start &
        event_date <= window_end,
      .(
        var_value = 1L,
        var_date = max(event_date, na.rm = TRUE)
      ),
      by = unique_col
    ]
  } else if (var_type == "continuous") {
    agg <- merged[
      !is.na(event_date) &
        !is.na(get(value_col)) &
        event_date >= window_start &
        event_date <= window_end,
      .(
        var_value = {
          vals <- get(value_col)
          dates <- event_date
          vals[which.max(dates)]
        },
        var_date = max(event_date, na.rm = TRUE)
      ),
      by = unique_col
    ]
  }

  # Rename and merge back
  setnames(agg, "var_value", var_name)
  if (keep_date) {
    setnames(agg, "var_date", date_col_name)
  } else {
    agg[, var_date := NULL]
  }

  final_dt <- merge(dt, agg, by = unique_col, all.x = TRUE)

  # 
  if (var_type == "binary") {
    final_dt[is.na(get(var_name)), (var_name) := 0L]
  }

  # Clean up temporary window columns
  final_dt[, c("window_start", "window_end") := NULL]

  return(final_dt)
}


#' Add binary variable (based on continuous variable logic with optional last event date)
#'
#' @export
add_binary_variable <- function(
  data, obs_path, var_name, med_code_list,
  start_col, start_offset_days = 0,
  end_col, end_offset_days = 0,
  unique_col = "pregid",
  exclude_previous_records = FALSE,
  previous_start_col = "pregstart",
  previous_end_col = "pregend",
  keep_date = TRUE
) {
  .add_variable_core(
    data = data, obs_path = obs_path, var_name = var_name, med_code_list = med_code_list,
    start_col = start_col, start_offset_days = start_offset_days,
    end_col = end_col, end_offset_days = end_offset_days,
    unique_col = unique_col, exclude_previous_records = exclude_previous_records,
    previous_start_col = previous_start_col, previous_end_col = previous_end_col,
    keep_date = keep_date,
    var_type = "binary" 
  )
}

#' Add continuous variable (with optional last event date)
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
  previous_end_col = "pregend",
  keep_date = TRUE
) {
  .add_variable_core(
    data = data, obs_path = obs_path, var_name = var_name, med_code_list = med_code_list,
    start_col = start_col, start_offset_days = start_offset_days,
    end_col = end_col, end_offset_days = end_offset_days,
    unique_col = unique_col, exclude_previous_records = exclude_previous_records,
    previous_start_col = previous_start_col, previous_end_col = previous_end_col,
    keep_date = keep_date,
    var_type = "continuous", 
    value_col = value_col 
  )
}
