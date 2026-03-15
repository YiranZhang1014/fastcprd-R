#' Preprocess event
#'
#' @param data data.table containing event data
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


#' Add previous outcome indicator
#'
#' @param df data.table
#' @param outcome_col Column indicating outcome (0/1 or TRUE/FALSE)
#' @param new_col_name Name for new column
#' @param time_col Time column for ordering (default: "pregstart")
#' @return data.table with added previous outcome column
#'
#' @export
#' Core internal function to add variables
#'
#' @noRd
.add_variable_core <- function(
  data,
  events_data,
  var_name,
  start_col,
  start_offset_days,
  end_col,
  end_offset_days,
  unique_col,
  exclude_previous_records = FALSE,
  previous_start_col,
  previous_end_col,
  keep_date = TRUE,
  var_type = c("binary", "continuous"),
  value_col = NULL,
  keep_record = c("first", "last") # Allow to keep first or last record within the time window
) {
  # Validate inputs
  var_type <- match.arg(var_type)
  keep_record <- match.arg(keep_record)

  # Check whether `events_data`contains necessary columns
  required_cols <- c("patid", "event_date")
  if (!all(required_cols %in% names(events_data))) {
    stop(glue::glue(
      "Error: 'events_data' must contain the following columns: {paste(required_cols, collapse=', ')}. Available columns: {paste(names(events_data), collapse=', ')}"
    ))
  }
  # Ensure `event_date` is of Date type, transform if necessary
  if (!inherits(events_data$event_date, "Date")) {
    events_data[, event_date := as.IDate(event_date)]
  }

  # Make a copy of the input data to avoid modifying the original
  dt <- copy(data)

  if (var_type == "continuous" && !value_col %in% names(events_data)) {
    stop(glue::glue(
      "Error: Column '{value_col}' not found in event data. Available columns: {paste(names(events_data), collapse=', ')}"
    ))
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
  merged <- merge(
    dt,
    events_data,
    by = "patid",
    all.x = TRUE,
    allow.cartesian = TRUE
  )

  if (exclude_previous_records) {
    prev_dt <- dt[, .(
      patid,
      prev_start = get(previous_start_col),
      prev_end = get(previous_end_col)
    )]

    tmp_dt <- merge(
      merged,
      prev_dt,
      by = "patid",
      all.x = FALSE,
      allow.cartesian = TRUE
    )

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
        var_date = if (keep_record == "last") {
          max(event_date, na.rm = TRUE)
        } else {
          min(event_date, na.rm = TRUE)
        }
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
          if (keep_record == "last") {
            vals[which.max(dates)]
          } else {
            vals[which.min(dates)]
          }
        },
        var_date = if (keep_record == "last") {
          max(event_date, na.rm = TRUE)
        } else {
          min(event_date, na.rm = TRUE)
        }
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

  # Fill missing binary values with 0
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
  data,
  obs_path = NULL,
  med_code_list = NULL,
  hes_epi_path = NULL,
  icd_code_list = NULL,
  var_name,
  start_col,
  start_offset_days = 0,
  end_col,
  end_offset_days = 0,
  unique_col = "pregid",
  exclude_previous_records = FALSE,
  previous_start_col = "pregstart",
  previous_end_col = "pregend",
  keep_date = TRUE,
  keep_record = "last"
) {
  # Check the obs_path and hes_epi_path
  if (!file.exists(obs_path) && !file.exists(hes_epi_path)) {
    stop(glue::glue(
      "Error: Neither 'obs_path' nor 'hes_epi_path' exists. Please provide a valid path for at least one of them."
    ))
  }

  # Read and extract events from obs_path and hes_epi_path if they exist
  if (file.exists(obs_path)) {
    med_extracted_dt <- extract_event_and_date.aurum_obs(
      X = obs_path,
      med_code_list = med_code_list
    )
  } else {
    (med_extracted_dt <- NULL)
  }

  if (file.exists(hes_epi_path)) {
    icd_extracted_dt <- extract_event_and_date.hes_episodes(
      X = hes_epi_path,
      icd_code_list = icd_code_list
    )
  } else {
    (icd_extracted_dt <- NULL)
  }

  # Merge extracted data
  merged_dt <- rbindlist(list(med_extracted_dt, icd_extracted_dt), fill = TRUE)

  # Execute core variable building function
  .add_variable_core(
    data = data,
    events_data = merged_dt,
    var_name = var_name,
    start_col = start_col,
    start_offset_days = start_offset_days,
    end_col = end_col,
    end_offset_days = end_offset_days,
    unique_col = unique_col,
    exclude_previous_records = exclude_previous_records,
    previous_start_col = previous_start_col,
    previous_end_col = previous_end_col,
    keep_date = keep_date,
    var_type = "binary",
    keep_record = keep_record
  )
}

#' Add continuous variable (with optional last event date)
#'
#' @export
add_continuous_variable <- function(
  data,
  obs_path = NULL,
  med_code_list = NULL,
  hes_epi_path = NULL,
  icd_code_list = NULL,
  var_name,
  start_col,
  start_offset_days = 0,
  end_col,
  end_offset_days = 0,
  unique_col = "pregid",
  value_col = "value",
  exclude_previous_records = FALSE,
  previous_start_col = "pregstart",
  previous_end_col = "pregend",
  keep_date = TRUE,
  keep_record = "last"
) {
  # Check the obs_path and hes_epi_path
  if (!file.exists(obs_path)) {
    stop(glue::glue(
      "Error: 'obs_path' does not exist. Continuous variable extraction requires a valid 'obs_path'."
    ))
  }

  # Read and extract events from obs_path and hes_epi_path if they exist
  if (file.exists(obs_path)) {
    med_extracted_dt <- extract_event_and_date.aurum_obs(
      X = obs_path,
      med_code_list = med_code_list
    )
  } else {
    (med_extracted_dt <- NULL)
  }
  # If `hes_epi_path` set, warning that it will be ignored for continuous variable extraction
  if (!is.null(hes_epi_path)) {
    warning(glue::glue(
      "Warning: 'hes_epi_path' is provided but will be ignored for continuous variable extraction. Only 'obs_path' will be used."
    ))
  }

  .add_variable_core(
    data = data,
    events_data = med_extracted_dt, # Only use obs data for continuous variable
    var_name = var_name,
    start_col = start_col,
    start_offset_days = start_offset_days,
    end_col = end_col,
    end_offset_days = end_offset_days,
    unique_col = unique_col,
    exclude_previous_records = exclude_previous_records,
    previous_start_col = previous_start_col,
    previous_end_col = previous_end_col,
    keep_date = keep_date,
    var_type = "continuous",
    keep_record = keep_record, 
    value_col = value_col
  )
}
