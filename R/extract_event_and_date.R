# Extract events and corresponding dates from CPRD Aurum and HES episodes tables

extract_event_and_date <- function(X, ...) {
  UseMethod("extract_event_and_date")
}

extract_event_and_date.default <- function(X, ...) {
  stop("No method available for class ", class(data))
}

#' Extract events and corresponding dates from CPRD Aurum observation table
#'
#' @param X A list containing the path to the CPRD Aurum observation table.
#' @param med_code_list A vector of medical code IDs to filter the observation records.
#' @return A data.table containing the patient ID, medical code ID, event date, and value for the specified medical codes.
extract_event_and_date.aurum_obs <- function(X, med_code_list) {
  # Check if the input is a list containing the data path, or a character string representing the path
  if (is.list(X) && !is.null(X$data_path)) {
    data_path <- X$data_path
  } else if (is.character(X)) {
    data_path <- X
  } else {
    stop("Input X must be a list containing 'data_path' or a valid file path.")
  }

  # Check file existence
  if (!file.exists(data_path)) {
    stop(paste(
      "Observation data file not found at",
      data_path,
      ". Please check the path."
    ))
  }

  # Check the `med_code_list` is character or interger type
  if (!is.character(med_code_list) && !bit64::is.integer64(med_code_list)) {
    stop("med_code_list must be a character or integer64 (bit64) vector.")
  }

  # Extract from original dataset
  message(paste(data_path, "exists. Starting extraction..."))
  res_dt <- extract_from_table(
    table_name = "observation",
    table_path = data_path,
    col_name = "medcodeid",
    value_list = med_code_list,
    select_cols = c("patid", "medcodeid", "obsdate", "value")
  )

  # Process extracted data
  message(paste("Extraction completed. Processing data..."))
  # Rename
  setnames(res_dt, "obsdate", "event_date")
  # Convert patid to integer64 and event_date to IDate format
  res_dt[, patid := bit64::as.integer64(patid)]
  res_dt[, event_date := as.IDate(event_date)]

  return(res_dt)
}

#' Extract events and corresponding dates from HES episodes table
#'
#' @param X A data.table or a list containing the path to the HES episodes table.
#' @param icd_code_list A vector of ICD codes to filter the episodes records.
#' @return A data.table containing the patient ID and the corresponding event date for the specified ICD codes.
extract_event_and_date.hes_episodes <- function(X, icd_code_list) {
  # Make sure the `icd_code_list` is character type for substring operation
  icd_code_list <- as.character(icd_code_list)
  # Cut the `icd_code_list` to 5 characters to match the format in HES episodes table
  icd_code_list <- substr(icd_code_list, 1, 5)

  # Check if X is a data.table or list,
  # if data.table, use it directly; if list, read the data from the specified path
  if (is.data.table(X)) {
    dt <- X
  } else if (is.list(X) && !is.null(X$data_path)) {
    data_path <- X$data_path
    dt <- data.table::fread(data_path)
  } else if (is.character(X) && file.exists(X)) {
    dt <- data.table::fread(X)
  } else {
    stop("Input X must be a data.table or a list containing 'data_path'.")
  }
  # Extract needed columns
  dt <- dt[, .(patid, epistart, epiend, ICD)]

  # Make sure the `ICD` column is character type for filtering
  dt[, ICD := as.character(ICD)]

  # Filter rows based on the provided ICD code list
  dt <- dt[ICD %in% icd_code_list]

  # Convert patid to integer64 and epistart, epiend to IDate format
  dt[, patid := bit64::as.integer64(patid)]
  dt[, epistart := as.IDate(epistart, format = "%d/%m/%Y")]
  dt[, epiend := as.IDate(epiend, format = "%d/%m/%Y")]

  # Fill the missing `epistart` with `epiend`
  dt[is.na(epistart), epistart := epiend]
  # Remove rows with missing `epistart` (which means both `epistart` and `epiend` are missing)
  dt <- dt[!is.na(epistart)]
  # Remove `epiend` column as it's no longer needed
  dt[, epiend := NULL]

  # Rename columns
  setnames(dt, "epistart", "event_date")

  return(dt)
}
