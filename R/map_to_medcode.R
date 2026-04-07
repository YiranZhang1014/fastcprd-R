map_to_medcode <- function(X, ...) {
  UseMethod("map_to_medcode")
}

#' @export
map_to_medcode.default <- function(X, ...) {
  stop("No method available for class ", class(X))
}

#' This function maps SNOMED CT concept IDs to CPRD medcode IDs using a provided code dictionary.
#'
#' @param X A vector of SNOMED CT concept IDs.
#' @param code_dict_path The path to the code dictionary file.
#' @return A vector of CPRD medcode IDs corresponding to the input SNOMED CT concept IDs.
#'
#' @export
map_to_medcode.snomed <- function(X, code_dict_path) {
  # Check if X is a sequence of numbers
  if (!is.numeric(X) && !bit64::is.integer64(X) && !is.character(X)) {
    stop("Input X must be a sequence of numbers.")
  }
  # Convert X to character
  X <- as.character(X)

  # Read the code dictionary
  code_dict <- data.table::fread(code_dict_path)
  # extract all code list
  medcodeids <- code_dict[code_dict$snomedctconceptid %in% X, medcodeid]

  return(medcodeids)
}


#' Maps READ codes to CPRD medcode IDs using a provided code dictionary.
#' 
#' @param X A vector of READ codes (either original or cleansed).
#' @param code_dict_path The path to the code dictionary file.
#' @return A vector of CPRD medcode IDs corresponding to the input READ codes.
#' 
#' @export
map_to_medcode.read <- function(X, code_dict_path) {
  # Check if X is a sequence of characters
  if (!is.character(X)) {
    stop("Input X must be a sequence of characters.")
  }
  # Read the code dictionary
  code_dict <- data.table::fread(code_dict_path)
  # extract all code list
  medcodeids_original <- code_dict[code_dict$originalreadcode %in% X, medcodeid]
  medcodeids_cleansed <- code_dict[code_dict$cleansedreadcode %in% X, medcodeid]
  medcodeids <- unique(c(medcodeids_original, medcodeids_cleansed))

  return(medcodeids)
}
