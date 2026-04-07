# R/hes_schemas.R

#' Default lined HES-APC Table Schemas
#'
#' This object defines the default schemas for the HES-APC tables.
#' Each table's schema is represented as a named vector, where the names are the column names
#' and the values are the corresponding data types.
#' This structure allows for easy reference and validation when working with HES-APC data in R.
#'
#' @format A list with table names as keys (in lowercase) and named character vectors as values.
#'   Each named vector contains column names as names and SQL data types as values.
#'
#' @details
#' Currently supported tables:
#' \itemize{
#'   \item patient - Patient demographic and registration information
#'   \item observation - Clinical observations and measurements
#'   \item consultation - Consultation details
#'   \item referral - Referral details
#' }
#'
#' @export
default_hes_schemas <- list(
  # === HES Maternity ===
  hes_maternity = c(
    patid = "VARCHAR",
    spno = "BIGINT",
    epistart = "DATE",
    epiend = "DATE",
    eorder = "INTEGER",
    epidur = "INTEGER",
    numbaby = "CHARACTER",
    numtailb = "INTEGER",
    matordr = "INTEGER",
    neocare = "INTEGER",
    wellbaby = "CHARACTER",
    anasdate = "DATE",
    birordr = "CHARACTER",
    birstat = "INTEGER",
    biresus = "INTEGER",
    sexbaby = "CHARACTER",
    birweit = "INTEGER",
    delmeth = "CHARACTER",
    delonset = "INTEGER",
    delinten = "INTEGER",
    delplac = "INTEGER",
    delchang = "INTEGER",
    delprean = "INTEGER",
    delposan = "INTEGER",
    delstat = "INTEGER",
    anagest = "INTEGER",
    gestat = "INTEGER",
    numpreg = "INTEGER",
    matage = "INTEGER",
    neodur = "INTEGER",
    antedur = "INTEGER",
    postdur = "INTEGER"
  )
  # ===  ===
)
