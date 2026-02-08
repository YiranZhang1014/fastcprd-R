# R/cprd_schemas.R

#' Default CPRD Table Schemas
#'
#' This object defines the default schemas for the CPRD tables.
#' Each table's schema is represented as a named vector, where the names are the column names
#' and the values are the corresponding data types.
#' This structure allows for easy reference and validation when working with CPRD data in R.
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
default_cprd_schemas <- list(
  # === Patient ===
  patient = c(
    patid = "VARCHAR",
    pracid = "INTEGER",
    usualgpstaffid = "VARCHAR",
    gender = "INTEGER",
    yob = "INTEGER",
    mob = "INTEGER",
    emis_ddate = "DATE",
    regstartdate = "DATE",
    patienttypeid = "INTEGER",
    regenddate = "DATE",
    acceptable = "INTEGER",
    cprd_ddate = "DATE"
  ),
  # === Consultation ===
  consultation = c(
    patid = "VARCHAR",
    consid = "VARCHAR",
    pracid = "INTEGER",
    consdate = "DATE",
    enterdate = "DATE",
    staffid = "VARCHAR",
    conssourceid = "VARCHAR",
    cprdconstypeid = "INTEGER",
    consmedcodeid = "VARCHAR"
  ),
  # === Observation ===
  observation = c(
    patid = "VARCHAR",
    consid = "VARCHAR",
    pracid = "INTEGER",
    obsid = "VARCHAR",
    obsdate = "DATE",
    enterdate = "DATE",
    staffid = "VARCHAR",
    parentobsid = "VARCHAR",
    medcodeid = "VARCHAR",
    value = "DOUBLE",
    numunitid = "INTEGER",
    obstypeid = "INTEGER",
    numrangelow = "DOUBLE",
    numrangehigh = "DOUBLE",
    probobsid = "VARCHAR"
  ),

  # === Referral ===
  referral = c(
    patid = "VARCHAR",
    obsid = "VARCHAR",
    pracid = "INTEGER",
    refsourceorgid = "INTEGER",
    reftargetorgid = "INTEGER",
    refurgencyid = "INTEGER",
    refservicetypeid = "INTEGER",
    refmodeid = "INTEGER"
  ),

  # === Problem ===
  problem = c(
    patid = "VARCHAR",
    obsid = "VARCHAR",
    pracid = "INTEGER",
    parentprobobsid = "VARCHAR",
    probenddate = "DATE",
    expduration = "INTEGER",
    lastrevdate = "DATE",
    lastrevstaffid = "VARCHAR",
    parentprobrelid = "INTEGER",
    probstatusid = "INTEGER",
    signid = "INTEGER"
  ),

  # === DrugIssue ===
  drugissue = c(
    patid = "VARCHAR",
    issueid = "VARCHAR",
    pracid = "INTEGER",
    probobsid = "VARCHAR",
    drugrecid = "VARCHAR",
    issuedate = "DATE",
    enterdate = "DATE",
    staffid = "VARCHAR",
    prodcodeid = "VARCHAR",
    dosageid = "VARCHAR",
    quantity = "DOUBLE",
    quantunitid = "INTEGER",
    duration = "INTEGER",
    estnhscost = "DOUBLE"
  )
)
