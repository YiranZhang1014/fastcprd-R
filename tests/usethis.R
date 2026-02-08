library(usethis)

# Create DESCRIPTION file with basic metadata
use_description(fields = list(Title = "Fast Processing of CPRD Data using DuckDB"))

# Add authors and maintainers
use_author("Yiran", "Zhang", email = "yiran.zhang@manchester.ac.uk")

# Add dependencies
use_package("duckdb")
use_package("data.table")
use_package("glue")
use_package("DBI")

# Set up documentation generation tool roxygen2
use_roxygen_md()

# Create SQL folder structure (manually create directory or use R)
dir.create(file.path("inst", "sql"), recursive = TRUE)

# Create testing framework
use_testthat()

# Create a test file for the extract_from_table function
usethis::use_test("extract_from_table")
