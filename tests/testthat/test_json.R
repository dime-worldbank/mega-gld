# tests/testthat.R
library(testthat)

repo_root <- normalizePath("..", mustWork = TRUE)

source(file.path(repo_root, "helpers", "config.R"))
source(file.path(repo_root, "helpers", "json_text.R"))
source(file.path(repo_root, "helpers", "json_builder.R"))
source(file.path(repo_root, "helpers", "json_pipeline_3a.R"))
source(file.path(repo_root, "helpers", "gh_links_3a.R"))

test_dir("tests/testthat")


make_minimal_row <- function(overrides = list()) {
  base <- list(
    filename = "USA_2020_LFS_V01_M_V01_A_GLD",
    survey_extended = "Labor Force Survey",
    survey_clean = "LFS",
    year = 2020,
    quarter = NA_character_,
    A_version = 1,
    country = "USA",
    classification = "Official Use",
    version_label = NA_character_,
    gh_url = NA_character_,
    producers_name = NA_character_,
    household_level = FALSE,
    data_access_note = NA_character_,
    geog_coverage = NA_character_
  )

  as_tibble(modifyList(base, overrides))
}

# test mandatory fields
test_that("make_mdl_json includes required idno and title", {
  row <- make_minimal_row()
  countries <- tibble(code = "USA", name = "United States")

  js <- make_mdl_json(row, countries)

  expect_true(!is.null(js$idno))
  expect_true(nzchar(js$idno))

  title <- js$study_desc$title_statement$title
  expect_true(!is.null(title))
  expect_true(nzchar(title))
})


# test fallbacks


