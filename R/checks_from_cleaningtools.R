library(tidyverse)
library(glue)
library(cleaningtools)
library(httr)
library(supporteR)

source("R/support_functions.R")
source("support_files/credentials.R")

# global options can be set to further simplify things
options("openxlsx.dateFormat" = "dd/mm/yyyy")


# some needed explanation -------------------------------------------------

# come to handle other specify (putting the recodings back and handling the other text)
# formatting of logic checks and using that function


# read data ---------------------------------------------------------------

loc_data <- "inputs/UGA2305_land_and_energy_data.xlsx"

df_tool_data <- readxl::read_excel(loc_data)

# tool
loc_tool <- "inputs/land_and_energy_tool.xlsx"
df_survey <- readxl::read_excel(loc_tool, sheet = "survey") 
df_choices <- readxl::read_excel(loc_tool, sheet = "choices")


# download audit files
download_audit_files(df = df_tool_data, 
                     uuid_column = "_uuid", 
                     audit_dir = "inputs/audit_files", 
                     usr = user_acc, 
                     pass = user_pss)
# zip audit files folder
if (dir.exists("inputs/audit_files")) {
  zip::zip(zipfile = "inputs/audit_files.zip", 
           # files = "inputs/audit_files/",
           files = list.dirs(path = "inputs/audit_files/", full.names = TRUE, recursive = FALSE),
           mode = "cherry-pick")
}

# check pii ---------------------------------------------------------------

pii_cols <- c("telephone","contact","name","gps","latitude","logitude","contact","geopoint")

output_from_data <- cleaningtools::check_pii(dataset = df_tool_data, words_to_look = "date", uuid_column = "_uuid")
output_from_data$potential_PII


# duration ----------------------------------------------------------------
# readd audit file
audit_list_data <- cleaningtools::create_audit_list(audit_zip_path = "inputs/audit_files.zip")
# add duration from audit
df_tool_data_with_audit_time <- cleaningtools::add_duration_from_audit(df_tool_data, uuid_column = "_uuid", audit_list = audit_list_data)

# check duration
check_duration <- cleaningtools::check_duration(dataset = df_tool_data_with_audit_time, 
                                                column_to_check = "duration_audit_sum_all_minutes",
                                                uuid_column = "_uuid",
                                                log_name = "duration_log",
                                                lower_bound = 20,
                                                higher_bound = 120)

check_duration$duration_log %>% view()

# outliers ----------------------------------------------------------------
outlier_cols_not_4_checking <- df_tool_data %>% 
  select(matches("geopoint|gps|_index|_submit|submission|_sample_|^_id$")) %>% 
  colnames()
list_check_outliers <- cleaningtools::check_outliers(dataset = df_tool_data, 
                                                     uuid_column = "_uuid",
                                                     sm_separator = "/",
                                                     strongness_factor = 3,
                                                     columns_not_to_check = outlier_cols_not_4_checking) 

df_potential_outliers <- list_check_outliers$potential_outliers
df_check_outliers <- df_potential_outliers #%>% 
  # filter(!str_detect(string = question, pattern = "geopoint|gps|_index|uuid|_submit|submission|_sample_|^_id$|\\/"))


# Check for value ---------------------------------------------------------

list_check_value <- cleaningtools::check_value(dataset = df_tool_data, 
                                               uuid_column = "_uuid", 
                                               element_name = "checked_dataset", 
                                               values_to_look = c(666, 99, 999, 9999, 98, 88, 888, 8888))

df_potental_check_value <- list_check_value$flaged_value 

df_check_value <- df_potental_check_value %>% 
  filter(!str_detect(string = question, pattern = "geopoint|gps|_index|uuid|_submit|submission|_sample_|^_id$|\\/"))


# check logics ------------------------------------------------------------

cleaningtools::check_logical(df_tool_data,
                             uuid_column = "_uuid",
                             check_to_perform = "kap_stove_type_owned == \"three_stone_fire\" & kap_fuels_mostly_used == \"briquettes\"",
                             columns_to_clean = "kap_stove_type_owned, kap_fuels_mostly_used",
                             description = "use the three stone fire, but neither charcoal nor wood are their main fuels"
) %>% head()


# check for duplicates ----------------------------------------------------

# With the gower distance (soft duplicates)

soft_duplicates <- check_soft_duplicates(
  dataset = df_tool_data,
  kobo_survey = df_survey,
  uuid_column = "_uuid",
  idnk_value = "dk",
  sm_separator = "/",
  log_name = "soft_duplicate_log",
  threshold = 7,
  return_all_results = FALSE
)

soft_duplicates$soft_duplicate_log %>% 
  view()


# by enumerator

group_by_enum_raw_data <- df_tool_data %>%
  dplyr::group_by(meta_enumerator_id) %>% 
  filter(n()>1)

soft_per_enum <- group_by_enum_raw_data %>%
  dplyr::group_split() %>%
  purrr::map(~ check_soft_duplicates(
    dataset = .,
    kobo_survey = df_survey,
    uuid_column = "_uuid", idnk_value = "dk",
    sm_separator = "/",
    log_name = "soft_duplicate_log",
    threshold = 7, 
    return_all_results = TRUE
  ))

soft_per_enum %>%
  purrr::map(~ .[["soft_duplicate_log"]]) %>%
  purrr::map2(
    .y = dplyr::group_keys(group_by_enum_raw_data) %>% unlist(),
    ~ dplyr::mutate(.x, enum = .y)
  ) %>%
  do.call(dplyr::bind_rows, .) %>%
  head() %>% 
  view()

# check the food consumption score ----------------------------------------





# check others values -----------------------------------------------------

output <- cleaningtools::check_others(
  dataset = df_tool_data,
  uuid_column = "_uuid",
  columns_to_check = names(df_tool_data %>%
                             dplyr::select(ends_with("_other")) %>%
                             dplyr::select(-contains("/")))
)

output$other_log %>% view()

# test other

other_logic_detected <- cleaningtools::create_logic_for_other(kobo_survey = df_survey , 
                                                          sm_separator = "/", 
                                                          dataset = df_tool_data)
write_csv(other_logic_detected, file = "outputs/other_logic_detected.csv")

# Exporting the flags in excel --------------------------------------------

# create_combined_log()
list_log <- df_tool_data_with_audit_time %>%
  check_pii(uuid_column = "_uuid") %>%
  check_duration(column_to_check = "duration_audit_sum_all_minutes",
                 uuid_column = "_uuid",
                 log_name = "duration_log",
                 lower_bound = 20,
                 higher_bound = 120) %>% 
  check_outliers(uuid_column = "_uuid", sm_separator = "/",
                 strongness_factor = 3, columns_not_to_check = outlier_cols_not_4_checking) %>% 
  # check_soft_duplicates(kobo_survey = df_survey,
  #                       uuid_column = "_uuid",
  #                       idnk_value = "dk",
  #                       sm_separator = "/",
  #                       log_name = "soft_duplicate_log",
  #                       threshold = 7,
  #                       return_all_results = FALSE) %>%
  check_value(uuid_column = "_uuid", values_to_look = c(666, 99, 999, 9999, 98, 88, 888, 8888)) # %>%
  # check_others(uuid_column = "_uuid",
  #   columns_to_check = names(df_tool_data %>%dplyr::select(ends_with("_other")) %>% dplyr::select(-contains("/")))
  # ) %>% 
  # create_combined_log()

list_log$duration_log %>% view()

# other checks
df_other_checks <- cts_format_other_specify(input_tool_data = df_tool_data, 
                                                    input_uuid_col = "_uuid", 
                                                    input_survey = df_survey, 
                                                    input_choices = df_choices)

# add other checks to the list
list_log$other_log <- df_other_checks

# combine the log
df_combined_log <- create_combined_log(dataset_name = "checked_dataset", list_of_log = list_log)

# add_info_to_cleaning_log()
add_with_info <- add_info_to_cleaning_log(list_of_log = df_combined_log,
                                          dataset = "checked_dataset",
                                          cleaning_log = "cleaning_log",
                                          dataset_uuid_column = "_uuid",
                                          cleaning_log_uuid_column = "uuid",
                                          information_to_add = c("meta_enumerator_id", "today")
)

add_with_info$cleaning_log %>% 
  head() %>% 
  view()

# create_xlsx_cleaning_log()
add_with_info |>
  create_xlsx_cleaning_log(
    kobo_survey = df_survey,
    kobo_choices = df_choices,
    use_dropdown = TRUE,
    output_path = "outputs/mycleaninglog.xlsx"
  )



# recreate_parent_column()
cleaningtools::recreate_parent_column(dataset = test_data, uuid_column = "uuid", sm_separator = ".") |> head()

df_updated_sosm_log <- cleaningtools::recreate_parent_column(dataset = df_tool_data, 
                                      uuid_column = "_uuid", 
                                      kobo_survey = df_survey, 
                                      kobo_choices = df_choices, 
                                      sm_separator = "/", 
                                      cleaning_log_to_append = df_filled_cl %>% filter(!question %in% c("duration_audit_sum_all_ms", "duration_audit_sum_all_minutes"), !uuid %in% c("all"))
                                      )
