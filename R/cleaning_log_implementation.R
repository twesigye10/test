library(tidyverse)
library(glue)
library(cleaningtools)
library(httr)

loc_data <- "inputs/UGA2305_land_and_energy_data.xlsx"

df_tool_data <- readxl::read_excel(loc_data)


# create a clean data -----------------------------------------------------

df_filled_cl <- readxl::read_excel("outputs/review_mycleaninglog.xlsx", sheet = "cleaning_log")

# check the cleaning log
df_cl_review <- cleaningtools::review_cleaning_log(
  raw_dataset = df_tool_data,
  raw_data_uuid_column = "_uuid",
  cleaning_log = df_filled_cl,
  cleaning_log_change_type_column = "change_type",
  change_response_value = "change_response",
  cleaning_log_question_column = "question",
  cleaning_log_uuid_column = "uuid",
  cleaning_log_new_value_column = "new_value"
)

# create the clean data from the raw data and cleaning log
df_cleaning_data <- cleaningtools::create_clean_data(
  raw_dataset = df_tool_data,
  raw_data_uuid_column = "_uuid",
  cleaning_log = df_filled_cl %>% filter(!question %in% c("duration_audit_sum_all_ms", "duration_audit_sum_all_minutes"), !uuid %in% c("all")),
  cleaning_log_change_type_column = "change_type",
  change_response_value = "change_response",
  NA_response_value = "blank_response",
  no_change_value = "no_action",
  remove_survey_value = "remove_survey",
  cleaning_log_question_column = "question",
  cleaning_log_uuid_column = "uuid",
  cleaning_log_new_value_column = "new_value"
)

openxlsx::write.xlsx(df_cleaning_data, paste0("outputs/", butteR::date_file_prefix(), 
                                              "_cleaning_data_sample.xlsx"))