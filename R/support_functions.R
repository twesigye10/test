
# download audit files ----------------------------------------------------

download_audit_files <- function(df, uuid_column = "_uuid", audit_dir, usr, pass){
    if (!"httr" %in% installed.packages()) 
        stop("The package is httr is required!")
    
    if (is.na(audit_dir) || audit_dir == "") 
        stop("The path for storing audit files can't be empty!")
    
    if (is.na(usr) || usr == "") 
        stop("Username can't be empty!")
    
    if (is.na(pass) || pass == "") 
        stop("Password can't be empty!")
    
    # checking if the output directory is already available
    if (!dir.exists(audit_dir)) {
        dir.create(audit_dir)
        if (dir.exists(audit_dir)) {
            cat("Attention: The audit file directory was created in", audit_dir,"\n")
        }
    }
    
    # checking if creating output directory was successful
    if (!dir.exists(audit_dir))
        stop("download_audit_fils was not able to create the output directory!")
    # checking if uuid column exists in data set
    if (!uuid_column %in% names(df))
        stop("The column ", uuid_column, " is not available in data set.")
    # checking if column audit_URL exists in data set
    if (!uuid_column %in% names(df))
        stop("The column ", uuid_column, " is not available in data set.")
    if (!"audit_URL" %in% names(df))
        stop("Error: the column audit_URL is not available in data set.")
    
    # getting the list of uuids that are already downloaded
    available_audits <- dir(audit_dir)
    
    # excluding uuids that their audit files are already downloaded
    df <- df[!df[[uuid_column]] %in% available_audits,]
    
    audits_endpoint_link <- df[["audit_URL"]]
    names(audits_endpoint_link) <- df[[uuid_column]]
    audits_endpoint_link <- na.omit(audits_endpoint_link)
    
    if (length(audits_endpoint_link) > 0) {
        # iterating over each audit endpoint from data
        for (i in 1:length(audits_endpoint_link)) {
            uuid = names(audits_endpoint_link[i])
            endpoint_link_i <- audits_endpoint_link[i]
            cat("Downloading audit file for", uuid, "\n")
            
            # requesting data
            audit_file <- content(GET(endpoint_link_i,
                                      authenticate(usr, pass),
                                      timeout(1000),
                                      progress()), "text", encoding = "UTF-8")
            
            if (!is.na(audit_file)) {
                if (length(audit_file) > 2) {
                    dir.create(paste0(audit_dir, "/", uuid), showWarnings = F)
                    write.csv(audit_file, paste0(audit_dir, "/", uuid, "/audit.csv"), row.names = F)
                }else if(!audit_file == "Attachment not found"){
                    if (grepl("[eventnodestartend]", audit_file)) {
                        dir.create(paste0(audit_dir, "/", uuid), showWarnings = F)
                        write.table(audit_file, paste0(audit_dir, "/", uuid, "/audit.csv"), row.names = F, col.names = FALSE, quote = F)
                    } else{
                        cat("Error: Downloading audit was unsucessful!\n")
                    }
                }
            } else{
                cat("Error: Downloading audit was unsucessful!\n")
            }
        }
    } else{
        cat("Attention: All audit files for given data set is downloaded!")
    }
}



# other specify data extract cleaningtools format -------------------------

cts_format_other_specify <- function(input_tool_data,
                                     input_uuid_col = "_uuid",
                                     input_survey,
                                     input_choices) {
  
  # add and rename some columns
  df_data <- input_tool_data %>%
    mutate("i.check.uuid" := as.character(!!sym(input_uuid_col)))
  
  # get questions with other
  others_colnames <-  df_data %>%
    select(ends_with("_other"), -contains("/")) %>%
    colnames()
  
  # data.frame for holding _other response data
  df_other_response_data <- purrr::map_dfr(.x = others_colnames,
                                           .f = ~{
                                             df_data %>%
                                               select(-contains("/")) %>%
                                               select(i.check.uuid,
                                                      other_text = as.character(.x),
                                                      current_value = str_replace_all(string = .x, pattern = "_other$", replacement = "")) %>%
                                               filter(!is.na(other_text), !other_text %in% c(" ", "NA")) %>%
                                               mutate(other_name = .x,
                                                      int.my_current_val_extract = ifelse(str_detect(current_value, "\\bother\\b"), str_extract_all(string = current_value, pattern = "\\bother\\b|\\w+_other\\b"), current_value),
                                                      value = "",
                                                      parent_qn = str_replace_all(string = .x, pattern = "_other$", replacement = ""),
                                                      across(.cols = !contains("date"), .fns = as.character))
                                           })
  
  # arrange the data
  df_data_arranged <- df_other_response_data %>%
    arrange(i.check.uuid)
  
  # get choices to add to the _other responses extracted
  df_grouped_choices <- input_choices %>%
    group_by(list_name) %>%
    summarise(choice_options = stringr::str_trunc(paste(name, collapse = " : "), 1000)) %>%
    arrange(list_name)
  
  # extract parent question and join survey for extracting list_name
  df_data_parent_qns <- df_data_arranged %>%
    left_join(input_survey %>% select(name, type), by = c("parent_qn"="name")) %>%
    separate(col = type, into = c("select_type", "list_name"), sep =" ", remove = TRUE, extra = "drop" ) %>%
    rename(name = parent_qn)
  
  # join other responses with choice options based on list_name
  df_join_other_response_with_choices <- df_data_parent_qns %>%
    left_join(df_grouped_choices, by = "list_name") %>%
    mutate(issue_id = "other_checks",
           issue = "recode other",
           checked_by = "",
           checked_date = as_date(today()),
           comment = "",
           reviewed = "",
           adjust_log = ""
    ) %>%
    filter(str_detect(string = current_value, pattern = "other\\b|\\w+_other\\b"))
  
  # care for select_one and select_multiple (all are now change_response)
  output <- list()
  
  # select_one checks
  output$select_one <- df_join_other_response_with_choices %>%
    filter(str_detect(select_type, c("select_one|select one"))) %>%
    mutate(i.check.change_type = "change_response") %>%
    slice(rep(1:n(), each = 2)) %>%
    group_by(i.check.uuid, i.check.change_type,  name, current_value) %>%
    mutate(rank = row_number(),
           i.check.question = case_when(rank == 1 ~ other_name,
                                        rank == 2 ~ name),
           i.check.old_value = case_when(rank == 1 ~ as.character(other_text),
                                         rank == 2 ~ as.character(current_value)),
           i.check.new_value = case_when(rank == 1 ~ "NA",
                                         rank == 2 ~ NA_character_)
    ) %>%
    ungroup()
  
  # select_multiple checks
  output$select_mu_data <- df_join_other_response_with_choices %>%
    filter(str_detect(select_type, c("select_multiple|select multiple"))) %>%
    mutate(i.check.change_type = "change_response") %>%
    slice(rep(1:n(), each = 3)) %>%
    group_by(i.check.uuid, i.check.change_type,  name, current_value) %>%
    mutate(rank = row_number(),
           i.check.question = case_when(rank == 1 ~ other_name,
                                        rank == 2 ~ paste0(name, "/", int.my_current_val_extract),
                                        rank == 3 ~ paste0(name, "/")),
           i.check.old_value = case_when(rank == 1 ~ as.character(other_text),
                                         rank == 2 ~ "1",
                                         rank == 3 ~ NA_character_),
           i.check.new_value = case_when(rank == 1 ~ NA_character_,
                                         rank == 2 ~ "0",
                                         rank == 3 ~ NA_character_)
    ) %>%
    ungroup()
  
  # merge other checks
  merged_other_checks <- bind_rows(output) %>%
    mutate(i.check.issue = issue,
           i.check.other_text = other_text,
           i.check.comment = comment,
           i.check.so_sm_choices = choice_options) %>%
    batch_select_rename()
}

# other specify data extract cleaningtools format repeats -------------------------
cts_format_other_specify_repeats <- function(input_repeat_data,
                                             input_uuid_col = "_submission__uuid",
                                             input_survey,
                                             input_choices,
                                             input_sheet_name,
                                             input_index_col = "_index") {
  
  # add and rename some columns
  df_data <- input_repeat_data %>%
    mutate("i.check.uuid" := as.character(!!sym(input_uuid_col)),
           "index" := as.character(!!sym(input_index_col)))
  
  # get questions with other
  others_colnames <-  df_data %>%
    select(ends_with("_other"), -contains("/")) %>%
    colnames()
  
  # data.frame for holding _other response data
  df_other_response_data <- purrr::map_dfr(.x = others_colnames,
                                           .f = ~{
                                             df_data %>%
                                               select(-contains("/")) %>%
                                               select(i.check.uuid,
                                                      other_text = as.character(.x),
                                                      current_value = str_replace_all(string = .x, pattern = "_other$", replacement = ""),
                                                      index) %>%
                                               filter(!is.na(other_text), !other_text %in% c(" ", "NA")) %>%
                                               mutate(other_name = .x,
                                                      int.my_current_val_extract = ifelse(str_detect(current_value, "\\bother\\b"), str_extract_all(string = current_value, pattern = "\\bother\\b|\\w+_other\\b"), current_value),
                                                      value = "",
                                                      parent_qn = str_replace_all(string = .x, pattern = "_other$", replacement = ""),
                                                      across(.cols = !contains("date"), .fns = as.character))
                                           })
  
  
  # arrange the data
  df_data_arranged <- df_other_response_data %>%
    arrange(i.check.uuid, index)
  
  # get choices to add to the _other responses extracted
  df_grouped_choices <- input_choices %>%
    group_by(list_name) %>%
    summarise(choice_options = stringr::str_trunc(paste(name, collapse = " : "), 1000)) %>%
    arrange(list_name)
  
  # extract parent question and join survey for extracting list_name
  df_data_parent_qns <- df_data_arranged %>%
    left_join(input_survey %>% select(name, type), by = c("parent_qn"="name")) %>%
    separate(col = type, into = c("select_type", "list_name"), sep =" ", remove = TRUE, extra = "drop" ) %>%
    rename(name = parent_qn)
  
  # join other responses with choice options based on list_name
  df_join_other_response_with_choices <- df_data_parent_qns %>%
    left_join(df_grouped_choices, by = "list_name") %>%
    mutate(issue = "recode other",
           comment = ""
    ) %>%
    filter(str_detect(string = current_value, pattern = "other\\b|\\w+_other\\b"))
  
  # care for select_one and select_multiple (change_response, add_option, remove_option)
  output <- list()
  # select_one checks
  output$select_one <- df_join_other_response_with_choices %>%
    filter(str_detect(select_type, c("select_one|select one"))) %>%
    mutate(i.check.change_type = "change_response") %>%
    slice(rep(1:n(), each = 2)) %>%
    group_by(i.check.uuid, index, i.check.change_type,  name, current_value) %>%
    mutate(rank = row_number(),
           i.check.question = case_when(rank == 1 ~ other_name,
                                        rank == 2 ~ name),
           i.check.old_value = case_when(rank == 1 ~ as.character(other_text),
                                         rank == 2 ~ as.character(current_value)),
           i.check.new_value = case_when(rank == 1 ~ "NA",
                                         rank == 2 ~ NA_character_)
    ) %>%
    ungroup()
  
  # select_multiple checks
  output$select_mu_data <- df_join_other_response_with_choices %>%
    filter(str_detect(select_type, c("select_multiple|select multiple"))) %>%
    mutate(i.check.change_type = "change_response") %>%
    slice(rep(1:n(), each = 3)) %>%
    group_by(i.check.uuid, index, i.check.change_type,  name, current_value) %>%
    mutate(rank = row_number(),
           i.check.question = case_when(rank == 1 ~ other_name,
                                        rank == 2 ~ paste0(name, "/", int.my_current_val_extract),
                                        rank == 3 ~ paste0(name, "/")),
           i.check.old_value = case_when(rank == 1 ~ as.character(other_text),
                                         rank == 2 ~ "1",
                                         rank == 3 ~ NA_character_),
           i.check.new_value = case_when(rank == 1 ~ NA_character_,
                                         rank == 2 ~ "0",
                                         rank == 3 ~ NA_character_)
    ) %>%
    ungroup()
  
  # merge other checks
  merged_other_checks <- bind_rows(output) %>%
    mutate(i.check.issue = issue,
           i.check.other_text = other_text,
           i.check.comment = comment,
           i.check.so_sm_choices = choice_options,
           i.check.sheet = input_sheet_name,
           i.check.index = index) %>%
    batch_select_rename()
}
