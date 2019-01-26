# R/postgres extraction code for Courtney Harding
# TAY 11 Dec 2018
library(CamdenCoalitionDataWarehouse)
library(RPostgres)
library(data.table)

## Crosswalk/category definitions -------------------------------------------------




##################################### Fields/components:
# demographics, first_visit, first_arrest, last_visit, last_arrest
# arrest_encounters total_ed	six_mo_bin	cooper	virtua	lourdes	medicaid	commercial	charity_self	medicare	unclassified_payor	dual	other_payor
# (CCS codes, wide)
# (SA & MH, now needs to be <misc>, <anxiety>, <depression uni- or bi-polar>)
# (statute violations, wide)


########################
# MH_sum, MH_dx
# SA_sum, SA_dx
# *_sum = sum of the number of diagnoses for this person within this timebin
# *.dx = number of unique diagnosis codes (within the CCS category) for this person in this timebin



## load groupers and crosswalks -----------------------------------------------------------------
load_groupers_and_crosswalks = function(){
  ccs_file = "/home/airflow/valid_cciccs_9only.csv"
  ccs_grouper <<- read.csv(ccs_file)
  statutes_grouper_file = "/home/airflow/Statutes_v1_ch.csv"
  statutes_grouper <<- read.csv(statutes_grouper_file)
}


## generate full, wide analysis table --------------------------------------------------------
generate_full_table = function(){
  load_groupers_and_crosswalks()
  raw_ccpd_keys = get_all_ccpd_keys()
  raw_claims_keys = get_all_claims_keys()
  # clean keys
  message("getting keys..")
  ccpd_keys = filter_ccpd_data(raw_ccpd_keys)
  claims_keys = filter_claims_data(raw_claims_keys)
  remove(raw_ccpd_keys)
  remove(raw_claims_keys)
  gc()
  # generate demographics table
  message("creating demographics table..")
  person_table = create_demographics_table(ccpd_keys, claims_keys)
  # generate wide tables (ccs, statutes, hospital, csh)
  message("generating statutes_wide..")
  statutes_wide = generate_statutes_wide_table(ccpd_keys, person_table, statutes_grouper)
  gc()
  message("generating ccs_wide..")
  ccs_wide = generate_ccs_wide_table(claims_keys, person_table, ccs_grouper)
  message("generating hospital_wide..")
  hospital_wide = generate_hospital_wide_table(claims_keys, person_table)
  message("generating csh_wide..")
  csh_wide = generate_CSH_wide_table(claims_keys, person_table, ccs_grouper)
  #
  message("moving on..")
  # add unique row keys
  statutes_wide$row_key = paste0(statutes_wide$person_key, "_",  statutes_wide$timebin)
  ccs_wide$row_key = paste0(ccs_wide$person_key, "_",  ccs_wide$timebin)
  hospital_wide$row_key = paste0(hospital_wide$person_key, "_",  hospital_wide$timebin)
  csh_wide$row_key = paste0(csh_wide$person_key, "_",  csh_wide$timebin)
  # get rid of columns we don't need or can't include
  person_table$dob = NULL
  person_table <<- person_table
  # now, join it all together
  all_row_keys = unique(c(statutes_wide$row_key, hospital_wide$row_key, csh_wide$row_key))
  rows_table = data.table(row_key = all_row_keys)
  rows_table$person_key = str_sub(rows_table$row_key, 1, 36)
  rows_demographics = left_join(rows_table, person_table, by = 'person_key')
  rows_demographics_hospital = left_join(rows_demographics, hospital_wide, by = 'row_key')
  rows_demographics_hospital_csh = left_join(rows_demographics_hospital, csh_wide, by = 'row_key')
  final_table = left_join(rows_demographics_hospital_csh, statutes_wide, by = 'row_key')
  #
  final_table$coal_timebin = dplyr::coalesce(final_table$timebin, final_table$timebin.x, final_table$timebin.y)
  #
  # and finally, clean up the duplicated fields that we picked up from the joins
  # final_table$timebin = final_table$timebin.x
  final_table$person_key = final_table$person_key.x
  final_table$hosp_cooper = final_table$c
  final_table$hosp_lourdes = final_table$l
  final_table$hosp_virtua = final_table$v
  final_table$n_ed_visits_perbin = final_table$hosp_cooper + final_table$hosp_lourdes + final_table$hosp_virtua
  #
  final_table$c = NULL
  final_table$v = NULL
  final_table$l = NULL
  final_table$timebin = NULL
  final_table$timebin.x = NULL
  final_table$timebin.y = NULL
  final_table$person_key.x = NULL
  final_table$person_key.y = NULL
  final_table$person_key.x.x = NULL
  final_table$person_key.y.y = NULL
  #
  final_table$NA.y = NULL
  final_table$NA.x = NULL
  #
  final_table$timebin = final_table$coal_timebin
  final_table$coal_timebin = NULL
  #
  return(final_table)
}





# visit_id, facility, admit_datetime, discharge_datetime, visit_type,
# sex, dob, race, ethnicity, cchp_financial_class, primary_payor_class, mco, payor_count,
# secondary_payor_classes, dual, payor_flags, admit_dx_code_array, dx_code_array,
# primary_dx_count, dx_poa_array, code_version, ccs_chronic_codes, primary_ccs_category_code, all_ccs_category_codes

## get all claims keys -----------------------------------------
get_all_claims_keys = function(){
  db_conn = get_db_conn()
  on.exit(dbDisconnect(db_conn))
  #
  sql = paste0("SELECT * from raw_data.harm_claims_view WHERE visit_type = 'ed';")
  long_claims_keys = RPostgres::dbGetQuery(db_conn, sql)
  #
  # now we deduplicate on row_key...
  claims_keys = long_claims_keys[!duplicated(long_claims_keys[,c('row_key')]),]
  #
  return(claims_keys)
}


## get all ccpd keys -----------------------------------------
get_all_ccpd_keys = function(){
  db_conn = get_db_conn()
  on.exit(dbDisconnect(db_conn))
  #
  sql = paste0("SELECT c.source_tag, b.person_key, c.row_key, c.booking_number, c.case_number, c.arrest_date, c.ucr_code, c.statute_description, c.sex, c.race, c.dob ",
               "FROM identifiers.identifiers a, linkage_testing.person_table b, raw_data.arrests_xls_withdoubles_view c WHERE ",
               "a.source = 'ccpd' AND a.institutional = '' AND a.ident_key = b.ident_key AND b.person_key = c.person_key;")
  long_ccpd_keys = RPostgres::dbGetQuery(db_conn, sql)
  #
  # now we deduplicate on row_key...
  ccpd_keys = long_ccpd_keys[!duplicated(long_ccpd_keys[,c('row_key')]),]
  #
  return(ccpd_keys)
}

# cooper, virtua, lourdes, medicaid, commercial, charity_self, medicare, unclassified_payor, dual, other_payor
# ccs001, ccs002... ccs670, MH_sum, MH_dx, SA_sum, SA_dx
# arson,	child,	cjust,	disord,	drug,	licen,	mrdmh,	other,	prop,	pubord,	sex_ofnc,	traffic,	viol,	viol_c,	viol_s,	viol_w,	weap


## filter claims data -------------------------------------------------------------
filter_claims_data = function(raw_claims_keys){
  #
  ### Before anything else, we want to filter the claims data as follows:
  ### 1. Remove any visits not within 2010-01-01 to 2014-12-31.
  ### 2. Select only ED visits (exclude inpatient, outpatient, observation).
  ### 3. Select only ED visits to Cooper/Virtua/Lourdes.
  raw_claims_keys$year = str_sub(raw_claims_keys$admit_datetime, 1, 4)
  raw_claims_keys$institution = str_sub(raw_claims_keys$visit_id, 1, 1)
  claims_keys = dplyr::filter(raw_claims_keys,
                         year %in% c('2010', '2011', '2012', '2013', '2014'),
                         institution %in% c('c', 'v', 'l'),
                         visit_type == 'ed')
  #
  claims_keys$date = str_sub(claims_keys$admit_datetime, 1, 10)
  #
  claims_keys$age_months_at_event = ( 12 * (as.numeric(str_sub(claims_keys$date, 1, 4)) - as.numeric(str_sub(claims_keys$dob, 1, 4)))
                                 + (as.numeric(str_sub(claims_keys$date, 6, 7)) - as.numeric(str_sub(claims_keys$dob, 6, 7))))
  #
  #### 12 months * 18 years = 216 months, so select all events where the arrestee was greater than 215 months.
  #
  claims_keys = dplyr::filter(claims_keys, age_months_at_event > 215)
  #
  #### add noise to ages
  offsets = c(-3, -2, -1, 0, 1, 2, 3)
  claims_keys$age_noise = sample(offsets, size = nrow(claims_keys), replace = TRUE)
  claims_keys$age_months_at_event = claims_keys$age_noise + claims_keys$age_months_at_event
  #
  #
  #### now merge together admit_dx_code_array, dx_code_array, and dx_poa_array so we have all dx_codes in a single field.
  claims_keys$all_dx_codes = paste0(claims_keys$admit_dx_code_array, ",", claims_keys$dx_code_array, ",", claims_keys$dx_poa_array)
  #
  #### finally, we're only concerned with the first 3 dx_codes, so separate those out from the rest.
  claims_keys$first3_dx_codes = lapply(claims_keys$all_dx_codes, function(x) first_three_dx(x))
  return(claims_keys)
}


## return first three unique dx_codes -------------------------------------------------
first_three_dx = function(dx_codes){
  # here dx_codes is a single string containing multiple comma-separated dx_codes.
  dx_split = unlist(str_split(dx_codes, pattern = ",", n = 100))
  #
  unique_dx_split = unique(dx_split)
  unique_dx_split = unique_dx_split[nchar(unique_dx_split) > 0]
  #
  first_three = paste0(unique_dx_split[1:3], collapse = ",")
  #
  return(first_three)
}


## filter ccpd data --------------------------------------------------
filter_ccpd_data = function(raw_ccpd_keys){
  #
  #### What are we filtering from CCPD?
  #### -anyone under 18 at time of arrest
  raw_ccpd_keys$age_months_at_event = ( 12 * (as.numeric(str_sub(raw_ccpd_keys$arrest_date, 1, 4)) - as.numeric(str_sub(raw_ccpd_keys$dob, 1, 4)))
                                     + (as.numeric(str_sub(raw_ccpd_keys$arrest_date, 6, 7)) - as.numeric(str_sub(raw_ccpd_keys$dob, 6, 7))))
  #
  #### 12 months * 18 years = 216 months, so select all events where the arrestee was greater than 215 months.
  #
  ccpd_keys = dplyr::filter(raw_ccpd_keys, age_months_at_event > 215)
  #
  #### add noise to ages
  offsets = c(-3, -2, -1, 0, 1, 2, 3)
  ccpd_keys$age_noise = sample(offsets, size = nrow(ccpd_keys), replace = TRUE)
  ccpd_keys$age_months_at_event = ccpd_keys$age_noise + ccpd_keys$age_months_at_event
  #
  return(ccpd_keys)
}







## create demographics table ----------------------------------
create_demographics_table = function(ccpd_keys, claims_keys){
  #
  #
  claims = claims_keys
  claims$date = str_sub(claims$admit_datetime, 1, 10)
  #
  ccpd_keys$year = str_sub(ccpd_keys$arrest_date, 1, 4)
  ccpd_keys$date = ccpd_keys$arrest_date
  ccpd_keys$institution = 'ccpd'
  ccpd = dplyr::filter(ccpd_keys, year %in% c('2010', '2011', '2012', '2013', '2014'))
  print(paste0("ccpd_keys rows:", nrow(ccpd_keys), "   in 2010-2014:", nrow(ccpd)))
  #
  #
  ccpd_common = dplyr::select(ccpd, person_key, institution, row_key, date, dob, sex, race)
  claims_common = dplyr::select(claims, person_key, institution, row_key, date, dob, sex, race, ethnicity)
  claims_common = race_ethn_xwalk(claims_common)
  claims_common$ethnicity = NULL
  claims_common$paste_hosp = NULL
  common = rbind(ccpd_common, claims_common)
  #
  #
  ### age (in months) as of 2010-01-01
  common$age_months_jan2010  = ( 12 * (2010 - as.numeric(str_sub(common$dob, 1, 4))) + as.numeric(str_sub(common$dob, 6, 7)))
  #
  ### total appearances
  data.table::setDT(common)[, overall_person_freq :=.N, by=.(person_key)]
  #
  ### total arrests
  data.table::setDT(ccpd_common)[, ccpd_person_freq :=.N, by=.(person_key)]
  #
  ### total hospital visits
  data.table::setDT(claims_common)[, claims_person_freq :=.N, by=.(person_key)]
  #
  #
  #
  ### first and last visit dates (from claims)
  claims_person = claims_common %>%
    group_by(person_key) %>%
    summarize(first_claims_date = min(date), last_claims_date = max(date), race = dplyr::first(race), claims_person_freq = dplyr::first(claims_person_freq))
  #
  ### first and last arrest dates (from arrests)
  ccpd_person = ccpd_common %>%
    group_by(person_key) %>%
    summarize(first_arrest_date = min(date), last_arrest_date = max(date), sex = dplyr::first(sex), ccpd_person_freq = dplyr::first(ccpd_person_freq))
  #
  #
  ########################### person_key demographics
  #
  ### race, ethnicity, sex (truncate 'Male', 'Female' to 'M', 'F')
  claims_demographics = dplyr::select(claims, person_key, sex, race, ethnicity)
  claims_demographics = claims_demographics[!duplicated(claims_demographics[,c('person_key')]),]
  claims_demographics$sex = str_sub(claims_demographics$sex, 1, 1)
  #
  ########################## and now to join it all back together
  person_table = common[!duplicated(common[,c('person_key')]),]
  person_table$institution = NULL
  person_table$date = NULL
  person_table$age_months_at_event = NULL
  person_table$row_key = NULL
  #
  cl_join = dplyr::left_join(person_table, claims_person, by = 'person_key')
  pd_join = dplyr::left_join(person_table, ccpd_person, by = 'person_key')
  demog_join = dplyr::left_join(person_table, claims_demographics, by = 'person_key')
  #
  #
  #
  person_table$sex = pd_join$sex.x
  person_table$race = pd_join$race
  # person_table$ethnicity = demog_join$ethnicity
  #
  person_table$claims_person_freq = cl_join$claims_person_freq
  person_table$ccpd_person_freq = pd_join$ccpd_person_freq
  #
  person_table$first_arrest_date = pd_join$first_arrest_date
  person_table$last_arrest_date = pd_join$last_arrest_date
  #
  person_table$first_claims_date = cl_join$first_claims_date
  person_table$last_claims_date = cl_join$last_claims_date
  #
  #
  return(person_table)
}


## generate ccs groupings wide table -------------------------------------------------
generate_ccs_wide_table = function(claims_keys, person_table, ccs_grouper){
  claims_keys$ymd_date = str_sub(claims_keys$admit_datetime, 1, 10)
  #
  claims_keys$timebin = get_date_code_from_ymd(claims_keys$ymd_date)
  #
  message("       creating claims_narrow..")
  claims_narrow = dplyr::select(claims_keys, person_key, visit_id, timebin, first3_dx_codes)
  names(claims_narrow) = c('person_key', 'visit_id', 'timebin', 'dx_code_array')
  gc()
  #
  message("       creating claims_long..")
  claims_long = claims_narrow %>%
    mutate(unravel = str_split(dx_code_array, ",")) %>%
    unnest %>%
    mutate(dx_code_array = str_trim(unravel))
  #
  remove(claims_narrow)
  gc()
  #### We need to remove the 'z_' that was placed in front of each icd9 code earlier.
  ccs_grouper$icd_code = str_sub(ccs_grouper$icd_code, 3)
  #
  message("       matching dx_codes to ccs categories..")
  claims_long$ccs = ccs_grouper$ccs_category[match(unlist(claims_long$dx_code_array), ccs_grouper$icd_code)]
  #
  gc()
  #### rather than just a number, let's make the CCS categories (which will become column names) a bit easier to work with.
  claims_long$ccs = paste0("ccs", str_pad(claims_long$ccs, 3, pad = "0"))
  #
  #### the dcast function here turns our long table (one row per diagnosis code) into a wide table
  #### with one row per person and a separate column for ccs dx_code group
  message("       dcast to claims_ccs_wide..")
  claims_ccs_wide = dcast(as.data.table(claims_long), person_key + timebin ~ ccs)
  #
  gc()
  #
  return(claims_ccs_wide)
}


## generate financial class wide table  -------------------------------------------------
generate_finclass_wide_table = function(claims_keys, person_table){
  claims_keys$ymd_date = str_sub(claims_keys$admit_datetime, 1, 10)
  #
  claims_keys$timebin = get_date_code_from_ymd(claims_keys$ymd_date)
  #
  claims_narrow = dplyr::select(claims_keys, person_key, visit_id, timebin, cchp_financial_class)
  #
  #
  #### the dcast function here turns our long table (one row per diagnosis code) into a wide table
  #### with one row per person and a separate column for ccs dx_code group
  claims_payor_wide = dcast(claims_narrow, person_key + timebin ~ cchp_financial_class)
  #
  #
  return(claims_payor_wide)
  #
  #
}



## generate payor wide table -------------------------------------------------
generate_payor_wide_table = function(claims_keys, person_table){
  claims_keys$ymd_date = str_sub(claims_keys$admit_datetime, 1, 10)
  #
  claims_keys$timebin = get_date_code_from_ymd(claims_keys$ymd_date)
  #
  claims_narrow = dplyr::select(claims_keys, person_key, visit_id, timebin, primary_payor_class)
  #
  #
  #### the dcast function here turns our long table (one row per diagnosis code) into a wide table
  #### with one row per person and a separate column for ccs dx_code group
  claims_payor_wide = dcast(claims_narrow, person_key + timebin ~ cchp_financial_class)
  #
  #
  return(claims_payor_wide)
  #
  #
}

## generate hospital wide table -------------------------------------------------
generate_hospital_wide_table = function(claims_keys, person_table){
  claims_keys$ymd_date = str_sub(claims_keys$admit_datetime, 1, 10)
  #
  claims_keys$timebin = get_date_code_from_ymd(claims_keys$ymd_date)
  #
  claims_narrow = dplyr::select(claims_keys, person_key, visit_id, timebin, institution)
  #
  #
  #### the dcast function here turns our long table (one row per diagnosis code) into a wide table
  #### with one row per person and a separate column for ccs dx_code group
  claims_hosp_wide = dcast(claims_narrow, person_key + timebin ~ institution)
  #
  #
  return(claims_hosp_wide)
  #
  #
}




## generate grouped statutes wide table ------------------------------------
generate_statutes_wide_table = function(ccpd_keys, person_table, statutes_grouper){
  #
  ccpd_keys$timebin = get_date_code_from_ymd(ccpd_keys$arrest_date)
  #
  #### the CCPD data already has one row per statute violation, so no need to unravel it first.
  #### using the dawn_dirty_name column as a key, there are only 3 rows that fail to get placed into a group.
  ccpd_keys$statute_group = statutes_grouper$CH_code_USE.THIS[match(unlist(ccpd_keys$statute_description), statutes_grouper$dawn_dirty_name)]
  #
  #### the dcast function here turns our long table (one row per violation) into a wide table
  #### with one row per person and a separate column for each statute violation group
  ccpd_wide = dcast(ccpd_keys, person_key + timebin ~ statute_group)
  ccpd_wide$person_key_timebin = paste0(ccpd_wide$person_key, "_", ccpd_wide$timebin)
  #
  #### Now, we need to add a count of the number of arrests within each person_key & timebin.
  #### Oftentimes arrestees are charged with multiple statute violations in a single arrest event.
  #### Assuming that nobody has more than one arrest event per day, we can just take all unique combinations of
  #### arrest_date and timebin, and obtain our counts from there.
  ccpd_keys$arrest_date_bin = paste0(ccpd_keys$arrest_date, "_", ccpd_keys$timebin)
  ccpd_keys_per_bin = ccpd_keys[!duplicated(ccpd_keys[,c('arrest_date_bin', 'person_key')]),]
  #
  #### next count the number of times each person_key & timebin combination occurs
  ccpd_keys_per_bin$person_key_timebin = paste0(ccpd_keys_per_bin$person_key, "_", ccpd_keys_per_bin$timebin)
  data.table::setDT(ccpd_keys_per_bin)[, person_key_timebin_freq :=.N, by=.(person_key_timebin)]
  #
  #### take only what we need..
  ccpd_keys_n_per_bin = dplyr::select(ccpd_keys_per_bin, person_key_timebin, person_key_timebin_freq)
  names(ccpd_keys_n_per_bin) = c('person_key_timebin', 'n_arrests_in_timebin')
  #### finally, join it all back together.
  final_ccpd_wide = left_join(ccpd_wide, ccpd_keys_n_per_bin, by = 'person_key_timebin')
  #
  return(final_ccpd_wide)
}



## get date code from YMD date -------------------------------------------
get_date_code_from_ymd = function(ymd_date){
  year = as.numeric(str_sub(ymd_date, 1, 4))
  month = as.numeric(str_sub(ymd_date, 6, 7))
  #
  code = ((year - 2009) * 2) - 1
  idx = which(month > 6)
  code[idx] = code[idx] + 1
  return(code)
}



## generate CSH wide table ------------------------------------
generate_CSH_wide_table = function(claims_keys, person_table, csh_grouper){
  #
  claims_keys$ymd_date = str_sub(claims_keys$admit_datetime, 1, 10)
  #
  claims_keys$timebin = get_date_code_from_ymd(claims_keys$ymd_date)
  #
  claims_narrow = dplyr::select(claims_keys, person_key, visit_id, timebin, first3_dx_codes)
  names(claims_narrow) = c('person_key', 'visit_id', 'timebin', 'dx_code_array')
  #
  claims_long = claims_narrow %>%
    mutate(unravel = str_split(dx_code_array, ",")) %>%
    unnest %>%
    mutate(dx_code_array = str_trim(unravel))
  #
  #### We need to remove the 'z_' that was placed in front of each icd9 code earlier.
  csh_grouper$icd_code = str_sub(csh_grouper$icd_code, 3)
  #
  claims_long$csh = csh_grouper$CH_label_USE.THIS[match(unlist(claims_long$dx_code_array), csh_grouper$icd_code)]
  #
  #### rather than just a number, let's make the CCS categories (which will become column names) a bit easier to work with.
  # claims_long$csh = paste0("csh", str_pad(claims_long$csh, 3, pad = "0"))
  #
  #### the dcast function here turns our long table (one row per diagnosis code) into a wide table
  #### with one row per person and a separate column for ccs dx_code group
  claims_csh_wide = dcast(claims_long, person_key + timebin ~ csh)
  #
  return(claims_csh_wide)
}



## race & ethnicity crosswalk -------------------------------------
race_ethn_xwalk = function(input_table){
  xwalk = read.csv("~/ccpd_hosp_race_xwalk.csv")
  #
  xwalk$paste_hosp = paste0(xwalk$hosp_race, xwalk$hosp_ethn)
  input_table$paste_hosp = paste0(input_table$race, input_table$ethnicity)
  #
  input_table$race = xwalk$ccpd_race[match(unlist(input_table$paste_hosp),xwalk$paste_hosp)]
  return(input_table)
}











































