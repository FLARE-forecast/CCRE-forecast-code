library(tidyverse)
library(lubridate)

lake_directory <- here::here()
setwd(lake_directory)
forecast_site <- "ccre"
configure_run_file <- "configure_run.yml"
config_set_name <- "default"

#' Source the R files in the repository
files.sources <- list.files(file.path(lake_directory, "R"), full.names = TRUE)
sapply(files.sources[grepl(".R$", files.sources)], source)

Sys.setenv("AWS_DEFAULT_REGION" = "renc",
           "AWS_S3_ENDPOINT" = "osn.xsede.org",
           "USE_HTTPS" = TRUE)

FLAREr::ignore_sigpipe()

noaa_ready <- TRUE


while(noaa_ready){
  
  message("Beginning generate targets")
  
  #' Generate the `config_obs` object and create directories if necessary
  
  config_obs <- FLAREr::initialize_obs_processing(lake_directory, observation_yml = "observation_processing.yml", config_set_name = config_set_name)
  configure_run_file <- "configure_run.yml"
  config <- FLAREr::set_configuration(configure_run_file,lake_directory, config_set_name = config_set_name)
  
  
  dir.create(file.path(lake_directory, "targets", config$location$site_id), showWarnings = FALSE)
  #' Clone or pull from data repositories
  
  FLAREr::get_git_repo(lake_directory,
                       # directory = config_obs$insitu_obs_fname,
                       directory = config_obs$realtime_temp_location,
                       # directory = config_obs$realtime_insitu_location,
                       git_repo = "https://github.com/FLARE-forecast/CCRE-data.git"
  )
  
  #' Download files from EDI
  
  FLAREr::get_edi_file(#edi_https = "https://pasta.lternet.edu/package/data/eml/edi/1069/1/57267535da5ab0687d2fee52083699f8", #calwalk EDI ##update this url when data is published 
    edi_https = 'https://portal-s.edirepository.org/nis/dataviewer?packageid=edi.719.20&entityid=2ecdcd6114591d6a798ecce9050c13c7',
    file = config_obs$insitu_obs_fname[2],
    lake_directory)
  
  FLAREr::get_edi_file(#edi_https = "https://pasta.lternet.edu/package/data/eml/edi/1069/1/b391093432e38eee7c7cc34ae977d553", #depth offset EDI ##update this url when data is published 
    edi_https = 'https://portal-s.edirepository.org/nis/dataviewer?packageid=edi.719.20&entityid=da210012e686ffbde699fb6e49cb0c9c',
    file = config_obs$insitu_obs_fname[3],
    lake_directory)
  
  
  #' Clean up observed insitu measurements
  cleaned_insitu_file <- in_situ_qaqc(insitu_obs_fname = file.path(lake_directory,"data_raw", config_obs$insitu_obs_fname),
                                      data_location = file.path(lake_directory,"data_raw"),
                                      maintenance_file = file.path(lake_directory, "data_raw", config_obs$maintenance_file),
                                      ctd_fname = NA,
                                      nutrients_fname =  NA,
                                      secchi_fname = NA,
                                      cleaned_insitu_file = file.path(lake_directory,"targets", 
                                                                      config$location$site_id,
                                                                      paste0(config$location$site_id,"-targets-insitu.csv")),
                                      site_id = config$location$site_id,
                                      config = config_obs)
  
  #' Move targets to s3 bucket
  
  message("Successfully generated targets")
  
  # FLAREr::put_targets(site_id = config_obs$lake_name_code,
  #                     cleaned_insitu_file,
  #                     cleaned_met_file = NA,
  #                     cleaned_inflow_file = NA,
  #                     use_s3 = config$run_config$use_s3,
  #                     config = config)
  
  message("Successfully moved targets to s3 bucket")
  
  
  
  # Run FLARE
  output <- FLAREr::run_flare(lake_directory = lake_directory,
                              configure_run_file = configure_run_file,
                              config_set_name = config_set_name)
  
  
  forecast_start_datetime <- lubridate::as_datetime(config$run_config$forecast_start_datetime) + lubridate::days(1)
  start_datetime <- lubridate::as_datetime(config$run_config$forecast_start_datetime) - lubridate::days(1)
  restart_file <- paste0(config$location$site_id,"-", (lubridate::as_date(forecast_start_datetime)- days(1)), "-",config$run_config$sim_name ,".nc")
  
  FLAREr::update_run_config2(lake_directory = lake_directory,
                             configure_run_file = configure_run_file, 
                             restart_file = restart_file, 
                             start_datetime = start_datetime, 
                             end_datetime = NA, 
                             forecast_start_datetime = forecast_start_datetime,  
                             forecast_horizon = config$run_config$forecast_horizon,
                             sim_name = config$run_config$sim_name, 
                             site_id = config$location$site_id,
                             configure_flare = config$run_config$configure_flare, 
                             configure_obs = config$run_config$configure_obs, 
                             use_s3 = config$run_config$use_s3,
                             bucket = config$s3$warm_start$bucket,
                             endpoint = config$s3$warm_start$endpoint,
                             use_https = TRUE)
  
  RCurl::url.exists("https://hc-ping.com/551392ce-43f3-49b1-8a57-6a60bad1c377", timeout = 5)
  
  noaa_ready <- FLAREr::check_noaa_present_arrow(lake_directory,
                                                 configure_run_file,
                                                 config_set_name = config_set_name)
}