library(tidyverse)
library(lubridate)

#remotes::install_github('flare-forecast/FLAREr@single-parameter')
#remotes::install_github("cboettig/aws.s3")

Sys.setenv('GLM_PATH'='GLM3r')

lake_directory <- here::here()
setwd(lake_directory)
forecast_site <- "ccre"
configure_run_file <- "configure_run.yml"
config_set_name <- "glm_flare_v3"

#' Source the R files in the repository
walk(list.files(file.path(lake_directory, "R"), full.names = TRUE), source)

Sys.setenv("AWS_DEFAULT_REGION" = "renc",
           "AWS_S3_ENDPOINT" = "osn.xsede.org",
           "USE_HTTPS" = TRUE)

config_obs <- yaml::read_yaml(file.path(lake_directory,'configuration',config_set_name,'observation_processing.yml'))
configure_run_file <- "configure_run.yml"
config <- FLAREr:::set_up_simulation(configure_run_file,lake_directory, config_set_name = config_set_name)

#FLAREr:::ignore_sigpipe()

noaa_ready <- TRUE

message("Beginning generate targets")
source('./workflows/glm_flare_v3/generate_targets.R')

while(noaa_ready){
  
  config <- FLAREr:::set_up_simulation(configure_run_file,lake_directory, config_set_name = config_set_name)
  
  # Run FLARE
  message('Running forecast...')
  ## could this function return the forecast_df object? Could feed directly into scoring from here
  output <- FLAREr:::run_flare(lake_directory = lake_directory,
                              configure_run_file = configure_run_file,
                              config_set_name = config_set_name)
  
  
  
  message("Scoring forecasts")
  forecast_s3 <- arrow::s3_bucket(bucket = config$s3$forecasts_parquet$bucket, endpoint_override = config$s3$forecasts_parquet$endpoint, anonymous = TRUE)
  forecast_df <- arrow::open_dataset(forecast_s3) |>
    dplyr::mutate(reference_date = lubridate::as_date(reference_date)) |>
    dplyr::filter(model_id == 'glm_flare_v3',
                  site_id == forecast_site,
                  reference_date == lubridate::as_datetime(config$run_config$forecast_start_datetime)) |>
    dplyr::collect()
  
 #if(config$run_config$use_s3){
    #past_days <- lubridate::as_date(forecast_df$reference_datetime[1]) - lubridate::days(config$run_config$forecast_horizon)
    past_days <- lubridate::as_date(lubridate::as_date(config$run_config$forecast_start_datetime) - lubridate::days(config$run_config$forecast_horizon))
    
    #vars <- arrow_env_vars()
    past_s3 <- arrow::s3_bucket(bucket = config$s3$forecasts_parquet$bucket, endpoint_override = config$s3$forecasts_parquet$endpoint, anonymous = TRUE)
    past_forecasts <- arrow::open_dataset(past_s3) |>
      dplyr::mutate(reference_date = lubridate::as_date(reference_date)) |>
      dplyr::filter(model_id == 'glm_flare_v3',
                    site_id == forecast_site,
                    reference_date == past_days) |>
      dplyr::collect()
    #unset_arrow_vars(vars)
 # }else{
 #   past_forecasts <- NULL
 # }
  
  combined_forecasts <- dplyr::bind_rows(forecast_df, past_forecasts)
  
  targets_df <- read_csv(file.path(config$file_path$qaqc_data_directory,paste0(config$location$site_id, "-targets-insitu.csv")),show_col_types = FALSE)
  
  #combined_forecasts <- arrow::open_dataset('./forecasts/parquet/site_id=ccre/model_id=glm_flare_v3/reference_date=2024-09-03/part-0.parquet') |> collect()
  
  
  message('running scoring function')
  scoring <- generate_forecast_score_arrow(targets_df = targets_df,
                                           forecast_df = combined_forecasts, ## only works if dataframe returned from output
                                           use_s3 = TRUE,
                                           bucket = config$s3$scores$bucket,
                                           endpoint = config$s3$scores$endpoint,
                                           local_directory = './CCRE-forecast-code/scores/ccre',
                                           variable_types = c("state","parameter"))
  
  
  
  forecast_start_datetime <- lubridate::as_datetime(config$run_config$forecast_start_datetime) + lubridate::days(1)
  start_datetime <- lubridate::as_datetime(config$run_config$forecast_start_datetime) - lubridate::days(1)
  restart_file <- paste0(config$location$site_id,"-", (lubridate::as_date(forecast_start_datetime) - lubridate::days(1)), "-",config$run_config$sim_name ,".nc")


  message('updating run configuration')
  FLAREr:::update_run_config(lake_directory = lake_directory,
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
                             bucket = config$s3$restart$bucket,
                             endpoint = config$s3$restart$endpoint,
                             use_https = TRUE)
  
  RCurl::url.exists("https://hc-ping.com/551392ce-43f3-49b1-8a57-6a60bad1c377", timeout = 5)
  
  noaa_ready <- FLAREr:::check_noaa_present(lake_directory,
                                                 configure_run_file,
                                                 config_set_name = config_set_name)
}
