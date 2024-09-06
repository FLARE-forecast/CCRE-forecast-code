
lake_directory <- here::here()
config_set_name <- "reforecast"

configure_run_file <- "configure_run.yml"
config <- FLAREr::set_configuration(configure_run_file,lake_directory, config_set_name = config_set_name)

config_obs <- FLAREr::initialize_obs_processing(lake_directory, 
                                                observation_yml = "observation_processing.yml", 
                                                config_set_name = config_set_name)

inflow_targets <- read_csv(file.path(config_obs$file_path$targets_directory, config_obs$site_id, 
                                     paste0(config_obs$site_id,"-targets-inflow.csv")), show_col_types = FALSE)





minioclient::mc_alias_set("s3_store",
                          config$s3$drivers$endpoint,
                          Sys.getenv("OSN_KEY"),
                          Sys.getenv("OSN_SECRET"))

mc_ls(paste0("s3_store/","bio230121-bucket01/ccre-reforecast/forecasts/parquet/site_id=ccre/model_id=glm_flare_reforecast/reference_date=2022-01-01"))




# ccre_reforecast <- arrow::s3_bucket(file.path("bio230121-bucket01/ccre-reforecast/forecasts/parquet/site_id=ccre/model_id=glm_flare_reforecast/reference_date=2022-01-01"),
#                                      endpoint_override = 'renc.osn.xsede.org',
#                                      anonymous = TRUE)

library(arrow)
library(tidyverse)

ccre_reforecast <- arrow::s3_bucket(file.path("bio230121-bucket01/ccre-reforecast/scores/parquet/"),
                                    endpoint_override = 'renc.osn.xsede.org',
                                    anonymous = TRUE)

ccre_df <- arrow::open_dataset(ccre_reforecast) |> 
  filter(site_id == 'ccre', 
         model_id == 'glm_flare_reforecast',
         variable == 'temperature',
         depth == 0.5) |> 
  collect()
