library(tidyverse)
library(lubridate)

dir.create(file.path(lake_directory, "targets", config$location$site_id), showWarnings = FALSE)
#' Clone or pull from data repositories

FLAREr:::get_git_repo(lake_directory,
                     # directory = config_obs$insitu_obs_fname,
                      directory = config_obs$realtime_temp_location,
                     # directory = config_obs$realtime_insitu_location,
                     git_repo = "https://github.com/FLARE-forecast/CCRE-data.git"
                     )

#' Download files from EDI

get_edi_file(#edi_https = "https://pasta.lternet.edu/package/data/eml/edi/1069/1/57267535da5ab0687d2fee52083699f8", #calwalk EDI ##update this url when data is published 
                     #edi_https = 'https://portal-s.edirepository.org/nis/dataviewer?packageid=edi.719.20&entityid=2ecdcd6114591d6a798ecce9050c13c7',
  edi_https = 'https://pasta.lternet.edu/package/data/eml/edi/1069/3/4afb209b30ebed898334badd3819d854',
  file = config_obs$insitu_obs_fname[2],
                     lake_directory)

get_edi_file(#edi_https = "https://pasta.lternet.edu/package/data/eml/edi/1069/1/b391093432e38eee7c7cc34ae977d553", #depth offset EDI ##update this url when data is published 
                     #edi_https = 'https://portal-s.edirepository.org/nis/dataviewer?packageid=edi.719.20&entityid=da210012e686ffbde699fb6e49cb0c9c',
                     edi_https = 'https://pasta.lternet.edu/package/data/eml/edi/1069/3/be32110f0fcd4a1704d351fead0224d4',
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

FLAREr:::put_targets(site_id = config_obs$site_id,
                    cleaned_insitu_file,
                    cleaned_met_file = NA,
                    cleaned_inflow_file = NA,
                    use_s3 = config$run_config$use_s3,
                    config = config)

message("Successfully moved targets to s3 bucket")
