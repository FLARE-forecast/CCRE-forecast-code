temp_oxy_chla_qaqc <- function(realtime_file,
                               qaqc_file,
                               maintenance_file,
                               offset_file,
                               config){

    
    #ccrdata=data_file
    #change column names
    CCRDATA_COL_NAMES = c("DateTime", "RECORD", "CR3000_Batt_V", "CR3000Panel_Temp_C", 
                           "ThermistorTemp_C_1", "ThermistorTemp_C_2", "ThermistorTemp_C_3", "ThermistorTemp_C_4",
                           "ThermistorTemp_C_5", "ThermistorTemp_C_6", "ThermistorTemp_C_7", "ThermistorTemp_C_8",
                           "ThermistorTemp_C_9","ThermistorTemp_C_10","ThermistorTemp_C_11", "ThermistorTemp_C_12",
                           "ThermistorTemp_C_13","EXO_Date_1", "EXO_Time_1", "EXOTemp_C_1", "EXOCond_uScm_1",
                           "EXOSpCond_uScm_1", "EXOTDS_mgL_1", "EXODOsat_percent_1", "EXODO_mgL_1", "EXOChla_RFU_1",
                           "EXOChla_ugL_1", "EXOBGAPC_RFU_1", "EXOBGAPC_ugL_1", "EXOfDOM_RFU_1", "EXOfDOM_QSU_1",
                           "EXO_pressure_psi_1", "EXO_depth_m_1", "EXO_battery_V_1", "EXO_cablepower_V_1", "EXO_wiper_V_1",
                           "EXO_Date_9", "EXO_Time_9", "EXOTemp_C_9", "EXOCond_uScm_9",
                           "EXOSpCond_uScm_9", "EXOTDS_mgL_9", "EXODOsat_percent_9", "EXODO_mgL_9", 
                           "EXOfDOM_RFU_9", "EXOfDOM_QSU_9","EXO_pressure_psi_9", "EXO_depth_m_9", "EXO_battery_V_9",
                           "EXO_cablepower_V_9", "EXO_wiper_V_9","Lvl_psi_13", "LvlTemp_C_13")
    
    
    #Fouling factor for Chla on EXO
    EXO_FOULING_FACTOR <- 4
    
    #Adjustment period of time to stabilization after cleaning in seconds
    ADJ_PERIOD = 2*60*60 
    
    # read ccr catwalk data and maintenance log
    # NOTE: date-times throughout this script are processed as UTC
    ccrdata <- readr::read_csv(realtime_file, skip=1, col_names = CCRDATA_COL_NAMES,
                         col_types = cols(.default = col_double(), DateTime = col_datetime()))
    
    #drop rows when DateTime = NA and drop dups
    ccrdata <- ccrdata[!is.na(ccrdata$DateTime),] %>%
      dplyr::distinct(DateTime, .keep_all= TRUE) #taking out the duplicate values 
    
  ####################################################################################################################################### 
  #read in maintenance log
  log <- readr::read_csv(maintenance_file,
                    col_types = cols(
                      .default = col_character(),
                      TIMESTAMP_start = col_datetime("%Y-%m-%d %H:%M:%S%*"),
                      TIMESTAMP_end = col_datetime("%Y-%m-%d %H:%M:%S%*"),
                      flag = col_integer()))
    
    #Filter out the 7 flag because it is already NAs in the dataset and not maintenance
    log= log%>% dplyr::filter(flag!=7)
    
    #should remove 18.19, (R,S)  AK and AL 37 and 38, these are the EXO date and time columns 
    for(j in c(5:17, 20:36, 39:53)) { #for loop to create new columns in data frame; Updated from BVR which was c(5:23,26:46)
      ccrdata[,paste0("Flag_",colnames(ccrdata[j]))] <- 0 #creates flag column + name of variable
      ccrdata[c(which(is.na(ccrdata[,j]))),paste0("Flag_",colnames(ccrdata[j]))] <-7 #puts in flag 7 if value not collected
    }
    
    # modify ccrdata based on the information in the log
    for(i in 1:nrow(log)){
      # get start and end time of one maintenance event
      start <- log$TIMESTAMP_start[i]
      end <- log$TIMESTAMP_end[i]
      
      
      # get indices of columns affected by maintenance
      if(grepl("^\\d+$", log$colnumber[i])) # single num
      {
        maintenance_cols <- intersect(c(2:53), as.integer(log$colnumber[i])) #was 2:46 for BVR 
      }
      
      else if(grepl("^c\\(\\s*\\d+\\s*(;\\s*\\d+\\s*)*\\)$", log$colnumber[i])) # c(x;y;...)
      {
        maintenance_cols <- dplyr::intersect(c(2:53), as.integer(unlist(regmatches(log$colnumber[i],
                                                                            gregexpr("\\d+", log$colnumber[i])))))      #was 2:46 for BVR 
      }
      else if(grepl("^c\\(\\s*\\d+\\s*:\\s*\\d+\\s*\\)$", log$colnumber[i])) # c(x:y)
      {
        bounds <- as.integer(unlist(regmatches(log$colnumber[i], gregexpr("\\d+", log$colnumber[i]))))
        maintenance_cols <- dplyr::intersect(c(2:53), c(bounds[1]:bounds[2]))    #was 2:46 for BVR 
      }
      else
      {
        warning(paste("Could not parse column colnumber in row", i, "of the maintenance log. Skipping maintenance for",
                      "that row. The value of colnumber should be in one of three formats: a single number (\"47\"), a",
                      "semicolon-separated list of numbers in c() (\"c(47;48;49)\"), or a range of numbers in c() (\"c(47:74)\").",
                      "Other values (even valid calls to c()) will not be parsed properly."))
        next
      }
      
      # remove EXO_Date and EXO_Time columns from the list of maintenance columns, because they will be deleted later
      maintenance_cols <- setdiff(maintenance_cols, c(18, 19, 37, 38)) # was c(24, 25) for BVR
      
      if(length(maintenance_cols) == 0)
      {
        warning(paste("Did not parse any valid data columns in row", i, "of the maintenance log. Valid columns have",
                      "indices 2 through 53, excluding 18,19,37, and 38, which are deleted by this script. Skipping maintenance for that row."))
        next
      }
      
      #index the Flag columns
      if(grepl("^\\d+$", log$flag[i])) # single num
        {
        flag_cols <- intersect(c(54:97), as.integer(log$flag[i])) #was c(47:86) for BVR
        
      } else if(grepl("^c\\(\\s*\\d+\\s*(;\\s*\\d+\\s*)*\\)$", log$flag[i])) # c(x;y;...)
      {
        flag_cols <- intersect(c(54:97), as.integer(unlist(regmatches(log$flag[i],
                                                                      gregexpr("\\d+", log$flag[i])))))
      } else if(grepl("^c\\(\\s*\\d+\\s*:\\s*\\d+\\s*\\)$", log$flag[i])) # c(x:y)
        {
        bounds_flag <- as.integer(unlist(regmatches(log$flag[i], gregexpr("\\d+", log$flag[i]))))
        flag_cols <- dplyr::intersect(c(54:97), c(bounds_flag[1]:bounds_flag[2]))
      } else {
        warning(paste("Could not parse column flag in row", i, "of the maintenance log. Skipping maintenance for",
                      "that row. The value of colnumber should be in one of three formats: a single number (\"47\"), a",
                      "semicolon-separated list of numbers in c() (\"c(47;48;49)\"), or a range of numbers in c() (\"c(47:74)\").",
                      "Other values (even valid calls to c()) will not be parsed properly."))
        next
      }
      
      #Get the Maintenance Flag 
      
      flag <- log$flag[i]
      
      # replace relevant data with NAs and set flags while maintenance was in effect
      if(flag!=5) {
        ccrdata[ccrdata$DateTime >= start & ccrdata$DateTime <= end, maintenance_cols] <- NA
        ccrdata[ccrdata$DateTime >= start & ccrdata$DateTime <= end, flag_cols] <- flag
      } else {
        ccrdata[ccrdata$DateTime >= start & ccrdata$DateTime <= end, flag_cols] <- flag
        next
      }
      #Add the 2 hour adjustment for DO 

      if (log$colnumber[i]=="c(1:53)" && flag==1){
        DO_col=c("EXODOsat_percent_1", "EXODO_mgL_1", "EXODOsat_percent_9","EXODO_mgL_9")
        DO_flag_col=c("Flag_EXODOsat_percent_1", "Flag_EXODO_mgL_1", "Flag_EXODOsat_percent_9","Flag_EXODO_mgL_9")
        ccrdata[ccrdata$DateTime>start&ccrdata$DateTime<end+ADJ_PERIOD,DO_col] <- NA
        ccrdata[ccrdata$DateTime>start&ccrdata$DateTime<end+ADJ_PERIOD,DO_flag_col] <- flag
      }
      else if(log$colnumber[i] %in% c(" 24"," 25") && flag==1){
        DO_col=c("EXODOsat_percent_1", "EXODO_mgL_1")
        DO_flag_col=c("Flag_EXODOsat_percent_1", "Flag_EXODO_mgL_1")
        ccrdata[ccrdata$DateTime>start&ccrdata$DateTime<end+ADJ_PERIOD,DO_col] <- NA
        ccrdata[ccrdata$DateTime>start&ccrdata$DateTime<end+ADJ_PERIOD,DO_flag_col] <- flag
        
      }
      else if(log$colnumber[i] %in% c(" 43"," 44") && flag==1){
        DO_col=c("EXODOsat_percent_9","EXODO_mgL_9")
        DO_flag_col=c("Flag_EXODOsat_percent_9","Flag_EXODO_mgL_9")
        ccrdata[ccrdata$DateTime>start&ccrdata$DateTime<end+ADJ_PERIOD,DO_col] <- NA
        ccrdata[ccrdata$DateTime>start&ccrdata$DateTime<end+ADJ_PERIOD,DO_flag_col] <- flag
        
      }
      # else if (log$colnumber[i] %in% c(" c(26:44"," 30"," 31") && flag==1){
      #   DO_col=c("EXODOsat_percent_1_5", "EXODO_mgL_1_5")
      #   DO_flag_col=c("Flag_EXODOsat_percent_1_5", "Flag_EXODO_mgL_1_5")
      #   ccrdata[ccrdata$DateTime>start&ccrdata$DateTime<end+ADJ_PERIOD,DO_col] <- NA
      #   ccrdata[ccrdata$DateTime>start&ccrdata$DateTime<end+ADJ_PERIOD,DO_flag_col] <-1
      #   
      # }
      else{
        warning(paste("No DO to time to adjust in row",i,"."))
        
      }
    }
  
  ##################################################################################################################  
    #Set negative DO values to 0 and Flag_DO for NA values
    ccrdata <- ccrdata %>%  #RDO at 5m
      dplyr::mutate(Flag_EXODO_mgL_1 = ifelse(EXODO_mgL_1 < 0 & !is.na(EXODO_mgL_1) , 3, Flag_EXODO_mgL_1),#Add a flag for DO<0
             Flag_EXODOsat_percent_1 = ifelse(EXODOsat_percent_1 < 0 & !is.na(EXODOsat_percent_1) , 3, Flag_EXODOsat_percent_1),
             Flag_EXODO_mgL_1 = ifelse(Flag_EXODO_mgL_1 < 0, 0, Flag_EXODO_mgL_1), #Change negative to 0
             EXODOsat_percent_1 = ifelse(EXODOsat_percent_1 < 0, 0, EXODOsat_percent_1), #Change negative %sat to 0
             
             Flag_EXODO_mgL_9 = ifelse(EXODO_mgL_9 < 0 & !is.na(EXODO_mgL_9), 3, Flag_EXODO_mgL_9), #repeat for 13m
             Flag_EXODOsat_percent_9 = ifelse(EXODOsat_percent_9 < 0 & !is.na(EXODOsat_percent_9) , 3, Flag_EXODOsat_percent_9),
             EXODO_mgL_9 = ifelse(EXODO_mgL_9 < 0, 0, EXODO_mgL_9),
             EXODOsat_percent_9 = ifelse(EXODOsat_percent_9 < 0, 0, EXODOsat_percent_9)
             
             # Flag_EXODO_mgL_1_5 = ifelse(EXODO_mgL_1_5 < 0 & !is.na(EXODO_mgL_1_5), 3, Flag_EXODO_mgL_1_5), #and for 1m
             # Flag_EXODOsat_percent_1_5 = ifelse(EXODOsat_percent_1_5 < 0 & !is.na(EXODOsat_percent_1_5) , 3, Flag_EXODOsat_percent_1_5),
             # EXODO_mgL_1_5 = ifelse(EXODO_mgL_1_5 < 0, 0, EXODO_mgL_1_5),
             # EXODOsat_percent_1_5 = ifelse(EXODOsat_percent_1_5 <0, 0, EXODOsat_percent_1_5)
      )
    
    ########################################################################################################################  
    # find chla and phyo on the EXO sonde sensor data that differs from the mean by more than the standard deviation times a given factor, and
    # replace with NAs between October 2018 and March 2019, due to sensor fouling
    Chla_RFU_1_mean <- mean(ccrdata$EXOChla_RFU_1, na.rm = TRUE)
    Chla_ugL_1_mean <- mean(ccrdata$EXOChla_ugL_1, na.rm = TRUE)
    BGAPC_RFU_1_mean <- mean(ccrdata$EXOBGAPC_RFU_1, na.rm = TRUE)
    BGAPC_ugL_1_mean <- mean(ccrdata$EXOBGAPC_ugL_1, na.rm = TRUE)
    Chla_RFU_1_threshold <- EXO_FOULING_FACTOR * sd(ccrdata$EXOChla_RFU_1, na.rm = TRUE)
    Chla_ugL_1_threshold <- EXO_FOULING_FACTOR * sd(ccrdata$EXOChla_ugL_1, na.rm = TRUE)
    BGAPC_RFU_1_threshold <- EXO_FOULING_FACTOR * sd(ccrdata$EXOBGAPC_RFU_1, na.rm = TRUE)
    BGAPC_ugL_1_threshold <- EXO_FOULING_FACTOR * sd(ccrdata$EXOBGAPC_ugL_1, na.rm = TRUE)
    
    
    # QAQC on major chl outliers using DWH's method: datapoint set to NA if data is greater than 4*sd different from both previous and following datapoint
    ccrdata <- ccrdata %>% 
      dplyr::mutate(Chla_ugL = lag(EXOChla_ugL_1, 0),
             Chla_ugL_lag1 = lag(EXOChla_ugL_1, 1),
             Chla_ugL_lead1 = lead(EXOChla_ugL_1, 1)) %>%  #These mutates create columns for current fDOM, fDOM before and fDOM after. These are used to run ifelse QAQC loops
      dplyr::mutate(Flag_EXOChla_ugL_1 = ifelse(Chla_ugL < 0 & !is.na(Chla_ugL), 3, Flag_EXOChla_ugL_1)) %>% 
      dplyr::mutate(EXOChla_ugL_1 = ifelse(Chla_ugL < 0 & !is.na(Chla_ugL), 0, EXOChla_ugL_1)) %>% 
      dplyr::mutate(EXOChla_ugL_1 = ifelse((abs(Chla_ugL_lag1 - Chla_ugL) > (Chla_ugL_1_threshold))  & (abs(Chla_ugL_lead1 - Chla_ugL) > (Chla_ugL_1_threshold) & !is.na(Chla_ugL)), 
                                      NA, EXOChla_ugL_1)) %>%   
      dplyr::mutate(Flag_EXOChla_ugL_1 = ifelse((abs(Chla_ugL_lag1 - Chla_ugL) > (Chla_ugL_1_threshold))  & (abs(Chla_ugL_lead1 - Chla_ugL) > (Chla_ugL_1_threshold)) & !is.na(Chla_ugL), 
                                           2, Flag_EXOChla_ugL_1)) %>%
      dplyr::mutate(Flag_EXOChla_ugL_1 = ifelse(is.na(Flag_EXOChla_ugL_1), 2, Flag_EXOChla_ugL_1))%>%
      dplyr::select(-Chla_ugL, -Chla_ugL_lag1, -Chla_ugL_lead1)
    
    #Chla_RFU QAQC
    ccrdata <- ccrdata %>% 
      dplyr::mutate(Chla_RFU = lag(EXOChla_RFU_1, 0),
             Chla_RFU_lag1 = lag(EXOChla_RFU_1, 1),
             Chla_RFU_lead1 = lead(EXOChla_RFU_1, 1)) %>%  #These mutates create columns for current fDOM, fDOM before and fDOM after. These are used to run ifelse QAQC loops
      dplyr::mutate(Flag_EXOChla_RFU_1 = ifelse(Chla_RFU < 0 & !is.na(Chla_RFU), 3, Flag_EXOChla_RFU_1)) %>% 
      dplyr::mutate(EXOChla_RFU_1 = ifelse(Chla_RFU < 0 & !is.na(Chla_RFU), 0, EXOChla_RFU_1)) %>% 
      dplyr::mutate(EXOChla_RFU_1 = ifelse((abs(Chla_RFU_lag1 - Chla_RFU) > (Chla_RFU_1_threshold))  & (abs(Chla_RFU_lead1 - Chla_RFU) > (Chla_RFU_1_threshold) & !is.na(Chla_RFU)), 
                                      NA, EXOChla_RFU_1)) %>%   
      dplyr::mutate(Flag_EXOChla_RFU_1 = ifelse((abs(Chla_RFU_lag1 - Chla_RFU) > (Chla_RFU_1_threshold))  & (abs(Chla_RFU_lead1 - Chla_RFU) > (Chla_RFU_1_threshold)) & !is.na(Chla_RFU), 
                                           2, Flag_EXOChla_RFU_1)) %>%
      dplyr::mutate(Flag_EXOChla_RFU_1 = ifelse(is.na(Flag_EXOChla_RFU_1), 2, Flag_EXOChla_RFU_1))%>%
      dplyr::select(-Chla_RFU, -Chla_RFU_lag1, -Chla_RFU_lead1)
    
    # QAQC on major chl outliers using DWH's method: datapoint set to NA if data is greater than 4*sd different from both previous and following datapoint
    ccrdata <- ccrdata %>% 
      dplyr::mutate(phyco_ugL = lag(EXOBGAPC_ugL_1, 0),
             phyco_ugL_lag1 = lag(EXOBGAPC_ugL_1, 1),
             phyco_ugL_lead1 = lead(EXOBGAPC_ugL_1, 1)) %>%  #These mutates create columns for current fDOM, fDOM before and fDOM after. These are used to run ifelse QAQC loops
      dplyr::mutate(Flag_EXOBGAPC_ugL_1 = ifelse(phyco_ugL < 0 & !is.na(phyco_ugL), 3, Flag_EXOBGAPC_ugL_1)) %>% 
      dplyr::mutate(EXOBGAPC_ugL_1 = ifelse(phyco_ugL < 0 & !is.na(phyco_ugL), 0, EXOBGAPC_ugL_1)) %>% 
      dplyr::mutate(EXOBGAPC_ugL_1 = ifelse((abs(phyco_ugL_lag1 - phyco_ugL) > (BGAPC_ugL_1_threshold))  & (abs(phyco_ugL_lead1 - phyco_ugL) > (BGAPC_ugL_1_threshold) & !is.na(phyco_ugL)), 
                                       NA, EXOBGAPC_ugL_1)) %>%   
      dplyr::mutate(Flag_EXOBGAPC_ugL_1 = ifelse((abs(phyco_ugL_lag1 - phyco_ugL) > (BGAPC_ugL_1_threshold))  & (abs(phyco_ugL_lead1 - phyco_ugL) > (BGAPC_ugL_1_threshold) & !is.na(phyco_ugL)), 
                                            2, Flag_EXOBGAPC_ugL_1)) %>%
      dplyr::mutate(Flag_EXOBGAPC_ugL_1 = ifelse(is.na(Flag_EXOBGAPC_ugL_1),2,Flag_EXOBGAPC_ugL_1))%>%
      dplyr::select(-phyco_ugL, -phyco_ugL_lag1, -phyco_ugL_lead1)
    
    #Phyco QAQC for RFU
    ccrdata <- ccrdata %>% 
      dplyr::mutate(phyco_RFU = lag(EXOBGAPC_RFU_1, 0),
             phyco_RFU_lag1 = lag(EXOBGAPC_RFU_1, 1),
             phyco_RFU_lead1 = lead(EXOBGAPC_RFU_1, 1)) %>%  #These mutates create columns for current fDOM, fDOM before and fDOM after. These are used to run ifelse QAQC loops
      dplyr::mutate(Flag_EXOBGAPC_RFU_1 = ifelse(phyco_RFU < 0 & !is.na(phyco_RFU), 3, Flag_EXOBGAPC_RFU_1)) %>% 
      dplyr::mutate(EXOBGAPC_RFU_1 = ifelse(phyco_RFU < 0 & !is.na(phyco_RFU), 0, EXOBGAPC_RFU_1)) %>% 
      dplyr::mutate(EXOBGAPC_RFU_1 = ifelse((abs(phyco_RFU_lag1 - phyco_RFU) > (BGAPC_RFU_1_threshold))  & (abs(phyco_RFU_lead1 - phyco_RFU) > (BGAPC_RFU_1_threshold) & !is.na(phyco_RFU)), 
                                       NA, EXOBGAPC_RFU_1)) %>%   
      dplyr::mutate(Flag_EXOBGAPC_RFU_1 = ifelse((abs(phyco_RFU_lag1 - phyco_RFU) > (BGAPC_RFU_1_threshold))  & (abs(phyco_RFU_lead1 - phyco_RFU) > (BGAPC_RFU_1_threshold) & !is.na(phyco_RFU)), 
                                            2, Flag_EXOBGAPC_RFU_1)) %>%
      dplyr::mutate(Flag_EXOBGAPC_RFU_1 = ifelse(is.na(Flag_EXOBGAPC_RFU_1),2,Flag_EXOBGAPC_RFU_1))%>%
      dplyr::select(-phyco_RFU, -phyco_RFU_lag1, -phyco_RFU_lead1)
    
    
    
    # flag EXO sonde sensor data of value above 4 * standard deviation at other times but leave them in the dataset
    ccrdata <- ccrdata %>%
      dplyr::mutate(Flag_EXOBGAPC_RFU_1 = ifelse(! is.na(EXOBGAPC_RFU_1) & abs(EXOBGAPC_RFU_1 - BGAPC_RFU_1_mean) > BGAPC_RFU_1_threshold,
                                            5, Flag_EXOBGAPC_RFU_1)) %>%
      dplyr::mutate(Flag_EXOBGAPC_ugL_1 = ifelse( ! is.na(EXOBGAPC_ugL_1) & abs(EXOBGAPC_ugL_1 - BGAPC_ugL_1_mean) > BGAPC_ugL_1_threshold,
                                             5, Flag_EXOBGAPC_ugL_1)) %>%
      dplyr::mutate(Flag_EXOChla_RFU_1 = ifelse(! is.na(EXOChla_RFU_1) & abs(EXOChla_RFU_1 - Chla_RFU_1_mean) > Chla_RFU_1_threshold,
                                           5, Flag_EXOChla_RFU_1)) %>%
      dplyr::mutate(Flag_EXOChla_ugL_1 = ifelse(! is.na(EXOChla_ugL_1) & abs(EXOChla_ugL_1 - Chla_ugL_1_mean) > Chla_ugL_1_threshold,
                                           5, Flag_EXOChla_ugL_1)) 
    
    
    
    ####################################################################################################################################  
    # fdom qaqc----
    # QAQC from DWH to remove major outliers from fDOM data that are 2 sd's greater than the previous and following datapoint
    sd_fDOM_QSU_1 <- sd(ccrdata$EXOfDOM_QSU_1, na.rm = TRUE) #deteriming the standard deviation of fDOM data 
    sd_fDOM_QSU_9 <- sd(ccrdata$EXOfDOM_QSU_9, na.rm = TRUE)
    mean_fDOM_QSU_1 <- mean(ccrdata$EXOfDOM_QSU_1, na.rm = TRUE)
    mean_fDOM_QSU_9 <- mean(ccrdata$EXOfDOM_QSU_9, na.rm = TRUE)
    
    sd_fDOM_RFU_1 <- sd(ccrdata$EXOfDOM_RFU_1, na.rm = TRUE) #deteriming the standard deviation of fDOM data 
    sd_fDOM_RFU_9 <- sd(ccrdata$EXOfDOM_RFU_9, na.rm = TRUE)
    mean_fDOM_RFU_1 <- mean(ccrdata$EXOfDOM_RFU_1, na.rm = TRUE)
    mean_fDOM_RFU_9 <- mean(ccrdata$EXOfDOM_RFU_9, na.rm = TRUE)
    
    
    #
    ccrdata <- ccrdata%>% 
      dplyr::mutate(fDOM1 = lag(EXOfDOM_QSU_1, 0),
             fDOM_lag1 = lag(EXOfDOM_QSU_1, 1),
             fDOM_lead1 = lead(EXOfDOM_QSU_1, 1)) %>%   #These mutates create columns for current fDOM, fDOM before and fDOM after. These are used to run ifelse QAQC loops
      dplyr::mutate(Flag_EXOfDOM_QSU_1 = ifelse(fDOM1 < 0 & !is.na(fDOM1), 2, Flag_EXOfDOM_QSU_1),
             EXOfDOM_QSU_1 = ifelse(fDOM1 < 0 & !is.na(fDOM1), NA, EXOfDOM_QSU_1),
             EXOfDOM_RFU_1 = ifelse(fDOM1 < 0 & !is.na(fDOM1), NA, EXOfDOM_RFU_1))%>%
      #These mutates are QAQCing for negative fDOM QSU values and setting these to NA and making a flag for these. This was done outside of the 2 sd deviation rule because there were two negative points in a row and one was not removed with the follwoing if else statements. 
      dplyr::mutate(EXOfDOM_QSU_1 = ifelse(
        ( abs(fDOM_lag1 - fDOM1) > (2*sd_fDOM_QSU_1)   )  & ( abs(fDOM_lead1 - fDOM1) > (2*sd_fDOM_QSU_1)  & !is.na(fDOM1) ), NA, EXOfDOM_QSU_1
      )) %>%   #QAQC to remove outliers for QSU fDOM data 
      dplyr::mutate(EXOfDOM_RFU_1 = ifelse(
        ( abs(fDOM_lag1 - fDOM1) > (2*sd_fDOM_RFU_1)   )  & ( abs(fDOM_lead1 - fDOM1) > (2*sd_fDOM_RFU_1)  & !is.na(fDOM1)  ), NA, EXOfDOM_RFU_1
      )) %>% #QAQC to remove outliers for RFU fDOM data
      dplyr::mutate(Flag_EXOfDOM_RFU_1 = ifelse(
        ( abs(fDOM_lag1 - fDOM1) > (2*sd_fDOM_RFU_1)   )  & ( abs(fDOM_lead1 - fDOM1) > (2*sd_fDOM_RFU_1)  & !is.na(fDOM1)  ), 2, Flag_EXOfDOM_RFU_1
      ))  %>%  #QAQC to set flags for data that was set to NA after applying 2 S.D. QAQC 
      dplyr::select(-fDOM1, -fDOM_lag1, -fDOM_lead1)#This removes the columns used to run ifelse statements since they are no longer needed.
    
    ccrdata <- ccrdata%>%
      dplyr::mutate(fDOM9 = lag(EXOfDOM_QSU_9, 0),
             fDOM_lag9 = lag(EXOfDOM_QSU_9, 1),
             fDOM_lead9 = lead(EXOfDOM_QSU_9, 1)) %>%  #These mutates create columns for current fDOM, fDOM before and fDOM after. These are used to run ifelse QAQC loops
      dplyr::mutate(Flag_EXOfDOM_QSU_9 = ifelse(fDOM9 < 0 & !is.na(fDOM9), 2, Flag_EXOfDOM_QSU_9),
             EXOfDOM_QSU_9 = ifelse(fDOM9 < 0 & !is.na(fDOM9), NA, EXOfDOM_QSU_9),
             EXOfDOM_RFU_9 = ifelse(fDOM9 < 0 & !is.na(fDOM9), NA, EXOfDOM_RFU_9))%>%
      #Flag_EXOfDOM_9 = ifelse(fDOM < 0, 2, Flag_EXOfDOM_9)) %>% #These mutates are QAQCing for negative fDOM QSU values and setting these to NA and making a flag for these. This was done outside of the 2 sd deviation rule because there were two negative points in a row and one was not removed with the follwoing if else statements. 
      dplyr::mutate(EXOfDOM_QSU_9 = ifelse(
        ( abs(fDOM_lag9 - fDOM9) > (2*sd_fDOM_QSU_9)   )  & ( abs(fDOM_lead9 - fDOM9) > (2*sd_fDOM_QSU_9)  & !is.na(fDOM9) ), NA, EXOfDOM_QSU_9
      )) %>%  #QAQC to remove outliers for QSU fDOM data 
      dplyr::mutate(EXOfDOM_RFU_9 = ifelse(
        ( abs(fDOM_lag9 - fDOM9) > (2*sd_fDOM_RFU_9)   )  & ( abs(fDOM_lead9 - fDOM9) > (2*sd_fDOM_RFU_9)  & !is.na(fDOM9)  ), NA, EXOfDOM_RFU_9
      )) %>% #QAQC to remove outliers for RFU fDOM data
      dplyr::mutate(Flag_EXOfDOM_RFU_9 = ifelse(
        ( abs(fDOM_lag9 - fDOM9) > (2*sd_fDOM_RFU_9)   )  & ( abs(fDOM_lead9 - fDOM9) > (2*sd_fDOM_RFU_9)  & !is.na(fDOM9)  ), 2, Flag_EXOfDOM_RFU_9
      ))  %>%  #QAQC to set flags for data that was set to NA after applying 2 S.D. QAQC 
      dplyr::select(-fDOM9, -fDOM_lag9, -fDOM_lead9)#This removes the columns used to run ifelse statements since they are no longer needed.
    
    
    
   
    # flag EXO sonde sensor data of value above 4 * standard deviation at other times but leave them in the dataset. Probably won't flag anything but for consistenacy. 
    ccrdata <- ccrdata %>%
      dplyr::mutate(Flag_EXOfDOM_RFU_1 = ifelse(! is.na(EXOfDOM_RFU_1) & abs(EXOfDOM_RFU_1 - mean_fDOM_RFU_1) > (4*sd_fDOM_RFU_1),
                                           5, Flag_EXOfDOM_RFU_1)) %>%
      dplyr::mutate(Flag_EXOfDOM_QSU_1 = ifelse( ! is.na(EXOfDOM_QSU_1) & abs(EXOfDOM_QSU_1 - mean_fDOM_QSU_1) > (4*sd_fDOM_QSU_1),
                                            5, Flag_EXOfDOM_QSU_1)) %>% 
      dplyr::mutate(Flag_EXOfDOM_RFU_9 = ifelse( ! is.na(EXOfDOM_RFU_9) & abs(EXOfDOM_RFU_9 - mean_fDOM_RFU_9) > (4*sd_fDOM_RFU_9),
                                                   5, Flag_EXOfDOM_RFU_9)) %>% 
      dplyr::mutate(Flag_EXOfDOM_QSU_9 = ifelse( ! is.na(EXOfDOM_QSU_9) & abs(EXOfDOM_QSU_9 - mean_fDOM_QSU_9) > (4*sd_fDOM_QSU_9),
                                                   5, Flag_EXOfDOM_QSU_9)) 
      
    
    #####################################################################################################################################  
    #QAQC from DWH to remove major outliers from conductity, specific conductivity and TDS data that are 2 sd's greater than the previous and following datapoint
    sd_2_cond_1 <- 2*sd(ccrdata$EXOCond_uScm_1, na.rm = TRUE)
    sd_2_cond_9 <- 2*sd(ccrdata$EXOCond_uScm_9, na.rm = TRUE)
    sd_2_spcond_1 <- 2*sd(ccrdata$EXOSpCond_uScm_1, na.rm = TRUE)
    sd_2_spcond_9 <- 2*sd(ccrdata$EXOSpCond_uScm_9, na.rm = TRUE)
    sd_2_TDS_1 <- 2*sd(ccrdata$EXOTDS_mgL_1, na.rm = TRUE)
    sd_2_TDS_9 <- 2*sd(ccrdata$EXOTDS_mgL_9, na.rm = TRUE) 
    
    # QAQC on major conductivity outliers using DWH's method: datapoint set to NA if data is greater than 2*sd different from both previous and following datapoint
    #Remove any data below 1 because it is an outlier and give it a Flag Code of 2
    ccrdata <- ccrdata %>% 
      dplyr::mutate(Cond1 = lag(EXOCond_uScm_1, 0),
             Cond_lag1 = lag(EXOCond_uScm_1, 1),
             Cond_lead1 = lead(EXOCond_uScm_1, 1)) %>%  #These mutates create columns for current fDOM, fDOM before and fDOM after. These are used to run ifelse QAQC loops
      dplyr::mutate(Flag_EXOCond_1 = ifelse(Cond1 < 0 & !is.na(Cond1), 3, 0)) %>%
      dplyr::mutate(Flag_EXOCond_1 = ifelse(Cond1 < 1 & !is.na(Cond1), 2, Flag_EXOCond_1)) %>%#Remove any points less than 1
      dplyr::mutate(EXOCond_uScm_1 = ifelse(Cond1 < 0 & !is.na(Cond1), 0, EXOCond_uScm_1)) %>%
      dplyr::mutate(EXOCond_uScm_1 = ifelse(Cond1 < 1 & !is.na(Cond1), NA, EXOCond_uScm_1)) %>%
      dplyr::mutate(EXOCond_uScm_1 = ifelse((abs(Cond_lag1 - Cond1) > (sd_2_cond_1))  & (abs(Cond_lead1 - Cond1) > (sd_2_cond_1) & !is.na(Cond1)), 
                                     NA, EXOCond_uScm_1)) %>%   
      dplyr::mutate(Flag_EXOCond_1 = ifelse((abs(Cond_lag1 - Cond1) > (sd_2_cond_1))  & (abs(Cond_lead1 - Cond1) > (sd_2_cond_1) & !is.na(Cond1)), 
                                     2, Flag_EXOCond_1)) %>% 
      dplyr::select(-Cond1, -Cond_lag1, -Cond_lead1)
    
    ccrdata <- ccrdata %>% 
      dplyr::mutate(Cond9 = lag(EXOCond_uScm_9, 0),
             Cond_lag9 = lag(EXOCond_uScm_9, 1),
             Cond_lead9 = lead(EXOCond_uScm_9, 1)) %>%  #These mutates create columns for current fDOM, fDOM before and fDOM after. These are used to run ifelse QAQC loops
      dplyr::mutate(Flag_EXOCond_9 = ifelse(Cond9 < 0 & !is.na(Cond9), 3, 0)) %>%
      dplyr::mutate(Flag_EXOCond_9 = ifelse(Cond9 < 1 & !is.na(Cond9), 2, Flag_EXOCond_9)) %>%#Remove any points less than 9
      dplyr::mutate(EXOCond_uScm_9 = ifelse(Cond9 < 0 & !is.na(Cond9), 0, EXOCond_uScm_9)) %>%
      dplyr::mutate(EXOCond_uScm_9 = ifelse(Cond9 < 1 & !is.na(Cond9), NA, EXOCond_uScm_9)) %>%
      dplyr::mutate(EXOCond_uScm_9 = ifelse((abs(Cond_lag9 - Cond9) > (sd_2_cond_9))  & (abs(Cond_lead9 - Cond9) > (sd_2_cond_9) & !is.na(Cond9)), 
                                     NA, EXOCond_uScm_9)) %>%   
      dplyr::mutate(Flag_EXOCond_9 = ifelse((abs(Cond_lag9 - Cond9) > (sd_2_cond_9))  & (abs(Cond_lead9 - Cond9) > (sd_2_cond_9) & !is.na(Cond9)), 
                                     2, Flag_EXOCond_9)) %>% 
      dplyr::select(-Cond9, -Cond_lag9, -Cond_lead9)
    
    # QAQC on major Specific conductivity outliers using DWH's method: datapoint set to NA if data is greater than 2*sd different from both previous and following datapoint
    #Remove any data below 1 because it is an outlier and give it a Flag Code of 2
    ccrdata <- ccrdata %>% 
      dplyr::mutate(SpCond1 = lag(EXOSpCond_uScm_1, 0),
             SpCond_lag1 = lag(EXOSpCond_uScm_1, 1),
             SpCond_lead1 = lead(EXOSpCond_uScm_1, 1)) %>%  #These mutates create columns for current fDOM, fDOM before and fDOM after. These are used to run ifelse QAQC loops
      dplyr::mutate(Flag_EXOSpCond_1 = ifelse(SpCond1 < 0 & !is.na(SpCond1), 3, 0)) %>% 
      dplyr::mutate(Flag_EXOSpCond_1 = ifelse(SpCond1 < 1 & !is.na(SpCond1), 2, Flag_EXOSpCond_1)) %>%
      dplyr::mutate(EXOSpCond_uScm_1 = ifelse(SpCond1 < 0 & !is.na(SpCond1), 0, EXOSpCond_uScm_1)) %>% 
      dplyr::mutate(EXOSpCond_uScm_1 = ifelse(SpCond1 < 1 & !is.na(SpCond1), NA, EXOSpCond_uScm_1)) %>%
      dplyr::mutate(EXOSpCond_uScm_1 = ifelse((abs(SpCond_lag1 - SpCond1) > (sd_2_spcond_1))  & (abs(SpCond_lead1 - SpCond1) > (sd_2_spcond_1) & !is.na(SpCond1)), 
                                       NA, EXOSpCond_uScm_1)) %>%   
      dplyr::mutate(Flag_EXOSpCond_1 = ifelse((abs(SpCond_lag1 - SpCond1) > (sd_2_spcond_1))  & (abs(SpCond_lead1 - SpCond1) > (sd_2_spcond_1)) & !is.na(SpCond1), 
                                       2, Flag_EXOSpCond_1)) %>% 
      dplyr::select(-SpCond1, -SpCond_lag1, -SpCond_lead1)
    
    ccrdata <- ccrdata %>% 
      dplyr::mutate(SpCond9 = lag(EXOSpCond_uScm_9, 0),
             SpCond_lag9 = lag(EXOSpCond_uScm_9, 1),
             SpCond_lead9 = lead(EXOSpCond_uScm_9, 1)) %>%  #These mutates create columns for current fDOM, fDOM before and fDOM after. These are used to run ifelse QAQC loops
      dplyr::mutate(Flag_EXOSpCond_9 = ifelse(SpCond9 < 0 & !is.na(SpCond9), 3, 0)) %>% 
      dplyr::mutate(Flag_EXOSpCond_9 = ifelse(SpCond9 < 1 & !is.na(SpCond9), 2, Flag_EXOSpCond_9)) %>%
      dplyr::mutate(EXOSpCond_uScm_9 = ifelse(SpCond9 < 0 & !is.na(SpCond9), 0, EXOSpCond_uScm_9)) %>% 
      dplyr::mutate(EXOSpCond_uScm_9 = ifelse(SpCond9 < 1 & !is.na(SpCond9), NA, EXOSpCond_uScm_9)) %>%
      dplyr::mutate(EXOSpCond_uScm_9 = ifelse((abs(SpCond_lag9 - SpCond9) > (sd_2_spcond_9))  & (abs(SpCond_lead9 - SpCond9) > (sd_2_spcond_9) & !is.na(SpCond9)), 
                                       NA, EXOSpCond_uScm_9)) %>%   
      dplyr::mutate(Flag_EXOSpCond_9 = ifelse((abs(SpCond_lag9 - SpCond9) > (sd_2_spcond_9))  & (abs(SpCond_lead9 - SpCond9) > (sd_2_spcond_9)) & !is.na(SpCond9), 
                                       2, Flag_EXOSpCond_9)) %>% 
      dplyr::select(-SpCond9, -SpCond_lag9, -SpCond_lead9)
    
    # QAQC on major TDS outliers using DWH's method: datapoint set to NA if data is greater than 2*sd different from both previous and following datapoint
    #Remove any data below 1 because it is an outlier and give it a Flag Code of 2
    ccrdata <- ccrdata %>% 
      dplyr::mutate(TDS1 = lag(EXOTDS_mgL_1, 0),
             TDS_lag1 = lag(EXOTDS_mgL_1, 1),
             TDS_lead1 = lead(EXOTDS_mgL_1, 1)) %>%  #These mutates create columns for current fDOM, fDOM before and fDOM after. These are used to run ifelse QAQC loops
      dplyr::mutate(Flag_EXOTDS_1 = ifelse(TDS1 < 0 & !is.na(TDS1), 3, 0)) %>% 
      dplyr::mutate(Flag_EXOTDS_1 = ifelse(TDS1 < 1 & !is.na(TDS1), 2, Flag_EXOTDS_1)) %>%
      dplyr::mutate(EXOTDS_mgL_1 = ifelse(TDS1 < 0 & !is.na(TDS1), 0, EXOTDS_mgL_1)) %>%
      dplyr::mutate(EXOTDS_mgL_1 = ifelse(TDS1 < 1 & !is.na(TDS1), NA, EXOTDS_mgL_1)) %>% 
      dplyr::mutate(EXOTDS_mgL_1 = ifelse((abs(TDS_lag1 - TDS1) > (sd_2_TDS_1))  & (abs(TDS_lead1 - TDS1) > (sd_2_TDS_1) & !is.na(TDS1)), 
                                   NA, EXOTDS_mgL_1)) %>%   
      dplyr::mutate(Flag_EXOTDS_1 = ifelse((abs(TDS_lag1 - TDS1) > (sd_2_TDS_1))  & (abs(TDS_lead1 - TDS1) > (sd_2_TDS_1)) & !is.na(TDS1), 
                                    2, Flag_EXOTDS_1)) %>% 
      dplyr::select(-TDS1, -TDS_lag1, -TDS_lead1)
    
    ccrdata <- ccrdata %>% 
      dplyr::mutate(TDS9 = lag(EXOTDS_mgL_9, 0),
             TDS_lag9 = lag(EXOTDS_mgL_9, 1),
             TDS_lead9 = lead(EXOTDS_mgL_9, 1)) %>%  #These mutates create columns for current fDOM, fDOM before and fDOM after. These are used to run ifelse QAQC loops
      dplyr::mutate(Flag_EXOTDS_9 = ifelse(TDS9 < 0 & !is.na(TDS9), 3, 0)) %>% 
      dplyr::mutate(Flag_EXOTDS_9 = ifelse(TDS9 < 1 & !is.na(TDS9), 2, Flag_EXOTDS_9)) %>%
      dplyr::mutate(EXOTDS_mgL_9 = ifelse(TDS9 < 0 & !is.na(TDS9), 0, EXOTDS_mgL_9)) %>%
      dplyr::mutate(EXOTDS_mgL_9 = ifelse(TDS9 < 1 & !is.na(TDS9), NA, EXOTDS_mgL_9)) %>% 
      dplyr::mutate(EXOTDS_mgL_9 = ifelse((abs(TDS_lag9 - TDS9) > (sd_2_TDS_9))  & (abs(TDS_lead9 - TDS9) > (sd_2_TDS_9) & !is.na(TDS9)), 
                                   NA, EXOTDS_mgL_9)) %>%   
      dplyr::mutate(Flag_EXOTDS_9 = ifelse((abs(TDS_lag9 - TDS9) > (sd_2_TDS_9))  & (abs(TDS_lead9 - TDS9) > (sd_2_TDS_9)) & !is.na(TDS9), 
                                    2, Flag_EXOTDS_9)) %>% 
      dplyr::select(-TDS9, -TDS_lag9, -TDS_lead9)
    
    # flag EXO sonde sensor data of value above 4 * standard deviation at other times but leave them in the dataset. Probably won't flag anything but for consistenacy. 
    ccrdata <- ccrdata %>%
      dplyr::mutate(Flag_EXOCond_uScm_1 = ifelse(! is.na(EXOCond_uScm_1) & abs(EXOCond_uScm_1 - mean(EXOCond_uScm_1, na.rm = T)) > (4* sd(EXOCond_uScm_1, na.rm = T)),
                                            5, Flag_EXOCond_uScm_1)) %>%
      dplyr::mutate(Flag_EXOSpCond_uScm_1 = ifelse( ! is.na(EXOSpCond_uScm_1) & abs(EXOSpCond_uScm_1 - mean(EXOSpCond_uScm_1, na.rm = T)) > (4* sd(EXOSpCond_uScm_1, na.rm = T)),
                                               5, Flag_EXOSpCond_uScm_1)) %>%
      dplyr::mutate(Flag_EXOTDS_mgL_1 = ifelse( ! is.na(EXOTDS_mgL_1) & abs(EXOTDS_mgL_1 - mean(EXOTDS_mgL_1, na.rm = T)) > (4* sd(EXOTDS_mgL_1, na.rm = T)),
                                           5, Flag_EXOTDS_mgL_1)) %>% 
      dplyr::mutate(Flag_EXOCond_uScm_9 = ifelse(! is.na(EXOCond_uScm_9) & abs(EXOCond_uScm_9 - mean(EXOCond_uScm_9, na.rm = T)) > (4* sd(EXOCond_uScm_9, na.rm = T)),
                                                   5, Flag_EXOCond_uScm_9)) %>%
      dplyr::mutate(Flag_EXOSpCond_uScm_9 = ifelse( ! is.na(EXOSpCond_uScm_9) & abs(EXOSpCond_uScm_9 - mean(EXOSpCond_uScm_9, na.rm = T)) > (4* sd(EXOSpCond_uScm_9, na.rm = T)),
                                                      5, Flag_EXOSpCond_uScm_9)) %>%
      dplyr::mutate(Flag_EXOTDS_mgL_9 = ifelse( ! is.na(EXOTDS_mgL_9) & abs(EXOTDS_mgL_9 - mean(EXOTDS_mgL_9, na.rm = T)) > (4* sd(EXOTDS_mgL_9, na.rm = T)),
                                                  5, Flag_EXOTDS_mgL_9)) 
    
    
  ########################################################################################################################### 
  #create depth column
  ccrdata=ccrdata %>% dplyr::mutate(Depth_m_13=Lvl_psi_13*0.70455) #1psi=2.31ft, 1ft=0.305m
    
    #offsets from ccr
    ccrdata=ccrdata %>%
      dplyr::mutate(
        depth_1=Depth_m_13-18.94, #Gets depth of thermistor 1
        depth_2=Depth_m_13-18.065, #Gets depth of thermistor 2
        depth_3=Depth_m_13-17.07, #Gets depth of thermistor 3
        depth_4=Depth_m_13-16.075) #Gets depth of thermistor 4. This will have to be recalculated if/when the thermistor comees out of the water.
    
    
    #change the temp to NA when the thermistor is clearly out of the water which we used to determine the depth of the temp string
    #negative depths are changed to NA
    #when depth is NA then the depth of the sensors can't be calculated and are set to NA
  
    #for thermistor at position 1 when it was out of the water 
    ccrdata=ccrdata %>%
      dplyr::mutate(Flag_ThermistorTemp_C_1= ifelse(is.na(Lvl_psi_13) & !is.na(ThermistorTemp_C_1) ,2,Flag_ThermistorTemp_C_1))%>%
      dplyr::mutate(ThermistorTemp_C_1= ifelse(is.na(Lvl_psi_13) & !is.na(ThermistorTemp_C_1) ,NA,ThermistorTemp_C_1))%>%
      dplyr::mutate(Flag_ThermistorTemp_C_1= ifelse(!is.na(depth_1) & depth_1<0 ,2,Flag_ThermistorTemp_C_1))%>%
      dplyr::mutate(ThermistorTemp_C_1=ifelse(!is.na(depth_1) & depth_1<0,NA,ThermistorTemp_C_1))
    
    
    #for thermistor at position 2 when it was out of the water or depth is not recorded
    ccrdata=ccrdata %>%
      dplyr::mutate(Flag_ThermistorTemp_C_2= ifelse(is.na(Lvl_psi_13) & !is.na(ThermistorTemp_C_2),2,Flag_ThermistorTemp_C_2))%>%#this is when the pressure sensor was unplugged
      dplyr::mutate(ThermistorTemp_C_2= ifelse(is.na(Lvl_psi_13) & !is.na(ThermistorTemp_C_2),NA,ThermistorTemp_C_2))%>%
      dplyr::mutate(Flag_ThermistorTemp_C_2= ifelse(!is.na(depth_2) & depth_2<0 ,2,Flag_ThermistorTemp_C_2))%>%
      dplyr::mutate(ThermistorTemp_C_2=ifelse(!is.na(depth_2) & depth_2<0,NA,ThermistorTemp_C_2))
    
    #for thermistor at position 3 when it was out of the water or depth is not recorded
    ccrdata=ccrdata %>%
      dplyr::mutate(Flag_ThermistorTemp_C_3= ifelse(!is.na(depth_3) & depth_3<0 ,2,Flag_ThermistorTemp_C_3),
        ThermistorTemp_C_3=ifelse(!is.na(depth_3) & depth_3<0,NA,ThermistorTemp_C_3))
    
    #for thermistor at position 4 when it was out of the water 
    ccrdata=ccrdata%>%
      dplyr::mutate(
        Flag_ThermistorTemp_C_4= ifelse(!is.na(depth_4) & depth_4<0 ,2,Flag_ThermistorTemp_C_4),
        ThermistorTemp_C_4=ifelse(!is.na(depth_4) & depth_4<0,NA,ThermistorTemp_C_4))
    
    
    #take out the depth columns for thermisotrs depths after you set the values to NA
    ccrdata=ccrdata%>%
      dplyr::select(-depth_1,-depth_2, -depth_3, -depth_4)
    
  ################################################################################################################################  
  # delete EXO_Date and EXO_Time columns
  ccrdata <- ccrdata %>% dplyr::select(-EXO_Date_1,-EXO_Date_9,-EXO_Time_1,-EXO_Time_9)
  
  # reorder columns
  ccrdata <- ccrdata %>% dplyr::select(DateTime, ThermistorTemp_C_1:ThermistorTemp_C_13,
                                       EXOTemp_C_1, EXOCond_uScm_1,
                                       EXOSpCond_uScm_1, EXOTDS_mgL_1, EXODOsat_percent_1, EXODO_mgL_1, EXOChla_RFU_1,
                                       EXOChla_ugL_1, EXOBGAPC_RFU_1, EXOBGAPC_ugL_1, EXOfDOM_RFU_1, EXOfDOM_QSU_1,
                                       EXO_pressure_psi_1, EXO_depth_m_1, EXO_battery_V_1, EXO_cablepower_V_1, EXO_wiper_V_1,
                                       EXOTemp_C_9, EXOCond_uScm_9,
                                       EXOSpCond_uScm_9, EXOTDS_mgL_9, EXODOsat_percent_9, EXODO_mgL_9, 
                                       EXOfDOM_RFU_9, EXOfDOM_QSU_9,EXO_pressure_psi_9, EXO_depth_m_9, EXO_battery_V_9,
                                       EXO_cablepower_V_9, EXO_wiper_V_9,
                                       Lvl_psi_13, Depth_m_13, LvlTemp_C_13,   # removed LvlDepth_m_13,
                                       RECORD, CR3000_Batt_V, CR3000Panel_Temp_C)
    
    # replace NaNs with NAs
    ccrdata[is.na(ccrdata)] <- NA
    
    #order by date and time
    ccrdata <- ccrdata[order(ccrdata$DateTime),]
    
    # convert datetimes to characters so that they are properly formatted in the output file
    ccrdata$DateTime <- as.character(ccrdata$DateTime)
    
  
  if(!is.na(realtime_file)){
    #Different lakes are going to have to modify this for their temperature data format

    d1_og <- ccrdata

    if(!is.na(qaqc_file)){
      d2 <- read.csv(qaqc_file, na.strings = 'NA', stringsAsFactors = FALSE)

      #subset d1 to only dates in d2
      d1 <- d1_og[d1_og$DateTime %in% d2$DateTime,]
      d2 <- d2[d2$DateTime %in% d1$DateTime,]
    }

    d1_og$DateTime <- lubridate::as_datetime(d1_og$DateTime,tz = "UTC")
    d1$TIMESTAMP <- lubridate::as_datetime(d1$DateTime,tz = "UTC")

    d3 <-  data.frame(TIMESTAMP = d1_og$DateTime,
                      wtr_1 = d1_og$ThermistorTemp_C_1, wtr_2 = d1_og$ThermistorTemp_C_2,
                      wtr_3 = d1_og$ThermistorTemp_C_3, wtr_4 = d1_og$ThermistorTemp_C_4,
                      wtr_5 = d1_og$ThermistorTemp_C_5, wtr_6 = d1_og$ThermistorTemp_C_6,
                      wtr_7 = d1_og$ThermistorTemp_C_7, wtr_8 = d1_og$ThermistorTemp_C_8,
                      wtr_9 = d1_og$ThermistorTemp_C_9, wtr_10 = d1_og$ThermistorTemp_C_10,
                      wtr_11 = d1_og$ThermistorTemp_C_11, wtr_12 = d1_og$ThermistorTemp_C_12,
                      wtr_13 = d1_og$ThermistorTemp_C_13, wtr_1_exo = d1_og$EXOTemp_C_1,
                      Chla_1 = d1_og$EXOChla_ugL_1, doobs_1 = d1_og$EXODO_mgL_1,
                      fDOM_1 = d1_og$EXOfDOM_QSU_1, bgapc_1 = d1_og$EXOBGAPC_ugL_1,
                      depth_1 = d1_og$EXO_depth_m_1,
                      wtr_9_exo = d1_og$EXOTemp_C_9,
                      doobs_9 = d1_og$EXODO_mgL_9,  #Chla_9 = d1_og$EXOChla_ugL_9,
                      fDOM_9 = d1_og$EXOfDOM_QSU_9, #bgapc_9 = d1_og$EXOBGAPC_ugL_9,
                      depth_9 = d1_og$EXO_depth_m_9,
                      Depth_m_13=d1_og$Depth_m_13)

    if(!is.na(qaqc_file)){
      TIMESTAMP_in <- as_datetime(d1$DateTime,tz = "EST")
      d1$TIMESTAMP <- with_tz(TIMESTAMP_in,tz = "UTC")

      TIMESTAMP_in <- as_datetime(d2$DateTime,tz = "EST")
      d2$TIMESTAMP <- with_tz(TIMESTAMP_in,tz = "UTC")



     d4 <- data.frame(TIMESTAMP = d2$DateTime,
                      wtr_1 = d2$ThermistorTemp_C_1, wtr_2 = d2$ThermistorTemp_C_2,
                      wtr_3 = d2$ThermistorTemp_C_3, wtr_4 = d2$ThermistorTemp_C_4,
                      wtr_5 = d2$ThermistorTemp_C_5, wtr_6 = d2$ThermistorTemp_C_6,
                      wtr_7 = d2$ThermistorTemp_C_7, wtr_8 = d2$ThermistorTemp_C_8,
                      wtr_9 = d2$ThermistorTemp_C_9, wtr_10 = d2$ThermistorTemp_C_10,
                      wtr_11 = d2$ThermistorTemp_C_11, wtr_12 = d2$ThermistorTemp_C_12,
                      wtr_13 = d2$ThermistorTemp_C_13, wtr_1_exo = d2$EXOTemp_C_1,
                      Chla_1 = d2$EXOChla_ugL_1, doobs_1 = d2$EXODO_mgL_1,
                      fDOM_1 = d2$EXOfDOM_QSU_1, bgapc_1 = d2$EXOBGAPC_ugL_1,
                      depth_1 = d2$EXO_depth_m_1,
                      wtr_9_exo = d2$EXOTemp_C_9,
                      doobs_9 = d2$EXODO_mgL_9,  #Chla_9 = d2$EXOChla_ugL_9,
                      fDOM_9 = d2$EXOfDOM_QSU_9, #bgapc_9 = d2$EXOBGAPC_ugL_9,
                      depth_9 = d2$EXO_depth_m_9,
                      Depth_m_13=d2$LvlDepth_m_13)

    d <- rbind(d3,d4)
    } else{
      d <- d3
    }

  }else{
    #Different lakes are going to have to modify this for their temperature data format
    d1 <- ccrdata
    
    TIMESTAMP_in <- lubridate::as_datetime(d1$DateTime,tz = "UTC") # "UTC" was input_file_tz
    d1$TIMESTAMP <- lubridate::with_tz(TIMESTAMP_in,tz = "UTC")
    
    d <-  data.frame(TIMESTAMP = d1$DateTime, 
                     wtr_1 = d1$ThermistorTemp_C_1, wtr_2 = d1$ThermistorTemp_C_2,
                     wtr_3 = d1$ThermistorTemp_C_3, wtr_4 = d1$ThermistorTemp_C_4,
                     wtr_5 = d1$ThermistorTemp_C_5, wtr_6 = d1$ThermistorTemp_C_6,
                     wtr_7 = d1$ThermistorTemp_C_7, wtr_8 = d1$ThermistorTemp_C_8,
                     wtr_9 = d1$ThermistorTemp_C_9, wtr_10 = d1$ThermistorTemp_C_10,
                     wtr_11 = d1$ThermistorTemp_C_11, wtr_12 = d1$ThermistorTemp_C_12,
                     wtr_13 = d1$ThermistorTemp_C_13, wtr_1_exo = d1$EXOTemp_C_1,
                     Chla_1 = d1$EXOChla_ugL_1, doobs_1 = d1$EXODO_mgL_1,
                     fDOM_1 = d1$EXOfDOM_QSU_1, bgapc_1 = d1$EXOBGAPC_ugL_1,
                     depth_1 = d1$EXO_depth_m_1,
                     wtr_9_exo = d1$EXOTemp_C_9,
                     Chla_9 = d1$EXOChla_ugL_9, doobs_9 = d1$EXODO_mgL_9,
                     fDOM_9 = d1$EXOfDOM_QSU_9, bgapc_9 = d1$EXOBGAPC_ugL_9,
                     depth_9 = d1$EXO_depth_m_9, 
                     Depth_m_13=d1$Depth_m_13)
  }
  
  
  #d$fDOM_1_5 <- config$exo_sensor_2_grab_sample_fdom[1] + config$exo_sensor_2_grab_sample_fdom[2] * d$fDOM_1_5
  
  #oxygen unit conversion
  #d$doobs_1_5 <- d$doobs_1_5*1000/32  #mg/L (obs units) -> mmol/m3 (glm units)
  #d$doobs_6 <- d$doobs_6*1000/32  #mg/L (obs units) -> mmol/m3 (glm units)
  #d$doobs_13 <- d$doobs_13*1000/32  #mg/L (obs units) -> mmol/m3 (glm units)
  
  #d$Chla_1_5 <-  config$exo_sensor_2_ctd_chla[1] +  d$Chla_1_5 *  config$exo_sensor_2_ctd_chla[2]
  #d$doobs_1_5 <- config$exo_sensor_2_ctd_do_1_5[1]  +   d$doobs_1_5 * config$exo_sensor_2_ctd_do_1[2]
  #d$doobs_6 <- config$do_sensor_2_ctd_do_6[1] +   d$doobs_6 * config$do_sensor_2_ctd_do_6[2]
  #d$doobs_13 <- config$do_sensor_2_ctd_do_13[1] +   d$doobs_13 * config$do_sensor_2_ctd_do_13[2]
  
  ######################################################################################
  # END OF QAQC - now transform df from wide to long
  
  d <- d %>%
    dplyr::mutate(day = lubridate::day(TIMESTAMP),
                  year = lubridate::year(TIMESTAMP),
                  hour = lubridate::hour(TIMESTAMP),
                  month = lubridate::month(TIMESTAMP)) %>%
    dplyr::group_by(day, year, hour, month) %>%
    dplyr::select(-TIMESTAMP) %>%
    dplyr::summarise_all(mean, na.rm = TRUE) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(day = as.numeric(day),
                  hour = as.numeric(hour)) %>%
    dplyr::mutate(day = ifelse(as.numeric(day) < 10, paste0("0",day),day),
                  hour = ifelse(as.numeric(hour) < 10, paste0("0",hour),hour)) %>%
    dplyr::mutate(timestamp = lubridate::as_datetime(paste0(year,"-",month,"-",day," ",hour,":00:00"),tz = "UTC")) %>%
    dplyr::arrange(timestamp)

      d_therm <- d %>%
    dplyr::select(timestamp, wtr_1, wtr_2, wtr_3, wtr_4, wtr_5, wtr_6, 
                  wtr_7,wtr_8, wtr_9, wtr_10, wtr_11, wtr_12, wtr_13, Depth_m_13) %>%
    dplyr::rename(
      "0.1" = wtr_1,
      "1.0" = wtr_2,
      "2.0" = wtr_3,
      "3.0" = wtr_4,
      "4.0" = wtr_5,
      "5.0" = wtr_6,
      "6.0" = wtr_7,
      "7.0" = wtr_8,
      "8.0" = wtr_9,
      "10.0" = wtr_10,
      "11.0" = wtr_11,
      "15.0" = wtr_12,
      "19.0" = wtr_13) %>%
    tidyr::pivot_longer(cols = -c(timestamp,Depth_m_13), names_to = "depth", values_to = "observed") %>%
    dplyr::mutate(variable = "temperature",
                  method = "thermistor",
                  observed = ifelse(is.nan(observed), NA, observed))
  
  #no DO sensors in CCR 
  # d_do_temp <- d %>% 
  #   dplyr::select(timestamp, wtr_6_do, wtr_13_do, Depth_m_13) %>%
  #   dplyr::rename("6.0" = wtr_6_do,
  #                 "13.0" = wtr_13_do) %>%
  #   tidyr::pivot_longer(cols = -c(timestamp,Depth_m_13), names_to = "depth", values_to = "observed") %>%
  #   dplyr::mutate(variable = "temperature",
  #                 method = "do_sensor",
  #                 observed = ifelse(is.nan(observed), NA, observed))
  
  d_exo_temp <- d %>%
    dplyr::select(timestamp, wtr_1_exo, wtr_9_exo, Depth_m_13) %>%
    dplyr::rename("1.0" = wtr_1_exo,
                  "9.0" = wtr_9_exo) %>%
    tidyr::pivot_longer(cols = -c(timestamp, Depth_m_13), names_to = "depth", values_to = "observed") %>%
    mutate(variable = "temperature",
           method = "exo_sensor",
           observed = ifelse(is.nan(observed), NA, observed))
  
  # d_do_do <- d %>%
  #   dplyr::select(timestamp, doobs_6, doobs_13, Depth_m_13) %>%
  #   dplyr::rename("6.0" = doobs_6,
  #          "13.0" = doobs_13) %>%
  #   tidyr::pivot_longer(cols = -c(timestamp, Depth_m_13), names_to = "depth", values_to = "observed") %>%
  #   mutate(variable = "oxygen",
  #          method = "do_sensor",
  #          observed = ifelse(is.nan(observed), NA, observed))
  
  d_exo_do <- d %>%
    dplyr::select(timestamp, doobs_1, doobs_9, Depth_m_13) %>%
    dplyr::rename("1.0" = doobs_1,
                  "9.0" = doobs_9) %>%
    tidyr::pivot_longer(cols = -c(timestamp, Depth_m_13), names_to = "depth", values_to = "observed") %>%
    mutate(variable = "oxygen",
           method = "exo_sensor",
           observed = ifelse(is.nan(observed), NA, observed))
  
  d_exo_fdom <- d %>%
    dplyr::select(timestamp, fDOM_1, fDOM_9, Depth_m_13) %>%
    dplyr::rename("1.0" = fDOM_1,
                  "9.0" = fDOM_9) %>%
    tidyr::pivot_longer(cols = -c(timestamp, Depth_m_13), names_to = "depth", values_to = "observed") %>%
    mutate(variable = "fdom",
           method = "exo_sensor",
           observed = ifelse(is.nan(observed), NA, observed))
  
  d_exo_chla <- d %>%
    dplyr::select(timestamp, Chla_1, Depth_m_13) %>%
    dplyr::rename("1.0" = Chla_1) %>%
    tidyr::pivot_longer(cols = -c(timestamp, Depth_m_13), names_to = "depth", values_to = "observed") %>%
    mutate(variable = "chla",
           method = "exo_sensor",
           observed = ifelse(is.nan(observed), NA, observed))
  
  d_exo_bgapc <- d %>%
    dplyr::select(timestamp, bgapc_1, Depth_m_13) %>%
    dplyr::rename("1." = bgapc_1) %>%
    tidyr::pivot_longer(cols = -c(timestamp, Depth_m_13), names_to = "depth", values_to = "observed") %>%
    mutate(variable = "bgapc",
           method = "exo_sensor",
           observed = ifelse(is.nan(observed), NA, observed))
  
  d <- rbind(d_therm,d_exo_temp,d_exo_do,d_exo_fdom,
             d_exo_chla,d_exo_bgapc) #,d_do_temp,d_do_do
  
  d <- d %>% dplyr::mutate(depth = depth) 
  
  d <- d %>%
    dplyr::rename(time = timestamp)
  
  #drop NA rows and negative depths
  d <- d[!is.na(d$observed),]
  
  d <- d[d$depth >=0,]
  
  ######################################################################################
  #now read in the offset file and calculate depth for each sensor/position
  offsets <- readr::read_csv(offset_file) %>% dplyr::select(-c(Reservoir,Site))
  
  #make depth numeric so it matches offset position col
  d$depth <- as.numeric(d$depth)
  
  #rename depth to position for left_join
  names(d)[3] <- "Position"
  
  #add in offsets to df
  ccr_depths <- dplyr::left_join(d, offsets)
  
  #load plotly for this section only 
  #library(plyr)
  
  # # The pressure sensor was moved to be in line with the bottom thermistor. The top two thermistors has slid closer to each other
  # # and were re-secured about a meter a part from each other. Because of this we need to filter before 2021-04-05 13:20:00 EST
  # # and after. The top two thermistors exact offset will have to be determined again when the water level is high enough again. 
  # ccr_pre_05APR21=ccr_depths%>%
  #   dplyr::filter(time<="2021-04-05 13:20")%>%
  #   dplyr::mutate(Sensor_depth=Depth_m_13-Offset_before_05APR21)%>% #this gives you the depth of the thermistors from the surface
  #   dplyr::mutate(depth=round_any(Sensor_depth, 0.5))#Round to the nearest 0.5
  # 
  # ccr_post_05APR21=ccr_depths%>%
  #   dplyr::filter(time>"2021-04-05 13:20")%>%
  #   dplyr::mutate(Sensor_depth=Depth_m_13-Offset_after_05APR21)%>% #this gives you the depth of the thermistor from the surface
  #   dplyr::mutate(depth=plyr::round_any(Sensor_depth, 0.5)) #Round to the nearest 0.5
  # 
  # #unload plotly becuase it messes w/ dplyr
  # detach("package:plyr", unload = TRUE)
  # 
  # # combine the pre April 5th and the post April 5th. Drop if the values are NA. Drop if the sensor depth is NA because can't
  # # figure out the depth of the sensors. This will give you a depth for each sensor reading. 
  # ccr_by_depth=ccr_pre_05APR21%>%
  #   rbind(.,ccr_post_05APR21)%>%
  #   dplyr::filter(!is.na(observed))%>%
  #   dplyr::filter(!is.na(depth))%>%
  #   dplyr::select(-Offset_before_05APR21, -Offset_after_05APR21, -Distance_above_sediments, -Depth_m_13, -Sensor_depth, -Position)
  # 
  # #drop NA rows and negative depths
  # ccr_by_depth <- ccr_by_depth[!is.na(ccr_by_depth$observed),]
  # 
  # ccr_by_depth <- ccr_by_depth[ccr_by_depth$depth >=0,]
  # 
  # #rename 0 depth to be 0.1
  # ccr_by_depth$depth[ccr_by_depth$depth == 0] <- 0.1
  # 
  # # write to output file
  # return(ccr_by_depth)
  
  #removing lines about becasue no pressure issue at CCR
  #write to output file 
  return(ccr_depths)
}


#figs
#plot(d$timestamp[d$variable=="oxygen" & d$depth==1.5], d$value[d$variable=="oxygen"& d$depth==1.5],col="magenta", xlab="",ylab="Dissolved oxygen (mg/L)",type="l", ylim=c(0,440))
#points(d$timestamp[d$variable=="oxygen" & d$depth=="6.0"], d$value[d$variable=="oxygen"& d$depth=="6.0"],col="black",type="l")
#points(d$timestamp[d$variable=="oxygen" & d$depth=="13.0"], d$value[d$variable=="oxygen"& d$depth=="13.0"],col="mediumseagreen",type="l")
#legend("bottomright", legend=c("1m", "6m", "13m"), text.col=c("magenta","black","mediumseagreen"), bty='n')

# ###Prep RemoveMet for final file version
# 
#   names(log)=c("Station", "DateTime_start","DateTime_end", "Parameter", "ColumnNumber", "Flag", "Notes")
#   
#   log=log%>%
#   mutate(Reservoir="BVR")%>%
#   mutate(Site=50)%>%
#   mutate(Station="BVR_sensor_string")%>%
#   select(Reservoir, Site, Station, DateTime_start, DateTime_end, Parameter, ColumnNumber, Flag, Notes)
# 
# 
# write.csv(log, output2_file, row.names = FALSE, quote=FALSE)


# example usage


#qaqc('https://github.com/FLARE-forecast/BVRE-data/raw/bvre-platform-data/BVRplatform.csv',
#      'https://raw.githubusercontent.com/CareyLabVT/ManualDownloadsSCCData/master/BVRplatform_manual_2020.csv',
#      "https://raw.githubusercontent.com/FLARE-forecast/BVRE-data/bvre-platform-data/BVR_maintenance_log.txt",
#       "BVRplatform_clean.csv", 
#     "BVR_Maintenance_2020.csv")
