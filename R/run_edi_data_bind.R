# Package ID: edi.1069.1 Cataloging System:https://pasta.edirepository.org.
# Data set title: Time series of high-frequency sensor data measuring water temperature, dissolved oxygen, pressure, conductivity,           specific conductance, total dissolved solids, chlorophyll a, phycocyanin, and fluorescent dissolved organic matter at discrete depths           in Carvins Cove Reservoir, Virginia, USA in 2021.
# Data set creator:  Cayelan Carey - Virginia Tech 
# Data set creator:  Adrienne Breef-Pilz - Virginia Tech 
# Data set creator:  Dexter Howard - Virginia Tech 
# Contact:  Cayelan Carey -  Virginia Tech  - cayelan@vt.edu
# Stylesheet v2.11 for metadata conversion into program: John H. Porter, Univ. Virginia, jporter@virginia.edu 

inUrl1  <- "https://pasta.lternet.edu/package/data/eml/edi/1069/1/57267535da5ab0687d2fee52083699f8" 
infile1 <- tempfile()
try(download.file(inUrl1,infile1,method="curl"))
if (is.na(file.size(infile1))) download.file(inUrl1,infile1,method="auto")


dt1 <-read.csv(infile1,header=F 
               ,skip=1
               ,sep=","  
               , col.names=c(
                 "Reservoir",     
                 "Site",     
                 "DateTime",     
                 "ThermistorTemp_C_1",     
                 "ThermistorTemp_C_2",     
                 "ThermistorTemp_C_3",     
                 "ThermistorTemp_C_4",     
                 "ThermistorTemp_C_5",     
                 "ThermistorTemp_C_6",     
                 "ThermistorTemp_C_7",     
                 "ThermistorTemp_C_8",     
                 "ThermistorTemp_C_9",     
                 "ThermistorTemp_C_10",     
                 "ThermistorTemp_C_11",     
                 "ThermistorTemp_C_12",     
                 "ThermistorTemp_C_13",     
                 "EXOTemp_C_1",     
                 "EXOCond_uScm_1",     
                 "EXOSpCond_uScm_1",     
                 "EXOTDS_mgL_1",     
                 "EXODOsat_percent_1",     
                 "EXODO_mgL_1",     
                 "EXOChla_RFU_1",     
                 "EXOChla_ugL_1",     
                 "EXOBGAPC_RFU_1",     
                 "EXOBGAPC_ugL_1",     
                 "EXOfDOM_RFU_1",     
                 "EXOfDOM_QSU_1",     
                 "EXO_pressure_psi_1",     
                 "EXO_depth_m_1",     
                 "EXO_battery_V_1",     
                 "EXO_cablepower_V_1",     
                 "EXO_wiper_V_1",     
                 "EXOTemp_C_9",     
                 "EXOCond_uScm_9",     
                 "EXOSpCond_uScm_9",     
                 "EXOTDS_mgL_9",     
                 "EXODOsat_percent_9",     
                 "EXODO_mgL_9",     
                 "EXOfDOM_RFU_9",     
                 "EXOfDOM_QSU_9",     
                 "EXO_pressure_psi_9",     
                 "EXO_depth_m_9",     
                 "EXO_battery_V_9",     
                 "EXO_cablepower_V_9",     
                 "EXO_wiper_V_9",     
                 "Lvl_psi_13",     
                 "LvlDepth_m_13",     
                 "LvlTemp_C_13",     
                 "RECORD",     
                 "CR3000_Batt_V",     
                 "CR3000Panel_Temp_C",     
                 "Flag_Temp_1",     
                 "Flag_Temp_2",     
                 "Flag_Temp_3",     
                 "Flag_Temp_4",     
                 "Flag_Temp_5",     
                 "Flag_Temp_6",     
                 "Flag_Temp_7",     
                 "Flag_Temp_8",     
                 "Flag_Temp_9",     
                 "Flag_Temp_10",     
                 "Flag_Temp_11",     
                 "Flag_Temp_12",     
                 "Flag_Temp_13",     
                 "Flag_Pres_13",     
                 "Flag_EXOTemp_1",     
                 "Flag_EXOCond_1",     
                 "Flag_EXOSpCond_1",     
                 "Flag_EXOTDS_1",     
                 "Flag_EXODO_sat_1",     
                 "Flag_EXODO_obs_1",     
                 "Flag_EXOChla_RFU_1",     
                 "Flag_EXOChla_ugL_1",     
                 "Flag_EXOPhyco_RFU_1",     
                 "Flag_EXOPhyco_ugL_1",     
                 "Flag_EXOfDOM_1",     
                 "Flag_EXOPres_1",     
                 "Flag_EXOdep_1",     
                 "Flag_EXObat_1",     
                 "Flag_EXOcab_1",     
                 "Flag_EXOwip_1",     
                 "Flag_EXOTemp_9",     
                 "Flag_EXOCond_9",     
                 "Flag_EXOSpCond_9",     
                 "Flag_EXOTDS_9",     
                 "Flag_EXODO_sat_9",     
                 "Flag_EXODO_obs_9",     
                 "Flag_EXOfDOM_9",     
                 "Flag_EXOPres_9",     
                 "Flag_EXOdep_9",     
                 "Flag_EXObat_9",     
                 "Flag_EXOcab_9",     
                 "Flag_EXOwip_9"    ), check.names=TRUE)

unlink(infile1)

# Fix any interval or ratio columns mistakenly read in as nominal and nominal columns read as numeric or dates read as strings

if (class(dt1$Reservoir)!="factor") dt1$Reservoir<- as.factor(dt1$Reservoir)
if (class(dt1$Site)=="factor") dt1$Site <-as.numeric(levels(dt1$Site))[as.integer(dt1$Site) ]               
if (class(dt1$Site)=="character") dt1$Site <-as.numeric(dt1$Site)                                   
# attempting to convert dt1$DateTime dateTime string to R date structure (date or POSIXct)                                
tmpDateFormat<-"%Y-%m-%d %H:%M:%S" 
tmp1DateTime<-as.POSIXct(dt1$DateTime,format=tmpDateFormat)
# Keep the new dates only if they all converted correctly
if(length(tmp1DateTime) == length(tmp1DateTime[!is.na(tmp1DateTime)])){dt1$DateTime <- tmp1DateTime } else {print("Date conversion failed for dt1$DateTime. Please inspect the data and do the date conversion yourself.")}                                                                    
rm(tmpDateFormat,tmp1DateTime) 
if (class(dt1$ThermistorTemp_C_1)=="factor") dt1$ThermistorTemp_C_1 <-as.numeric(levels(dt1$ThermistorTemp_C_1))[as.integer(dt1$ThermistorTemp_C_1) ]               
if (class(dt1$ThermistorTemp_C_1)=="character") dt1$ThermistorTemp_C_1 <-as.numeric(dt1$ThermistorTemp_C_1)
if (class(dt1$ThermistorTemp_C_2)=="factor") dt1$ThermistorTemp_C_2 <-as.numeric(levels(dt1$ThermistorTemp_C_2))[as.integer(dt1$ThermistorTemp_C_2) ]               
if (class(dt1$ThermistorTemp_C_2)=="character") dt1$ThermistorTemp_C_2 <-as.numeric(dt1$ThermistorTemp_C_2)
if (class(dt1$ThermistorTemp_C_3)=="factor") dt1$ThermistorTemp_C_3 <-as.numeric(levels(dt1$ThermistorTemp_C_3))[as.integer(dt1$ThermistorTemp_C_3) ]               
if (class(dt1$ThermistorTemp_C_3)=="character") dt1$ThermistorTemp_C_3 <-as.numeric(dt1$ThermistorTemp_C_3)
if (class(dt1$ThermistorTemp_C_4)=="factor") dt1$ThermistorTemp_C_4 <-as.numeric(levels(dt1$ThermistorTemp_C_4))[as.integer(dt1$ThermistorTemp_C_4) ]               
if (class(dt1$ThermistorTemp_C_4)=="character") dt1$ThermistorTemp_C_4 <-as.numeric(dt1$ThermistorTemp_C_4)
if (class(dt1$ThermistorTemp_C_5)=="factor") dt1$ThermistorTemp_C_5 <-as.numeric(levels(dt1$ThermistorTemp_C_5))[as.integer(dt1$ThermistorTemp_C_5) ]               
if (class(dt1$ThermistorTemp_C_5)=="character") dt1$ThermistorTemp_C_5 <-as.numeric(dt1$ThermistorTemp_C_5)
if (class(dt1$ThermistorTemp_C_6)=="factor") dt1$ThermistorTemp_C_6 <-as.numeric(levels(dt1$ThermistorTemp_C_6))[as.integer(dt1$ThermistorTemp_C_6) ]               
if (class(dt1$ThermistorTemp_C_6)=="character") dt1$ThermistorTemp_C_6 <-as.numeric(dt1$ThermistorTemp_C_6)
if (class(dt1$ThermistorTemp_C_7)=="factor") dt1$ThermistorTemp_C_7 <-as.numeric(levels(dt1$ThermistorTemp_C_7))[as.integer(dt1$ThermistorTemp_C_7) ]               
if (class(dt1$ThermistorTemp_C_7)=="character") dt1$ThermistorTemp_C_7 <-as.numeric(dt1$ThermistorTemp_C_7)
if (class(dt1$ThermistorTemp_C_8)=="factor") dt1$ThermistorTemp_C_8 <-as.numeric(levels(dt1$ThermistorTemp_C_8))[as.integer(dt1$ThermistorTemp_C_8) ]               
if (class(dt1$ThermistorTemp_C_8)=="character") dt1$ThermistorTemp_C_8 <-as.numeric(dt1$ThermistorTemp_C_8)
if (class(dt1$ThermistorTemp_C_9)=="factor") dt1$ThermistorTemp_C_9 <-as.numeric(levels(dt1$ThermistorTemp_C_9))[as.integer(dt1$ThermistorTemp_C_9) ]               
if (class(dt1$ThermistorTemp_C_9)=="character") dt1$ThermistorTemp_C_9 <-as.numeric(dt1$ThermistorTemp_C_9)
if (class(dt1$ThermistorTemp_C_10)=="factor") dt1$ThermistorTemp_C_10 <-as.numeric(levels(dt1$ThermistorTemp_C_10))[as.integer(dt1$ThermistorTemp_C_10) ]               
if (class(dt1$ThermistorTemp_C_10)=="character") dt1$ThermistorTemp_C_10 <-as.numeric(dt1$ThermistorTemp_C_10)
if (class(dt1$ThermistorTemp_C_11)=="factor") dt1$ThermistorTemp_C_11 <-as.numeric(levels(dt1$ThermistorTemp_C_11))[as.integer(dt1$ThermistorTemp_C_11) ]               
if (class(dt1$ThermistorTemp_C_11)=="character") dt1$ThermistorTemp_C_11 <-as.numeric(dt1$ThermistorTemp_C_11)
if (class(dt1$ThermistorTemp_C_12)=="factor") dt1$ThermistorTemp_C_12 <-as.numeric(levels(dt1$ThermistorTemp_C_12))[as.integer(dt1$ThermistorTemp_C_12) ]               
if (class(dt1$ThermistorTemp_C_12)=="character") dt1$ThermistorTemp_C_12 <-as.numeric(dt1$ThermistorTemp_C_12)
if (class(dt1$ThermistorTemp_C_13)=="factor") dt1$ThermistorTemp_C_13 <-as.numeric(levels(dt1$ThermistorTemp_C_13))[as.integer(dt1$ThermistorTemp_C_13) ]               
if (class(dt1$ThermistorTemp_C_13)=="character") dt1$ThermistorTemp_C_13 <-as.numeric(dt1$ThermistorTemp_C_13)
if (class(dt1$EXOTemp_C_1)=="factor") dt1$EXOTemp_C_1 <-as.numeric(levels(dt1$EXOTemp_C_1))[as.integer(dt1$EXOTemp_C_1) ]               
if (class(dt1$EXOTemp_C_1)=="character") dt1$EXOTemp_C_1 <-as.numeric(dt1$EXOTemp_C_1)
if (class(dt1$EXOCond_uScm_1)=="factor") dt1$EXOCond_uScm_1 <-as.numeric(levels(dt1$EXOCond_uScm_1))[as.integer(dt1$EXOCond_uScm_1) ]               
if (class(dt1$EXOCond_uScm_1)=="character") dt1$EXOCond_uScm_1 <-as.numeric(dt1$EXOCond_uScm_1)
if (class(dt1$EXOSpCond_uScm_1)=="factor") dt1$EXOSpCond_uScm_1 <-as.numeric(levels(dt1$EXOSpCond_uScm_1))[as.integer(dt1$EXOSpCond_uScm_1) ]               
if (class(dt1$EXOSpCond_uScm_1)=="character") dt1$EXOSpCond_uScm_1 <-as.numeric(dt1$EXOSpCond_uScm_1)
if (class(dt1$EXOTDS_mgL_1)=="factor") dt1$EXOTDS_mgL_1 <-as.numeric(levels(dt1$EXOTDS_mgL_1))[as.integer(dt1$EXOTDS_mgL_1) ]               
if (class(dt1$EXOTDS_mgL_1)=="character") dt1$EXOTDS_mgL_1 <-as.numeric(dt1$EXOTDS_mgL_1)
if (class(dt1$EXODOsat_percent_1)=="factor") dt1$EXODOsat_percent_1 <-as.numeric(levels(dt1$EXODOsat_percent_1))[as.integer(dt1$EXODOsat_percent_1) ]               
if (class(dt1$EXODOsat_percent_1)=="character") dt1$EXODOsat_percent_1 <-as.numeric(dt1$EXODOsat_percent_1)
if (class(dt1$EXODO_mgL_1)=="factor") dt1$EXODO_mgL_1 <-as.numeric(levels(dt1$EXODO_mgL_1))[as.integer(dt1$EXODO_mgL_1) ]               
if (class(dt1$EXODO_mgL_1)=="character") dt1$EXODO_mgL_1 <-as.numeric(dt1$EXODO_mgL_1)
if (class(dt1$EXOChla_RFU_1)=="factor") dt1$EXOChla_RFU_1 <-as.numeric(levels(dt1$EXOChla_RFU_1))[as.integer(dt1$EXOChla_RFU_1) ]               
if (class(dt1$EXOChla_RFU_1)=="character") dt1$EXOChla_RFU_1 <-as.numeric(dt1$EXOChla_RFU_1)
if (class(dt1$EXOChla_ugL_1)=="factor") dt1$EXOChla_ugL_1 <-as.numeric(levels(dt1$EXOChla_ugL_1))[as.integer(dt1$EXOChla_ugL_1) ]               
if (class(dt1$EXOChla_ugL_1)=="character") dt1$EXOChla_ugL_1 <-as.numeric(dt1$EXOChla_ugL_1)
if (class(dt1$EXOBGAPC_RFU_1)=="factor") dt1$EXOBGAPC_RFU_1 <-as.numeric(levels(dt1$EXOBGAPC_RFU_1))[as.integer(dt1$EXOBGAPC_RFU_1) ]               
if (class(dt1$EXOBGAPC_RFU_1)=="character") dt1$EXOBGAPC_RFU_1 <-as.numeric(dt1$EXOBGAPC_RFU_1)
if (class(dt1$EXOBGAPC_ugL_1)=="factor") dt1$EXOBGAPC_ugL_1 <-as.numeric(levels(dt1$EXOBGAPC_ugL_1))[as.integer(dt1$EXOBGAPC_ugL_1) ]               
if (class(dt1$EXOBGAPC_ugL_1)=="character") dt1$EXOBGAPC_ugL_1 <-as.numeric(dt1$EXOBGAPC_ugL_1)
if (class(dt1$EXOfDOM_RFU_1)=="factor") dt1$EXOfDOM_RFU_1 <-as.numeric(levels(dt1$EXOfDOM_RFU_1))[as.integer(dt1$EXOfDOM_RFU_1) ]               
if (class(dt1$EXOfDOM_RFU_1)=="character") dt1$EXOfDOM_RFU_1 <-as.numeric(dt1$EXOfDOM_RFU_1)
if (class(dt1$EXOfDOM_QSU_1)=="factor") dt1$EXOfDOM_QSU_1 <-as.numeric(levels(dt1$EXOfDOM_QSU_1))[as.integer(dt1$EXOfDOM_QSU_1) ]               
if (class(dt1$EXOfDOM_QSU_1)=="character") dt1$EXOfDOM_QSU_1 <-as.numeric(dt1$EXOfDOM_QSU_1)
if (class(dt1$EXO_pressure_psi_1)=="factor") dt1$EXO_pressure_psi_1 <-as.numeric(levels(dt1$EXO_pressure_psi_1))[as.integer(dt1$EXO_pressure_psi_1) ]               
if (class(dt1$EXO_pressure_psi_1)=="character") dt1$EXO_pressure_psi_1 <-as.numeric(dt1$EXO_pressure_psi_1)
if (class(dt1$EXO_depth_m_1)=="factor") dt1$EXO_depth_m_1 <-as.numeric(levels(dt1$EXO_depth_m_1))[as.integer(dt1$EXO_depth_m_1) ]               
if (class(dt1$EXO_depth_m_1)=="character") dt1$EXO_depth_m_1 <-as.numeric(dt1$EXO_depth_m_1)
if (class(dt1$EXO_battery_V_1)=="factor") dt1$EXO_battery_V_1 <-as.numeric(levels(dt1$EXO_battery_V_1))[as.integer(dt1$EXO_battery_V_1) ]               
if (class(dt1$EXO_battery_V_1)=="character") dt1$EXO_battery_V_1 <-as.numeric(dt1$EXO_battery_V_1)
if (class(dt1$EXO_cablepower_V_1)=="factor") dt1$EXO_cablepower_V_1 <-as.numeric(levels(dt1$EXO_cablepower_V_1))[as.integer(dt1$EXO_cablepower_V_1) ]               
if (class(dt1$EXO_cablepower_V_1)=="character") dt1$EXO_cablepower_V_1 <-as.numeric(dt1$EXO_cablepower_V_1)
if (class(dt1$EXO_wiper_V_1)=="factor") dt1$EXO_wiper_V_1 <-as.numeric(levels(dt1$EXO_wiper_V_1))[as.integer(dt1$EXO_wiper_V_1) ]               
if (class(dt1$EXO_wiper_V_1)=="character") dt1$EXO_wiper_V_1 <-as.numeric(dt1$EXO_wiper_V_1)
if (class(dt1$EXOTemp_C_9)=="factor") dt1$EXOTemp_C_9 <-as.numeric(levels(dt1$EXOTemp_C_9))[as.integer(dt1$EXOTemp_C_9) ]               
if (class(dt1$EXOTemp_C_9)=="character") dt1$EXOTemp_C_9 <-as.numeric(dt1$EXOTemp_C_9)
if (class(dt1$EXOCond_uScm_9)=="factor") dt1$EXOCond_uScm_9 <-as.numeric(levels(dt1$EXOCond_uScm_9))[as.integer(dt1$EXOCond_uScm_9) ]               
if (class(dt1$EXOCond_uScm_9)=="character") dt1$EXOCond_uScm_9 <-as.numeric(dt1$EXOCond_uScm_9)
if (class(dt1$EXOSpCond_uScm_9)=="factor") dt1$EXOSpCond_uScm_9 <-as.numeric(levels(dt1$EXOSpCond_uScm_9))[as.integer(dt1$EXOSpCond_uScm_9) ]               
if (class(dt1$EXOSpCond_uScm_9)=="character") dt1$EXOSpCond_uScm_9 <-as.numeric(dt1$EXOSpCond_uScm_9)
if (class(dt1$EXOTDS_mgL_9)=="factor") dt1$EXOTDS_mgL_9 <-as.numeric(levels(dt1$EXOTDS_mgL_9))[as.integer(dt1$EXOTDS_mgL_9) ]               
if (class(dt1$EXOTDS_mgL_9)=="character") dt1$EXOTDS_mgL_9 <-as.numeric(dt1$EXOTDS_mgL_9)
if (class(dt1$EXODOsat_percent_9)=="factor") dt1$EXODOsat_percent_9 <-as.numeric(levels(dt1$EXODOsat_percent_9))[as.integer(dt1$EXODOsat_percent_9) ]               
if (class(dt1$EXODOsat_percent_9)=="character") dt1$EXODOsat_percent_9 <-as.numeric(dt1$EXODOsat_percent_9)
if (class(dt1$EXODO_mgL_9)=="factor") dt1$EXODO_mgL_9 <-as.numeric(levels(dt1$EXODO_mgL_9))[as.integer(dt1$EXODO_mgL_9) ]               
if (class(dt1$EXODO_mgL_9)=="character") dt1$EXODO_mgL_9 <-as.numeric(dt1$EXODO_mgL_9)
if (class(dt1$EXOfDOM_RFU_9)=="factor") dt1$EXOfDOM_RFU_9 <-as.numeric(levels(dt1$EXOfDOM_RFU_9))[as.integer(dt1$EXOfDOM_RFU_9) ]               
if (class(dt1$EXOfDOM_RFU_9)=="character") dt1$EXOfDOM_RFU_9 <-as.numeric(dt1$EXOfDOM_RFU_9)
if (class(dt1$EXOfDOM_QSU_9)=="factor") dt1$EXOfDOM_QSU_9 <-as.numeric(levels(dt1$EXOfDOM_QSU_9))[as.integer(dt1$EXOfDOM_QSU_9) ]               
if (class(dt1$EXOfDOM_QSU_9)=="character") dt1$EXOfDOM_QSU_9 <-as.numeric(dt1$EXOfDOM_QSU_9)
if (class(dt1$EXO_pressure_psi_9)=="factor") dt1$EXO_pressure_psi_9 <-as.numeric(levels(dt1$EXO_pressure_psi_9))[as.integer(dt1$EXO_pressure_psi_9) ]               
if (class(dt1$EXO_pressure_psi_9)=="character") dt1$EXO_pressure_psi_9 <-as.numeric(dt1$EXO_pressure_psi_9)
if (class(dt1$EXO_depth_m_9)=="factor") dt1$EXO_depth_m_9 <-as.numeric(levels(dt1$EXO_depth_m_9))[as.integer(dt1$EXO_depth_m_9) ]               
if (class(dt1$EXO_depth_m_9)=="character") dt1$EXO_depth_m_9 <-as.numeric(dt1$EXO_depth_m_9)
if (class(dt1$EXO_battery_V_9)=="factor") dt1$EXO_battery_V_9 <-as.numeric(levels(dt1$EXO_battery_V_9))[as.integer(dt1$EXO_battery_V_9) ]               
if (class(dt1$EXO_battery_V_9)=="character") dt1$EXO_battery_V_9 <-as.numeric(dt1$EXO_battery_V_9)
if (class(dt1$EXO_cablepower_V_9)=="factor") dt1$EXO_cablepower_V_9 <-as.numeric(levels(dt1$EXO_cablepower_V_9))[as.integer(dt1$EXO_cablepower_V_9) ]               
if (class(dt1$EXO_cablepower_V_9)=="character") dt1$EXO_cablepower_V_9 <-as.numeric(dt1$EXO_cablepower_V_9)
if (class(dt1$EXO_wiper_V_9)=="factor") dt1$EXO_wiper_V_9 <-as.numeric(levels(dt1$EXO_wiper_V_9))[as.integer(dt1$EXO_wiper_V_9) ]               
if (class(dt1$EXO_wiper_V_9)=="character") dt1$EXO_wiper_V_9 <-as.numeric(dt1$EXO_wiper_V_9)
if (class(dt1$Lvl_psi_13)=="factor") dt1$Lvl_psi_13 <-as.numeric(levels(dt1$Lvl_psi_13))[as.integer(dt1$Lvl_psi_13) ]               
if (class(dt1$Lvl_psi_13)=="character") dt1$Lvl_psi_13 <-as.numeric(dt1$Lvl_psi_13)
if (class(dt1$LvlDepth_m_13)=="factor") dt1$LvlDepth_m_13 <-as.numeric(levels(dt1$LvlDepth_m_13))[as.integer(dt1$LvlDepth_m_13) ]               
if (class(dt1$LvlDepth_m_13)=="character") dt1$LvlDepth_m_13 <-as.numeric(dt1$LvlDepth_m_13)
if (class(dt1$LvlTemp_C_13)=="factor") dt1$LvlTemp_C_13 <-as.numeric(levels(dt1$LvlTemp_C_13))[as.integer(dt1$LvlTemp_C_13) ]               
if (class(dt1$LvlTemp_C_13)=="character") dt1$LvlTemp_C_13 <-as.numeric(dt1$LvlTemp_C_13)
if (class(dt1$RECORD)=="factor") dt1$RECORD <-as.numeric(levels(dt1$RECORD))[as.integer(dt1$RECORD) ]               
if (class(dt1$RECORD)=="character") dt1$RECORD <-as.numeric(dt1$RECORD)
if (class(dt1$CR3000_Batt_V)=="factor") dt1$CR3000_Batt_V <-as.numeric(levels(dt1$CR3000_Batt_V))[as.integer(dt1$CR3000_Batt_V) ]               
if (class(dt1$CR3000_Batt_V)=="character") dt1$CR3000_Batt_V <-as.numeric(dt1$CR3000_Batt_V)
if (class(dt1$CR3000Panel_Temp_C)=="factor") dt1$CR3000Panel_Temp_C <-as.numeric(levels(dt1$CR3000Panel_Temp_C))[as.integer(dt1$CR3000Panel_Temp_C) ]               
if (class(dt1$CR3000Panel_Temp_C)=="character") dt1$CR3000Panel_Temp_C <-as.numeric(dt1$CR3000Panel_Temp_C)
if (class(dt1$Flag_Temp_1)!="factor") dt1$Flag_Temp_1<- as.factor(dt1$Flag_Temp_1)
if (class(dt1$Flag_Temp_2)!="factor") dt1$Flag_Temp_2<- as.factor(dt1$Flag_Temp_2)
if (class(dt1$Flag_Temp_3)!="factor") dt1$Flag_Temp_3<- as.factor(dt1$Flag_Temp_3)
if (class(dt1$Flag_Temp_4)!="factor") dt1$Flag_Temp_4<- as.factor(dt1$Flag_Temp_4)
if (class(dt1$Flag_Temp_5)!="factor") dt1$Flag_Temp_5<- as.factor(dt1$Flag_Temp_5)
if (class(dt1$Flag_Temp_6)!="factor") dt1$Flag_Temp_6<- as.factor(dt1$Flag_Temp_6)
if (class(dt1$Flag_Temp_7)!="factor") dt1$Flag_Temp_7<- as.factor(dt1$Flag_Temp_7)
if (class(dt1$Flag_Temp_8)!="factor") dt1$Flag_Temp_8<- as.factor(dt1$Flag_Temp_8)
if (class(dt1$Flag_Temp_9)!="factor") dt1$Flag_Temp_9<- as.factor(dt1$Flag_Temp_9)
if (class(dt1$Flag_Temp_10)!="factor") dt1$Flag_Temp_10<- as.factor(dt1$Flag_Temp_10)
if (class(dt1$Flag_Temp_11)!="factor") dt1$Flag_Temp_11<- as.factor(dt1$Flag_Temp_11)
if (class(dt1$Flag_Temp_12)!="factor") dt1$Flag_Temp_12<- as.factor(dt1$Flag_Temp_12)
if (class(dt1$Flag_Temp_13)!="factor") dt1$Flag_Temp_13<- as.factor(dt1$Flag_Temp_13)
if (class(dt1$Flag_Pres_13)!="factor") dt1$Flag_Pres_13<- as.factor(dt1$Flag_Pres_13)
if (class(dt1$Flag_EXOTemp_1)!="factor") dt1$Flag_EXOTemp_1<- as.factor(dt1$Flag_EXOTemp_1)
if (class(dt1$Flag_EXOCond_1)!="factor") dt1$Flag_EXOCond_1<- as.factor(dt1$Flag_EXOCond_1)
if (class(dt1$Flag_EXOSpCond_1)!="factor") dt1$Flag_EXOSpCond_1<- as.factor(dt1$Flag_EXOSpCond_1)
if (class(dt1$Flag_EXOTDS_1)!="factor") dt1$Flag_EXOTDS_1<- as.factor(dt1$Flag_EXOTDS_1)
if (class(dt1$Flag_EXODO_sat_1)!="factor") dt1$Flag_EXODO_sat_1<- as.factor(dt1$Flag_EXODO_sat_1)
if (class(dt1$Flag_EXODO_obs_1)!="factor") dt1$Flag_EXODO_obs_1<- as.factor(dt1$Flag_EXODO_obs_1)
if (class(dt1$Flag_EXOChla_RFU_1)!="factor") dt1$Flag_EXOChla_RFU_1<- as.factor(dt1$Flag_EXOChla_RFU_1)
if (class(dt1$Flag_EXOChla_ugL_1)!="factor") dt1$Flag_EXOChla_ugL_1<- as.factor(dt1$Flag_EXOChla_ugL_1)
if (class(dt1$Flag_EXOPhyco_RFU_1)!="factor") dt1$Flag_EXOPhyco_RFU_1<- as.factor(dt1$Flag_EXOPhyco_RFU_1)
if (class(dt1$Flag_EXOPhyco_ugL_1)!="factor") dt1$Flag_EXOPhyco_ugL_1<- as.factor(dt1$Flag_EXOPhyco_ugL_1)
if (class(dt1$Flag_EXOfDOM_1)!="factor") dt1$Flag_EXOfDOM_1<- as.factor(dt1$Flag_EXOfDOM_1)
if (class(dt1$Flag_EXOPres_1)!="factor") dt1$Flag_EXOPres_1<- as.factor(dt1$Flag_EXOPres_1)
if (class(dt1$Flag_EXOdep_1)!="factor") dt1$Flag_EXOdep_1<- as.factor(dt1$Flag_EXOdep_1)
if (class(dt1$Flag_EXObat_1)!="factor") dt1$Flag_EXObat_1<- as.factor(dt1$Flag_EXObat_1)
if (class(dt1$Flag_EXOcab_1)!="factor") dt1$Flag_EXOcab_1<- as.factor(dt1$Flag_EXOcab_1)
if (class(dt1$Flag_EXOwip_1)!="factor") dt1$Flag_EXOwip_1<- as.factor(dt1$Flag_EXOwip_1)
if (class(dt1$Flag_EXOTemp_9)!="factor") dt1$Flag_EXOTemp_9<- as.factor(dt1$Flag_EXOTemp_9)
if (class(dt1$Flag_EXOCond_9)!="factor") dt1$Flag_EXOCond_9<- as.factor(dt1$Flag_EXOCond_9)
if (class(dt1$Flag_EXOSpCond_9)!="factor") dt1$Flag_EXOSpCond_9<- as.factor(dt1$Flag_EXOSpCond_9)
if (class(dt1$Flag_EXOTDS_9)!="factor") dt1$Flag_EXOTDS_9<- as.factor(dt1$Flag_EXOTDS_9)
if (class(dt1$Flag_EXODO_sat_9)!="factor") dt1$Flag_EXODO_sat_9<- as.factor(dt1$Flag_EXODO_sat_9)
if (class(dt1$Flag_EXODO_obs_9)!="factor") dt1$Flag_EXODO_obs_9<- as.factor(dt1$Flag_EXODO_obs_9)
if (class(dt1$Flag_EXOfDOM_9)!="factor") dt1$Flag_EXOfDOM_9<- as.factor(dt1$Flag_EXOfDOM_9)
if (class(dt1$Flag_EXOPres_9)!="factor") dt1$Flag_EXOPres_9<- as.factor(dt1$Flag_EXOPres_9)
if (class(dt1$Flag_EXOdep_9)!="factor") dt1$Flag_EXOdep_9<- as.factor(dt1$Flag_EXOdep_9)
if (class(dt1$Flag_EXObat_9)!="factor") dt1$Flag_EXObat_9<- as.factor(dt1$Flag_EXObat_9)
if (class(dt1$Flag_EXOcab_9)!="factor") dt1$Flag_EXOcab_9<- as.factor(dt1$Flag_EXOcab_9)
if (class(dt1$Flag_EXOwip_9)!="factor") dt1$Flag_EXOwip_9<- as.factor(dt1$Flag_EXOwip_9)


edi_file <- dt1

source('R/edi_qaqc_combine.R')
library(tidyverse)
library(lubridate)
library(yaml)
library(EDIutils)

# open config file
config_file <- read_yaml('~/CCRE-forecast-code/configuration/default/observation_processing.yml')

## read offset file 
entity_names <- read_data_entity_names(packageId = 'edi.1069.1')
offset <- read_data_entity(packageId = 'edi.1069.1', entityId = entity_names$entityId[2])
offset <- readr::read_csv(file = offset)

L1_file <- read.csv('https://raw.githubusercontent.com/addelany/CCRE-data/ccre-dam-data/ccre-waterquality_L1.csv', 
                    na.strings = 'NA', stringsAsFactors = FALSE)

test_df <- qaqc_edi_combine(realtime_file = L1_file, qaqc_file = edi_file, offset_file = offset, config = config_file)
