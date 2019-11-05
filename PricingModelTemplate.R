rm(list = ls())
########################################################
# Package load, Source Pricing Model function, set working directory, and set up DB connection.
library(data.table)
library(odbc)
library(lubridate)
source("//admiral.local/shares/depts/pricing/PricingModel/R/R_Pricing_Model.R")

#####################################################################################
# This loads the tables for the pricing model
####################################################################################
if(!exists('premium_sql_pull')){premium_sql_pull <- getPremLoss()}
if(!exists('quote_sql_pull')){quote_sql_pull <- getQuoteData()}

####################################################################################
# This is the part of the code where you select which variables you want from PM_premium_F
# It then joins this dataset to the premium data gathered above
####################################################################################
rm(sql_prem,sqldata_prem, pm_premloss, sql_quote, pm_quote)
# original pull
sql_prem <- "select pmr.VehicleTermKey
, CurrentCarrierExt
, PriorBILimit_F
, Limit_BI
, Limit_PD
, Limit_UMBI
, Limit_UMPD
, TermNumber 
from data.PM_Reporting as pmr join data.PCRating as pcr 
on pmr.VehicleFixedID = pcr.VehicleFixedID and pmr.JobID = pcr.JobID 
join data.PCRatingSup as sup 
on pmr.VehicleFixedID = sup.VehicleFixedID and pmr.JobID = sup.JobID 
where pmr.BoundIndicator = 1;"
# "select pmr.VehicleTermKey
# , pmr.PrimaryDriverAge as PrimAge
# , pmr.PrimaryDriverGender as PrimGen
# , case when [State] = 'MD' then 'MD' else 'CW' end as [State]
# , case when AgeDifference is NULL THEN 'z) Single Driv'
#                  WHEN AgeDifference < -25 THEN 'a) Less than -25'
# WHEN -25 <= AgeDifference and AgeDifference <= -13 THEN 'b) -25 to -13'
# WHEN -12 <= AgeDifference and AgeDifference <= -8 THEN 'c) -12 to -8'
# WHEN -7 <= AgeDifference and AgeDifference <= -6 THEN 'd) -7 to -6'
# WHEN -5 <= AgeDifference and AgeDifference <= -4 THEN 'e) -5 to -4'
# WHEN  -3 <= AgeDifference and AgeDifference <= -2 THEN 'f) -3 to -2'
# WHEN -1 <= AgeDifference and AgeDifference <= 0 THEN 'g) -1 to 0'
# WHEN  1 <= AgeDifference and AgeDifference <= 2 THEN 'h) 1 to 2'
# WHEN  3 <= AgeDifference and AgeDifference <= 5 THEN 'i) 3 to 5'
# WHEN  6 <= AgeDifference and AgeDifference <= 7 THEN 'j) 6 to 7'
# WHEN  8 <= AgeDifference and AgeDifference <= 12 THEN 'k) 8 to 12'
# WHEN  13 <= AgeDifference and AgeDifference <= 25 THEN  'l) 13 to 25'
# WHEN  26 <= AgeDifference THEN 'm) More than 25' end as YNP_Group
# , PrimaryDriverYearsLicensed as YearsLic
# , YNPMainVehicleFlag as YNPMainVehicle
# ,PrimaryDriverMaritalStatus_F
# from data.PCRating as pcr join data.PM_Reporting as pmr on pcr.JobID = pmr.JobID and pcr.VehicleFixedID = pmr.VehicleFixedID where BoundIndicator = 1;"

#"select ESigDiscount, TermNumber_F, VehicleTermKey from Data.PM_Reporting where BoundIndicator = 1"

sqldata_prem  <- getQuery(sql_prem) 
pm_premloss   <- merge(premium_sql_pull, sqldata_prem, all.x = F, by = "VehicleTermKey")
#pm_premloss1 <- merge(pm_premloss, adj, all.x = T, by = c("PrimAge", "PrimGen", "State", "YearsLic", "YNPMainVehicle", "YNP_Group"))

##########################################################################################
# This is where you grab the same factors for quotes. If the variable doesn't exist for 
# quotes, you can assign it a constant value
# e.g. 1 as TermNumber_F
##########################################################################################
sql_quote <- "select pcr.JobID as RowKey
, CurrentCarrierExt
, PriorBILimit_F
, Limit_BI
, Limit_PD
, Limit_UMBI
, Limit_UMPD
, TermNumber 
from data.PM_Reporting as pmr join data.PCRating as pcr 
on pmr.VehicleFixedID = pcr.VehicleFixedID and pmr.JobID = pcr.JobID 
join data.PCRatingSup as pcs 
on pmr.VehicleFixedID = pcs.VehicleFixedID and pmr.JobID = pcs.JobID
JOIN DataWarehouse.dbo.DimQuote DQ 
 on DQ.QuoteVersionKey  = pcs.QuoteVersionKey
where pmr.BoundIndicator = 0
AND DQ.QuoteVersionDateTime >= '2019-06-30'
AND (DQ.LastQuoteVersionIndicator = 'T' OR DQ.QuoteVersionBoundIndicator = 'T');"
# "select pcr.JobID as  RowKey, pmr.PrimaryDriverAge as PrimAge, pmr.PrimaryDriverGender as PrimGen, pcr.[State] FROM   SandboxPricing.data.PM PMQ
#        JOIN data.PM_Reporting PMR
# ON pmq.JobID = pmr.JobID
# AND pmq.VehicleFixedID = pmr.VehicleFixedID
# JOIN data.PCRating PCR
# ON PCR.JobID = pmq.JobID
# AND pcr.VehicleFixedID = pmq.VehicleFixedID
# JOIN data.PCRatingSup pcs
# ON pcs.JobID = pcr.JobID
# AND pcs.VehicleFixedID = pcr.VehicleFixedID
# JOIN DataWarehouse.dbo.DimQuote DQ 
# on DQ.QuoteVersionKey  = pcs.QuoteVersionKey
# WHERE  1=1
# and pmq.BoundIndicator = 0 
# AND DQ.QuoteVersionDateTime >= '2015-01-01'
# AND (DQ.LastQuoteVersionIndicator = 'T' OR DQ.QuoteVersionBoundIndicator = 'T');"

sqldata_quote  <- getQuery(sql_quote)
pm_quote       <- merge(quote_sql_pull, sqldata_quote, all.x = F, by = "RowKey")

##########################################################################################
# This is where amazing happens. This uses the datasets constructed above and performs all 
# the necessary transformations to create the pricing model. It will then open and output
# the pricing model
##########################################################################################


pm <- pricingModel(premLossData = pm_premloss
                   , quoteData = pm_quote
                   , premLossStartDate  = "2016-09-30" , EndDate  = "2019-09-30"
                   , salequoteStartDate = "2019-06-30" 
                   , renewalStartDate   = "2019-06-30" 
                   , rateFactors = c("PolicyStateCode_F", 'CurrentCarrierExt', 'PriorBILimit_F', 'Limit_BI', 'Limit_PD', 'Limit_UMBI', 'Limit_UMPD', 'TermNumber')
                   , saveToOutput = F
                   , returnOutput = F
)

View(adj)
pm_premloss[PrimAge == adj[1,PrimAge], ]
