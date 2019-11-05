require(odbc)
require(data.table)
require(lubridate)

#############################################
########### Pricing Model Source ############
#############################################
# Elephant Auto Insurance

# Functions:
# refreshPremLoss - Fetches Premium/Loss information and saves to PM_PremLoss.RData in PricingModel\RData Folder
# refreshQuotes   - Fetches Quote information and saves to PM_Quote.RData in PricingModel\RData Folder
# pricingModelAgg - Runs Pricing Model on PM_PremLoss/PM_Quote. Outputs a list as argument to be passed to excelOutput()
# excelOutput     - Takes list from pricingModelAgg and populates new pricing model, saves it
# pricingModel    - Wrapper for pricingModelAgg and excelOutput

# Debugging

#save(pm, file=paste0(datafolder, "pm.Rdata"))
#save(loss, file=paste0(datafolder, "loss.Rdata"))
#save(ibnr, file=paste0(datafolder, "ibnr.Rdata"))
#save(quote, file=paste0(datafolder, "quote.Rdata"))

#load(paste0(pdrive, "pm.Rdata"))
#load(paste0(pdrive, "loss.Rdata"))
#load(paste0(pdrive, "ibnr.Rdata"))
#load(paste0(pdrive, "quote.Rdata"))

getQuery <- function(sql, server = 'vaprd-dw', database = 'SandboxPricing'){
  con <- dbConnect(odbc::odbc(),
                   driver = "ODBC Driver 13 for SQL Server",
                   server = server,
                   database = database,
                   trusted_connection = "Yes")
  rs <- dbSendQuery(con, sql)
  fetchedrows <- dbFetch(rs)
  dbClearResult(rs)
  dbDisconnect(con)
  return(as.data.table(fetchedrows))
}

#############################################
#############################################
# Refresh PremLoss & Quote Tables

getPremLoss <- function(){
  
  sql_pm <- "
SELECT pm.[PolicyTermKey],
       pm.[VehicleTermKey],
       pm.PolicyNumber                   AS [OrigCorePolicyNumber],
       pm.PolicyNumber                   AS [CorePolicyNumber],
       pm.PeriodStart                    [PolicyTermStartEffDate],
       pm.PeriodEnd                      PolicyTermExpirationEffDate,
       Year(PM.PeriodStart) AS UWYear,
       --UWYear,
       [TermType],
       UpdateTime                        AS [TransactionDateTime],
       Case when  pm.TransactionType = 'Submission' then 'New Business' else pm.TransactionType end as TransactionType,
       [CancelReason_F],
       CancellationDate,
       [FinanceChannel_F],
       pf.PolicyStateCode_F,
       [VehicleCount],
       pm.state                          [PolicyStateCode],
       MaxLoadDate                       AS [EarnedAsOfDate],
       MaxLoadDate                       AS [RetainedAsOfDate],
       [Retained6Months],
       [Retained12Months],
       pm.JobID                          [TransactionJobID],
       pm.VehicleFixedID                 [SourceSystemVehicleID],
       [PrimaryDriverAge],
       [PrimaryDriverGender],
       RetentionTier2                    AS [RetentionTierProb],
       [RetentionTier2_Calculated],
       [VehicleYearNumber],
       [SelectedBILimit_f],
       OptionalCoverage_F                AS [OptionalCoverageType],
       DownPaymentsMonthNumber AS DownPaymentMonthsNumber ,
       ( pm.[WP_BI] + pm.[WP_EXP] + pm.[WP_COLL]
         + pm.[WP_COMP] + pm.[WP_CUST] + pm.[WP_LOAN]
         + pm.[WP_MED] + pm.[WP_PIP] + pm.[WP_ILC]
         + pm.[WP_PD] + pm.[WP_RR] + pm.[WP_ERS]
         + pm.[WP_UMBI] + pm.[WP_UMPD] ) AS WP_Total,
       pm.[WP_BI]                        WP_BI,
       pm.[WP_EXP]                       WP_BIFEE,
       pm.[WP_COLL]                      WP_COL,
       pm.[WP_COMP]                      WP_COMP,
       pm.[WP_CUST]                      WP_CE,
       pm.[WP_LOAN]                      WP_LOAN,
       pm.[WP_MED]                       WP_MED,
       pm.[WP_PIP]                       WP_PIP,
       pm.[WP_ILC]                       WP_PIPWAGE,
       pm.[WP_PD]                        WP_PD,
       pm.[WP_RR]                        WP_RENT,
       pm.[WP_ERS]                       WP_TOW,
       pm.[WP_UMBI]                      WP_UMBI,
       pm.[WP_UMPD]                      WP_UMPD,
       0                                 AS WE_TOTAL,
       CASE
         WHEN WP_BI > 0 THEN 1
         ELSE 0
       END                               AS WE_BI,
       CASE
         WHEN [WP_EXP] > 0 THEN 1
         ELSE 0
       END                               AS WE_BIFEE,
       CASE
         WHEN [WP_COLL] > 0 THEN 1
         ELSE 0
       END                               AS WE_COL,
       CASE
         WHEN [WP_COMP] > 0 THEN 1
         ELSE 0
       END                               AS WE_COMP,
       CASE
         WHEN [WP_CUST] > 0 THEN 1
         ELSE 0
       END                               AS WE_CE,
       CASE
         WHEN [WP_LOAN] > 0 THEN 1
         ELSE 0
       END                               AS WE_LOAN,
       CASE
         WHEN [WP_MED] > 0 THEN 1
         ELSE 0
       END                               AS WE_MED,
       CASE
         WHEN [WP_PIP] > 0 THEN 1
         ELSE 0
       END                               AS WE_PIP,
       CASE
         WHEN [WP_ILC] > 0 THEN 1
         ELSE 0
       END                               AS WE_PIPWAGE,
       CASE
         WHEN [WP_PD] > 0 THEN 1
         ELSE 0
       END                               AS WE_PD,
       CASE
         WHEN [WP_RR] > 0 THEN 1
         ELSE 0
       END                               AS WE_RENT,
       CASE
         WHEN [WP_ERS] > 0 THEN 1
         ELSE 0
       END                               AS WE_TOW,
       CASE
         WHEN [WP_UMBI] > 0 THEN 1
         ELSE 0
       END                               AS WE_UMBI,
       CASE
         WHEN [WP_UMPD] > 0 THEN 1
         ELSE 0
       END                               AS WE_UMPD,
       ISNULL([EP_TOTAL] ,0)                       EP_TOTAL,
       ISNULL([EP_BI]    ,0)                       EP_BI,
       ISNULL([EP_EXP]   ,0)                       EP_BIFEE,
       ISNULL([EP_COLL]  ,0)                       EP_COL,
       ISNULL([EP_COMP]  ,0)                       EP_COMP,
       ISNULL([EP_CE]    ,0)                       EP_CE,
       ISNULL([EP_LOAN]  ,0)                       EP_LOAN,
       ISNULL([EP_MED]   ,0)                       EP_MED,
       ISNULL([EP_PIP]   ,0)                       EP_PIP,
       ISNULL([EP_ILC]   ,0)                       EP_PIPWAGE,
       ISNULL([EP_PD]    ,0)                       EP_PD,
       ISNULL([EP_RR]    ,0)                       EP_RENT,
       ISNULL([EP_ERS]   ,0)                       EP_TOW,
       ISNULL([EP_UMBI]  ,0)                       EP_UMBI,
       ISNULL([EP_UMPD]  ,0)                       EP_UMPD,
       [CLEP_BI]                         CLEP_BI,
       [CLEP_EXP]                        CLEP_BIFEE,
       [CLEP_COLL]                       CLEP_COL,
       [CLEP_COMP]                       CLEP_COMP,
       [CLEP_CE]                         CLEP_CE,
       [CLEP_LOAN]                       CLEP_LOAN,
       [CLEP_MED]                        CLEP_MED,
       [CLEP_PIP]                        CLEP_PIP,
       [CLEP_ILC]                        CLEP_PIPWAGE,
       [CLEP_PD]                         CLEP_PD,
       [CLEP_RR]                         CLEP_RENT,
       [CLEP_ERS]                        CLEP_TOW,
       [CLEP_UMBI]                       CLEP_UMBI,
       [CLEP_UMPD]                       CLEP_UMPD,
       [EE_BI]                           EE_BI,
       [EE_EXP]                          EE_BIFEE,
       [EE_COLL]                         EE_COL,
       [EE_COMP]                         EE_COMP,
       [EE_CE]                           EE_CE,
       [EE_LOAN]                         EE_LOAN,
       [EE_MED]                          EE_MED,
       [EE_PIP]                          EE_PIP,
       [EE_ILC]                          EE_PIPWAGE,
       [EE_PD]                           EE_PD,
       EE_RR                             EE_RENT,
       [EE_ERS]                          EE_TOW,
       [EE_UMBI]                         EE_UMBI,
       [EE_UMPD]                         EE_UMPD
FROM   [SandboxPricing].Data.PM pm
       JOIN SandboxPricing.Data.PM_Reporting pf
         ON pm.VehicleTermKey = pf.VehicleTermKey
WHERE  PM.BoundIndicator = 1 
AND PM.PeriodStart >= '2015-01-01'
"
  
  sql_loss <- "SELECT [VehicleTermKey]
      ,[PolicyTermKey]
  ,[OrigCorePolicyNumber]
  ,[CorePolicyNumber]
  ,[CurrentCorePolicyNumber]
  ,[PolicyTermStartEffDate]
  ,[PolicyTermExpirationEffDate]
  ,[VehicleIdentificationNumber]
  ,[VehicleMakeName]
  ,[VehicleModelName]
  ,[ClaimNumber]
  ,[ExposureID]
  ,[SourceSystemTransactionNumber]
  ,[LossDate]
  ,[TransactionDate]
  ,[FiCalDate]
  ,CASE WHEN CatastropheKey != 1 AND CoverageType = 'Comprehensive' then 'COMP_CAT' else CoverageType End As CoverageType
  ,[NetPaidClaims]
  ,[NetCaseReserves]
  ,[Payment]
  ,[Reserve]
  ,[RecoveryReserve]
  ,[Salvage]
  ,[Subrogation]
  ,[CatastropheKey] FROM SandboxPricing.Data.TransactionLoss"
  
  sql_ibnr <- "SELECT [VehicleTermKey]
      ,[PolicyTermKey]
      ,[PeriodStart] AS PolicyTermStartEffDate
      ,[MonthAgeDateKey]
      ,[IBNR_BI]
      ,[IBNR_COL]
      ,[IBNR_COMP]
      ,[IBNR_CE]
      ,[IBNR_LOAN]
      ,[IBNR_MED]
      ,[IBNR_PIP]
      ,[IBNR_PIPWAGE]
      ,[IBNR_PD]
      ,[IBNR_RENT]
      ,[IBNR_UMBI]
      ,[IBNR_UMPD]
      ,[IBNR_TOTAL]
  FROM [SandboxPricing].[Data].[SummaryIBNR]"
  

  pm    <- getQuery(sql_pm)
  maxearn <- max(pm$EarnedAsOfDate)
  
  loss  <- getQuery(sql_loss)
  loss  <- loss[TransactionDate <= maxearn]
  
  ibnr  <- getQuery(sql_ibnr)
  
  setkey(pm  , VehicleTermKey)
  setkey(loss, VehicleTermKey)
  setkey(ibnr, VehicleTermKey)
  
  ibnr_covs <- c("BI", "PD", "COMP", "COL", "UMBI", "UMPD", "MED", "PIP", "PIPWAGE", "LOAN", "CE", "RENT")
  loss_covs <- c("BI", "PD", "COMP", "COMP_CAT", "COL", "UMBI", "UMPD", "MED", "PIP", "LOAN", "CE", "RENT", "PIPWAGE", "TOW")
  covmap <- list(BI="BodilyInjury", PD="PropertyDamage", COMP="Comprehensive", COMP_CAT = 'COMP_CAT', COL="Collision", UMBI="UMBI"
                 , UMPD="UMPD", RENT="RentalReimbursement", MED="MedPay", PIP="PIP"                 
                 , LOAN="LoanGap", TOW="TowingLabor", CE="CustomEquipment", PIPWAGE="PIPLostWages", UNK="Unknown")
  
  # Change full coverage names to short names
  loss[, CoverageType := as.factor(CoverageType)]
  levels(loss$CoverageType) <- covmap
  
  # Summarize, creating reinsurance loss totals and claim counts, add in IBNR and pivot to single row per VTK
  loss_summary <- loss[, .(LOSS_RI = sum(NetPaidClaims + NetCaseReserves), LOSS_CC = uniqueN(ClaimNumber)), by = c("VehicleTermKey", "CoverageType")]
  loss_totalcc <- loss[, .(LOSS_CC_TOTAL = uniqueN(ClaimNumber)), by = VehicleTermKey]
  loss_pivot <- dcast.data.table(loss_summary, VehicleTermKey ~ CoverageType, value.var = c("LOSS_RI", "LOSS_CC"), fill = 0)[loss_totalcc]
  
  # Merge in IBNR and then cleanup
  loss_ibnr <- merge(ibnr, loss_pivot, by = "VehicleTermKey", all.x = TRUE)
  for (j in names(loss_ibnr)) set(loss_ibnr, which(is.na(loss_ibnr[[j]])), j, 0)
  loss_ibnr[,(paste0("LOSS_RI_", ibnr_covs)) := Map("+", mget(paste0("LOSS_RI_", ibnr_covs)), mget(paste0("IBNR_", ibnr_covs)))]
  loss_ibnr[,(c(paste0("IBNR_", ibnr_covs), "IBNR_TOTAL", "MonthAgeDateKey", "PolicyTermStartEffDate")) := NULL]
  
  # Capping
  for(j in loss_covs){
    loss_ibnr[,paste0(c("LOSS_25_", "LOSS_100_"), j) := mget(paste0("LOSS_RI_", j))]
    set(loss_ibnr, which(loss_ibnr[[paste0("LOSS_RI_", j)]] > 25000), paste0("LOSS_25_", j), 25000)
    set(loss_ibnr, which(loss_ibnr[[paste0("LOSS_RI_", j)]] > 100000), paste0("LOSS_100_", j), 100000)
  }
  
  #############################################
  # Join to PM_Premium, order columns and save base table to PM Folder
  
  prem_covs     <- c("BI", "PD", "COL", "COMP", "CE", "LOAN", "MED", "PIP", "PIPWAGE", "RENT", "TOW", "UMBI", "UMPD", "BIFEE")

  idvar         <- c("VehicleTermKey", "PolicyTermKey","OrigCorePolicyNumber","CorePolicyNumber", 
                     "PolicyTermStartEffDate", "PolicyTermExpirationEffDate", "PolicyStateCode_F","UWYear"
                    ,"TermType","TransactionDateTime", "TransactionType", "CancelReason_F", "CancellationDate" ,
                    "FinanceChannel_F", "TransactionJobID", "SourceSystemVehicleID","VehicleCount", "PrimaryDriverAge", 
                    "PrimaryDriverGender", "RetentionTierProb", "RetentionTier2_Calculated", "VehicleYearNumber", "SelectedBILimit_f", "OptionalCoverageType", 
                    "DownPaymentMonthsNumber")
  retvar        <- c("RetainedAsOfDate", "Retained6Months", "Retained12Months", "EarnedAsOfDate")
  
  wp_cols       <- paste0("WP_"      , prem_covs)
  adj_wp_cols   <- paste0("ADJ_WP_"  , prem_covs)
  ee_cols       <- paste0("EE_"      , prem_covs)
  ep_cols       <- paste0("EP_"      , c(prem_covs, "TOTAL"))
  clep_cols     <- paste0("CLEP_"    , prem_covs)
  loss_cc_cols  <- paste0("LOSS_CC_" , c(loss_covs, "TOTAL"))
  loss_ri_cols  <- paste0("LOSS_RI_" , loss_covs)
  loss_25_cols  <- paste0("LOSS_25_" , loss_covs)
  loss_100_cols <- paste0("LOSS_100_", loss_covs)
  
  pm_premloss <- loss_ibnr[pm, c(idvar, retvar, wp_cols, ep_cols, ee_cols, clep_cols
                            , loss_cc_cols, loss_ri_cols, loss_25_cols, loss_100_cols), with = F]
  
  pm_premloss <- pm_premloss[ , FinanceChannel := ifelse(FinanceChannel_F == 'PriceComparison','PC',
                                                  ifelse(FinanceChannel_F == 'WebOnly','Web','Agent'))]
  

  return(pm_premloss)
}

getQuoteData <- function(){


  sql_quote <- "SELECT PMQ.JobID             RowKey,
       PMQ.State             AS PolicyStateCode,
       PMR.FinanceChannel_F  AS FinanceChannel,
       pcs.QuoteID           AS QuoteID,
       PMQ.VehicleFixedID    AS VehicleKey,
       DQ.QuoteVersionDateTime        AS QuoteVersionDate,
       Year(PMQ.PeriodStart) AS UWYear,
       PMR.VehicleCount,
       PCR.WP_BI,
       PCR.WP_PD,
       PCR.WP_UMBI,
       PCR.WP_UMPD,
       PCR.WP_COLL           WP_COL,
       PCR.WP_COMP,
       PCR.WP_PIP,
       PCR.WP_ILC            WP_PIPWAGE,
       PCR.WP_LOAN,
       PCR.WP_MED,
       PCR.WP_CUST           WP_CE,
       PCR.WP_RR             WP_RENT,
       PCR.WP_ERS            WP_TOW,
       PCR.WP_EXP            WP_BIFEE
--select count(*)
FROM   SandboxPricing.data.PM PMQ
       JOIN data.PM_Reporting PMR
         ON pmq.JobID = pmr.JobID
            AND pmq.VehicleFixedID = pmr.VehicleFixedID
       JOIN data.PCRating PCR
         ON PCR.JobID = pmq.JobID
            AND pcr.VehicleFixedID = pmq.VehicleFixedID
       JOIN data.PCRatingSup pcs
         ON pcs.JobID = pcr.JobID
            AND pcs.VehicleFixedID = pcr.VehicleFixedID
		JOIN DataWarehouse.dbo.DimQuote DQ 
		on DQ.QuoteVersionKey  = pcs.QuoteVersionKey
WHERE  1=1
  --and pmq.BoundIndicator = 0 
  AND DQ.QuoteVersionDateTime >= '2015-01-01'
  AND (DQ.LastQuoteVersionIndicator = 'T' OR DQ.QuoteVersionBoundIndicator = 'T')
"
  
  pm_quote <- getQuery(sql_quote)
  pm_quote <- pm_quote[ , FinanceChannel := ifelse(FinanceChannel == 'PriceComparison','PC',
                                            ifelse(FinanceChannel == 'WebOnly','Web','Agent'))]
  

  return(pm_quote)
}


#############################################
#############################################
#Pricing Model Source
premLossData = pm_premloss
quoteData = pm_quote
premLossStartDate  = "2017-01-01"
EndDate = '2019-07-01'
salequoteStartDate = "2019-01-01"
renewalStartDate   = "2019-01-01"
rateFactors = c("PolicyStateCode_F", "PrimaryDriverEducation", "PrimaryDriverMaritalStatus_F")
returnOutput = F

pricingModelAgg <- function(premLossData, quoteData
                            , premLossStartDate, EndDate
                            , salequoteStartDate
                            , renewalStartDate
                            , rateFactors
                            , returnOutput){
  
  `%ni%` <- Negate(`%in%`)  
  
  if(length(rateFactors) >= 9) stop("You're trying to use too many rating factors. Right now the limit is 8 or less.")
  
  if(is.null(quote)){cat("No Quote Data")}
  
  ########################### Init Col Listings
  
  month1<- as.Date(EndDate) %m-% months(1)

  month1              <- ymd(month1)
  premLossStartDate   <- ymd(premLossStartDate)  
  EndDate             <- ymd(EndDate)     
  salequoteStartDate  <- ymd(salequoteStartDate) 
  renewalStartDate    <- ymd(renewalStartDate)    
  premLossData$PolicyTermStartEffDate <- ymd(premLossData$PolicyTermStartEffDate)
  
  premLossData <- premLossData[TermType == 'New Business' | is.na(TermType), TermType := 'NB']
  premLossData <- premLossData[TermType == 'Renewal'     , TermType := 'RN']
  premLossData <- premLossData[TransactionType == 'Renewal'     , TermType := 'RN']
  premLossData <- premLossData[TransactionType == 'New Business', TermType := 'NB']
  

  ####################################################################
  # Clean Up Term and UWYear Errors and limit to 3 distinct UW Years  

  
  # We have switched the logic to look back 1,2,3 years instead of the Actual UWYear
  
  premLossData$UWYear <- ifelse(premLossData$PolicyTermStartEffDate >= ymd(EndDate) - years(1) , 1, premLossData$UWYear)
  premLossData$UWYear <- ifelse(premLossData$PolicyTermStartEffDate >= ymd(EndDate) - years(2) & premLossData$PolicyTermStartEffDate < ymd(EndDate) - years(1) , 2, premLossData$UWYear)
  premLossData$UWYear <- ifelse(premLossData$PolicyTermStartEffDate < ymd(EndDate) - years(2), 3, premLossData$UWYear)
  

    quoteData$UWYear <- ifelse(quoteData$QuoteVersionDate >= ymd(EndDate) - years(1), 1, quoteData$UWYear)
    quoteData$UWYear <- ifelse(quoteData$QuoteVersionDate >= ymd(EndDate) - years(2) & quoteData$QuoteVersionDate < ymd(EndDate) - years(1), 2, quoteData$UWYear)
    quoteData$UWYear <- ifelse(quoteData$QuoteVersionDate < ymd(EndDate) - years(2), 3, quoteData$UWYear)

  

  ####################################################################################################################
  
  loss_covs     <- c("BI", "PD", "COL", "COMP", "COMP_CAT", "CE", "LOAN", "MED", "PIP", "PIPWAGE", "RENT", "TOW", "UMBI", "UMPD")
  prem_covs     <- c("BI", "PD", "COL", "COMP", "CE", "LOAN", "MED", "PIP", "PIPWAGE", "RENT", "TOW", "UMBI", "UMPD", "BIFEE")
  
  #Quote Columns by Channel, Coverage and Vehicle/Policy: PC - Price Comparison, W - Web Only, A - Everything Else
  
  pcvq_cols     <- paste0("PCVQ_",prem_covs)
  pcpq_cols     <- paste0("PCPQ_",prem_covs)
  wvq_cols      <- paste0("WVQ_",prem_covs)
  wpq_cols      <- paste0("WPQ_",prem_covs)
  avq_cols      <- paste0("AVQ_",prem_covs)
  apq_cols      <- paste0("APQ_",prem_covs)
  tvq_cols      <- paste0('TVQ_', prem_covs)
  tpq_cols      <- paste0('TPQ_', prem_covs)
  
  pcvq_colsM1 <- paste0(pcvq_cols, 'M1')
  pcvq_colsQB <- paste0(pcvq_cols, 'QB')
  pcpq_colsM1 <- paste0(pcpq_cols, 'M1')
  pcpq_colsQB <- paste0(pcpq_cols, 'QB')
  wvq_colsM1  <- paste0(wvq_cols, 'M1' )
  wvq_colsQB  <- paste0(wvq_cols, 'QB' )
  wpq_colsM1  <- paste0(wpq_cols, 'M1' )
  wpq_colsQB  <- paste0(wpq_cols, 'QB' )
  avq_colsM1  <- paste0(avq_cols, 'M1' )
  avq_colsQB  <- paste0(avq_cols, 'QB' )
  apq_colsM1  <- paste0(apq_cols, 'M1' )
  apq_colsQB  <- paste0(apq_cols, 'QB' )
  tvq_colsM1  <- paste0(tvq_cols, 'M1')
  tvq_colsQB  <- paste0(tvq_cols, 'QB')
  tpq_colsM1  <- paste0(tpq_cols, 'M1')
  tpq_colsQB   <- paste0(tpq_cols, 'QB')
  
  quote_cols <- c(pcvq_colsQB
                 , wvq_colsQB  
                 , avq_colsQB   
                 , tpq_colsQB) 
  
  #Sale Columns by Channel, Coverage and Vehicle/Policy: PC - Price Comparison, W - Web Only, A - Everything Else
  
  pcvs_cols     <- paste0("PCVS_",prem_covs)
  pcps_cols     <- paste0("PCPS_",prem_covs)
  wvs_cols      <- paste0("WVS_",prem_covs)
  wps_cols      <- paste0("WPS_",prem_covs)
  avs_cols      <- paste0("AVS_",prem_covs)
  aps_cols      <- paste0("APS_",prem_covs)
  tvs_cols      <- paste0('TVS_', prem_covs)
  tps_cols      <- paste0('TPS_', prem_covs)
  
  pcvs_colsQB     <- paste0(pcvs_cols, 'QB' )
  pcvs_colsPB     <- paste0(pcvs_cols, 'PB' )
  pcps_colsQB    <- paste0(pcps_cols, 'QB' )
  pcps_colsPB     <- paste0(pcps_cols, 'PB' )
  wvs_colsQB      <- paste0(wvs_cols, 'QB' )
  wvs_colsPB      <- paste0(wvs_cols, 'PB' )
  wps_colsQB      <- paste0(wps_cols, 'QB' )
  wps_colsPB      <- paste0(wps_cols, 'PB' )
  avs_colsQB      <- paste0(avs_cols, 'QB' )
  avs_colsPB      <- paste0(avs_cols, 'PB' )
  aps_colsQB      <- paste0(aps_cols, 'QB' )
  aps_colsPB      <- paste0(aps_cols, 'PB' )
  tvs_colsQB      <- paste0(tvs_cols,'QB')
  tvs_colsPB      <- paste0(tvs_cols,'PB') 
  tps_colsQB      <- paste0(tps_cols,'QB')
  tps_colsPB      <- paste0(tps_cols,'PB')
  

  
########################Business Mix Changes JVF####################################
  
  BM_Age23      <-paste0("BMAge23_", 'WP_BI')
  BM_Gender     <-paste0("BMGender_", 'WP_BI')
  BM_RT80Plus   <-paste0("BMRTPlus_",'WP_BI')
  BM_Veh2015    <-paste0("BMVeh2015_",'WP_BI')
  BM_MinLim     <-paste0("BMMinLimit_",'WP_BI')
  BM_LiaOnly    <-paste0("BMLiaOnly_",'WP_BI')
  BM_PayFull    <-paste0("BMPayFull_",'WP_BI')
  
  sales_cols <- c(pcvs_colsQB,pcvs_colsPB
                  ,wvs_colsQB,wvs_colsPB 
                  ,avs_colsQB 
                  ,avs_colsPB,tvs_colsQB
                  ,tvs_colsPB,tps_colsQB,tps_colsPB,BM_Age23
                  ,BM_Gender,BM_RT80Plus,BM_Veh2015,BM_MinLim,BM_LiaOnly,BM_PayFull) 
    
########################End Business Mix Changes###################################### 
  #Renewal Columns
  
  vgen_cols     <- paste0("VGEN_",prem_covs)
  vren_cols     <- paste0("VREN_",prem_covs)
  
  vgen_colsQB     <- paste0(vgen_cols, 'QB' )
  vgen_colsPB     <- paste0(vgen_cols, 'PB' )
  vren_colsQB     <- paste0(vren_cols, 'QB')
  vren_colsPB     <- paste0(vren_cols, 'PB')
  
  #Written Premium and SAS/R Adjusted Written Premium Columns
  
  wp_cols       <- paste0("WP_"      , prem_covs)
  adj_wp_cols   <- paste0("ADJ_WP_"  , prem_covs)
  adj_wp_m_cols <- paste0("ADJ_WP_MULT_"  , prem_covs)
  
  wp_colsQB <- paste0(wp_cols,'QB')
  wp_colsPB <- paste0(wp_cols,'PB')
  adj_wp_colsQB <- paste0(adj_wp_cols,'QB')
  adj_wp_colsPB <- paste0(adj_wp_cols,'PB')
  adj_wp_m_colsQB <- paste0(adj_wp_m_cols, 'QB') 
  adj_wp_m_colsPB <- paste0(adj_wp_m_cols, 'PB')
  
  #Earned Exposure, Earned Premium and Clep
  
  ee_cols       <- paste0("EE_"      , prem_covs)
  ep_cols       <- paste0("EP_"      , c(prem_covs, "TOTAL"))
  clep_cols     <- paste0("CLEP_"    , prem_covs)
  
  #Loss
  
  loss_cc_cols  <- paste0("LOSS_CC_" , c("TOTAL", loss_covs))
  loss_ri_cols  <- paste0("LOSS_RI_" , loss_covs)
  loss_25_cols  <- paste0("LOSS_25_" , loss_covs)
  loss_100_cols <- paste0("LOSS_100_", loss_covs)
  
  ########################### Sale/Quote/Renew
  
  # Two functions to fix weird VPP bug thingy

  count_prem_vehs <- function(x){ sum(ifelse(x >= 1, 1, 0))}
  
  count_prem_pols <- function(x,VehicleCount){ sum(ifelse(x >= 1, x/VehicleCount, 0))}
  
  count_renewals <- function(x){sum(ifelse(x>0,1,0))}
  
  # All quote and sale stats could/should be re-written as a function. A task for another day. 
  
  # Quote


  PC_Veh_salestatsQB      <- premLossData[PolicyTermStartEffDate >= salequoteStartDate & PolicyTermStartEffDate < EndDate 
                                          & TransactionType == "New Business" & FinanceChannel == "PC"
                                          , setNames(lapply(.SD,count_prem_vehs),pcvs_colsQB)
                                          , by = c('UWYear', rateFactors), .SDcols = wp_cols]
  
  Web_Veh_salestatsQB      <- premLossData[PolicyTermStartEffDate >= salequoteStartDate & PolicyTermStartEffDate < EndDate 
                                         & TransactionType == "New Business" & FinanceChannel == "Web" 
                                         , setNames(lapply(.SD,count_prem_vehs),wvs_colsQB)
                                         , by = c('UWYear', rateFactors), .SDcols = wp_cols]

  
  Agent_Veh_salestatsQB      <- premLossData[PolicyTermStartEffDate >= salequoteStartDate & PolicyTermStartEffDate < EndDate   
                                           & TransactionType == "New Business" & FinanceChannel == "Agent" 
                                           , setNames(lapply(.SD,count_prem_vehs),avs_colsQB)
                                           , by = c('UWYear', rateFactors), .SDcols = wp_cols]
  
  
  Total_Veh_salestatsPB <- premLossData[PolicyTermStartEffDate >= premLossStartDate & PolicyTermStartEffDate < EndDate
                                       & TransactionType == "New Business" 
                                       , setNames(lapply(.SD,count_prem_vehs), tvs_colsPB)
                                       , by = c('UWYear', rateFactors), .SDcols = wp_cols]
  

  Total_Pol_salestatsQB <- premLossData[PolicyTermStartEffDate >= salequoteStartDate & PolicyTermStartEffDate < EndDate
                                       & TransactionType== 'New Business'
                                       , lapply(.SD,count_prem_vehs)
                                       , by = c('UWYear', rateFactors, 'PolicyTermKey', 'VehicleCount'), .SDcols = wp_cols][
                                         , setNames(lapply(.SD, count_prem_pols, VehicleCount=VehicleCount), tps_colsQB)
                                         , by =c('UWYear', rateFactors), .SDcols = wp_cols]
  
  Total_Pol_salestatsPB <- premLossData[PolicyTermStartEffDate >= premLossStartDate & PolicyTermStartEffDate < EndDate
                                       & TransactionType== 'New Business'
                                       , lapply(.SD,count_prem_vehs)
                                       , by = c('UWYear', rateFactors, 'PolicyTermKey', 'VehicleCount'), .SDcols = wp_cols][
                                         , setNames(lapply(.SD, count_prem_pols, VehicleCount=VehicleCount), tps_colsPB)
                                         , by =c('UWYear', rateFactors), .SDcols = wp_cols]

  tbls <- list( PC_Veh_salestatsQB,  
                Web_Veh_salestatsQB, 
                Agent_Veh_salestatsQB,
               Total_Veh_salestatsPB,
                Total_Pol_salestatsQB, Total_Pol_salestatsPB
  )
  salestats <- Reduce(function(...) merge(..., all=T, by = c('UWYear', rateFactors)), tbls)
  salestats <- salestats[,TermType := 'NB']
  
  
  if(!is.null(quoteData)){
    
    PC_Veh_quotestatsQB         <- quoteData[QuoteVersionDate >= salequoteStartDate & QuoteVersionDate < EndDate 
                                             & FinanceChannel == "PC"
                                             , setNames(lapply(.SD,count_prem_vehs),pcvq_colsQB)
                                             , by = c('UWYear', rateFactors), .SDcols = wp_cols]
    
    Web_Veh_quotestatsQB      <- quoteData[QuoteVersionDate >= salequoteStartDate & QuoteVersionDate < EndDate 
                                           & FinanceChannel == "Web" 
                                           , setNames(lapply(.SD,count_prem_vehs),wvq_colsQB)
                                           , by = c('UWYear', rateFactors), .SDcols = wp_cols]
    
    
    Agent_Veh_quotestatsQB    <- quoteData[QuoteVersionDate >= salequoteStartDate & QuoteVersionDate < EndDate 
                                           & FinanceChannel == "Agent" 
                                           , setNames(lapply(.SD,count_prem_vehs),avq_colsQB)
                                           , by = c('UWYear', rateFactors), .SDcols = wp_cols]
    
    
    Total_Pol_quotestatsQB<- quoteData[QuoteVersionDate >= salequoteStartDate & QuoteVersionDate < EndDate 
                                       , setNames(lapply(.SD,count_prem_vehs), apq_cols)
                                       , by = c('UWYear', rateFactors,'QuoteID','VehicleCount'), .SDcols = wp_cols][
                                         , setNames(lapply(.SD,count_prem_pols, VehicleCount = VehicleCount), tpq_colsQB)
                                         , by = c('UWYear', rateFactors), .SDcols = apq_cols]
    
    
    
    
    tbls <- list(PC_Veh_quotestatsQB,
                 Web_Veh_quotestatsQB,
                 Agent_Veh_quotestatsQB,
                 Total_Pol_quotestatsQB)
    quotestats <- Reduce(function(...) merge(..., all=T, by = c('UWYear', rateFactors)) , tbls)
    quotestats <- quotestats[,TermType := 'NB']
    
    
  } else {
    quotestats <- copy(salestats)
    quotestats[,(sales_cols) := NULL]
    quotestats[,(quote_cols) := 0]
  }
  
  
  rateFactors <- c('TermType', 'UWYear',rateFactors)
  
  ######################################################Business Mix#######################################  
  BM_Age23_salestats      <- premLossData[PolicyTermStartEffDate >= premLossStartDate & PolicyTermStartEffDate < EndDate 
                                          & !is.na(PrimaryDriverAge)
                                          & ((TransactionType=='New Business' & WP_BI > 0) | (TransactionType == 'Renewal' & EP_BI > 0))
                                          , setNames(lapply(.SD, function(x) sum(ifelse(x > 0, as.double(PrimaryDriverAge), 0))), BM_Age23)
                                          , by = c(rateFactors), .SDcols = 'WP_BI' ]
  
  BM_Gender_salestats       <- premLossData[PolicyTermStartEffDate >= premLossStartDate & PolicyTermStartEffDate < EndDate
                                            & PrimaryDriverGender == 'M' 
                                            & ((TransactionType=='New Business' & WP_BI > 0) | (TransactionType == 'Renewal' & EP_BI > 0))
                                            , setNames(lapply(.SD,count_prem_vehs),BM_Gender)
                                            , by=c(rateFactors), .SDcols = 'WP_BI' ]
  
  BM_LiaOnly_salestats       <- premLossData[PolicyTermStartEffDate >= premLossStartDate & PolicyTermStartEffDate < EndDate
                                             & OptionalCoverageType == 'None' 
                                             & ((TransactionType=='New Business' & WP_BI > 0) | (TransactionType == 'Renewal' & EP_BI > 0))
                                             , setNames(lapply(.SD,count_prem_vehs),BM_LiaOnly)
                                             , by=c(rateFactors), .SDcols = 'WP_BI' ]
  
  BM_MinLim_salestats       <- premLossData[PolicyTermStartEffDate >= premLossStartDate & PolicyTermStartEffDate < EndDate
                                            & SelectedBILimit_f == 'Minimum' 
                                            & ((TransactionType=='New Business' & WP_BI > 0) | (TransactionType == 'Renewal' & EP_BI > 0))
                                            , setNames(lapply(.SD,count_prem_vehs),BM_MinLim)
                                            , by=c(rateFactors), .SDcols = 'WP_BI' ]
  
  BM_PayFull_salestats       <- premLossData[PolicyTermStartEffDate >= premLossStartDate & PolicyTermStartEffDate < EndDate
                                             & DownPaymentMonthsNumber >= 6 
                                             & ((TransactionType=='New Business' & WP_BI > 0) | (TransactionType == 'Renewal' & EP_BI > 0))
                                             , setNames(lapply(.SD,count_prem_vehs),BM_PayFull)
                                             , by=c(rateFactors), .SDcols = 'WP_BI' ]
  
  BM_RT80Plus_salestats       <- premLossData[PolicyTermStartEffDate >= premLossStartDate & PolicyTermStartEffDate < EndDate
                                              & RetentionTier2_Calculated >= 80
                                              & ((TransactionType=='New Business' & WP_BI > 0) | (TransactionType == 'Renewal' & EP_BI > 0))
                                              , setNames(lapply(.SD, count_prem_vehs),BM_RT80Plus)
                                              , by=c(rateFactors), .SDcols = 'WP_BI' ]
  
  BM_Veh2015_salestats       <- premLossData[PolicyTermStartEffDate >= premLossStartDate & PolicyTermStartEffDate < EndDate
                                             & !is.na(VehicleYearNumber)
                                             & ((TransactionType=='New Business' & WP_BI > 0) | (TransactionType == 'Renewal' & EP_BI > 0))
                                             , setNames(lapply(.SD,function(x) sum(ifelse( x > 0, as.double(VehicleYearNumber), 0))),BM_Veh2015)
                                             , by=c(rateFactors), .SDcols = 'WP_BI' ]
  
  tbls<- list(BM_Age23_salestats, BM_Gender_salestats, BM_LiaOnly_salestats, BM_MinLim_salestats, BM_PayFull_salestats, BM_RT80Plus_salestats, BM_Veh2015_salestats)
  
  statsBM <- Reduce(function(...) merge(..., all=T, by = c(rateFactors)), tbls)
  
  
  # Renew
  
  #count_renewals <- function(x){sum(ifelse(x>0,1,0))}
  
  
  veh_gen_renewalQB        <- premLossData[PolicyTermStartEffDate >= salequoteStartDate & PolicyTermStartEffDate < EndDate 
                                           & TransactionType == "Renewal"
                                           , setNames(lapply(.SD,count_renewals),vgen_colsQB)
                                           , by = rateFactors, .SDcols = wp_cols]
  
  veh_gen_renewalPB        <- premLossData[PolicyTermStartEffDate >= premLossStartDate & PolicyTermStartEffDate < EndDate 
                                           & TransactionType == "Renewal" 
                                           , setNames(lapply(.SD,count_renewals),vgen_colsPB)
                                           , by = rateFactors, .SDcols = wp_cols]
  
  veh_taken_renewalQB      <- premLossData[PolicyTermStartEffDate >= salequoteStartDate & PolicyTermStartEffDate < EndDate
                                           & TransactionType == "Renewal" & WP_BI > 0
                                           , setNames(mapply(function(x,y) sum(x > 0 & y > 0), mget(ep_cols[ep_cols != 'EP_TOTAL']), mget(wp_cols), SIMPLIFY = FALSE),vren_colsQB)
                                           , by = rateFactors]

  veh_taken_renewalPB      <- premLossData[PolicyTermStartEffDate >= premLossStartDate & PolicyTermStartEffDate < EndDate
                                           & TransactionType == "Renewal" & WP_BI > 0
                                           , setNames(mapply(function(x,y) sum(x > 0 & y > 0), mget(ep_cols[ep_cols != 'EP_TOTAL']), mget(wp_cols), SIMPLIFY = FALSE),vren_colsPB)
                                           , by = rateFactors]
  
  # veh_taken_renewalQB      <- premLossData[PolicyTermStartEffDate >= salequoteStartDate & PolicyTermStartEffDate < EndDate 
  #                                          & TransactionType == "Renewal" & WP_BI > 0 & !is.na(EP_BI)
  #                                          , setNames(lapply(.SD,count_renewals),vren_colsQB)
  #                                          , by = rateFactors, .SDcols = ep_cols[ep_cols != 'EP_TOTAL']]
  # 
  # veh_taken_renewalPB      <- premLossData[PolicyTermStartEffDate >= premLossStartDate & PolicyTermStartEffDate < EndDate 
  #                                          & TransactionType == "Renewal" & WP_BI > 0 & !is.na(EP_BI)
  #                                          , setNames(lapply(.SD,count_renewals),vren_colsPB)
  #                                          , by = rateFactors, .SDcols = ep_cols[ep_cols != 'EP_TOTAL']]
  


max6morenewLagged <- as.Date(max(premLossData$RetainedAsOfDate, na.rm = T)) %m-% months(6)


Month6Active <- premLossData[PolicyTermStartEffDate >= (as.Date(salequoteStartDate) %m-% months(6))
                             & PolicyTermStartEffDate < (as.Date(EndDate) %m-% months(6))
                             & PolicyTermStartEffDate < max6morenewLagged
                             & (TransactionType == "New Business" | TransactionType == "Renewal")
                             , sum(Retained6Months, na.rm=T)
                             , by = rateFactors]

Month6Active<-setNames(Month6Active, c(rateFactors, 'Month6Active'))

Month6Total <- premLossData[PolicyTermStartEffDate   >= (as.Date(salequoteStartDate) %m-% months(6))
                            & PolicyTermStartEffDate < (as.Date(EndDate) %m-% months(6))
                            & PolicyTermStartEffDate < max6morenewLagged
                            & (TransactionType == "New Business" | TransactionType == "Renewal")
                            , .N  
                            , by = rateFactors]

Month6Total<-setNames(Month6Total, c(rateFactors, 'Month6Total'))

mo6retnStats<-merge(Month6Active, Month6Total, by = rateFactors)

##############################################################################################
# Adding Cancellation
##############################################################################################

Cancel_NonPay <- premLossData[PolicyTermExpirationEffDate < EndDate
                              & PolicyTermStartEffDate >= (salequoteStartDate - years(1))
                              & (TransactionType =='Renewal' | TransactionType == 'New Business')
                              & CancelReason_F == 'Non-Payment'
                              & CancellationDate < PolicyTermExpirationEffDate - months(2)
                              , .(Cancels_NonPay = .N)
                              , by = rateFactors]

Cancel_Fraud <- premLossData[PolicyTermExpirationEffDate < EndDate
                             & PolicyTermStartEffDate >= (salequoteStartDate - years(1))
                             & (TransactionType =='Renewal' | TransactionType == 'New Business')
                             & CancelReason_F == 'Fraud/UW'
                             & CancellationDate < PolicyTermExpirationEffDate - months(2)
                             , .(Cancels_Fraud = .N)
                             , by = rateFactors]

Cancel_Other <- premLossData[PolicyTermExpirationEffDate < EndDate
                             & PolicyTermStartEffDate >= (salequoteStartDate - years(1))
                             & (TransactionType =='Renewal' | TransactionType == 'New Business')
                             & (CancelReason_F != 'Fraud/UW' & CancelReason_F != 'Non-Payment')
                             & !is.na(CancelReason_F)
                             & CancellationDate < PolicyTermExpirationEffDate - months(2)
                             , .(Cancels_Other = .N)
                             , by = rateFactors]


Total1Year <- premLossData[PolicyTermExpirationEffDate < EndDate
                           & PolicyTermStartEffDate >= (salequoteStartDate - years(1))
                           & (TransactionType == 'Renewal' | TransactionType == 'New Business')
                           , .(Total1Year = .N)
                           , by = rateFactors]

tbls <- list(Total1Year, Cancel_Other, Cancel_Fraud, Cancel_NonPay)
Cancellations <- Reduce(function(...) merge(..., all = T, by = c(rateFactors)), tbls)


########################### 3 Day Cancellation ################################


ThreeDayCancelQBTotal <- premLossData[PolicyTermStartEffDate < (EndDate - days(3))
                                  & PolicyTermStartEffDate >= salequoteStartDate
                                  & (TransactionType =='Renewal' | TransactionType == 'New Business')
                                  & EP_BI > 0
                                  , .(CXX_QBTotal = .N)
                                  , by = rateFactors]

ThreeDayCancelPBTotal <- premLossData[PolicyTermStartEffDate < (EndDate - days(3))
                                & PolicyTermStartEffDate >= premLossStartDate
                                & (TransactionType =='Renewal' | TransactionType == 'New Business')
                                & EP_BI > 0
                                , .(CXX_PBTotal = .N)
                                , by = rateFactors]

CancelThreeDayQB <- premLossData[PolicyTermStartEffDate < (EndDate - days(3))
                                      & PolicyTermStartEffDate >= salequoteStartDate
                                      & (TransactionType =='Renewal' | TransactionType == 'New Business')
                                      & CancellationDate <= PolicyTermStartEffDate + days(3)
                                      & EP_BI > 0
                                      , .(CXX_ThreeDays_QB = .N)
                                      , by = rateFactors]

CancelThreeDayPB <- premLossData[PolicyTermStartEffDate < (EndDate - days(3))
                                      & PolicyTermStartEffDate >= premLossStartDate
                                      & (TransactionType =='Renewal' | TransactionType == 'New Business')
                                      & CancellationDate <= PolicyTermStartEffDate + days(3)
                                      & EP_BI > 0
                                      , .(CXX_ThreeDays_Total = .N)
                                      , by = rateFactors]

tbls <- list( ThreeDayCancelQBTotal, ThreeDayCancelPBTotal
             , CancelThreeDayQB, CancelThreeDayPB)
ThreeDayCancellation <- Reduce(function(...) merge(..., all = T, by = c(rateFactors)), tbls)


###################### WP #####################################################

  # Adjusted WP numbers
  unassignedAdj <- adj_wp_cols[adj_wp_cols %ni% names(premLossData)]
  if(length(unassignedAdj) > 0) premLossData[,(unassignedAdj) := 1.00]
  premLossData[,(adj_wp_m_cols) := mget(adj_wp_cols)]
  premLossData[,(adj_wp_cols)   := Map("*", mget(adj_wp_cols), mget(wp_cols))]
  
  # Aggregate
  premloss <- premLossData[PolicyTermStartEffDate < EndDate 
                           & PolicyTermStartEffDate >= premLossStartDate
                     , lapply(.SD, sum, na.rm = T) 
                     , by = rateFactors
                     , .SDcols = c(ee_cols, ep_cols, clep_cols, loss_cc_cols, loss_25_cols, loss_100_cols, loss_ri_cols)]
  
  premloss_adj_wp <- premLossData[(TransactionType == "New Business" & PolicyTermStartEffDate >= premLossStartDate & PolicyTermStartEffDate < EndDate) |
                                  (TransactionType == "Renewal"      & PolicyTermStartEffDate >= premLossStartDate & PolicyTermStartEffDate < EndDate & EP_TOTAL > 0)
                                , setNames(lapply(.SD, sum, na.rm = T), adj_wp_cols)
                                , by = rateFactors
                                , .SDcols =  adj_wp_cols]
  
  premloss_wpQB <- premLossData[(TransactionType == "New Business" & PolicyTermStartEffDate >= salequoteStartDate & PolicyTermStartEffDate < EndDate) |
                                (TransactionType == "Renewal"      & PolicyTermStartEffDate >= renewalStartDate & PolicyTermStartEffDate < EndDate & EP_TOTAL > 0)
                              , setNames(lapply(.SD, sum, na.rm = T), wp_colsQB)
                              , by = rateFactors
                              , .SDcols = c(wp_cols)]
  
  premloss_wpPB <- premLossData[(TransactionType == "New Business" & PolicyTermStartEffDate >= premLossStartDate & PolicyTermStartEffDate < EndDate) |
                                (TransactionType == "Renewal"      & PolicyTermStartEffDate >= premLossStartDate & PolicyTermStartEffDate < EndDate & EP_TOTAL > 0)
                              , setNames(lapply(.SD, sum, na.rm = T), wp_colsPB)
                              , by = rateFactors
                              , .SDcols = wp_cols]
  
  # Reset multipliers to original values
  premLossData[,(adj_wp_cols) := mget(adj_wp_m_cols)]
  premLossData[,(adj_wp_m_cols) := NULL]
  
  # Merge
  tbls <- list(premloss, premloss_adj_wp, 
               premloss_wpQB, premloss_wpPB, salestats, statsBM,  
               quotestats, mo6retnStats,
               veh_gen_renewalQB, veh_gen_renewalPB,
               veh_taken_renewalQB, veh_taken_renewalPB, Cancellations, ThreeDayCancellation)
  pm_output <- Reduce(function(...) merge(..., all=T, by = rateFactors), tbls)
  
  
  ########################### Output
  
  # Setup Columns for Output
  if(length(rateFactors) < 10){
  blankvar <- paste0("var", (1 + length(rateFactors)):10)
  rateFactors <- c(rateFactors, blankvar)
  pm_output[,(c(blankvar)) := "."]
  }
  
  pm_output[,("EP_TOTAL") := NULL]
  pm_output[,(c("NetPolQuotes")) := 0.00]
  salestat_colsQB   <- c(pcvs_colsQB,wvs_colsQB,avs_colsQB,tps_colsQB)
  salestat_colsPB   <- c(tvs_colsPB,tps_colsPB)
  BusMix_cols     <- c(BM_Age23, BM_Gender, BM_RT80Plus, BM_Veh2015, BM_MinLim, BM_LiaOnly, BM_PayFull)
  quotestat_colsQB  <- c(pcvq_colsQB,wvq_colsQB,avq_colsQB,tpq_colsQB)
  renewstat_colsQB  <- c(vgen_colsQB,vren_colsQB)
  renewstat_colsPB  <- c(vgen_colsPB,vren_colsPB)
  renew6stat_cols <- c("Month6Total", "Month6Active")
  ep_cols         <- paste0("EP_", prem_covs)
  clep_cols       <- paste0("CLEP_", prem_covs)
  cancel_cols <- c('Total1Year', 'CXX_QBTotal', 'CXX_PBTotal'
                   , 'Cancels_Other', 'Cancels_Fraud', 'Cancels_NonPay'
                   , 'CXX_ThreeDays_QB', 'CXX_ThreeDays_Total')
  
  # Reorder cols
  if(length(rateFactors) < 10){
    output_col_order <- c(rateFactors, "NetPolQuotes",quotestat_colsQB, 
                          salestat_colsQB,salestat_colsPB, 
                          vgen_colsQB, vgen_colsPB,
                          vren_colsQB, vren_colsPB,
                          BusMix_cols, wp_colsQB, wp_colsPB,
                          adj_wp_cols, ee_cols, ep_cols, clep_cols, renew6stat_cols, cancel_cols,
                          loss_cc_cols, loss_25_cols, loss_100_cols, loss_ri_cols)
  }else{
    output_col_order <- c(rateFactors, "NetPolQuotes", quotestat_colsQB, 
                          salestat_colsQB,salestat_colsPB, 
                          vgen_colsQB, vgen_colsPB,
                          vren_colsQB, vren_colsPB,
                          BusMix_cols, wp_colsQB, wp_colsPB,
                          adj_wp_cols, ee_cols, ep_cols, clep_cols, renew6stat_cols, cancel_cols,
                          loss_cc_cols, loss_25_cols, loss_100_cols, loss_ri_cols)
    
  }


  setcolorder(pm_output, output_col_order)

  # Set Missing values
  for (j in names(pm_output)[names(pm_output) %ni% c(rateFactors)]) set(pm_output, which(is.na(pm_output[[j]])), j, 0.00)
  for (j in rateFactors) set(pm_output, which(is.na(pm_output[[j]])), j, NA)
  
  ########################### Facts Page
  
  sasdate <- function(datestring){
    paste0(sprintf("%02d", lubridate::day(datestring))
           , toupper(lubridate::month(datestring, label = T))
           , substr(lubridate::year(datestring), 3, 4))
  }
 
  if(length(rateFactors) < 10){
    facts <- c(paste0(sasdate(premLossStartDate), ' - ', sasdate(EndDate))
               , paste0(sasdate(salequoteStartDate), ' - ', sasdate(EndDate))
               , rateFactors)
  }   
  else{
    facts <- c(paste0(sasdate(premLossStartDate), ' - ', sasdate(EndDate))
               , paste0(sasdate(salequoteStartDate), ' - ', sasdate(EndDate))
               , rateFactors
               )
    
    
  }
  
  if(returnOutput == F){return(list(facts, pm_output, '12'))}
  else{return(pm_output)}
  
}

excelOutput <- function(pmlist, saveFlag, 
                        templateLocation = '//admiral.local/Shares/Depts/Pricing/PricingModel/PricingModelTemplate_Years.xlsb'){
  
  # Load Req Package
  suppressWarnings(suppressMessages(require(excel.link)))
  suppressWarnings(suppressMessages(require(RDCOMClient)))
  
  `%ni%` <- Negate(`%in%`)  
  
  # This code is mostly the same as xl.get.excel(), but it prevents it from opening blank workbooks
  excel_CLSID = "{00020400-0000-0000-C000-000000000046}"
  excel_hwnd = unlist(options("excel_hwnd"))
  succ = FALSE
  if (!is.null(excel_hwnd)) {
    xls = getCOMInstance_hWnd(excel_CLSID, excel_hwnd)
    if (!(is.null(xls) || ("COMErrorString" %in% class(xls)))) {
      succ = TRUE
      xls = xls[["Application"]]
    }
  }
  if (!succ) {
    xls = getCOMInstance("Excel.Application", force = FALSE, silent = TRUE)
    if (is.null(xls) || ("COMErrorString" %in% class(xls))) {
      xls = getCOMInstance("Excel.Application", force = TRUE, silent = TRUE)
      xls[["Visible"]] = TRUE
    }
    else {
      if (!xls[["Visible"]]) {
        xls[["Visible"]] = TRUE
      }
    }
  }
  
  # Similar to xl.workbooks(), returns the listing of currently open workbooks
  wb.count = xls[["Workbooks"]][["Count"]]
  wblist <- sapply(seq_len(wb.count), function(wb) xls[["Workbooks"]][[wb]][["Name"]])
  
  
  if("PricingModelTemplate_Years.xlsb" %ni% wblist){
    # If we don't already have the pricing model open, open it
    pmpath <- templateLocation
    xl.wb = xls[["Workbooks"]]$Open(pmpath)
  }
  
  # Paste in data and save in output folder with username_date_time
  xl.workbook.activate("PricingModelTemplate_Years.xlsb")
  xl.sheet.activate("PFBase(All)")
  xl[A11] <- data.table(matrix(NA, nrow = 10000, ncol = 304))
  xl[A11] <- pmlist[[2]]
  xl.sheet.activate("Facts")
  xl[A2] <- t(pmlist[[1]])
  xl.sheet.activate("PFCalc(All)")
  xl[X2] <- pmlist[[3]] 
  
  xls$Run("Trim_Cells_Array_Method")
  xls$Run("createRanges")
  xls$Run("CopyFormatting")
  xls$Run("HideCols")
  
  
  
  if(saveFlag == T) {
    tempname <- paste0("pm_", Sys.getenv("USERNAME"), format(Sys.time(), "_%Y%m%d_%H%M%S"), ".xlsb")
    pmpath <- "//admiral.local/Shares/Depts/Pricing/PricingModel/Output/"
    xls[["ActiveWorkBook"]]$SaveAs(paste0(pmpath, tempname))
  }
  
  
}

# Wrapper around pricingModelAgg and excelOutput
pricingModel <- function(premLossData, quoteData = NULL
                           , premLossStartDate, EndDate
                           , salequoteStartDate 
                           , renewalStartDate 
                           , rateFactors
                           , saveToOutput = F
                           , returnOutput = F ){
  
  output <- pricingModelAgg(premLossData, quoteData
                            , premLossStartDate, EndDate
                            , salequoteStartDate 
                            , renewalStartDate 
                            , rateFactors
                            , returnOutput)
  
## Added for debugging. Nice to look at output before creating the pricing model without having to mess with internals too much
  
  if(returnOutput == T){ return(output); }
  
  else{excelOutput(output, saveFlag = saveToOutput)}
  
}
