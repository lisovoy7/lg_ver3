library(RODBC)
library(data.table)
library(dplyr)
library(sqldf)
library(matrixStats)
library(RcppRoll)
library(DBI)
library(odbc)

Sys.time()

setwd("/home/andrey.lisovoy/TEMP_ANDREY/HOF_Legends/Legends3/")

connImpala <-        "Impala"
connHBIPA <-         "Driver=ODBC Driver 13 for SQL Server; Server=10.145.16.21; Database=Dwh_Pacific; Uid=r_reader; Pwd=hIDGPJ4ioW2R5AaW7ap7" 
connHBIPA_Analysis <-"Driver=ODBC Driver 13 for SQL Server; Server=10.145.16.22; Database=Analysis   ; Uid=r_rw    ; Pwd=hIDGPJ4ioW2R5AaW7ap7"

impala.query = function (channel.name , query){
  channel <- odbcConnect(channel.name)
  dt <- data.table(sqlQuery(channel, query, stringsAsFactors = FALSE))
  odbcClose(channel)
  dt
}

impala.save = function (impala.schema, impala.table, df){
  channel.name <- dbConnect(odbc::odbc() , "Impala",  schema=impala.schema) 
  dbWriteTable(channel.name, impala.table, df[1 : min(1024 , nrow(df)) ] , overwrite=T) 
  i=1
  while(i < ceil(nrow(df)/1024) ){
    dbWriteTable(channel.name, impala.table, df[ (1024*i+1) : min(1024*(i+1) , nrow(df)) ], append=T)
    i <- i+1
  }
  dbDisconnect(channel.name)
}

hbipa.query = function (channel.name , query){
  channel <- odbcDriverConnect(channel.name)
  dt <- data.table(sqlQuery(channel, query, stringsAsFactors = FALSE))
  odbcClose(channel)
  dt
}

min.trstierid <- 3
max.rev.trstierid.gold <- 50
max.rev.trstierid.platinum.plus <- 200

nondep.min.gamelevelid <- 25

money.to.score <- 0.12
promo.duration <- 67
hist.duration <- 365 
buffer.hist.to.sim <- 0

shift_days <- 100
decay_ratio <- 0.07
min_decay <- 0.5
floor_perc <- 0.5

coins.per.400.dollar <- 10000000
coins.per.100.dollar <- 1000000

sim.date.start <- as.Date('2018-07-20')
sim.dateid.start = as.integer(gsub("-", "", sim.date.start ))
sim.dateid.end = as.integer(gsub("-", "", (sim.date.start+(promo.duration-1)) ))
seg.dateid.start = as.integer(gsub("-", "", (sim.date.start-(hist.duration+buffer.hist.to.sim) )))
seg.dateid.end = as.integer(gsub("-", "", (sim.date.start-(buffer.hist.to.sim+1) )))
sim.month.start = as.numeric(substr(as.character(sim.dateid.start),1,6))
sim.month.end = as.numeric(substr(as.character(sim.dateid.end),1,6))
seg.syssnapshotdateid.end = as.integer(gsub("-", "", (sim.date.start-(buffer.hist.to.sim))))
seg.month.start <- as.numeric(substr(as.character(seg.dateid.start),1,6))
seg.month.end <- as.numeric(substr(as.character(seg.dateid.end),1,6))

LevelFreemiumCoef <- data.table(gamelevelid = seq(1:20000) , freemium.coef = 1 )
LevelMultiplier <- hbipa.query(connHBIPA , paste("select level_id as gamelevelid, offer_coins_multiplier_base as multiplier
                                                  from [HOF-HBIP].[sources].[dbo].[MRR_level] 
                                                  where platform_type_id=1 and group_id=1"))
LevelFreqBet <- merge(LevelFreemiumCoef , LevelMultiplier, by= c('gamelevelid'), all.x = FALSE,all.y = FALSE)
LevelFreqBet[ , freqbet := 800 * freemium.coef * multiplier]
LevelFreqBet[between(gamelevelid, 1, 24) , freqbet := freqbet/2] ## making up better expirience for users in first levels

SegmentsDict <- as.data.table(read_delim("~/TEMP_ANDREY/HOF_Legends/Legends3/segments_config.csv", "\t", escape_double = FALSE, trim_ws = TRUE))


BetchangeScore <- data.table()
betchange.max <- 1e9
for (i in SegmentsDict$segment){
  tmp <- data.table(segment=i, betchange_lb=c(0, 1.5^seq(-18,20)))
  tmp[ , betchange_ub := lead(betchange_lb, default = betchange.max)]
  tmp[ , betchange_score := (betchange_lb+betchange_ub)/2]
  tmp[betchange_ub == betchange.max , betchange_score := 2*betchange_lb]
  BetchangeScore <- rbind(BetchangeScore,tmp)
}

HistTransRaw <- hbipa.query(connHBIPA , paste("
with
trans as(
  select userid,dateid,sum(MAmount) as gross_amount, sum(" ,money.to.score, " * b.Coins) as score_cashier, max(gamelevelid) as gamelevelid
  from [Dwh_Pacific].[dbo].[Dwh_Fact_Transaction] as a
  left join [Dwh_Pacific].[dbo].[Dwh_Dim_Pricelist] as b on CEILING(a.MAmount)=b.PriceID
  where TranStatusID=1 and dateid between " , seg.dateid.start , " and " , seg.dateid.end , "
  group by userid,dateid
)
select tmp2.userid, tmp2.dateid as hist_dateid_to, isnull(gross_amount,0) as gross_amount, isnull(score_cashier,0) as score_cashier
from(
    select userid, dateid
    from [Dwh_Pacific].[dbo].[Dwh_Dim_Date]
    cross join (select distinct userid from trans) as tmp
where dateid between " , seg.dateid.start , " and " , seg.dateid.end , ") as tmp2
left join trans on tmp2.userid=trans.userid and tmp2.dateid=trans.dateid"))

saveRDS(HistTransRaw, file = "HistTransRaw.rds" )

setkey(HistTransRaw, userid, hist_dateid_to)
HistTransRaw[, hist.gross.amount  := roll_sumr(gross_amount , promo.duration, na.rm = T) , userid]
HistTransRaw[, hist.score.cashier := roll_sumr(score_cashier, promo.duration, na.rm = T) , userid]
HistTransRaw[, hist_dateid_from := as.integer(roll_minr(hist_dateid_to, promo.duration, na.rm = T)) , userid]
HistTransRaw <- HistTransRaw[order(userid,-hist_dateid_to)]
HistTransRaw[, period_recency := 1:.N , userid]
HistTransRaw[, decay := ((1-floor_perc)/(1+exp(decay_ratio*(period_recency-shift_days)))+floor_perc)]
HistTransRaw[, hist.gross.amount.decay := hist.gross.amount * decay]
HistTransRaw[, hist.score.cashier.decay := hist.score.cashier * decay]
HistTransRaw <- HistTransRaw[order(userid,-hist.score.cashier.decay,-hist_dateid_to)]
HistTransRaw[!is.na(hist.score.cashier.decay), rn := 1:.N , userid]
HistTransRaw[, hist.max.gross.amount := max(hist.gross.amount, na.rm = T) , userid]
HistTransRaw[, hist.max.score.cashier := max(hist.score.cashier, na.rm = T) , userid]
HistTrans <- HistTransRaw[rn == 1 , -c("gross_amount","score_cashier","rn","hist.max.score.cashier")]

saveRDS(HistTrans, file = "HistTrans.rds" )
rm(HistTransRaw)

HistSpinsRawQuery <- impala.query(connImpala , paste("
with
trans as(
  select distinct userid
  from dwh.dwh_fact_transaction 
  where transtatusid=1 and dateid between " , seg.dateid.start , " and " , seg.dateid.end , "
)
select userid, dateid, gamelevelid, mtotalbetsamount/mbetscount as betamount, sum(mbetscount) as betscount
from dwh.dwh_fact_spin_agg
where month between " , seg.month.start , " and " , seg.month.end , " and dateid between " , seg.dateid.start , " and " , seg.dateid.end , "
        and userid in (select distinct userid from trans) and mtotalbetsamount>0
group by 1,2,3,4"))

saveRDS(HistSpinsRawQuery, file = "HistSpinsRawQuery.rds")

HistSpinsRaw <- merge(HistSpinsRawQuery , HistTrans[ , c('userid','hist_dateid_from','hist_dateid_to','decay')], by= c('userid'), all.x = TRUE,all.y = FALSE)
rm(HistSpinsRawQuery)
HistSpinsRaw <- HistSpinsRaw[dateid >= hist_dateid_from & dateid <= hist_dateid_to , ]
HistSpinsRaw <- merge(HistSpinsRaw , LevelFreqBet, by= c('gamelevelid'), all.x = TRUE,all.y = FALSE)
HistSpinsRaw[ , betchange := (betamount/freqbet)]
HistSpinsRaw <- as.data.table(sqldf("select a.* , b.betchange_score from HistSpinsRaw as a 
                                    left join BetchangeScore as b on b.segment=1 and a.betchange>=b.betchange_lb and a.betchange<betchange_ub") )

saveRDS(HistSpinsRaw, file = "HistSpinsRaw.rds")

HistSpins <- HistSpinsRaw[ ,  .(        hist.betscount=sum(betscount)
                                      , hist.wager=sum(betscount*betamount)
                                      , hist.median.betchange=weightedMedian(betchange,betscount)
                                      , hist.score.spins=sum(betscount * betchange_score)
                                      , hist.score.spins.decay=sum(betscount * betchange_score * decay, na.rm = T)
                                      , hist.betchange.sum=sum(betscount * betchange) ) , by=.(userid)]
HistSpins[is.na(hist.score.spins) , hist.score.spins := 0]
rm(HistSpinsRaw)

GameLevel <- impala.query(connImpala , paste("select userid, gamelevelid, llrecency from dwh.dwh_dim_user_hist where syssnapshotdateid=" ,seg.syssnapshotdateid.end))

HistData <- merge(HistTrans , HistSpins   , by= c('userid'), all.x = TRUE,all.y = FALSE)
HistData <- merge(HistData  , GameLevel, by= c('userid'), all.x = TRUE,all.y = FALSE)

rm(GameLevel)

HistData[is.na(hist.score.spins.decay) , c("hist.betscount", "hist.wager", "hist.score.spins.decay")] <- 0
HistData[hist.score.spins>0 & hist.score.cashier>0 , hist.spins.to.cashier.score.ratio := hist.score.spins/hist.score.cashier]
HistData[, isdepositor := 1]

saveRDS(HistData, file = "HistData.rds" )

NonDep <- impala.query(connImpala , paste("
select userid, gamelevelid, llrecency
from dwh.dwh_dim_user_hist
where gamelevelid>=", nondep.min.gamelevelid, " and syssnapshotdateid=" ,seg.syssnapshotdateid.end, " and ((depositscount=0) or (depositscount>0 and llrecency<=365 and ltdrecency>365))"))
NonDep[ , ':=' (hist.gross.amount.decay = 0, isdepositor = 0, hist.score.cashier.decay = 0, hist.score.spins.decay = 0)]

ChurnDep <- impala.query(connImpala , paste("
select userid, gamelevelid, llrecency
from dwh.dwh_dim_user_hist
where syssnapshotdateid=" ,seg.syssnapshotdateid.end, " and depositscount>0 and llrecency>365"))
ChurnDep[ , ':=' (hist.gross.amount.decay = 0, isdepositor = 1, hist.score.cashier.decay = 0, hist.score.spins.decay = 0)]

HistData <- rbind(HistData[, c("userid", "gamelevelid", "llrecency", "hist.gross.amount.decay", "isdepositor", "hist.score.cashier.decay", "hist.score.spins.decay")] , NonDep  [!(userid %in% HistData$userid) , ])
HistData <- rbind(HistData[, c("userid", "gamelevelid", "llrecency", "hist.gross.amount.decay", "isdepositor", "hist.score.cashier.decay", "hist.score.spins.decay")] , ChurnDep[!(userid %in% HistData$userid) , ])

HistData[, random := sample(0:99, nrow(HistData), replace=T)]

HistData[ , hist.personal.progress.decay := pmax(1.2*hist.score.cashier.decay, (hist.score.cashier.decay + hist.score.spins.decay))]
HistData[ , hist_personal_base_coins_cashier_decay := hist.score.cashier.decay/money.to.score]


for (i in SegmentsDict$segment){ 
  HistData[   isdepositor==SegmentsDict[segment==i,isdepositor]
            & llrecency < SegmentsDict[segment==i, max.llrecency]
            & between(hist.personal.progress.decay, SegmentsDict[segment==i,effort.from], SegmentsDict[segment==i,effort.to]) 
            & between(random, SegmentsDict[segment==i,random.from], SegmentsDict[segment==i,random.to])
            & between(gamelevelid, SegmentsDict[segment==i,gamelevelid.from], SegmentsDict[segment==i,gamelevelid.to]) ,segment := i ]
  
  HistData[segment==i , score.effort.with.uplifts := SegmentsDict[segment==i, effort.no.buffers * (1+uplift.buffer + mm.buffer)]]
}


## Special segments: overwrites everything. (1) high trs with low transactions last year (2) users who didnt buy last year but their LT maxDeposit is high
HighTrs <- impala.query(connImpala , paste("
with
trans as(
select userid, sum(mamount) as yearly_gross_revenue
from dwh.dwh_fact_transaction
where transtatusid=1 and dateid between " , seg.dateid.start , " and " , seg.dateid.end , "
group by 1)
  select a.userid, trstierid,0 as yearly_gross_revenue, gamelevelid, llrecency, 1 as isdepositor
  from dwh.dwh_dim_user_hist as a
  where syssnapshotdateid=" , seg.syssnapshotdateid.end , " and a.ltdrecency>" , hist.duration , " and a.depositmax>50 and a.trstierid<",min.trstierid,"
UNION ALL
  select a.userid, trstierid, nvl(yearly_gross_revenue,0) as yearly_gross_revenue, gamelevelid, llrecency, case when depositscount>0 then 1 else 0 end as isdepositor
  from dwh.dwh_dim_user_hist as a
  left join trans b on a.userid=b.userid
  where syssnapshotdateid=" ,seg.syssnapshotdateid.end, " and (  (nvl(yearly_gross_revenue,0)<",max.rev.trstierid.gold,"          and trstierid=",min.trstierid,") 
                                                          or     (nvl(yearly_gross_revenue,0)<",max.rev.trstierid.platinum.plus," and trstierid>",min.trstierid,")  )" ))

HighTrs <- HighTrs[ , .SD[1], by = .(userid)]  ## just to make sure there are no duplicates

## GOLD trs go to "trstierid.gold". rest of TRS (can be also 0,1,2) go to "trstierid.platinum.plus"
HighTrs[ , segment := ifelse(trstierid==min.trstierid, SegmentsDict[name=="trstierid.gold", segment], SegmentsDict[name=="trstierid.platinum.plus", segment]) ]


HighTrs[ , ':=' (hist.gross.amount.decay = 0, hist.score.cashier.decay = 0, hist.score.spins.decay = 0, hist.personal.progress.decay = 0, hist_personal_base_coins_cashier_decay = 0)]
HighTrs[, random := sample(0:99, nrow(HighTrs), replace=T)]
for(i in unique(HighTrs$segment)){ 
  HighTrs[segment==i , score.effort.with.uplifts := SegmentsDict[segment==i, effort.no.buffers * (1+uplift.buffer + mm.buffer)]] 
  HistData[userid %in% HighTrs[segment==i, userid], segment := i]
  }


FinalSegments <- rbind(HistData, HighTrs[!(userid %in% HistData$userid) , c("userid", "hist.gross.amount.decay", "isdepositor", "hist.score.cashier.decay", "hist.score.spins.decay", "random", "hist.personal.progress.decay", "hist_personal_base_coins_cashier_decay", "gamelevelid", "llrecency", "segment", "score.effort.with.uplifts") ])
FinalSegments <- merge(FinalSegments  , LevelMultiplier, by= c('gamelevelid'), all.x = TRUE,all.y = FALSE)
FinalSegments[ , hist_gross_amount_decay := hist.gross.amount.decay]

FinalSegments <- FinalSegments[ , .SD[1], by = .(userid)]  ## just to make sure there are no duplicates

FinalSegments[ , .(users = .N , multiplier95pct = quantile(multiplier,0.95,na.rm=T)) , by=.(segment)][order(segment)]
summary(FinalSegments)

FinalSegments[is.na(segment)]
FinalSegments[is.na(segment) , segment := SegmentsDict[name=="regular1", segment]]

impala.save("legends2_sim", "segment", FinalSegments[, c("userid", "segment", "hist_gross_amount_decay")])

Params <- as.data.table(read_delim("~/TEMP_ANDREY/HOF_Legends/Legends3/params.csv", "\t", escape_double = FALSE, trim_ws = TRUE))
Params <- Params[ParamChest == 'ALL' , c("ParamName", "ParamValue", "ParamChest")]

common_figures_total = as.numeric(Params[ParamName == "common_figures_total", ParamValue])
rare_figures_total = as.numeric(Params[ParamName == "rare_figures_total", ParamValue])
epic_figures_total = as.numeric(Params[ParamName == "epic_figures_total", ParamValue])

PlatinumMedals <- impala.query(connImpala , paste("
    select ceil(avg(total_common_medals)) total_common_medals, ceil(avg(total_rare_medals)) total_rare_medals, ceil(avg(total_epic_medals)) total_epic_medals
    from legends2_sim.chest_medals
    where chestname='platinum' "))
SegmentConfig <- SegmentsDict[ , c("segment", "name", "effort_platinum", "collect_silver",	"collect_gold",	"collect_platinum",	"collect_diamond",	"boost_collect_silver",	"boost_collect_gold",	"boost_collect_platinum",	"boost_collect_diamond",	"levelup_silver",	"levelup_gold",	"levelup_platinum",	"levelup_diamond")]

## Now in order to make the progress decreaseing for higher rarities, we use different factors:
SegmentConfig[ , ':=' (total_common_medals = 0.85 * effort_platinum * PlatinumMedals$total_common_medals,
                       total_rare_medals   = 1.00 * effort_platinum * PlatinumMedals$total_rare_medals,
                       total_epic_medals   = 1.15 * effort_platinum * PlatinumMedals$total_epic_medals)]
SegmentConfig[ , ':=' (single_fig_common_medals = ceil(total_common_medals/ common_figures_total),
                       single_fig_rare_medals   = ceil(total_rare_medals  / rare_figures_total),
                       single_fig_epic_medals   = ceil(total_epic_medals  / epic_figures_total) )]


impala.save("legends2_sim", "segment_config", SegmentConfig)


SimSpinsRaw <- impala.query(connImpala , paste("
select userid, gamelevelid, mtotalbetsamount/mbetscount as betamount, sum(mbetscount) as betscount
from dwh.dwh_fact_spin_agg
where mtotalbetsamount>0 and month between " ,sim.month.start , " and " , sim.month.end , " and dateid between " ,sim.dateid.start , " and " ,sim.dateid.end , "
group by 1,2,3"))

SimSpinsRaw <- merge(SimSpinsRaw , LevelFreqBet, by= c('gamelevelid'), all.x = TRUE,all.y = FALSE)
SimSpinsRaw <- merge(SimSpinsRaw , FinalSegments[ , c("userid","segment")], by= c('userid'), all.x = TRUE,all.y = FALSE)
SimSpinsRaw[is.na(segment), segment := 1]
SimSpinsRaw[ , betchange := (betamount/freqbet)]
SimSpinsRaw <- as.data.table(sqldf("SELECT * FROM SimSpinsRaw as a LEFT JOIN BetchangeScore as b 
                                   on a.segment=b.segment and a.betchange>=b.betchange_lb and a.betchange<betchange_ub") )

SimSpins <- SimSpinsRaw[ , .( sim.betscount=sum(betscount)
                              , sim.wager=sum(betscount*betamount)
                              , sim.median.gamelevelid=floor(median(gamelevelid))
                              , sim.median.betchange=weightedMedian(betchange,betscount)
                              , sim.score.spins=sum(betscount * betchange_score)
                              , sim.betchange.sum=sum(betscount * betchange) ) , by=.(userid,segment)]

SimTrans <- hbipa.query(connHBIPA , paste("
select userid, sum(Balance-BalanceBefore) as sim_actualcoins_cashier, sum(MAmount) as sim_gross_amount, sum(" ,money.to.score, " * b.Coins) as sim_score_cashier
from [Dwh_Pacific].[dbo].[Dwh_Fact_Transaction] as a
left join [Dwh_Pacific].[dbo].[Dwh_Dim_Pricelist] as b on CEILING(a.MAmount)=b.PriceID
where TranStatusID=1 and dateid between " ,sim.dateid.start , " and " ,sim.dateid.end , "
group by userid"))

SimData <- merge(SimSpins , SimTrans, by= c('userid'), all.x = TRUE,all.y = FALSE)
SimData <- merge(SimData , HistData, by= c('userid','segment'), all.x = TRUE,all.y = FALSE)
SimData[is.na(sim_gross_amount) , c("sim_actualcoins_cashier", "sim_gross_amount", "sim_score_cashier")] <- 0
SimData[ , sim.median.gamelevelid.rounded := (sim.median.gamelevelid-sim.median.gamelevelid%%50) ]
SimData[ , sim.score.cashier.and.spins := (sim_score_cashier + sim.score.spins)]
SimData[ , sim.score.progress := sim.score.cashier.and.spins / score.effort.with.uplifts]

# SimData <- merge(SimData , SimTransCoins, by= c('userid'), all.x = TRUE,all.y = FALSE)
# SimData[is.na(sim.trans.coins) , sim.trans.coins := 0]
# SimData <- merge(SimData , SimFreeCoins, by= c('userid'), all.x = TRUE,all.y = FALSE)
# SimData[is.na(sim.free.coins) , sim.free.coins := 0]

SimData[sim_gross_amount>10 , .(median(sim.score.progress, na.rm = T) , .N), by=.(segment)][order(segment)]
quantile(SimData[segment==20 , sim.score.progress], seq(0.8,1,0.01) , na.rm = T)
SimData[sim_gross_amount>10 , .(median(sim.score.spins/sim.score.cashier.and.spins, na.rm = T) , .N), by=.(segment)][order(segment)]
SimData[sim_gross_amount>10, .(median(sim.score.spins/sim.score.cashier.and.spins, na.rm = T) , .N), by=.(sim.median.gamelevelid.rounded)][order(sim.median.gamelevelid.rounded)]

SimData[, .(  score_5th_perc = quantile(sim.median.betchange,  0.05, na.rm = T) 
              , score_10th_perc = quantile(sim.median.betchange, 0.1,  na.rm = T)
              , score_90th_perc = quantile(sim.median.betchange, 0.9,  na.rm = T)
              , score_95th_perc = quantile(sim.median.betchange, 0.95, na.rm = T)
              , score_max = max(sim.median.betchange, na.rm = T) 
              , count = .N), by=.(segment)][order(segment)]

quantile(SimSpinsRaw[ , betchange] , seq(0,1,0.1) , na.rm = T) ## make sure all betchange range is covered in 'betchange_score'
Sys.time()
