require(RODBC)
require(data.table)
library(DBI)
library(odbc)
library(readr)
library(sqldf)

setwd("/home/andrey.lisovoy/TEMP_ANDREY/HOF_Legends/Legends3/")

options(scipen = 20)

hbipa.query = function (channel.name , query){
  channel <- odbcDriverConnect(channel.name)
  dt <- data.table(sqlQuery(channel, query, stringsAsFactors = FALSE))
  odbcClose(channel)
  dt
}

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

rounding = function (number , precision=0.05){
  round_vector <- c(1, 5, 10,50,100,500,1000,2500,5000,10000,25000,50000,100000,250000,500000,1e6,2.5e6,5e6,10e6,25e6,50e6,100e6,250e6,500e6,1e9,2.5e9,5e9,10e9)
  round_to = max(round_vector[abs(round(number/round_vector)*round_vector - number)< (precision*number)] , 1)
  max(round(number/round_to)*round_to, 1)
}



connAnalysis <- paste("Driver=ODBC Driver 13 for SQL Server; Server=10.145.16.22; Database=Analysis;    Uid=r_reader; Pwd=hIDGPJ4ioW2R5AaW7ap7" ,sep='' )
connHBIPA    <- paste("Driver=ODBC Driver 13 for SQL Server; Server=10.145.16.21; Database=Dwh_Pacific; Uid=r_reader; Pwd=hIDGPJ4ioW2R5AaW7ap7" ,sep='' )
connImpala   <- paste("Impala")


############################################################ UPLOAD TABLES ########################################################################################

expirience_table <- as.data.table(read_delim("~/TEMP_ANDREY/HOF_Legends/Legends3/expirience_table.csv", "\t", escape_double = FALSE, trim_ws = TRUE))
segments <- data.table(segment_id = seq(min(expirience_table$from_segment):max(expirience_table$to_segment)))
ExpirienceTable <- as.data.table(sqldf("SELECT * FROM segments as a JOIN expirience_table as b on segment_id between b.from_segment and b.to_segment") )
impala.save("legends2_sim", "expirience", ExpirienceTable)

level_mult_to_denomenator <- 800
LevelDenom <- hbipa.query(connHBIPA , paste("
                                            select level_id as gamelevelid, offer_coins_multiplier_base, " , level_mult_to_denomenator, " * offer_coins_multiplier_base as level_denom
                                            from [HOF-HBIP].[sources].[dbo].[MRR_level] 
                                            where platform_type_id=1 and group_id=1"))
LevelDenom[between(gamelevelid, 1, 24) , level_denom := level_denom/2] ## making up better expirience for users in first levels
impala.save("legends2_sim", "level_denom", LevelDenom)

PriceListTable <- as.data.table(read_delim("~/TEMP_ANDREY/HOF_Legends/Legends3/price_list_table.csv", "\t", escape_double = FALSE, trim_ws = TRUE))
PriceListTable[ , chestname_short := str_sub(chests, -1)]
PriceListTable[ , chestname := ifelse(chestname_short=="G", 'gold', ifelse(chestname_short=="P", 'platinum', ifelse(chestname_short=="D", 'diamond', 'unknown')))] 
PriceListTable[ , chest_amount := str_sub(chests, 1, nchar(chests)-1 )]
PriceListTableExpanded <- PriceListTable[rep(seq_len(nrow(PriceListTable)), PriceListTable$chest_amount), c("denom","chestname")]
impala.save("legends2_sim", "price_list", PriceListTableExpanded)

FullParams <- as.data.table(read_delim("~/TEMP_ANDREY/HOF_Legends/Legends3/params.csv", "\t", escape_double = FALSE, trim_ws = TRUE))
from_date_id=as.numeric(FullParams[ParamName		== "from_date_id", "ParamValue"])
to_date_id=as.numeric(FullParams[ParamName		== "to_date_id", "ParamValue"])
common_figures_total =as.numeric(FullParams[ParamName == "common_figures_total","ParamValue"])
rare_figures_total=as.numeric(FullParams[ParamName == "rare_figures_total","ParamValue"])
epic_figures_total=as.numeric(FullParams[ParamName == "epic_figures_total","ParamValue"])
row_num=as.numeric(FullParams[ParamName == "row_num","ParamValue"])

# rows <- 1e6
# RandomSeq <- data.table(index = seq(1:rows), random1 = runif(rows) , random2 = runif(rows) , random3 = runif(rows))
# impala.save("legends2_sim", "random_seq", RandomSeq)

############################################################ END - UPLOAD TABLES ########################################################################################



medals_generator <- function(min_medals,max_medals,mult){return(min_medals + mult*round((rtnorm(1,lower=0,upper=1,sd=0.25)*(max_medals-min_medals))/mult))}

Sys.time()
all_chest_config <- data.table()
input_param <- c("silver","gold","platinum","diamond")
for(chestname in input_param){
  Params <- FullParams[ParamChest == chestname , c("ParamName", "ParamValue", "ParamChest")]  
  
  common_0_prob=as.numeric(Params[ParamName == "common_0_prob","ParamValue"])
  common_1_prob=as.numeric(Params[ParamName == "common_1_prob","ParamValue"])
  common_2_prob=as.numeric(Params[ParamName == "common_2_prob","ParamValue"])
  common_3_prob=as.numeric(Params[ParamName == "common_3_prob","ParamValue"])
  rare_0_prob=as.numeric(Params[ParamName == "rare_0_prob","ParamValue"])
  rare_1_prob=as.numeric(Params[ParamName == "rare_1_prob","ParamValue"])
  rare_2_prob=as.numeric(Params[ParamName == "rare_2_prob","ParamValue"])
  rare_3_prob=as.numeric(Params[ParamName == "rare_3_prob","ParamValue"])
  epic_0_prob=as.numeric(Params[ParamName == "epic_0_prob","ParamValue"])
  epic_1_prob=as.numeric(Params[ParamName == "epic_1_prob","ParamValue"])
  epic_2_prob=as.numeric(Params[ParamName 	== "epic_2_prob","ParamValue"])
  min_total_common_medals=as.numeric(Params[ParamName == "min_total_common_medals","ParamValue"])
  max_total_common_medals=as.numeric(Params[ParamName == "max_total_common_medals","ParamValue"])
  common_medals_multiple= as.numeric(Params[ParamName		== "common_medals_multiple","ParamValue"])
  min_total_rare_medals  =as.numeric(Params[ParamName		== "min_total_rare_medals","ParamValue"])
  max_total_rare_medals  =as.numeric(Params[ParamName		== "max_total_rare_medals","ParamValue"])
  rare_medals_multiple  = as.numeric(Params[ParamName		== "rare_medals_multiple","ParamValue"])
  min_total_epic_medals  =as.numeric(Params[ParamName		== "min_total_epic_medals","ParamValue"])
  max_total_epic_medals  =as.numeric(Params[ParamName		== "max_total_epic_medals","ParamValue"])
  epic_medals_multiple  = as.numeric(Params[ParamName		== "epic_medals_multiple","ParamValue"])
  
  common_stacks <- sample(c(0:common_figures_total), row_num, prob=c(common_0_prob,common_1_prob,common_2_prob,common_3_prob), replace = T)
  rare_stacks <- sample(c(0:common_figures_total), row_num, prob=c(rare_0_prob,rare_1_prob,rare_2_prob,rare_3_prob), replace = T)
  epic_stacks <- sample(c(0:epic_figures_total), row_num, prob=c(epic_0_prob,epic_1_prob,epic_2_prob), replace = T)
  
  common_medals=data.table(COMMON_FIG_1=rep(0,row_num), COMMON_FIG_2=rep(0,row_num), COMMON_FIG_3=rep(0,row_num))
  rare_medals=data.table(RARE_FIG_1=rep(0,row_num), RARE_FIG_2=rep(0,row_num), RARE_FIG_3=rep(0,row_num))
  epic_medals=data.table(EPIC_FIG_1=rep(0,row_num), EPIC_FIG_2=rep(0,row_num))
  
  common_figures <- lapply(common_stacks, function(x) {sample(common_figures_total,x,replace=F)})
  rare_figures <- lapply(rare_stacks, function(x) {sample(rare_figures_total,x,replace=F)})
  epic_figures <- lapply(epic_stacks, function(x) {sample(epic_figures_total,x,replace=F)})
  
  for(i in which(common_stacks>0)){common_medals[i,common_figures[[i]] := lapply(.SD, function(x) medals_generator(min_total_common_medals,max_total_common_medals,common_medals_multiple) ), .SDcols = common_figures[[i]] ]}
  common_medals[, ':=' (COMMON_FIG_1 = round(1.0 * COMMON_FIG_1), COMMON_FIG_2 = round(0.85 * COMMON_FIG_2), COMMON_FIG_3 = round(1.15 * COMMON_FIG_3))]
  common_medals[, total_common_medals := apply(common_medals, 1, sum)]
  
  for(i in which(rare_stacks>0)){rare_medals[i,rare_figures[[i]] := lapply(.SD, function(x) medals_generator(min_total_rare_medals,max_total_rare_medals,rare_medals_multiple) ), .SDcols = rare_figures[[i]] ]}
  rare_medals[, ':=' (RARE_FIG_1 = round(1.0 * RARE_FIG_1), RARE_FIG_2 = round(1.15 * RARE_FIG_2), RARE_FIG_3 = round(0.85 * RARE_FIG_3))]
  rare_medals[, total_rare_medals := apply(rare_medals, 1, sum)]
  
  for(i in which(epic_stacks>0)){epic_medals[i,epic_figures[[i]] := lapply(.SD, function(x) medals_generator(min_total_epic_medals,max_total_epic_medals,epic_medals_multiple) ), .SDcols = epic_figures[[i]] ]}
  epic_medals[, ':=' (EPIC_FIG_1 = round(0.85 * EPIC_FIG_1), EPIC_FIG_2 = round(1.15 * EPIC_FIG_2))]
  epic_medals[, total_epic_medals := apply(epic_medals, 1, sum)]
  
  chest_config <- cbind(common_stacks, rare_stacks, epic_stacks, common_medals, rare_medals, epic_medals, INDEX=seq(1:row_num), chestname=chestname)
  all_chest_config <- rbind(all_chest_config , chest_config)
}
impala.save("legends2_sim", "chest_medals", all_chest_config)


###################################################### Pre_Computed Chests Validation ###################################################

impala.query(connImpala , paste("
select chestname, c, r, e, count(*)
from(
  select chestname,`index`
  ,sum(case when common_fig_1 >0 then 1 else 0 end + case when common_fig_2 >0 then 1 else 0 end + case when common_fig_3 >0 then 1 else 0 end) as c
  ,sum(case when rare_fig_1 >0 then 1 else 0 end + case when rare_fig_2 >0 then 1 else 0 end + case when rare_fig_3 >0 then 1 else 0 end) as r
  ,sum(case when epic_fig_1 >0 then 1 else 0 end + case when epic_fig_2 >0 then 1 else 0 end) as e
  from legends2_sim.chest_medals 
  group by 1,2
) as tmp 
group by 1,2,3,4 
order by 1,2,3,4"))

impala.query(connImpala , paste("
select chestname
, min(case when common_fig_1>0 then common_fig_1 else null end), max(common_fig_1), avg(case when common_fig_1>0 then common_fig_1 else null end)
, min(case when common_fig_2>0 then common_fig_2 else null end), max(common_fig_2), avg(case when common_fig_2>0 then common_fig_2 else null end)
, min(case when common_fig_3>0 then common_fig_3 else null end), max(common_fig_3), avg(case when common_fig_3>0 then common_fig_3 else null end)
, min(case when rare_fig_1>0 then rare_fig_1 else null end), max(rare_fig_1), avg(case when rare_fig_1>0 then rare_fig_1 else null end)
, min(case when rare_fig_2>0 then rare_fig_2 else null end), max(rare_fig_2), avg(case when rare_fig_2>0 then rare_fig_2 else null end)
, min(case when rare_fig_3>0 then rare_fig_3 else null end), max(rare_fig_3), avg(case when rare_fig_3>0 then rare_fig_3 else null end)
, min(case when epic_fig_1>0 then epic_fig_1 else null end), max(epic_fig_1), avg(case when epic_fig_1>0 then epic_fig_1 else null end)
, min(case when epic_fig_2>0 then epic_fig_2 else null end), max(epic_fig_2), avg(case when epic_fig_2>0 then epic_fig_2 else null end)
from legends2_sim.chest_medals 
group by 1"))



##############################################################################################################################################
#########################################################      COLLECT     ###################################################################

collect_wheel_daily_num=as.numeric(FullParams[ParamName	== "collect_wheel_daily_num", "ParamValue"])

impala.query(connImpala , paste("drop table legends2_sim.collect_raw"))
impala.query(connImpala , paste("
                                create table legends2_sim.collect_raw as
                                select tmp2.userid, ts, gamelevelid, event, dateid
                                , case when rnd.random1 <  (collect_silver) then 'silver'
                                when rnd.random1 <  (collect_silver + collect_gold) then 'gold'
                                when rnd.random1 <  (collect_silver + collect_gold + collect_platinum) then 'platinum'
                                when rnd.random1 <= (collect_silver + collect_gold + collect_platinum + collect_diamond) then 'diamond'
                                else 'unknown' end as chestname
                                from(
                                select tmp.*, row_number() over (partition by userid,dateid order by ts) as rn
                                ,((row_number() over (order by ts))%1e6)+1 as rn_modulo_1m
                                from(
                                select userid, ts,gamelevelid, 'collect_or_wheel' as event,dateid
                                FROM  dwh.dwh_fact_collectbonus 
                                where month between ",as.numeric(substr(as.character(from_date_id),1,6))  ," and " ,as.numeric(substr(as.character(to_date_id),1,6))
                                , " and dateid>=",from_date_id, " and dateid <=",to_date_id , "
                                UNION ALL
                                select userid, ts,gamelevelid, 'collect_or_wheel' as event,dateid
                                FROM  dwh.dwh_fact_minigame 
                                where month between ",as.numeric(substr(as.character(from_date_id),1,6))  ," and " ,as.numeric(substr(as.character(to_date_id),1,6))
                                , " and dateid>=",from_date_id, " and dateid <=",to_date_id , "
                                ) as tmp
                                ) as tmp2
                                left join legends2_sim.random_seq as rnd on rnd.index=tmp2.rn_modulo_1m
                                left join legends2_sim.segment as b on tmp2.userid=b.segment
                                left join legends2_sim.segment_config as c on nvl(b.segment,1) = c.segment
                                where rn<=" ,collect_wheel_daily_num))


##############################################################################################################################################
#########################################################      LEVELUP     ###################################################################

Params <- FullParams[ParamChest == "LevelUp" , c("ParamName", "ParamValue", "ParamChest")]  

lu_pmod_1_99=as.numeric(Params[ParamName		== "lu_pmod_1_99", "ParamValue"])
lu_pmod_100_199=as.numeric(Params[ParamName		== "lu_pmod_100_199", "ParamValue"])
lu_pmod_200_299=as.numeric(Params[ParamName		== "lu_pmod_200_299", "ParamValue"])
lu_pmod_300_399=as.numeric(Params[ParamName		== "lu_pmod_300_399", "ParamValue"])
lu_pmod_400_499=as.numeric(Params[ParamName		== "lu_pmod_400_499", "ParamValue"])
lu_pmod_500_plus=as.numeric(Params[ParamName		== "lu_pmod_500_plus", "ParamValue"])

impala.query(connImpala , paste("drop table legends2_sim.levelup_raw"))
impala.query(connImpala , paste("
                                create table legends2_sim.levelup_raw as
                                select tmp.userid, ts ,gamelevelid, 'level_up' as event,level_before
                                , case when rnd.random1 <  (levelup_silver) then 'silver'
                                when rnd.random1 <  (levelup_silver + levelup_gold) then 'gold'
                                when rnd.random1 <  (levelup_silver + levelup_gold + levelup_platinum) then 'platinum'
                                when rnd.random1 <= (levelup_silver + levelup_gold + levelup_platinum + levelup_diamond) then 'diamond'
                                else 'unknown' end as chestname
                                from(
                                select userid,fromdate as ts ,gamelevelid, gamelevelid-1 as level_before
                                ,((row_number() over (order by fromdate))%1e6)+1 as rn_modulo_1m
                                from dwh.dwh_fact_levelup 
                                where  ( (gamelevelid-1 between 1 and 99 and pmod(gamelevelid,",lu_pmod_1_99 ,")=0) or 
                                (gamelevelid-1 between 100 and 199 and pmod(gamelevelid,",lu_pmod_100_199,")=0) or 
                                (gamelevelid-1 between 200 and 299 and pmod(gamelevelid,",lu_pmod_200_299,")=0) or
                                (gamelevelid-1 between 300 and 399 and pmod(gamelevelid,",lu_pmod_300_399,")=0) or 
                                (gamelevelid-1 between 400 and 499 and pmod(gamelevelid,",lu_pmod_400_499,")=0) or
                                (gamelevelid-1 >=500  and pmod(gamelevelid,",lu_pmod_500_plus,")=0) ) 
                                and month between ",as.numeric(substr(as.character(from_date_id),1,6))  ," and " ,as.numeric(substr(as.character(to_date_id),1,6)) , "
                                and fromdatedateid >=",from_date_id, " and fromdatedateid <=",to_date_id, "
                                ) as tmp
                                left join legends2_sim.random_seq as rnd on rnd.index=tmp.rn_modulo_1m
                                left join legends2_sim.segment as b on tmp.userid=b.segment
                                left join legends2_sim.segment_config as c on nvl(b.segment,1) = c.segment"))


##############################################################################################################################################
#########################################################        SPIN      ###################################################################

DatesTable <- impala.query(connImpala , paste("select distinct dateid,dateshort from dwh.dwh_dim_date where dateid >=",from_date_id," and dateid <= ",to_date_id, "order by dateid"))
DatesTable$iteration <- rep(1:nrow(DatesTable), each=7)

print(DatesTable)

impala.query(connImpala , paste("DROP TABLE legends2_sim.spins_raw"))
for (i in 1:max(DatesTable$iteration)){
  create_or_insert <- ifelse(i==1, "CREATE TABLE legends2_sim.spins_raw as ", "INSERT INTO TABLE legends2_sim.spins_raw " )
  impala.query(connImpala , paste(create_or_insert , " 
                                  with
                                  bets as(
                                  select distinct mbetsamount,gamelevelid
                                  from dwh.dwh_fact_spin 
                                  where mbetsamount>0 and dateid between " , min(DatesTable[iteration==i , dateid]) ," and " , max(DatesTable[iteration==i , dateid]) , " 
                                  ),  
                                  config as(
                                  select a.mbetsamount,a.gamelevelid, c.segment_id, c.silver, c.gold, c.platinum, c.diamond, c.hit_rate
                                  from bets as a
                                  join legends2_sim.level_denom as b on a.gamelevelid=b.gamelevelid
                                  join legends2_sim.expirience as c on (a.mbetsamount/b.level_denom) between c.betchange_lb and c.betchange_ub
                                  ),
                                  spins as( 
                                  select a.userid, a.ts, a.dateid, a.gamelevelid, config.silver, config.gold, config.platinum, config.diamond, config.hit_rate
                                  , ((row_number() over (order by a.ts))%1e6)+1 as rn_modulo_1m
                                  from dwh.dwh_fact_spin as a
                                  left join legends2_sim.segment on segment.userid=a.userid 
                                  join config on a.gamelevelid=config.gamelevelid and a.mbetsamount=config.mbetsamount and nvl(segment.segment,1)=config.segment_id
                                  where a.mbetsamount>0 and dateid between " , min(DatesTable[iteration==i , dateid]) ," and " , max(DatesTable[iteration==i , dateid]) , " 
                                  )
                                  select userid,ts,gamelevelid,'spin' as event,dateid, index
                                  ,case when rnd.random2<silver then 'silver'
                                  when rnd.random2<(silver+gold) then 'gold'
                                  when rnd.random2<(silver+gold+platinum) then 'platinum'
                                  when rnd.random2<=(silver+gold+platinum+diamond) then 'diamond'
                                  else 'unknown' end as chestname 
                                  from spins
                                  left join legends2_sim.random_seq as rnd on rnd.index=spins.rn_modulo_1m
                                  where rnd.random1 < 1/hit_rate "))
}

##############################################################################################################################################
#########################################################        DEPOSIT   ###################################################################

impala.query(connImpala , paste("DROP TABLE legends2_sim.deposit_raw"))
impala.query(connImpala , paste("
                                CREATE TABLE legends2_sim.deposit_raw as
                                select userid, ts, gamelevelid, round(mamount,0) as denom, 'Deposit' as event, b.chestname
                                from dwh.dwh_fact_transaction as a
                                left join legends2_sim.price_list as b on round(mamount,0)=b.denom
                                where dateid >=",from_date_id," and dateid <= ",to_date_id," and transtatusid=1"))


##############################################################################################################################################
#########################################################     TOKENS - CONFIG   ###################################################################

PriceListTable <- as.data.table(read_delim("~/TEMP_ANDREY/HOF_Legends/Legends3/price_list_table.csv", "\t", escape_double = FALSE, trim_ws = TRUE))
impala.save("legends2_sim", "price_list_tokens", PriceListTable[,c("denom","tokens")])

segments_config <- as.data.table(read_delim("~/TEMP_ANDREY/HOF_Legends/Legends3/segments_config.csv", "\t", escape_double = FALSE, trim_ws = TRUE))

reels_config <- as.data.table(read_delim("~/TEMP_ANDREY/HOF_Legends/Legends3/legends3_reels_config.csv", "\t", escape_double = FALSE, trim_ws = TRUE))

promoid <- 79
# from_date_id <-20180420 ##20180224
# to_date_id <- 20180625 ##20180420
min_figureid <- 9
max_figureid <- 16

discount_on_purchase <- 0.13 ## finally (with spins+rankups) it will be higher

impala.query(connImpala , paste("DROP TABLE legends2_sim.deposit_raw_tokens"))
impala.query(connImpala , paste("
                                CREATE TABLE legends2_sim.deposit_raw_tokens as
                                select a.userid, ts, a.gamelevelid, round(mamount,0) as denom, 'Deposit' as event, b.tokens, nvl(segment,1) as segment, (a.moffercoinsamount * offer_coins_multiplier_base) as coins
                                from dwh.dwh_fact_transaction as a
                                left join legends2_sim.price_list_tokens as b on round(mamount,0)=b.denom
                                left join legends2_sim.segment on segment.userid=a.userid
                                left join legends2_sim.level_denom on level_denom.gamelevelid=a.gamelevelid
                                where dateid >=",from_date_id," and dateid < ",to_date_id," and transtatusid=1"))


tokens_spins_config <- as.data.table(read_delim("~/TEMP_ANDREY/HOF_Legends/Legends3/tokens_spins_config.csv", "\t", escape_double = FALSE, trim_ws = TRUE))
impala.save("legends2_sim", "tokens_spins_config", tokens_spins_config)
View(setcolorder(cast(tokens_spins_config, segment~chestname, value="probability", mean), c("segment","silver","gold","platinum","diamond")))
View(setcolorder(cast(tokens_spins_config, segment~chestname, value="tokens", mean), c("segment","silver","gold","platinum","diamond")))


impala.query(connImpala , paste("drop table legends2_sim.spins_raw_tokens"))
impala.query(connImpala , paste("
                                create table legends2_sim.spins_raw_tokens as
                                select spins.userid, ts, gamelevelid, 'spin' as event, spins.chestname, tokens, tokens.segment
                                from legends2_sim.spins_raw as spins
                                left join legends2_sim.random_seq as rnd on rnd.index=spins.index
                                left join legends2_sim.segment on segment.userid=spins.userid
                                join legends2_sim.tokens_spins_config as tokens on tokens.probability>0 and tokens.chestname=spins.chestname and nvl(segment.segment,1)=tokens.segment
                                where rnd.random3 < tokens.probability
                                "))

# impala.query(connImpala , paste("drop table legends2_sim.spins_raw_tokens"))
# impala.query(connImpala , paste("
#                                 CREATE TABLE legends2_sim.spins_raw_tokens as
#                                 select tmp.userid,  min(nvl(segment.segment,1)) as segment, sum(tokens) as tokens   
#                                 from(
#                                 select userid, lower(b.chesttypedesc) as chestname, ((row_number() over (order by ts))%1e6)+1 as index
#                                 from dwh.dwh_fact_hl_chest as a
#                                 join dwh.dwh_dim_hl_chesttype b on b.chesttypeid=a.chesttypeid
#                                 where hl_actionsk=2 and chesttriggertypeid=8 and promoid=",promoid,"
#                                 ) as tmp
#                                 left join legends2_sim.random_seq as rnd on rnd.index=tmp.index
#                                 left join legends2_sim.segment on segment.userid=tmp.userid
#                                 join legends2_sim.tokens_spins_config as tokens on tokens.probability>0 and tokens.chestname=tmp.chestname and nvl(segment.segment,1)=tokens.segment
#                                 where rnd.random3 < tokens.probability
#                                 group by 1 "))   


ev_raw <- reels_config[ , .(ev = sum(value*probability)), reel_type]

segments_config[ ,  ':=' (    bet_reel1 = sapply( (tokens_transaction/superspins_total)*(ev_raw$ev[1]/ev_raw$ev[2]), rounding) 
                           ,  bet_reel2 = sapply( (tokens_transaction/superspins_total)*(ev_raw$ev[2]/ev_raw$ev[2]), rounding)
                           ,  bet_reel3 = sapply( (tokens_transaction/superspins_total)*(ev_raw$ev[3]/ev_raw$ev[2]), rounding) )]

segments_config[ ,  ':=' (    ev_reel1 = bet_reel1 * tokens_to_base_coins * discount_on_purchase * level_mult_prcnt95
                           ,  ev_reel2 = bet_reel2 * tokens_to_base_coins * discount_on_purchase * level_mult_prcnt95
                           ,  ev_reel3 = bet_reel3 * tokens_to_base_coins * discount_on_purchase * level_mult_prcnt95)]

# segments_config[ , total_coins_reward_from_purchased_tokens := tokens_transaction * tokens_to_base_coins * discount_on_purchase * level_mult_prcnt95]
impala.save("legends2_sim", "tokens_config", segments_config[,c("segment","name","level_mult_prcnt95","superspins_total","superspins_rankups","tokens_to_base_coins","tokens_transaction", "bet_reel1", "bet_reel2", "bet_reel3", "ev_reel1", "ev_reel2", "ev_reel3")])
View(segments_config[,c("segment","name","level_mult_prcnt95","superspins_total","superspins_rankups","tokens_to_base_coins","tokens_transaction", "bet_reel1", "bet_reel2", "bet_reel3", "ev_reel1", "ev_reel2", "ev_reel3")])

reels_config_segments <- data.table(merge(data.frame(reels_config), data.frame(segments_config[ , c("segment" , "ev_reel1", "ev_reel2", "ev_reel3")]), by = NULL))

reels_config_segments[ , reel_id := segment]
reels_config_segments[reel_type==1, raw_wage := value * (ev_reel1 / reels_config[reel_type==1,sum(probability*value)])]
reels_config_segments[reel_type==2, raw_wage := value * (ev_reel2 / reels_config[reel_type==2,sum(probability*value)])]
reels_config_segments[reel_type==3, raw_wage := value * (ev_reel3 / reels_config[reel_type==3,sum(probability*value)])]
reels_config_segments[ , wage := sapply(reels_config_segments$raw_wage, rounding)]

tmp <- reels_config_segments[type %like% 'Common', .(common_jackpot = max(wage))  , .(reel_id)]
reels_config_segments <- merge(reels_config_segments , tmp, by ="reel_id")
reels_config_segments[type %like% 'Common', wage := common_jackpot]
tmp <- reels_config_segments[type %like% 'Rare',   .(rare_jackpot   = max(wage))  , .(reel_id)]
reels_config_segments <- merge(reels_config_segments , tmp, by ="reel_id")
reels_config_segments[type %like% 'Rare', wage := rare_jackpot]
tmp <- reels_config_segments[type %like% 'Epic',   .(epic_jackpot   = max(wage))  , .(reel_id)]
reels_config_segments <- merge(reels_config_segments , tmp, by ="reel_id")
reels_config_segments[type %like% 'Epic', wage := epic_jackpot]

reels_config_segments[ , (ev = sum(wage*probability)), .(segment, reel_type)]
write.csv(reels_config_segments[ , c("reel_id",	"reel_type",	"position",	"reward_type",	"figure_type",	"wage",	"probability")], "reels_config_segments.csv", row.names = F)


tokens_rankup_config <- as.data.table(read_csv("~/TEMP_ANDREY/HOF_Legends/Legends3/tokens_rankup_config.csv"))
figureid_table <- impala.query(connImpala , paste("select figurerarityid, figureid from dwh.dwh_dim_hl_figure where figureid between ",min_figureid, " and " , max_figureid))
tokens_rankup_config_full <- as.data.table(sqldf(paste(
  "SELECT segment, b.*, a.figureid, superspins_rankups, bet_reel1, bet_reel2, bet_reel3
  FROM figureid_table as a 
  JOIN tokens_rankup_config as b on a.figurerarityid=b.figurerarityid
  CROSS JOIN  segments_config ")))
tokens_rankup_config_full[,tokens_prcnt := tokens_weight/sum(tokens_weight) , by=.(segment)]
tokens_rankup_config_full[,tokens := ceil(tokens_prcnt * superspins_rankups * bet_reel2) ]
tokens_rankup_config_full[figureid %in% c(9,10,12) & rankid==1, tokens := ifelse((1/3)*bet_reel1 < tokens, tokens, ceil((1/3)*bet_reel1))]
impala.save("legends2_sim", "tokens_rankup_config", tokens_rankup_config_full[,c("segment","figureid","rankid","tokens")])
View(cast(tokens_rankup_config_full, segment~figureid+rankid, value="tokens", mean))

impala.query(connImpala , paste("DROP TABLE legends2_sim.rankup_raw_tokens"))
impala.query(connImpala , paste("
                                CREATE TABLE legends2_sim.rankup_raw_tokens as
                                select a.userid, b.figureid, a.rankid, dateid, 'RankUP' as event, tokens, b.segment
                                from dwh.dwh_fact_hl_milestone as a
                                left join legends2_sim.segment c on c.userid=a.userid
                                join legends2_sim.tokens_rankup_config as b on a.figureid=b.figureid and a.rankid=b.rankid and nvl(c.segment,1)=b.segment
                                where promoid=",promoid ," and hl_actionsk=5"))

##################################################### Economy Validations ##########################################################

## make sure the tutorial chest will guarantee 
tokens_rankup_config_full[figureid %in% c(9,10,12) & rankid==1, .(tutorial_tokens = sum(tokens) , sum(tokens)>=bet_reel1 ), .(segment, bet_reel1)]

## make sure the MAX CAP is higher than TOkens in Diamond chest 
merge(tokens_spins_config[ ,.(tokens_from_chest = max(tokens)) , segment], tokens_rankup_config_full[ ,.(tokens_max_cap = max(10 * bet_reel3)) , segment], by="segment")

## RankUp tokens senity: make sure the coins received from RankUp Tokens are ~15% out of regular RankUp coins: 'senity' col in 'segments_config' excel file

## Spins tokens senity: (1) median ratio of tokens from spins vs. deposits (2) received coins from spin tokens should be ~0.5% out of Wager (this is increase in PO)
ChestsTokensSenity <- impala.query(connImpala , paste("
                                                      with 
                                                      wager as(
                                                      select userid , sum(mtotalbetsamount) as wager
                                                      from dwh.dwh_fact_spin_agg 
                                                      where dateid between 20180420 and 20180625
                                                      group by 1 
                                                      having sum(mbetscount)>1000
                                                      )
                                                      ,tokens_spins as(
                                                      select a.userid, min(segment) as segment, sum(a.tokens) as tokens
                                                      from legends2_sim.spins_raw_tokens as a
                                                      group by 1)
                                                      select wager.userid, b.segment, round(ev_reel1*(tokens/c.bet_reel1) / wager,5) as po
                                                      from wager 
                                                      join tokens_spins as b on b.userid=wager.userid
                                                      join legends2_sim.tokens_config as c on c.segment=b.segment "))
View(ChestsTokensSenity[ ,.(quantile(po, 0.05),quantile(po, 0.50),quantile(po, 0.95),quantile(po, 0.99), .N) , segment][order(segment)])

## Deposits tokens:      received coins from purchased tokens   should be ~15% out of regular purchased coins
## Global tokens senity: received coins from all tokens sources should be ~1%  out of Wager
impala.query(connImpala , paste("
                                with
                                deposit as(
                                select userid, sum(denom) as mamount, sum(coins) as coins_purchased, sum(tokens) as deposit_tokens
                                from legends2_sim.deposit_raw_tokens as a
                                group by 1)
                                ,wager as(
                                select userid , sum(mtotalbetsamount) as wager
                                from dwh.dwh_fact_spin_agg 
                                where dateid >=",from_date_id," and dateid < ",to_date_id,"
                                group by 1 
                                having sum(mbetscount)>1000)
                                select nvl(a.segment,1) , appx_median(tokens) as promoTokens
                                , appx_median(((deposit_tokens/bet_reel1)*ev_reel1) / coins_purchased) as discount_purchase_only
                                , appx_median(((tokens/bet_reel1)*ev_reel1) / coins_purchased) as discount_all_sources
                                , appx_median((tokens/bet_reel1)*ev_reel1) as coins_tokens_all_sources
                                , appx_median(ev_reel1*(tokens/bet_reel1) / wager) as po
                                from(
                                select userid, sum(tokens) as tokens
                                from(
                                select userid, tokens
                                from legends2_sim.deposit_raw_tokens  
                                union all
                                select userid, tokens
                                from legends2_sim.rankup_raw_tokens
                                union all 
                                select userid, tokens
                                from legends2_sim.spins_raw_tokens
                                ) as t
                                group by 1
                                ) as tmp
                                join deposit on deposit.userid= tmp.userid and mamount>50
                                left join legends2_sim.segment a on a.userid=tmp.userid
                                left join legends2_sim.tokens_config as b on b.segment=nvl(a.segment,1)
                                left join wager on wager.userid=tmp.userid
                                group by 1 
                                order by 1"))


##############################################################################################################################################
#########################################################        SUMMARY   ###################################################################

common_1_min_total=as.numeric(FullParams[ParamName== "common_1_min_total", "ParamValue"])
common_2_min_total=as.numeric(FullParams[ParamName== "common_2_min_total", "ParamValue"])
common_3_min_total=as.numeric(FullParams[ParamName== "common_3_min_total", "ParamValue"])
common_4_min_total=as.numeric(FullParams[ParamName== "common_4_min_total", "ParamValue"])
common_5_min_total=as.numeric(FullParams[ParamName== "common_5_min_total", "ParamValue"])

rare_1_min_total=as.numeric(FullParams[ParamName== "rare_1_min_total", "ParamValue"])
rare_2_min_total=as.numeric(FullParams[ParamName== "rare_2_min_total", "ParamValue"])
rare_3_min_total=as.numeric(FullParams[ParamName== "rare_3_min_total", "ParamValue"])
rare_4_min_total=as.numeric(FullParams[ParamName== "rare_4_min_total", "ParamValue"])
rare_5_min_total=as.numeric(FullParams[ParamName== "rare_5_min_total", "ParamValue"])

epic_1_min_total=as.numeric(FullParams[ParamName== "epic_1_min_total", "ParamValue"])
epic_2_min_total=as.numeric(FullParams[ParamName== "epic_2_min_total", "ParamValue"])
epic_3_min_total=as.numeric(FullParams[ParamName== "epic_3_min_total", "ParamValue"])
epic_4_min_total=as.numeric(FullParams[ParamName== "epic_4_min_total", "ParamValue"])
epic_5_min_total=as.numeric(FullParams[ParamName== "epic_5_min_total", "ParamValue"])


impala.query(connImpala , paste("drop table legends2_sim.collectible_all_events"))
impala.query(connImpala , paste("
                                CREATE TABLE legends2_sim.collectible_all_events AS 
                                select userid,ts, month, day, gamelevelid, event, b.*, tmp2.rn_modulo_row_num
                                from(
                                select * ,  ((row_number() over (order by ts))%",row_num,")+1 as rn_modulo_row_num
                                from(
                                select userid,ts, month(ts) as month, day(ts) as day, cast(gamelevelid as int) as gamelevelid ,cast(event as string) as event,chestname
                                from  legends2_sim.collect_raw 
                                UNION ALL 
                                select userid,ts, month(ts) as month, day(ts) as day, cast(gamelevelid as int) as gamelevelid ,cast(event as string) as event,chestname 
                                from legends2_sim.levelup_raw 
                                UNION ALL 
                                select userid,ts, month(ts) as month, day(ts) as day, cast(gamelevelid as int) as gamelevelid ,cast(event as string) as event,chestname
                                from legends2_sim.spins_raw
                                UNION ALL 
                                select userid,ts, month(ts) as month, day(ts) as day, cast(gamelevelid as int) as gamelevelid ,cast(event as string) as event,chestname
                                from legends2_sim.deposit_raw
                                ) as tmp 
                                ) as tmp2
                                left join legends2_sim.chest_medals as b on b.chestname=tmp2.chestname and b.index=tmp2.rn_modulo_row_num"))

impala.query(connImpala , paste("drop table legends2_sim.collectible_running_totals"))
impala.query(connImpala , paste("
                                CREATE TABLE legends2_sim.collectible_running_totals AS 
                                select ",
                                paste(" sum(common_fig_", 1:common_figures_total,"_daily_grant) over (partition by userid order by month,day) as common_fig_",1:common_figures_total,"_running_total , " ,sep="",collapse = " "),
                                paste(" sum(rare_fig_",   1:rare_figures_total,"_daily_grant)   over (partition by userid order by month,day) as rare_fig_",  1:rare_figures_total,  "_running_total , " ,sep="",collapse = " "),
                                paste(" sum(epic_fig_",   1:epic_figures_total,"_daily_grant)   over (partition by userid order by month,day) as epic_fig_",  1:epic_figures_total,  "_running_total , " ,sep="",collapse = " "), "
                                tmp.*
                                from(
                                select userid,month,day,
                                min(case when event ='level_up' then gamelevelid-1 else gamelevelid end) min_level, 
                                max(case when event ='level_up' then gamelevelid-1 else gamelevelid end) max_level, 
                                sum(case when event='spin' then 1 else 0 end ) spin_grant_num, 
                                sum(case when event='collect_or_wheel' then 1 else 0 end ) collect_or_wheel_grant_num, 
                                sum(case when event='level_up' then 1 else 0 end ) level_up_num, 
                                sum(case when event='deposit' then 1 else 0 end ) deposit_grant_num, 
                                sum(case when chestname='silver' then 1 else 0 end ) silver_num, 
                                sum(case when chestname='gold' then 1 else 0 end ) gold_num, 
                                sum(case when chestname='platinum' then 1 else 0 end ) platinum_num, 
                                sum(case when chestname='diamond' then 1 else 0 end ) diamond_num, 
                                sum(case when common_stacks>0 then total_common_medals else 0 end ) total_common_medals,
                                sum(case when rare_stacks>0 then total_rare_medals else 0 end ) total_rare_medals,
                                sum(case when epic_stacks>0 then total_epic_medals else 0 end ) total_epic_medals, " ,
                                paste("sum(common_fig_", 1:common_figures_total,") common_fig_",1:common_figures_total,"_daily_grant, " ,sep="",collapse = " "),
                                paste("sum(rare_fig_", 1:rare_figures_total,") rare_fig_",1:rare_figures_total,"_daily_grant, " ,sep="",collapse = " "),
                                paste("sum(epic_fig_", 1:epic_figures_total,") epic_fig_",1:epic_figures_total,"_daily_grant, " ,sep="",collapse = " "), "
                                count(*) as total_lines
                                FROM legends2_sim.collectible_all_events 
                                group by userid,month,day
                                ) as tmp
                                order by userid,month,day ",sep=""))


impala.query(connImpala , paste("drop table legends2_sim.collectible_daily_stages_summary"))
impala.query(connImpala , paste("
                                CREATE TABLE legends2_sim.collectible_daily_stages_summary as 
                                select S.* ",
                                paste(  " ,case when common_fig_",1:common_figures_total,"_running_total<", common_1_min_total,"*single_fig_common_medals then 0 
                                        when common_fig_",1:common_figures_total,"_running_total<", common_2_min_total,"*single_fig_common_medals then 1 
                                        when common_fig_",1:common_figures_total,"_running_total<", common_3_min_total,"*single_fig_common_medals then 2 
                                        when common_fig_",1:common_figures_total,"_running_total<", common_4_min_total,"*single_fig_common_medals then 3 
                                        when common_fig_",1:common_figures_total,"_running_total<", common_5_min_total,"*single_fig_common_medals then 4 
                                        when common_fig_",1:common_figures_total,"_running_total>=",common_5_min_total,"*single_fig_common_medals then 5 
                                        else NULL end common_fig_",1:common_figures_total,"_figure_stage " ,sep="",collapse = " "),
                                paste(  " ,case when rare_fig_",1:rare_figures_total,"_running_total<", rare_1_min_total,"*single_fig_rare_medals then 0 
                                        when rare_fig_",1:rare_figures_total,"_running_total<", rare_2_min_total,"*single_fig_rare_medals then 1 
                                        when rare_fig_",1:rare_figures_total,"_running_total<", rare_3_min_total,"*single_fig_rare_medals then 2 
                                        when rare_fig_",1:rare_figures_total,"_running_total<", rare_4_min_total,"*single_fig_rare_medals then 3 
                                        when rare_fig_",1:rare_figures_total,"_running_total<", rare_5_min_total,"*single_fig_rare_medals then 4 
                                        when rare_fig_",1:rare_figures_total,"_running_total>=",rare_5_min_total,"*single_fig_rare_medals then 5 
                                        else NULL end rare_fig_",1:rare_figures_total,"_figure_stage " ,sep="",collapse = " "),
                                paste(  " ,case when epic_fig_",1:epic_figures_total,"_running_total<", epic_1_min_total,"*single_fig_epic_medals then 0 
                                        when epic_fig_",1:epic_figures_total,"_running_total<", epic_2_min_total,"*single_fig_epic_medals then 1 
                                        when epic_fig_",1:epic_figures_total,"_running_total<", epic_3_min_total,"*single_fig_epic_medals then 2 
                                        when epic_fig_",1:epic_figures_total,"_running_total<", epic_4_min_total,"*single_fig_epic_medals then 3 
                                        when epic_fig_",1:epic_figures_total,"_running_total<", epic_5_min_total,"*single_fig_epic_medals then 4 
                                        when epic_fig_",1:epic_figures_total,"_running_total>=",epic_5_min_total,"*single_fig_epic_medals then 5 
                                        else NULL end epic_fig_",1:epic_figures_total,"_figure_stage " ,sep="",collapse = " "),
                      " , IFNULL(seg.segment ,1) as segment
                      from legends2_sim.collectible_running_totals as S
                      left join legends2_sim.segment as seg on S.userid = seg.userid
                      left join legends2_sim.segment_config as effort on IFNULL(seg.segment,1) = effort.segment
                      order by S.userid,S.month,S.day", sep=""))

impala.query(connImpala , paste("drop table legends2_sim.collectible_chests_claims"))
impala.query(connImpala , paste("
                                CREATE TABLE legends2_sim.collectible_chests_claims AS 
                                select ifnull(segment, 1) as segment ,count(distinct S.userid) UniqueUsers,count(S.userid) Chests, event as Trigger, chestname as ChestType, to_date(ts) as ClaimDate
                                from legends2_sim.collectible_all_events S
                                left join legends2_sim.segment as seg on S.userid = seg.userid
                                group by Trigger, ChestType, ClaimDate,segment",sep=""))

impala.query(connImpala , paste("drop table legends2_sim.collectible_chests_totals"))
impala.query(connImpala , paste("
                                CREATE TABLE legends2_sim.collectible_chests_totals AS 
                                select to_date(ts) dt,chestname as ChestType,event as TriggerType, ifnull(segment, 1) as segment 
                                ,sum(case when common_stacks > 0 then total_common_medals else 0 end) total_common_medals
                                ,sum(case when rare_stacks > 0 then total_rare_medals else 0 end) total_rare_medals
                                ,sum(case when epic_stacks > 0 then total_epic_medals else 0 end ) total_epic_medals
                                ,count(distinct A.userid) dc_users
                                from legends2_sim.collectible_all_events A
                                left join legends2_sim.segment as seg on A.userid = seg.userid
                                group by dt, ChestType, TriggerType, segment",sep=""))






Sys.time()
