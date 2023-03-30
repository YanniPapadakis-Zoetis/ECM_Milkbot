-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # ECM Milkbot - V1
-- MAGIC 
-- MAGIC ## SQL  Implementation

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Get Python Functions

-- COMMAND ----------

-- MAGIC %run ./milkbot/sql_py_include

-- COMMAND ----------

-- MAGIC %md ### Get Model Inputs

-- COMMAND ----------

-- MAGIC %run ./DDH-ECM_Model_V1_INPUTS $LastECMRunDate="2023-03-01" $HerdID="10"

-- COMMAND ----------

desc ECMINPUTS

-- COMMAND ----------

select count(*) as N, 
  sum(case when MILK is null then 1 else 0 end) as milk_null, 
  sum(case when ECM is null then 1 else 0 end) as ecm_null, 
  sum(case when pctf is null then 1 else 0 end) as pctf_null, 
  sum(case when pctp is null then 1 else 0 end) as pctp_null 
from ecminputs

-- COMMAND ----------

select * from ECMINPUTS order by BDAT, ID, LAGR, LACT, DIM

-- COMMAND ----------

-- MAGIC %md # Herd & Lactaction Group Estimates

-- COMMAND ----------

create or replace temp view herdlact as
select ECMINPUTS.*, sigma_udf(DIM) as sigma
from ECMINPUTS
join ( 
  select DF.BDAT, DF.ID, DF.LACT, count(*) as cnt
  from ECMINPUTS DF
  where DF.ECM is not null and DF.ECM > 0 and DF.DIM >= 200 and DF.DIM < 350
  group by DF.BDAT, DF.ID, DF.LACT
  having cnt >= 2
  ) ACCEPT on ECMINPUTS.BDAT=ACCEPT.BDAT and ECMINPUTS.ID=ACCEPT.ID and ECMINPUTS.LACT=ACCEPT.LACT
where ECMINPUTS.ECM is not null and ECMINPUTS.DIM < 325;

select * from herdlact;

-- COMMAND ----------

select lagr, count(*)
from herdlact
group by lagr
order by lagr

-- COMMAND ----------

create or replace table mbot as
select group_name(CLCT.LAGR) as group, estimate_milkbot_params(CLCT.dim,CLCT.ecm,CLCT.sigma) as milkbot_pars
from(
  select LAGR,
  collect_list(DIM) as dim,
  collect_list(ECM) as ecm,
  collect_list(SIGMA) as sigma
  from herdlact
  group by LAGR
  ) CLCT
union
select "HERD" as group, estimate_milkbot_params(CLCTALL.dim,CLCTALL.ecm,CLCTALL.sigma) as milkbot_pars
from(
  select
  collect_list(DIM) as dim,
  collect_list(ECM) as ecm,
  collect_list(SIGMA) as sigma
  from herdlact
  ) CLCTALL;

-- COMMAND ----------

-- DBTITLE 1,Graph Results
select *, milkbot_est(dim, milkbot_pars) as ecm_est
from mbot, (select dim from graphdim where mod(dim, 5) = 0)
order by group, dim

-- COMMAND ----------

-- MAGIC %md ## Individual Estimate

-- COMMAND ----------

create or replace temp view indiv as
select ECMINPUTS.BDAT, ECMINPUTS.ID, ECMINPUTS.LACT, MBOT.group, ECMINPUTS.ECM, ECMINPUTS.DIM,
  milkbot_est(ECMINPUTS.DIM, MBOT.milkbot_pars) as ecm_est0, MBOT.milkbot_pars
from ECMINPUTS
join ( 
  select DF.BDAT, DF.ID, DF.LACT, count(*) as cnt
  from ECMINPUTS DF
  where DF.ECM is not null and DF.ECM > 0
  group by DF.BDAT, DF.ID, DF.LACT
  having cnt >= 5
  ) ACCEPT on ECMINPUTS.BDAT=ACCEPT.BDAT and ECMINPUTS.ID=ACCEPT.ID and ECMINPUTS.LACT=ACCEPT.LACT
join MBOT on group_name(ECMINPUTS.LAGR) = MBOT.group
where ECMINPUTS.ECM is not null and ECMINPUTS.DIM > 5;

select * from indiv;

-- COMMAND ----------

create or replace table cowlevel as
select a_adj.*, array(a_adj.geom_mean_ratio * mbot.milkbot_pars[0], mbot.milkbot_pars[1], mbot.milkbot_pars[2], mbot.milkbot_pars[3]) as milkbot_pars_adj
from (
  select group, bdat, id, lact, exp(avg(log(ecm/ecm_est0))) as geom_mean_ratio 
  from indiv 
  group by group, bdat, id, lact
  ) a_adj
join MBOT on a_adj.group = mbot.group

-- COMMAND ----------

select group, bdat, id, lact, sum(milkbot_est(dim, milkbot_pars_adj)) as m305
from cowlevel, (select dim from graphdim where dim between 1 and 305)
group by group, bdat, id, lact

-- COMMAND ----------


