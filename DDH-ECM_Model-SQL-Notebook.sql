-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # APACHE SQL

-- COMMAND ----------

set LastECMRunDate = DATE '2023-03-01'

-- COMMAND ----------

/****assembly of primary data set*******/
create or replace temp view PrimaryTestDayData as
select *, ntile(100) over (partition by SourceCode,SourceHerdID,TestDate,LAGR order by DIM asc) as PercentileDIM
from (
		select
			tde.SourceCode
			,tde.SourceHerdID
			,herd.LastTestDayEventDate
			,tde.SourceAnimalID
			,tde.EarTag as ID
			,tde.LactationNumber as LACT
            ,case when tde.LactationNumber>=4 then 4 else tde.LactationNumber end as LAGR
			,BirthDate as BDAT
			,tde.FreshDate as FDAT
			,cast(EventDateTime as date) as TestDate
			,DIM
			,MilkWeight as MILK
			,case 
					when tde.PercentFat>0 and tde.PercentProtein>0
						then round(
								tde.MilkWeight*(0.383*tde.PercentFat + 0.242*tde.PercentProtein + 0.7832) / 3.1138 /*International ECM formula*/
								,1)
					else null end as ECM
			,PercentFat as PCTF
			,PercentProtein as PCTP
		from delta.`abfss://governed@devazrddp01adls.dfs.core.windows.net/agb/FoundationTables/TestDayEvents/` tde
			inner join
			delta.`abfss://governed@devazrddp01adls.dfs.core.windows.net/agb/FoundationTables/Herd/` herd
				on tde.SourceCode=herd.SourceCode and tde.SourceHerdID=herd.SourceHerdID
		where 
				herd.LastTestDayEventDate >  ${hiveconf:LastECMRunDate}  /*only pull data from herds who have new test day data since last ECM model run*/
			and EventDateTime>=add_months(herd.LastTestDayEventDate, -36) /*3 years of data*/
			and tde.LactationNumber>0
			and MilkWeight is not null
			and DIM between 5 and 350 /*Based on herd-lactgroup and invidividual cow fitting code*/
			) as data


-- COMMAND ----------

select * from PrimaryTestDayData

-- COMMAND ----------

/****Summarize counts by herd, test date choose only those with >=20 observations*********/ 
create or replace temp view SelectedTestDates as
select
SourceCode
,SourceHerdID
,TestDate
,count(*) as N
from PrimaryTestDayData
where PercentileDIM<=90 /*restricting to observations at or below the 90th percentile for DIM within herd, lactation group, and test date*/
group by
SourceCode
,SourceHerdID
,TestDate
having count(*)>=20

-- COMMAND ----------

select * from SelectedTestDates

-- COMMAND ----------

select
ptd.*
from PrimaryTestDayData ptd
	inner join
	SelectedTestDates std /*select test dates within herd and lactation group (LAGR) with >=20 observations*/
		on ptd.SourceCode=std.SourceCode and ptd.SourceHerdID=std.SourceHerdID and ptd.TestDate=std.TestDate
where PercentileDIM<=90 /*restricting to observations at or below the 90th percentile for DIM within herd, lactation group, and test date*/

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # Original Code

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC <pre>
-- MAGIC declare @LastECMRunDate date='2023-03-01'  /*last time the ECM model was ran*/
-- MAGIC 
-- MAGIC /****assembly of primary data set*******/
-- MAGIC select
-- MAGIC *
-- MAGIC ,PercentileDIM=Percent_Rank() over (partition by SourceCode,SourceHerdID,TestDate,LAGR order by DIM asc)
-- MAGIC into #PrimaryTestDayData
-- MAGIC from (
-- MAGIC 		select
-- MAGIC 			tde.SourceCode
-- MAGIC 			,tde.SourceHerdID
-- MAGIC 			,herd.LastTestDayEventDate
-- MAGIC 			,tde.SourceAnimalID
-- MAGIC 			,tde.EarTag as ID
-- MAGIC 			,tde.LactationNumber as LACT
-- MAGIC 			,LAGR/*Lactation group (1,2,3, >=4)*/=case
-- MAGIC 					when tde.LactationNumber>=4 then 4 else tde.LactationNumber end
-- MAGIC 			,BirthDate as BDAT
-- MAGIC 			,tde.FreshDate as FDAT
-- MAGIC 			,cast(EventDateTime as date) as TestDate
-- MAGIC 			,DIM
-- MAGIC 			,MilkWeight as MILK
-- MAGIC 			,ECM/*EnergyCorrectedMilk_lb*/=case 
-- MAGIC 					when tde.PercentFat>0 and tde.PercentProtein>0
-- MAGIC 						then round(
-- MAGIC 								tde.MilkWeight*(0.383*tde.PercentFat + 0.242*tde.PercentProtein + 0.7832) / 3.1138 /*International ECM formula*/
-- MAGIC 								,1)
-- MAGIC 					else null end
-- MAGIC 			,PercentFat as PCTF
-- MAGIC 			,PercentProtein as PCTP
-- MAGIC 		from ddh.TestDayEvents_ProdCopy tde
-- MAGIC 			inner join
-- MAGIC 			ddh.herd_ProdCopy herd
-- MAGIC 				on tde.SourceCode=herd.SourceCode and tde.SourceHerdID=herd.SourceHerdID
-- MAGIC 		where 
-- MAGIC 				herd.LastTestDayEventDate>@LastECMRunDate  /*only pull data from herds who have new test day data since last ECM model run*/
-- MAGIC 			and	EventDateTime>=dateadd(d,-(365*3),herd.LastTestDayEventDate)  /*3 years of data*/
-- MAGIC 			and tde.LactationNumber>0
-- MAGIC 			and MilkWeight is not null
-- MAGIC 			and DIM between 5 and 350 /*Based on herd-lactgroup and invidividual cow fitting code*/
-- MAGIC 			) as data
-- MAGIC 
-- MAGIC /****Summarize counts by herd, test date choose only those with >=20 observations*********/ 
-- MAGIC select
-- MAGIC SourceCode
-- MAGIC ,SourceHerdID
-- MAGIC ,TestDate
-- MAGIC ,N=count(*)
-- MAGIC into #SelectedTestDates
-- MAGIC from #PrimaryTestDayData
-- MAGIC where PercentileDIM<=0.90 /*restricting to observations at or below the 90th percentile for DIM within herd, lactation group, and test date*/
-- MAGIC group by
-- MAGIC SourceCode
-- MAGIC ,SourceHerdID
-- MAGIC ,TestDate
-- MAGIC having count(*)>=20 
-- MAGIC 
-- MAGIC 
-- MAGIC select
-- MAGIC ptd.*
-- MAGIC from #PrimaryTestDayData ptd
-- MAGIC 	inner join
-- MAGIC 	#SelectedTestDates std /*select test dates within herd and lactation group (LAGR) with >=20 observations*/
-- MAGIC 		on ptd.SourceCode=std.SourceCode and ptd.SourceHerdID=std.SourceHerdID and ptd.TestDate=std.TestDate
-- MAGIC where PercentileDIM<=0.90 /*restricting to observations at or below the 90th percentile for DIM within herd, lactation group, and test date*/
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC drop table #PrimaryTestDayData
-- MAGIC drop table #SelectedTestDates
-- MAGIC </pre>

-- COMMAND ----------


