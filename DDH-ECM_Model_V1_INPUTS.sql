-- Databricks notebook source
CREATE WIDGET TEXT LastECMRunDate DEFAULT '2023-03-01'

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
				herd.LastTestDayEventDate >  DATE "${LastECMRunDate}"  /*only pull data from herds who have new test day data since last ECM model run*/
			and EventDateTime>=add_months(herd.LastTestDayEventDate, -36) /*3 years of data*/
			and tde.LactationNumber>0
			and MilkWeight is not null
			and DIM between 5 and 350 /*Based on herd-lactgroup and invidividual cow fitting code*/
			) as data


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

create or replace temp view ECMINPUTS as
select
ptd.*
from PrimaryTestDayData ptd
	inner join
	SelectedTestDates std /*select test dates within herd and lactation group (LAGR) with >=20 observations*/
		on ptd.SourceCode=std.SourceCode and ptd.SourceHerdID=std.SourceHerdID and ptd.TestDate=std.TestDate
where PercentileDIM<=90 /*restricting to observations at or below the 90th percentile for DIM within herd, lactation group, and test date*/

-- COMMAND ----------


