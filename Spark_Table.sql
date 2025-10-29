-- Databricks notebook source
drop database if exists Spark;
drop table if exists Fire_Service_Calls;

-- COMMAND ----------

CREATE DATABASE Spark

-- COMMAND ----------

CREATE TABLE Fire_Service_Calls(
  CallNumber integer,
  UnitID string,
  IncidentNumber integer,
  CallType string,
  CallDate string,
  WatchDate string,
  CallFinalDisposition string,
  AvailableDtTm string,
  Address string,
  City string,
  Zipcode integer,
  Battalion string, 
  StationArea string,
  Box string,
  OriginalPriority string,
  Priority string,
  Final_Priority integer,
  ALSUnit boolean,
  CallTypeGroup string,
  NumAlarms integer,
  UnitType string,
  UnitSequenceInCallDispatch integer,
  FirePreventionDistrict string,
  SupervisorDistrict string,
  Neighborhood string,
  Location string,
  RowID string,
  Delay float
) using parquet

-- COMMAND ----------

INSERT INTO Fire_Service_Calls VALUES (1234, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,null, null, null, null, null, null, null, null, null)

-- COMMAND ----------

SELECT * FROM Fire_Service_Calls

-- COMMAND ----------

truncate table Fire_Service_Calls

-- COMMAND ----------

insert into Fire_Service_Calls
select * from global_temp.Fire_Service_Calls

-- COMMAND ----------

select * from Fire_Service_Calls

-- COMMAND ----------

select count(*) from fire_service_calls

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ######
-- MAGIC Q1.How Many Distinct Type Of Calls Were MAde To The Fire Department ?
-- MAGIC
-- MAGIC

-- COMMAND ----------

Select count(distinct calltype) as Distinct_Calls
from fire_service_calls
Where calltype is not null

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Q2. What Were Distinct Type Of Calls Made To The Fire Department?

-- COMMAND ----------

select distinct calltype as Distinct_Type
from fire_service_calls
where CallType is not null

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Q3. Find Out All Response For delayed times greater than 5 mint?

-- COMMAND ----------

SELECT CallNumber, Delay
FROM fire_service_calls
where Delay > 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### What Were the Most Most Call Type?

-- COMMAND ----------

Select callType, count(*) as Count
FROM fire_service_calls
Where CallType is not null 
group by CallType
ORDER BY Count desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######Q5. What ZIP Code Accounted For Most Common Calls?

-- COMMAND ----------

select callType, zipCode, count(*) as Count
from fire_service_calls
where CallType is not null 
group by CallType, Zipcode
order by count desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### What San Francisco Neighborhoods Are In The ZIP codes 94102 and 94103?
-- MAGIC

-- COMMAND ----------

select  ZipCode, Neighborhood
from fire_service_calls
where Zipcode == 94102 OR ZipCode == 94103

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### What Was The Sum Of All Call Alarms, Average, Min And Max Of The Call Response Times?
-- MAGIC

-- COMMAND ----------

SELECT SUM(AvgNumAlarms + MinNumAlarms + MaxNumAlarms)  
FROM (  
    SELECT   
        AVG(NumAlarms) AS AvgNumAlarms,  
        MIN(NumAlarms) AS MinNumAlarms,  
        MAX(NumAlarms) AS MaxNumAlarms  
    FROM fire_service_calls  
    GROUP BY NumAlarms  
)   

-- COMMAND ----------

-- MAGIC %md
-- MAGIC