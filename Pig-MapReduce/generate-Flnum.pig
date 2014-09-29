--The purpose is to produce and store a table in HBase that will contain the rows grouped by airline and day and will have the number of delays > 15 min for each type

-- Describe the input data we want to process
REGISTER /home/ec2-user/seda/Project/Pig/trunk/contrib/piggybank/java/piggybank.jar;
on_time_perform = LOAD 'Data/Delays' USING org.apache.pig.piggybank.storage.CSVExcelStorage()  AS (Year:int, Quarter:int, Month:int, DayofMonth:int, DayOfWeek:int, FlightDate:chararray, UniqueCarrier:chararray, AirlineID:chararray, Carrier:chararray, TailNum:chararray, FlightNum:chararray, OriginAirportID:chararray, OriginAirportSeqID:chararray, OriginCityMarketID:chararray, Origin:chararray, OriginCityName:chararray, OriginState:chararray, OriginStateFips:chararray, OriginStateName:chararray, OriginWac:chararray, DestAirportID:chararray, DestAirportSeqID:chararray, DestCityMarketIDchararray, Dest:chararray, DestCityName:chararray, DestState:chararray, DestStateFips:chararray, DestStateName:chararray, DestWac:chararray, CRSDepTime:chararray, DepTime:chararray, DepDelay:int, DepDelayMinutes:int, DepDel15:int, DepartureDelayGroups:chararray, DepTimeBlk:chararray, TaxiOut:chararray, WheelsOff:chararray, WheelsOn:chararray, TaxiIn:chararray, CRSArrTime:chararray, ArrTime:chararray, ArrDelay:int, ArrDelayMinutes:int, ArrDel15:int, ArrivalDelayGroups:chararray, ArrTimeBlk:chararray, Cancelled:int, CancellationCode:chararray, Diverted:chararray, CRSElapsedTime:chararray, ActualElapsedTime:chararray, AirTime:chararray, Flights:chararray, Distance:chararray, DistanceGroup:chararray, CarrierDelay:int, WeatherDelay:int, NASDelay:int, SecurityDelay:int, LateAircraftDelay:int, FirstDepTime:chararray, TotalAddGTime:chararray, LongestAddGTime:chararray, DivAirportLandings:chararray, DivReachedDest:chararray, DivActualElapsedTime:chararray, DivArrDelay:chararray, DivDistance:chararray, Div1Airport:chararray, Div1AirportID:chararray, Div1AirportSeqID:chararray, Div1WheelsOn:chararray, Div1TotalGTime:chararray, Div1LongestGTime:chararray, Div1WheelsOff:chararray, Div1TailNum:chararray, Div2Airport:chararray, Div2AirportID:chararray, Div2AirportSeqID:chararray, Div2WheelsOn:chararray, Div2TotalGTime:chararray, Div2LongestGTime:chararray, Div2WheelsOff:chararray, Div2TailNum:chararray, Div3Airport:chararray, Div3AirportID:chararray, Div3AirportSeqID:chararray, Div3WheelsOn:chararray, Div3TotalGTime:chararray, Div3LongestGTime:chararray, Div3WheelsOff:chararray, Div3TailNum:chararray, Div4Airport:chararray, Div4AirportID:chararray, Div4AirportSeqID:chararray, Div4WheelsOn:chararray, Div4TotalGTime:chararray, Div4LongestGTime:chararray, Div4WheelsOff:chararray, Div4TailNum:chararray, Div5Airport:chararray, Div5AirportID:chararray, Div5AirportSeqID:chararray, Div5WheelsOn:chararray, Div5TotalGTime:chararray, Div5LongestGTime:chararray, Div5WheelsOff:chararray, Div5TailNum:chararray);

-- FOREACH .. GENERATE operator is used to specify the columns of interest
delays_dates = FOREACH on_time_perform GENERATE UniqueCarrier, Year, Quarter, Month, DayofMonth, DayOfWeek,  FlightDate, FlightNum, ArrDelay, ArrDel15, Cancelled, (CarrierDelay is null OR CarrierDelay==0 ? 0 : 1) AS CarrierDelay_Bool, (WeatherDelay is null OR WeatherDelay== 0 ? 0 : 1) AS WeatherDelay_Bool, (NASDelay is null OR NASDelay==0 ? 0 : 1) AS NASDelay_Bool, (SecurityDelay is null OR SecurityDelay == 0 ? 0 : 1) AS SecurityDelay_Bool, (LateAircraftDelay is null OR LateAircraftDelay == 0 ? 0 : 1) AS LateAircraftDelay_Bool;

--If the plane was delayed but the delay is not classifed add OtherDelay_Bool category
filt_other_delay = FOREACH delays_dates GENERATE UniqueCarrier, Year, Quarter, Month, DayofMonth, DayOfWeek,  FlightDate, FlightNum, ArrDelay, ArrDel15, Cancelled, CarrierDelay_Bool, WeatherDelay_Bool, NASDelay_Bool, SecurityDelay_Bool, LateAircraftDelay_Bool, (CarrierDelay_Bool == 1 OR WeatherDelay_Bool == 1 OR NASDelay_Bool == 1 OR SecurityDelay_Bool==1 OR LateAircraftDelay_Bool == 1 ? 0 : 1) AS OtherDelay_Bool;

--Apply a filter - consider only the records that have ArrDel15 1, meaning that a carrier was late for 15 minutes or more
filtered_delay = FILTER filt_other_delay BY (ArrDelay is not null AND ArrDel15 == 1 AND Cancelled != 1);


--------------------------------------------
--POPULATE THE CORRESPONDING TABLES FOR DATE---

-----
--GENERATE TABLES FOR PROCESSING FLIGHT NUMBER INFORMATION---
-----

--Group all_delays relation by carrier, flightNum and date fileds - this will produce granular date for each flight and each day that there was a flight
grouped_carrier_date_fnum = GROUP filtered_delay BY (UniqueCarrier,FlightNum, FlightDate);

-- Find the number of delays per each day, carrier, and flight number for each type of the delay
delay_num_date_fnum = FOREACH grouped_carrier_date_fnum GENERATE group, SUM(filtered_delay.CarrierDelay_Bool) AS CarrierDelay_Num, SUM(filtered_delay.WeatherDelay_Bool) AS WeatherDelay_Num, SUM(filtered_delay.NASDelay_Bool) AS NASDelay_Num, SUM(filtered_delay.SecurityDelay_Bool) AS SecurityDelay_Num, SUM(filtered_delay.LateAircraftDelay_Bool) AS LateAircraftDelay_Num, SUM(filtered_delay.OtherDelay_Bool) AS OtherDelay_Num;

--flatten the group containing the Carrier, FlighrNum and the date
delay_num_date_fnum_flat = FOREACH delay_num_date_fnum GENERATE FLATTEN(group) AS (UniqueCarrier, FlightNum, FlightDate) , CarrierDelay_Num, WeatherDelay_Num, NASDelay_Num, SecurityDelay_Num, LateAircraftDelay_Num, OtherDelay_Num;

--make the composite key for date
final_date_fnum = FOREACH delay_num_date_fnum_flat GENERATE CONCAT(CONCAT(CONCAT(UniqueCarrier,'_'), CONCAT(FlightNum,'_')), FlightDate) AS Key, CarrierDelay_Num, WeatherDelay_Num, NASDelay_Num, SecurityDelay_Num, LateAircraftDelay_Num, OtherDelay_Num;

---------------------------------------
----PROCESSING YEAR DATA --------------
--------------------------------------


-----
--GENERATE TABLES FOR PROCESSING FLIGHT NUMBER INFORMATION---
-----

--Group all_delays relation by carrier, flightNum and year fileds - this will produce granular year for each flight and each day that there was a flight
grouped_carrier_year_fnum = GROUP filtered_delay BY (UniqueCarrier,FlightNum, Year);

-- Find the number of delays per each day, carrier, and flight number for each type of the delay
delay_num_year_fnum = FOREACH grouped_carrier_year_fnum GENERATE group, SUM(filtered_delay.CarrierDelay_Bool) AS CarrierDelay_Num, SUM(filtered_delay.WeatherDelay_Bool) AS WeatherDelay_Num, SUM(filtered_delay.NASDelay_Bool) AS NASDelay_Num, SUM(filtered_delay.SecurityDelay_Bool) AS SecurityDelay_Num, SUM(filtered_delay.LateAircraftDelay_Bool) AS LateAircraftDelay_Num, SUM(filtered_delay.OtherDelay_Bool) AS OtherDelay_Num;

--flatten the group containing the Carrier, FlighrNum and the year
delay_num_year_fnum_flat = FOREACH delay_num_year_fnum GENERATE FLATTEN(group) AS (UniqueCarrier, FlightNum, Year) , CarrierDelay_Num, WeatherDelay_Num, NASDelay_Num, SecurityDelay_Num, LateAircraftDelay_Num, OtherDelay_Num;

--make the composite key for year
final_year_fnum = FOREACH delay_num_year_fnum_flat GENERATE CONCAT(CONCAT(CONCAT(UniqueCarrier,'_'), CONCAT(FlightNum,'_')), (chararray)Year) AS Key, CarrierDelay_Num, WeatherDelay_Num, NASDelay_Num, SecurityDelay_Num, LateAircraftDelay_Num, OtherDelay_Num;


--------------------------------------------
--POPULATE THE CORRESPONDING TABLES FOR MONTH---
----------------------------------------


-----
--GENERATE TABLES FOR PROCESSING FLIGHT NUMBER INFORMATION---
-----

--Group all_delays relation by carrier, flightNum and month fileds - this will produce granular month for each flight and each day that there was a flight
grouped_carrier_month_fnum = GROUP filtered_delay BY (UniqueCarrier,FlightNum, Month);

-- Find the number of delays per each day, carrier, and flight number for each type of the delay
delay_num_month_fnum = FOREACH grouped_carrier_month_fnum GENERATE group, SUM(filtered_delay.CarrierDelay_Bool) AS CarrierDelay_Num, SUM(filtered_delay.WeatherDelay_Bool) AS WeatherDelay_Num, SUM(filtered_delay.NASDelay_Bool) AS NASDelay_Num, SUM(filtered_delay.SecurityDelay_Bool) AS SecurityDelay_Num, SUM(filtered_delay.LateAircraftDelay_Bool) AS LateAircraftDelay_Num, SUM(filtered_delay.OtherDelay_Bool) AS OtherDelay_Num;

--flatten the group containing the Carrier, FlighrNum and the month
delay_num_month_fnum_flat = FOREACH delay_num_month_fnum GENERATE FLATTEN(group) AS (UniqueCarrier, FlightNum, Month) , CarrierDelay_Num, WeatherDelay_Num, NASDelay_Num, SecurityDelay_Num, LateAircraftDelay_Num, OtherDelay_Num;

--make the composite key for month
final_month_fnum = FOREACH delay_num_month_fnum_flat GENERATE CONCAT(CONCAT(CONCAT(UniqueCarrier,'_'), CONCAT(FlightNum,'_')), (chararray)Month) AS Key, CarrierDelay_Num, WeatherDelay_Num, NASDelay_Num, SecurityDelay_Num, LateAircraftDelay_Num, OtherDelay_Num;

---

STORE final_date_fnum INTO 'hbase://delay_flightNum_date'
USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(
'delay:CarrierDelay_Num, delay:WeatherDelay_Num, delay:NASDelay_Num, delay:SecurityDelay_Num, delay:LateAircraftDelay_Num, delay:OtherDelay_Num'
);

STORE final_year_fnum INTO 'hbase://delay_flightNum_year'
USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(
'delay:CarrierDelay_Num, delay:WeatherDelay_Num, delay:NASDelay_Num, delay:SecurityDelay_Num, delay:LateAircraftDelay_Num, delay:OtherDelay_Num'
);

STORE final_month_fnum INTO 'hbase://delay_flightNum_month'
USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(
'delay:CarrierDelay_Num, delay:WeatherDelay_Num, delay:NASDelay_Num, delay:SecurityDelay_Num, delay:LateAircraftDelay_Num, delay:OtherDelay_Num'
);






