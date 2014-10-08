--The purpose is to produce and store a table in HBase that will contain the rows grouped by airline and day and will have the number of delays > 15 min for each type

-- Describe the input data we want to process
REGISTER /home/ubuntu/seda/informed-traveler/Pig/contrib/piggybank/java/piggybank.jar;
on_time_perform = LOAD 'Data/Delays' USING org.apache.pig.piggybank.storage.CSVExcelStorage()  AS (Year:int, Quarter:int, Month:int, DayofMonth:int, DayOfWeek:int, FlightDate:chararray, UniqueCarrier:chararray, AirlineID:chararray, Carrier:chararray, TailNum:chararray, FlightNum:chararray, OriginAirportID:chararray, OriginAirportSeqID:chararray, OriginCityMarketID:chararray, Origin:chararray, OriginCityName:chararray, OriginState:chararray, OriginStateFips:chararray, OriginStateName:chararray, OriginWac:chararray, DestAirportID:chararray, DestAirportSeqID:chararray, DestCityMarketIDchararray, Dest:chararray, DestCityName:chararray, DestState:chararray, DestStateFips:chararray, DestStateName:chararray, DestWac:chararray, CRSDepTime:chararray, DepTime:chararray, DepDelay:int, DepDelayMinutes:int, DepDel15:int, DepartureDelayGroups:chararray, DepTimeBlk:chararray, TaxiOut:chararray, WheelsOff:chararray, WheelsOn:chararray, TaxiIn:chararray, CRSArrTime:chararray, ArrTime:chararray, ArrDelay:int, ArrDelayMinutes:int, ArrDel15:int, ArrivalDelayGroups:chararray, ArrTimeBlk:chararray, Cancelled:int, CancellationCode:chararray, Diverted:chararray, CRSElapsedTime:chararray, ActualElapsedTime:chararray, AirTime:chararray, Flights:chararray, Distance:chararray, DistanceGroup:chararray, CarrierDelay:int, WeatherDelay:int, NASDelay:int, SecurityDelay:int, LateAircraftDelay:int, FirstDepTime:chararray, TotalAddGTime:chararray, LongestAddGTime:chararray, DivAirportLandings:chararray, DivReachedDest:chararray, DivActualElapsedTime:chararray, DivArrDelay:chararray, DivDistance:chararray, Div1Airport:chararray, Div1AirportID:chararray, Div1AirportSeqID:chararray, Div1WheelsOn:chararray, Div1TotalGTime:chararray, Div1LongestGTime:chararray, Div1WheelsOff:chararray, Div1TailNum:chararray, Div2Airport:chararray, Div2AirportID:chararray, Div2AirportSeqID:chararray, Div2WheelsOn:chararray, Div2TotalGTime:chararray, Div2LongestGTime:chararray, Div2WheelsOff:chararray, Div2TailNum:chararray, Div3Airport:chararray, Div3AirportID:chararray, Div3AirportSeqID:chararray, Div3WheelsOn:chararray, Div3TotalGTime:chararray, Div3LongestGTime:chararray, Div3WheelsOff:chararray, Div3TailNum:chararray, Div4Airport:chararray, Div4AirportID:chararray, Div4AirportSeqID:chararray, Div4WheelsOn:chararray, Div4TotalGTime:chararray, Div4LongestGTime:chararray, Div4WheelsOff:chararray, Div4TailNum:chararray, Div5Airport:chararray, Div5AirportID:chararray, Div5AirportSeqID:chararray, Div5WheelsOn:chararray, Div5TotalGTime:chararray, Div5LongestGTime:chararray, Div5WheelsOff:chararray, Div5TailNum:chararray);

-- FOREACH .. GENERATE operator is used to specify the columns of interest
delays_dates = FOREACH on_time_perform GENERATE UniqueCarrier, Year, Quarter, Month, DayofMonth, DayOfWeek,  FlightDate, FlightNum, ArrDelay, ArrDel15, Cancelled;

--FIlter only the canceled flights to calculate the total number of flights per airline with a given granularity
filter_canceled = FILTER delays_dates BY Cancelled != 1;

---------------------------------------
----PROCESSING YEAR DATA that also includes Quarter - grouping is done with Quarter in mind --------------
--Group all_delays relation by carrier and year fileds - this will produce granular date for each flight and each day that there was a flight
grouped_carrier_year_quarter = GROUP filter_canceled BY (UniqueCarrier, FlightNum, Year, Quarter);

-- Find the number of delays per each day and carrier for each type of the delay
fcount_quarterly = FOREACH grouped_carrier_year_quarter GENERATE group, COUNT($1) AS Num_Flights;

--flatten the group containing the Carrier, FlightNum, Year and Quarter
fcount_quarterly_flat = FOREACH fcount_quarterly GENERATE FLATTEN(group) AS(UniqueCarrier, FlightNum, Year, Quarter), Num_Flights;

--make the composite key 
final_quarterly = FOREACH fcount_quarterly_flat GENERATE CONCAT(CONCAT(CONCAT(UniqueCarrier,'_'), CONCAT(FlightNum,'_')), CONCAT(CONCAT((chararray)Year,'_'), (chararray)Quarter)) AS Key, Num_Flights;

--------------------------------------------

STORE final_quarterly INTO 'hbase://quarterly_delays_fcount'
USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(
'count:Num_Flights'
);

