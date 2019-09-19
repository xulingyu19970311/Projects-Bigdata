flight = LOAD '/ds410/flightdata/2010-summary.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','No_MULTILINE','NOCHANGE','SKIP_INPUT_HEADER') AS (DEST_COUNTRY_NAME:chararray,ORIGIN_COUNTRY_NAME:chararray,count:int);
group_by_origin = GROUP flight by ORIGIN_COUNTRY_NAME;
/*Here we group by the origin */
 
count_dest = FOREACH group_by_origin GENERATE group, COUNT(flight.DEST_COUNTRY_NAME);
dump count_dest; 
STORE count_dest INTO 'destcount';
/* simply count the dest */

count_out = FOREACH group_by_origin GENERATE group,SUM (flight.count);
dump count_out;
STORE count_out INTO 'outcount';
/* sum up the cout */

result1 = FOREACH group_by_origin GENERATE group,SUM (flight.count) AS ct,COUNT(flight.DEST_COUNTRY_NAME) AS DE;
result2 = FILTER result1 BY DE>=3 AND ct>=100;
dump result2;
/* set 2 conditions and make the filter! */ 
