REGISTER cutload.jar;
records = LOAD '/user/crr41/hw_5/input/'
    USING com.hadoopbook.pig.CutLoadFunc('4-15,16-19,20-21,22-23,89-93,93-93')
    AS (station: chararray, year:int, month:int, day:int, temperature:float, quality: int);
filtered_records = FILTER records BY temperature != 9999 AND quality IN (0,1,4,5,9);
grp_records = GROUP filtered_records BY (station, year, month, day);
max_temps = FOREACH grp_records GENERATE FLATTEN(group) AS (station, year, month, day), MAX(filtered_records.temperature) AS max_temp;
grp_max_temps = GROUP max_temps BY (station, month, day);
avg_temps = FOREACH grp_max_temps GENERATE FLATTEN(group) AS (station, month, day), AVG(max_temps.max_temp);
STORE avg_temps INTO 'results_2';

