REGISTER cutload.jar;
records = LOAD '/user/crr41/hw_5/input/'
    USING com.hadoopbook.pig.CutLoadFunc('4-15,16-19,20-21,22-23,89-93,93-93')
    AS (station: chararray, year:int, month:int, day:int, temperature:float, quality: int);
filtered_records = FILTER records BY temperature != 9999 AND quality IN (0,1,4,5,9);
grouped_records = GROUP filtered_records BY (station, year, month, day);
max_temps = FOREACH grouped_records GENERATE FLATTEN(group) AS (station, year, month, day), MAX(filtered_records.temperature) AS max_temp;
grouped_max_temperatures = GROUP max_temps BY (station, month, day);
mean_temperatures = FOREACH grouped_max_temperatures GENERATE FLATTEN(group) AS (station, month, day), AVG(max_temps.max_temp);
DUMP mean_temperatures;

/*grouped_records = GROUP filtered_records BY (station,year,month,day);
flattened_max_temps = FOREACH max_temps GENERATE FLATTEN($0), $1 AS max_flat_temp;
DUMP flattened_max_temps;
DESCRIBE flattened_max_temps;
station_groups = GROUP flattened_max_temps BY (station, month, day);
DUMP station_groups;
DESCRIBE station_groups;
flattened_station_groups = FOREACH station_groups GENERATE FLATTEN($0), FLATTEN($1);
DUMP flattened_station_groups;
necessary_data = FOREACH flattened_station_groups GENERATE $0, $1, $2, $7; 
DUMP necessary_data;
group_necessary_data = GROUP necessary_data BY ($0,$1,$2);
DUMP group_necessary_data;
avg_temps = FOREACH group_necessary_data GENERATE FLATTEN(group), AVG(max_temps.temperature);
DUMP avg_temps;*/ 
