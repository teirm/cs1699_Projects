REGISTER cutload.jar;
records = LOAD '/user/crr41/hw_5/input/1901'
    USING com.hadoopbook.pig.CutLoadFunc('4-15,16-23,89-93')
    AS (station: chararray, date: int, temperature:int);
filtered_records = FILTER records BY temperature != 9999;
grouped_records = GROUP filtered_records BY date;
DUMP grouped_records;
max_temps = FOREACH grouped_records GENERATE FLATTEN(filtered_records.(station,date)), MAX(filtered_records.temperature);
grouped_temps = GROUP max_temps BY station;
DUMP grouped_temps;
