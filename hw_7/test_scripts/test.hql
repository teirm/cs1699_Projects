create table u_data (
    userid INT,
    moveid INT,
    rating iNT,
    unixtime STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;
