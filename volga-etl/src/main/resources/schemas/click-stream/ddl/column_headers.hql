USE clickstream;

ADD JAR ${CSV_SERDE_JAR};

CREATE TABLE `column_headers`
(
  column_header_name STRING
)
ROW FORMAT SERDE 'com.bizo.hive.serde.csv.CSVSerde'
STORED AS TEXTFILE;