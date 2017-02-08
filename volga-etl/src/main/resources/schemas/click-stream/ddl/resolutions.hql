USE clickstream;

ADD JAR ${CSV_SERDE_JAR};

CREATE TABLE `resolutions`
(
  resolution_id INT,
  resolution_name STRING
)
ROW FORMAT SERDE 'com.bizo.hive.serde.csv.CSVSerde'
STORED AS TEXTFILE;