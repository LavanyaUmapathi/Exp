USE clickstream;

ADD JAR ${CSV_SERDE_JAR};

CREATE TABLE `browser_types`
(
  browser_type_id INT,
  browser_type_name STRING
)
ROW FORMAT SERDE 'com.bizo.hive.serde.csv.CSVSerde'
STORED AS TEXTFILE;