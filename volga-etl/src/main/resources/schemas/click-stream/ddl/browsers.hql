USE clickstream;

ADD JAR ${CSV_SERDE_JAR};

CREATE TABLE `browsers`
(
  browser_id INT,
  browser_name STRING
)
ROW FORMAT SERDE 'com.bizo.hive.serde.csv.CSVSerde'
STORED AS TEXTFILE;
