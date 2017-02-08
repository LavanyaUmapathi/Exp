USE clickstream;

ADD JAR ${CSV_SERDE_JAR};

CREATE TABLE `countries`
(
  country_id INT,
  country_name STRING
)
ROW FORMAT SERDE 'com.bizo.hive.serde.csv.CSVSerde'
STORED AS TEXTFILE;