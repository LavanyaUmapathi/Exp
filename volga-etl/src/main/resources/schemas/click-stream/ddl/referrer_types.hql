USE clickstream;

ADD JAR ${CSV_SERDE_JAR};

CREATE TABLE `referrer_types`
(
  referrer_type_id INT,
  referrer_type_desc STRING,
  referrer_type_name STRING
)
ROW FORMAT SERDE 'com.bizo.hive.serde.csv.CSVSerde'
STORED AS TEXTFILE;