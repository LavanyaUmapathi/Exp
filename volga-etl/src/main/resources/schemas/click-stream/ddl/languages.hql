USE clickstream;

ADD JAR ${CSV_SERDE_JAR};

CREATE TABLE `languages`
(
  language_id INT,
  language_name STRING
)
ROW FORMAT SERDE 'com.bizo.hive.serde.csv.CSVSerde'
STORED AS TEXTFILE;