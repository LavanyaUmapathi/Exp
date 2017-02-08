USE maas;

DROP TABLE IF EXISTS contacts_stg;

CREATE TABLE contacts_stg (
user_id STRING,
first_name STRING,
last_name STRING,
emailAddress STRING,
role STRING,
link STRING,
dt BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
ESCAPED BY '\\'
COLLECTION ITEMS TERMINATED BY '|'
MAP KEYS TERMINATED BY '='
LINES TERMINATED BY '\n';

CREATE TABLE contacts (
user_id STRING,
first_name STRING,
last_name STRING,
emailAddress STRING,
role STRING,
link STRING,
dt BIGINT
)
STORED AS ORC;

LOAD DATA inpath '/user/asilva/configs/contact*' INTO TABLE contacts_stg;

--copy to ORC table
FROM contacts_stg stg
INSERT OVERWRITE TABLE contacts
 SELECT *;

DROP TABLE contacts_stg;


