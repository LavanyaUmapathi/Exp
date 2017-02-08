drop database if exists cloud_usage_events CASCADE;
create database if not exists cloud_usage_events;

use cloud_usage_events;

ADD JAR  /usr/hdp/current/hive-client/lib/csv-serde-1.1.2-0.11.0-all.jar;

drop table if exists  backup_bandwidthin_usage_events ;

create external table if not exists backup_bandwidthin_usage_events (
event_id String,
resource_id String,
resource_name String,
start_time  bigint,
end_time  bigint,
tenant_id String,
data_center String,
region String,
type String,
version String,
bandwidthin String,
resource_type String,
server_id String,
service_code String,
product_version String,
usage_category String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/backup_bandwidthin_usage_events';

drop table if exists  backup_bandwidthin_usage_events_orc ;

create table if not exists backup_bandwidthin_usage_events_orc (
event_id String,
resource_id String,
resource_name String,
start_time  bigint,
end_time  bigint,
tenant_id String,
data_center String,
region String,
type String,
version String,
bandwidthin String,
resource_type String,
server_id String,
service_code String,
product_version String,
usage_category String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
stored as ORC;

drop table if exists  backup_bandwidthout_usage_events ;

create external table if not exists backup_bandwidthout_usage_events (
event_id String,
resource_id String,
resource_name String,
start_time  bigint,
end_time  bigint,
tenant_id String,
data_center String,
region String,
type String,
version String,
bandwidthout String,
resource_type String,
server_id String,
service_code String,
product_version String,
usage_category String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/backup_bandwidthout_usage_events';


drop table if exists  backup_bandwidthout_usage_events_orc ;

create table if not exists backup_bandwidthout_usage_events_orc (
event_id String,
resource_id String,
resource_name String,
start_time  bigint,
end_time  bigint,
tenant_id String,
data_center String,
region String,
type String,
version String,
bandwidthout String,
resource_type String,
server_id String,
service_code String,
product_version String,
usage_category String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
stored as ORC;

drop table if exists  backup_license_usage_events ;

create external table if not exists backup_license_usage_events (
event_id String,
resource_id String,
resource_name String,
start_time  bigint,
end_time  bigint,
tenant_id String,
data_center String,
region String,
type String,
version String,
resource_type String,
server_id String,
service_code String,
product_version String,
usage_category String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/backup_license_usage_events';

drop table if exists  backup_license_usage_events_orc ;

create table if not exists backup_license_usage_events_orc (
event_id String,
resource_id String,
resource_name String,
start_time  bigint,
end_time  bigint,
tenant_id String,
data_center String,
region String,
type String,
version String,
resource_type String,
server_id String,
service_code String,
product_version String,
usage_category String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
stored as ORC;

drop table if exists  backup_storage_usage_events ;

create external table if not exists backup_storage_usage_events (
event_id String,
resource_id String,
resource_name String,
start_time  bigint,
end_time  bigint,
tenant_id String,
data_center String,
region String,
type String,
version String,
storage String,
resource_type String,
server_id String,
service_code String,
product_version String,
usage_category String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/backup_storage_usage_events';



drop table if exists  backup_storage_usage_events_orc ;

create table if not exists backup_storage_usage_events_orc (
event_id String,
resource_id String,
resource_name String,
start_time  bigint,
end_time  bigint,
tenant_id String,
data_center String,
region String,
type String,
version String,
storage String,
resource_type String,
server_id String,
service_code String,
product_version String,
usage_category String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
stored as ORC;



drop table if exists  cbs_usage_events ;

create external table if not exists cbs_usage_events (
event_id String,
resource_id String,
resource_name String,
start_time  bigint,
end_time  bigint,
tenant_id String,
data_center String,
region String,
type String,
version String,
provisioned String,
resource_type String,
service_code String,
product_type String,
product_version String,
usage_category String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/cbs_usage_events';


drop table if exists  cbs_usage_events_orc ;

create table if not exists cbs_usage_events_orc (
event_id String,
resource_id String,
resource_name String,
start_time  bigint,
end_time  bigint,
tenant_id String,
data_center String,
region String,
type String,
version String,
provisioned String,
resource_type String,
service_code String,
product_type String,
product_version String,
usage_category String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
stored as ORC;

drop table if exists  dbaas_usage_events ;

create external table if not exists dbaas_usage_events (
event_id String,
resource_id String,
resource_name String,
root_action String,
start_time  bigint,
end_time  bigint,
tenant_id String,
data_center String,
environment String,
region String,
type String,
version String,
memory String,
resource_type String,
service_code String,
storage String,
product_version String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/dbaas_usage_events';


drop table if exists  dbaas_usage_events_orc ;

create table if not exists dbaas_usage_events_orc (
event_id String,
resource_id String,
resource_name String,
root_action String,
start_time  bigint,
end_time  bigint,
tenant_id String,
data_center String,
environment String,
region String,
type String,
version String,
memory String,
resource_type String,
service_code String,
storage String,
product_version String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
stored as ORC;

drop table if exists  files_bandwidth_usage_events ;

create external table if not exists files_bandwidth_usage_events (
event_id String,
resource_id String,
resource_name String,
start_time  bigint,
end_time  bigint,
tenant_id String,
data_center String,
region String,
type String,
version String,
bandwidthin String,
bandwidthout String,
service_code String,
product_version String,
usage_category String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/files_bandwidth_usage_events';

drop table if exists  files_bandwidth_usage_events_orc ;

create table if not exists files_bandwidth_usage_events_orc (
event_id String,
resource_id String,
resource_name String,
start_time  bigint,
end_time  bigint,
tenant_id String,
data_center String,
region String,
type String,
version String,
bandwidthin String,
bandwidthout String,
service_code String,
product_version String,
usage_category String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
stored as ORC;

drop table if exists  files_cdnbandwidth_usage_events ;

create external table if not exists files_cdnbandwidth_usage_events (
event_id String,
resource_id String,
resource_name String,
start_time  bigint,
end_time  bigint,
tenant_id String,
data_center String,
region String,
type String,
version String,
cdnbandwidthout String,
service_code String,
product_version String,
usage_category String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/files_cdnbandwidth_usage_events';

drop table if exists  files_cdnbandwidth_usage_events_orc ;

create  table if not exists files_cdnbandwidth_usage_events_orc (
event_id String,
resource_id String,
resource_name String,
start_time  bigint,
end_time  bigint,
tenant_id String,
data_center String,
region String,
type String,
version String,
cdnbandwidthout String,
service_code String,
product_version String,
usage_category String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
stored as ORC;

drop table if exists  files_storage_usage_events ;

create external table if not exists files_storage_usage_events (
event_id String,
resource_id String,
resource_name String,
start_time  bigint,
end_time  bigint,
tenant_id String,
data_center String,
region String,
type String,
version String,
disk String,
freeops String,
costops String,
service_code String,
product_version String,
usage_category String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/files_storage_usage_events';

drop table if exists  files_storage_usage_events_orc ;

create  table if not exists files_storage_usage_events_orc (
event_id String,
resource_id String,
resource_name String,
start_time  bigint,
end_time  bigint,
tenant_id String,
data_center String,
region String,
type String,
version String,
disk String,
freeops String,
costops String,
service_code String,
product_version String,
usage_category String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
stored as ORC;

drop table if exists  glance ;

create external table if not exists glance (
title String,
id String,
summary String,
created String,
content String,
updated_ts bigint,
updated String,
dw_timestamp  bigint,
latest_run String,
event_generated_timestamp_ts  bigint
) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/glance';

drop table if exists  lbaas_usage_events ;

create external table if not exists lbaas_usage_events (
event_id String,
resource_id String,
resource_name String,
start_time  bigint,
end_time  bigint,
tenant_id String,
data_center String,
region String,
type String,
version String,
avgconcurrentconnections String,
avgconcurrentconnectionsssl String,
bandwidthin String,
bandwidthinssl String,
bandwidthout String,
bandwidthoutssl String,
numpolls String,
numvips String,
resource_type String,
service_code String,
sslmode String,
status String,
viptype String,
product_version String,
usage_category String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/lbaas_usage_events';

drop table if exists  lbaas_usage_events_orc ;

create table if not exists lbaas_usage_events_orc (
event_id String,
resource_id String,
resource_name String,
start_time  bigint,
end_time  bigint,
tenant_id String,
data_center String,
region String,
type String,
version String,
avgconcurrentconnections String,
avgconcurrentconnectionsssl String,
bandwidthin String,
bandwidthinssl String,
bandwidthout String,
bandwidthoutssl String,
numpolls String,
numvips String,
resource_type String,
service_code String,
sslmode String,
status String,
viptype String,
product_version String,
usage_category String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
stored as ORC;

drop table if exists  melange ;

create external table if not exists melange (
title String,
id String,
summary String,
created String,
content String,
updated String,
dw_timestamp  bigint,
latest_run String
) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/melange';

drop table if exists  monitoring_usage_events ;

create external table if not exists monitoring_usage_events (
event_id String,
resource_id String,
resource_name String,
start_time  bigint,
end_time  bigint,
tenant_id String,
type String,
version String,
check_type String,
monitoring_zones String,
resource_type String,
service_code String,
p_version String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/monitoring_usage_events';

drop table if exists  monitoring_usage_events_orc ;

create table if not exists monitoring_usage_events_orc (
event_id String,
resource_id String,
resource_name String,
start_time  bigint,
end_time  bigint,
tenant_id String,
type String,
version String,
check_type String,
monitoring_zones String,
resource_type String,
service_code String,
p_version String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
stored as ORC;

drop table if exists  mysql_sites_usage_events ;

create external table if not exists mysql_sites_usage_events (
event_id String,
resource_id String,
resource_name String,
event_time  bigint,
start_time  bigint,
end_time  bigint,
environment String,
is_new_account String,
tenant_id String,
data_center String,
region String,
type String,
version String,
group_name String,
num_files String,
storage String,
volume String,
bandwidth_out String,
compute_cycles String,
request_count String,
ssl_enabled String,
resource_type String,
service_code String,
product_type String,
product_version String,
usage_category String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/mysql_sites_usage_events';

drop table if exists  mysql_sites_usage_events_orc ;

create table if not exists mysql_sites_usage_events_orc (
event_id String,
resource_id String,
resource_name String,
event_time  bigint,
start_time  bigint,
end_time  bigint,
environment String,
is_new_account String,
tenant_id String,
data_center String,
region String,
type String,
version String,
group_name String,
num_files String,
storage String,
volume String,
bandwidth_out String,
compute_cycles String,
request_count String,
ssl_enabled String,
resource_type String,
service_code String,
product_type String,
product_version String,
usage_category String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
stored as ORC;

drop table if exists  netapp_sites_usage_events ;

create external table if not exists netapp_sites_usage_events (
event_id String,
resource_id String,
resource_name String,
event_time  bigint,
start_time  bigint,
end_time  bigint,
environment String,
is_new_account String,
tenant_id String,
data_center String,
region String,
type String,
version String,
group_name String,
num_files String,
storage String,
volume String,
bandwidth_out String,
compute_cycles String,
request_count String,
resource_type String,
service_code String,
product_type String,
product_version String,
usage_category String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/netapp_sites_usage_events';

drop table if exists  netapp_sites_usage_events_orc ;

create  table if not exists netapp_sites_usage_events_orc (
event_id String,
resource_id String,
resource_name String,
event_time  bigint,
start_time  bigint,
end_time  bigint,
environment String,
is_new_account String,
tenant_id String,
data_center String,
region String,
type String,
version String,
group_name String,
num_files String,
storage String,
volume String,
bandwidth_out String,
compute_cycles String,
request_count String,
resource_type String,
service_code String,
product_type String,
product_version String,
usage_category String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
stored as ORC;

drop table if exists  nova ;

create external table if not exists nova (
title String,
id String,
summary String,
created String,
content String,
updated_ts bigint,
updated String,
dw_timestamp  bigint,
latest_run String,
event_generated_timestamp_ts  bigint
) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/nova';

drop table if exists  queues_bandwidth_usage_events ;

create external table if not exists queues_bandwidth_usage_events (
event_id String,
start_time  bigint,
end_time  bigint,
environment String,
tenant_id String,
data_center String,
region String,
type String,
version String,
bandwidthinpublic String,
bandwidthinservicenet String,
bandwidthoutpublic String,
bandwidthoutservicenet String,
service_code String,
product_version String,
usage_category String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/queues_bandwidth_usage_events';


drop table if exists  queues_bandwidth_usage_events_orc ;

create  table if not exists queues_bandwidth_usage_events_orc (
event_id String,
start_time  bigint,
end_time  bigint,
environment String,
tenant_id String,
data_center String,
region String,
type String,
version String,
bandwidthinpublic String,
bandwidthinservicenet String,
bandwidthoutpublic String,
bandwidthoutservicenet String,
service_code String,
product_version String,
usage_category String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
stored as ORC;

drop table if exists  queues_queues_usage_events ;

create external table if not exists queues_queues_usage_events (
event_id String,
start_time  bigint,
end_time  bigint,
environment String,
tenant_id String,
data_center String,
region String,
type String,
version String,
request_count String,
service_code String,
product_version String,
usage_category String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/queues_queues_usage_events';


drop table if exists  queues_queues_usage_events_orc ;

create  table if not exists queues_queues_usage_events_orc (
event_id String,
start_time  bigint,
end_time  bigint,
environment String,
tenant_id String,
data_center String,
region String,
type String,
version String,
request_count String,
service_code String,
product_version String,
usage_category String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
stored as ORC;

drop table if exists  servers_bandwidth_usage_events ;

create external table if not exists servers_bandwidth_usage_events (
event_id String,
resource_id String,
resource_name String,
event_time  bigint,
start_time  bigint,
end_time  bigint,
environment String,
bandwidth_in String,
bandwidth_out String,
extra_private_ips String,
extra_public_ips String,
flavor String,
ismssql String,
ismssqlweb String,
ismanaged String,
isredhat String,
isselinux String,
iswindows String,
tenant_id String,
data_center String,
region String,
type String,
version String,
resource_type String,
service_code String,
product_type String,
product_version String,
usage_category String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/servers_bandwidth_usage_events';


drop table if exists  servers_bandwidth_usage_events_orc ;

create table if not exists servers_bandwidth_usage_events_orc (
event_id String,
resource_id String,
resource_name String,
event_time  bigint,
start_time  bigint,
end_time  bigint,
environment String,
bandwidth_in String,
bandwidth_out String,
extra_private_ips String,
extra_public_ips String,
flavor String,
ismssql String,
ismssqlweb String,
ismanaged String,
isredhat String,
isselinux String,
iswindows String,
tenant_id String,
data_center String,
region String,
type String,
version String,
resource_type String,
service_code String,
product_type String,
product_version String,
usage_category String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
stored as ORC;

drop table if exists  servers_rhel_usage_events ;

create external table if not exists servers_rhel_usage_events (
event_id String,
resource_id String,
resource_name String,
event_time  bigint,
start_time  bigint,
end_time  bigint,
environment String,
bandwidth_in String,
bandwidth_out String,
extra_private_ips String,
extra_public_ips String,
flavor String,
ismssql String,
ismssqlweb String,
ismanaged String,
isredhat String,
isselinux String,
iswindows String,
tenant_id String,
data_center String,
region String,
type String,
version String,
used String,
resource_type String,
service_code String,
product_type String,
product_version String,
usage_category String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/servers_rhel_usage_events';

drop table if exists  servers_rhel_usage_events_orc ;

create table if not exists servers_rhel_usage_events_orc (
event_id String,
resource_id String,
resource_name String,
event_time  bigint,
start_time  bigint,
end_time  bigint,
environment String,
bandwidth_in String,
bandwidth_out String,
extra_private_ips String,
extra_public_ips String,
flavor String,
ismssql String,
ismssqlweb String,
ismanaged String,
isredhat String,
isselinux String,
iswindows String,
tenant_id String,
data_center String,
region String,
type String,
version String,
used String,
resource_type String,
service_code String,
product_type String,
product_version String,
usage_category String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
stored as ORC;

drop table if exists  servers_usage_events ;

create external table if not exists servers_usage_events (
event_id String,
resource_id String,
resource_name String,
event_time  bigint,
start_time  bigint,
end_time  bigint,
environment String,
bandwidth_in String,
bandwidth_out String,
extra_private_ips String,
extra_public_ips String,
flavor String,
ismssql String,
ismssqlweb String,
ismanaged String,
isredhat String,
isselinux String,
iswindows String,
tenant_id String,
data_center String,
region String,
type String,
version String,
resource_type String,
service_code String,
product_type String,
product_version String,
usage_category String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/servers_usage_events';


drop table if exists  servers_usage_events_orc ;

create  table if not exists servers_usage_events_orc (
event_id String,
resource_id String,
resource_name String,
event_time  bigint,
start_time  bigint,
end_time  bigint,
environment String,
bandwidth_in String,
bandwidth_out String,
extra_private_ips String,
extra_public_ips String,
flavor String,
ismssql String,
ismssqlweb String,
ismanaged String,
isredhat String,
isselinux String,
iswindows String,
tenant_id String,
data_center String,
region String,
type String,
version String,
resource_type String,
service_code String,
product_type String,
product_version String,
usage_category String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
stored as ORC;


drop table if exists  sitessub_sites_usage_events ;

create external table if not exists sitessub_sites_usage_events (
event_id String,
resource_id String,
resource_name String,
event_time  bigint,
start_time  bigint,
end_time  bigint,
environment String,
is_new_account String,
tenant_id String,
data_center String,
region String,
type String,
version String,
group_name String,
num_files String,
storage String,
volume String,
bandwidth_out String,
compute_cycles String,
request_count String,
ssl_enabled String,
resource_type String,
service_code String,
product_type String,
product_version String,
usage_category String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/sitessub_sites_usage_events';

drop table if exists  sitessub_sites_usage_events_orc ;

create  table if not exists sitessub_sites_usage_events_orc (
event_id String,
resource_id String,
resource_name String,
event_time  bigint,
start_time  bigint,
end_time  bigint,
environment String,
is_new_account String,
tenant_id String,
data_center String,
region String,
type String,
version String,
group_name String,
num_files String,
storage String,
volume String,
bandwidth_out String,
compute_cycles String,
request_count String,
ssl_enabled String,
resource_type String,
service_code String,
product_type String,
product_version String,
usage_category String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
stored as ORC;

drop table if exists  ssl_sites_usage_events ;

create external table if not exists ssl_sites_usage_events (
event_id String,
resource_id String,
resource_name String,
event_time  bigint,
start_time  bigint,
end_time  bigint,
environment String,
is_new_account String,
tenant_id String,
data_center String,
region String,
type String,
version String,
group_name String,
num_files String,
storage String,
volume String,
bandwidth_out String,
compute_cycles String,
request_count String,
ssl_enabled String,
resource_type String,
service_code String,
product_type String,
product_version String,
usage_category String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/ssl_sites_usage_events';


drop table if exists  ssl_sites_usage_events_orc ;

create table if not exists ssl_sites_usage_events_orc (
event_id String,
resource_id String,
resource_name String,
event_time  bigint,
start_time  bigint,
end_time  bigint,
environment String,
is_new_account String,
tenant_id String,
data_center String,
region String,
type String,
version String,
group_name String,
num_files String,
storage String,
volume String,
bandwidth_out String,
compute_cycles String,
request_count String,
ssl_enabled String,
resource_type String,
service_code String,
product_type String,
product_version String,
usage_category String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
stored as ORC;
drop table if exists  cloud_backup ;

create external table if not exists cloud_backup (
ah_uuid String,
raw_xml String,
dw_timestamp_ts  bigint,
dw_timestamp  String
) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/cloud_backup';

drop table if exists  cloud_cbs ;

create external table if not exists cloud_cbs (
ah_uuid String,
raw_xml String,
dw_timestamp_ts  bigint,
dw_timestamp  String) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/cloud_cbs';

drop table if exists  cloud_dbaas ;

create external table if not exists cloud_dbaas (
ah_uuid String,
raw_xml String,
dw_timestamp_ts  bigint,
dw_timestamp  String) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/cloud_dbaas';

drop table if exists  cloud_files ;

create external table if not exists cloud_files (
ah_uuid String,
raw_xml String,
dw_timestamp_ts  bigint,
dw_timestamp  String) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/cloud_files';

drop table if exists  cloud_glance ;

create external table if not exists cloud_glance (
ah_uuid String,
raw_xml String,
dw_timestamp_ts  bigint,
dw_timestamp  String) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/cloud_glance';

drop table if exists  cloud_hadoop ;

create external table if not exists cloud_hadoop (
ah_uuid String,
raw_xml String,
dw_timestamp_ts  bigint,
dw_timestamp  String) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/cloud_hadoop';

drop table if exists  cloud_lbaas ;

create external table if not exists cloud_lbaas (
ah_uuid String,
raw_xml String,
dw_timestamp_ts  bigint,
dw_timestamp  String) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/cloud_lbaas';

drop table if exists  cloud_metered_sites ;

create external table if not exists cloud_metered_sites (
ah_uuid String,
raw_xml String,
dw_timestamp_ts  bigint,
dw_timestamp  String) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/cloud_metered_sites';

drop table if exists  cloud_monitoring ;

create external table if not exists cloud_monitoring (
ah_uuid String,
raw_xml String,
dw_timestamp_ts  bigint,
dw_timestamp  String) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/cloud_monitoring';

drop table if exists  cloud_mssql_sites ;

create external table if not exists cloud_mssql_sites (
ah_uuid String,
raw_xml String,
dw_timestamp_ts  bigint,
dw_timestamp  String) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/cloud_mssql_sites';

drop table if exists  cloud_mysql_sites ;

create external table if not exists cloud_mysql_sites (
ah_uuid String,
raw_xml String,
dw_timestamp_ts  bigint,
dw_timestamp  String) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/cloud_mysql_sites';

drop table if exists  cloud_netapp_sites ;

create external table if not exists cloud_netapp_sites (
ah_uuid String,
raw_xml String,
dw_timestamp_ts  bigint,
dw_timestamp  String) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/cloud_netapp_sites';

drop table if exists  cloud_nova ;

create external table if not exists cloud_nova (
ah_uuid String,
raw_xml String,
dw_timestamp_ts  bigint,
dw_timestamp  String) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/cloud_nova';

drop table if exists  cloud_queues ;

create external table if not exists cloud_queues (
ah_uuid String,
raw_xml String,
dw_timestamp_ts  bigint,
dw_timestamp  String) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/cloud_queues';

drop table if exists  cloud_servers ;

create external table if not exists cloud_servers (
ah_uuid String,
raw_xml String,
dw_timestamp_ts  bigint,
dw_timestamp String
)  partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/cloud_servers';

drop table if exists  cloud_sitessub_sites ;

create external table if not exists cloud_sitessub_sites (
ah_uuid String,
raw_xml String,
dw_timestamp_ts  bigint,
dw_timestamp  String) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/cloud_sitessub_sites';

drop table if exists  cloud_ssl_sites ;

create external table if not exists cloud_ssl_sites (
ah_uuid String,
raw_xml String,
dw_timestamp_ts  bigint,
dw_timestamp  String) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/cloud_ssl_sites';

drop table if exists  glance_hkg1 ;

create external table if not exists glance_hkg1 (
title String,
id String,
summary String,
created String,
content String,
updated_ts bigint,
updated String,
dw_timestamp  bigint,
latest_run String,
event_generated_timestamp_ts  bigint
) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/glance_hkg1';

drop table if exists  glance_iad3 ;

create external table if not exists glance_iad3 (
title String,
id String,
summary String,
created String,
content String,
updated_ts bigint,
updated String,
dw_timestamp  bigint,
latest_run String,
event_generated_timestamp_ts  bigint
) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/glance_iad3';

drop table if exists  glance_lon3 ;

create external table if not exists glance_lon3 (
title String,
id String,
summary String,
created String,
content String,
updated_ts bigint,
updated String,
dw_timestamp  bigint,
latest_run String,
event_generated_timestamp_ts  bigint
) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/glance_lon3';

drop table if exists  glance_ord1 ;

create external table if not exists glance_ord1 (
title String,
id String,
summary String,
created String,
content String,
updated_ts bigint,
updated String,
dw_timestamp  bigint,
latest_run String,
event_generated_timestamp_ts  bigint
) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/glance_ord1';

drop table if exists  glance_syd2 ;

create external table if not exists glance_syd2 (
title String,
id String,
summary String,
created String,
content String,
updated_ts bigint,
updated String,
dw_timestamp  bigint,
latest_run String,
event_generated_timestamp_ts  bigint
) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/glance_syd2';

drop table if exists  hadoop_hdp1_3_usage_events ;

create external table if not exists hadoop_hdp1_3_usage_events (
event_id String,
resource_id String,
resource_name String,
event_time  bigint,
start_time  bigint,
end_time  bigint,
environment String,
bandwidth_in String,
bandwidth_out String,
flavor_id String,
flavor_name String,
aggregated_cluster_duration String,
number_servers_in_cluster String,
tenant_id String,
data_center String,
region String,
type String,
version String,
resource_type String,
service_code String,
product_type String,
product_version String,
usage_category String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/hadoop_hdp1_3_usage_events';

drop table if exists  hadoop_hdp1_3_usage_events_orc ;

create table if not exists hadoop_hdp1_3_usage_events_orc (
event_id String,
resource_id String,
resource_name String,
event_time  bigint,
start_time  bigint,
end_time  bigint,
environment String,
bandwidth_in String,
bandwidth_out String,
flavor_id String,
flavor_name String,
aggregated_cluster_duration String,
number_servers_in_cluster String,
tenant_id String,
data_center String,
region String,
type String,
version String,
resource_type String,
service_code String,
product_type String,
product_version String,
usage_category String,
ah_uuid String,
source_system_name String,
ah_updated String,
ah_updatedts bigint,
dw_timestamp  bigint
) partitioned by (date String)
stored as ORC;

drop table if exists  nova_hkg1 ;

create external table if not exists nova_hkg1 (
title String,
id String,
summary String,
created String,
content String,
updated_ts bigint,
updated String,
dw_timestamp  bigint,
latest_run String,
event_generated_timestamp_ts  bigint
) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/nova_hkg1';

drop table if exists  nova_iad3 ;

create external table if not exists nova_iad3 (
title String,
id String,
summary String,
created String,
content String,
updated_ts bigint,
updated String,
dw_timestamp  bigint,
latest_run String,
event_generated_timestamp_ts  bigint
) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/nova_iad3';

drop table if exists  nova_lon3 ;

create external table if not exists nova_lon3 (
title String,
id String,
summary String,
created String,
content String,
updated_ts bigint,
updated String,
dw_timestamp  bigint,
latest_run String,
event_generated_timestamp_ts  bigint
) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/nova_lon3';

drop table if exists  nova_ord1 ;

create external table if not exists nova_ord1 (
title String,
id String,
summary String,
created String,
content String,
updated_ts bigint,
updated String,
dw_timestamp  bigint,
latest_run String,
event_generated_timestamp_ts  bigint
) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/nova_ord1';

drop table if exists  nova_syd2 ;

create external table if not exists nova_syd2 (
title String,
id String,
summary String,
created String,
content String,
updated_ts bigint,
updated String,
dw_timestamp  bigint,
latest_run String,
event_generated_timestamp_ts  bigint
) partitioned by (date String)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/cloud_usage_events.db/nova_syd2';

