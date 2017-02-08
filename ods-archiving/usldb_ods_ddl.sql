drop database if exists usldb_ods CASCADE;
create database if not exists usldb_ods;

use usldb_ods;

ADD JAR /usr/hdp/current/hive-client/lib/csv-serde-1.1.2-0.11.0-all.jar;

drop table if exists atom_marker;
create table if not exists atom_marker (
baseuri String,
uri String,
dw_timestamp bigint);

drop table if exists camel_messageprocessed;
create table if not exists camel_messageprocessed (
processorname String,
messageid String,
createdat bigint,
dw_timestamp bigint);

drop table if exists correlation;
create external table if not exists correlation (
correlation_id String,
correlation_key String,
timeout_date bigint,
is_enqueued String,
created_by String,
updated_by String,
created_date bigint,
updated_date bigint,
is_delayed String,
dw_timestamp bigint) partitioned by ( dt string)
LOCATION '/apps/hive/warehouse/usldb_ods.db/correlation';

drop table if exists details_correlation_xref;
create external table if not exists details_correlation_xref (
correlation_id String,
usage_id String,
created_by String,
updated_by String,
created_date bigint,
updated_date bigint,
dw_timestamp bigint) partitioned by ( dt string)
LOCATION '/apps/hive/warehouse/usldb_ods.db/details_correlation_xref';

drop table if exists glance_daily_log;
create table if not exists glance_daily_log (
log_msg String,
created_time bigint,
dw_timestamp bigint);

drop table if exists glance_la_accounts;
create table if not exists glance_la_accounts (
account_number String,
dw_timestamp bigint);

drop table if exists nova_la_accounts;
create table if not exists nova_la_accounts (
account_number String,
dw_timestamp bigint);

drop table if exists onm_image_mapping;
create table if not exists onm_image_mapping (
image_id String,
image_name String,
concurrent_id String,
created_date bigint,
dw_timestamp bigint);

drop table if exists summary;
create external table if not exists summary (
summary_id String,
service_code String,
resource_id String,
resource_name String,
data_center String,
region String,
resource_type String,
billing_account_number String,
service_level String,
created_by String,
updated_by String,
created_date bigint,
updated_date bigint,
start_timestamp bigint,
end_timestamp bigint,
dw_timestamp bigint,
product_event_type String,
correlation_key String,
corrected_summary String) partitioned by ( dt string)
LOCATION '/apps/hive/warehouse/usldb_ods.db/summary';

drop table if exists summary_attr_value;
create external table if not exists summary_attr_value (
summary_id String,
attribute_name String,
attribute_value String,
created_by String,
updated_by String,
created_date bigint,
updated_date bigint,
dw_timestamp bigint) partitioned by ( dt string)
LOCATION '/apps/hive/warehouse/usldb_ods.db/summary_attr_value';

drop table if exists temp_account;
create table if not exists temp_account (
account String,
dw_timestamp bigint);

drop table if exists tenant_account_mapping;
create table if not exists tenant_account_mapping (
tenant_id String,
account_number String,
created_date bigint,
dw_timestamp bigint);

drop table if exists tmp_summary_attribute_value;
create table if not exists tmp_summary_attribute_value (
record_id String,
record_attribute_name String,
record_attribute_type String,
record_attribute_value String,
version String,
created_date bigint,
created_by String,
updated_date bigint,
updated_by String,
dw_timestamp bigint);

drop table if exists usage_daily_daterange_xref;
create external table if not exists usage_daily_daterange_xref (
usage_xref_id String,
child_record_id String,
parent_record_id String,
version String,
created_date bigint,
created_by String,
updated_date bigint,
updated_by String,
dw_timestamp bigint) partitioned by ( dt string)
LOCATION '/apps/hive/warehouse/usldb_ods.db/usage_daily_daterange_xref';

drop table if exists usage_details;
create external table if not exists usage_details (
usage_id String,
service_name String,
marker_id String,
source_system String,
event_type String,
event_generated_timestamp bigint,
event_at_source_timestamp bigint,
instance_id String,
instance_display_name String,
instance_created_timestamp bigint,
instance_launched_timestamp bigint,
account_user_id String,
tenant_id String,
account_type String,
account_number String,
instance_type_id String,
instance_type_name String,
flavor_id String,
image_ref String,
image_id String,
image_name String,
image_options String,
new_instance_type_id String,
new_instance_type_name String,
audit_period_begin_timestamp bigint,
audit_period_end_timestamp bigint,
instance_state_id String,
instance_state_name String,
data_center String,
processing_times String,
version String,
created_date bigint,
created_by String,
updated_date bigint,
updated_by String,
region String,
summary_record_id String,
dw_timestamp bigint,
deleted_at bigint,
account_time_zone String,
snapshot_size String,
last_updated_at bigint,
status String) partitioned by ( dt string)
LOCATION '/apps/hive/warehouse/usldb_ods.db/usage_details';

drop table if exists usage_details_attribute_value;
create external table if not exists usage_details_attribute_value (
usage_id String,
attribute_name String,
attribute_type String,
attribute_value String,
version String,
created_date bigint,
created_by String,
updated_date bigint,
updated_by String,
dw_timestamp bigint) partitioned by ( dt string)
LOCATION '/apps/hive/warehouse/usldb_ods.db/usage_details_attribute_value';

drop table if exists usage_error_handling;
create table if not exists usage_error_handling (
log_level String,
status String,
description String,
cartridge String,
reprocess_eligible String,
usage_id String,
usage_data String,
source_system String,
version String,
created_date bigint,
created_by String,
updated_date bigint,
updated_by String,
dw_timestamp bigint,
service_name String,
marker_id String,
account_time_zone String,
data_center String,
region String);

drop table if exists usage_summary;
create external table if not exists usage_summary (
record_id String,
summary_level String,
instance_id String,
instance_display_name String,
account_type String,
account_number String,
flavor_id String,
instance_type_id String,
instance_type_name String,
image_id String,
image_options String,
image_name String,
start_timestamp bigint,
end_timestamp bigint,
data_center String,
processing_times String,
version String,
created_date bigint,
created_by String,
updated_date bigint,
updated_by String,
request_id String,
dw_timestamp bigint,
account_time_zone String,
product String,
region String,
usr_sequence_id String,
avg_snapshot_size String,
image_distro String,
brm_account_number String) partitioned by ( dt string)
LOCATION '/apps/hive/warehouse/usldb_ods.db/usage_summary';

drop table if exists usage_summarization_request;
create table if not exists usage_summarization_request (
request_id String,
account_number String,
effective_start_date bigint,
effective_end_date bigint,
status String,
status_desc String,
late_usage_days String,
version String,
created_date bigint,
created_by String,
updated_date bigint,
updated_by String,
product String,
dw_timestamp bigint,
usr_sequence_id String,
received_start_date bigint,
received_end_date bigint,
usage_type String,
account_time_zone String);

drop table if exists usage_summary_attribute_value;
create external table if not exists usage_summary_attribute_value (
record_id String,
record_attribute_name String,
record_attribute_type String,
record_attribute_value String,
version String,
created_date bigint,
created_by String,
updated_date bigint,
updated_by String,
dw_timestamp bigint) partitioned by ( dt string)
LOCATION '/apps/hive/warehouse/usldb_ods.db/usage_summary_attribute_value';

drop table if exists usg_bill_dtls_attr_value;
create external table if not exists usg_bill_dtls_attr_value (
usage_id String,
attribute_name String,
attribute_value String,
version String,
created_date bigint,
created_by String,
updated_date bigint,
updated_by String,
dw_timestamp bigint) partitioned by ( dt string)
LOCATION '/apps/hive/warehouse/usldb_ods.db/usg_bill_dtls_attr_value';

drop table if exists usg_bill_dtls_daily_xref;
create external table if not exists usg_bill_dtls_daily_xref (
usage_id String,
summary_record_id String,
version String,
created_date bigint,
created_by String,
updated_date bigint,
dw_timestamp bigint,
updated_by String) partitioned by ( dt string)
LOCATION '/apps/hive/warehouse/usldb_ods.db/usg_bill_dtls_daily_xref';

drop table if exists usg_billing_details;
create external table if not exists usg_billing_details (
usage_id String,
tenant_id String,
service_code String,
resource_id String,
resource_name String,
event_type String,
data_center String,
region String,
start_timestamp bigint,
end_timestamp bigint,
event_timestamp bigint,
resource_type String,
atomhopper_timestamp bigint,
account_number String,
source_system_name String,
service_level String,
program_id String,
usg_core_xsd_version String,
usg_prod_xsd_version String,
version String,
created_date bigint,
created_by String,
updated_date bigint,
updated_by String,
dw_timestamp bigint,
atommarker_id String,
environment String,
product_event String,
root_action String) partitioned by ( dt string)
LOCATION '/apps/hive/warehouse/usldb_ods.db/usg_billing_details';

drop table if exists usg_billing_error_handling;
create table if not exists usg_billing_error_handling (
usage_id String,
tenant_id String,
account_number String,
marker_id String,
service_code String,
resource_type String,
error_description String,
version String,
created_date bigint,
created_by String,
updated_date bigint,
updated_by String,
dw_timestamp bigint);

drop table if exists usg_billing_summary;
create external table if not exists usg_billing_summary (
summary_record_id String,
service_code String,
resource_id String,
resource_name String,
data_center String,
region String,
resource_type String,
account_number String,
source_system_name String,
service_level String,
summary_start_timestamp bigint,
summary_end_timestamp bigint,
attribute1_name String,
attribute1_value String,
attribute2_name String,
attribute2_value String,
attribute3_name String,
attribute3_value String,
attribute4_name String,
attribute4_value String,
attribute5_name String,
attribute5_value String,
attribute6_name String,
attribute6_value String,
attribute7_name String,
attribute7_value String,
attribute8_name String,
attribute8_value String,
attribute9_name String,
attribute9_value String,
attribute10_name String,
attribute10_value String,
attribute11_name String,
attribute11_value String,
attribute12_name String,
attribute12_value String,
attribute13_name String,
attribute13_value String,
attribute14_name String,
attribute14_value String,
attribute15_name String,
attribute15_value String,
attribute16_name String,
attribute16_value String,
attribute17_name String,
attribute17_value String,
attribute18_name String,
attribute18_value String,
attribute19_name String,
attribute19_value String,
attribute20_name String,
attribute20_value String,
attribute21_name String,
attribute21_value String,
attribute22_name String,
attribute22_value String,
attribute23_name String,
attribute23_value String,
attribute24_name String,
attribute24_value String,
attribute25_name String,
attribute25_value String,
program_id String,
version String,
created_date bigint,
created_by String,
updated_date bigint,
updated_by String,
dw_timestamp bigint,
product_event String) partitioned by ( dt string)
LOCATION '/apps/hive/warehouse/usldb_ods.db/usg_billing_summary';

drop table if exists usg_details_summary_xref;
create external table if not exists usg_details_summary_xref (
usage_id String,
summary_id String,
created_by String,
updated_by String,
created_date bigint,
updated_date bigint,
dw_timestamp bigint) partitioned by ( dt string)
LOCATION '/apps/hive/warehouse/usldb_ods.db/usg_details_summary_xref';

drop table if exists usg_error_details;
create external table if not exists usg_error_details (
error_id String,
raw_xml String,
error_status String,
error_details String,
processing_log String,
route_in_error String,
usage_id String,
tenant_id String,
service_code String,
event_type String,
data_center String,
region String,
resource_type String,
atomhopper_timestamp bigint,
source_system_name String,
usg_core_xsd_version String,
usg_prod_xsd_version String,
version String,
created_date bigint,
created_by String,
updated_date bigint,
updated_by String,
dw_timestamp bigint,
atommarker_id String) partitioned by ( dt string)
 row format serde 'com.bizo.hive.serde.csv.CSVSerde'
LOCATION '/apps/hive/warehouse/usldb_ods.db/usg_error_details';

drop table if exists usg_product_table;
create table if not exists usg_product_table (
service_code String,
description String,
active_flag String,
effective_start_date bigint,
effective_end_date bigint,
program_id String,
version String,
created_date bigint,
created_by String,
updated_date bigint,
updated_by String,
dw_timestamp bigint);
