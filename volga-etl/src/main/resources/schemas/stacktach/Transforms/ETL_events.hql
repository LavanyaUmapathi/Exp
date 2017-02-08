USE neha8380;

SET hive.hadoop.supports.splittable.combineinputformat = true;

DROP TABLE

IF EXISTS tmp_json_events_20150520;
	DROP TABLE

IF EXISTS tmp_events_20150520;
	DROP TABLE

IF EXISTS tmp_events_payload_20150520;
	DROP TABLE

IF EXISTS tmp_events_payload_imagemeta_20150520;
	DROP TABLE

IF EXISTS tmp_events_instance_20150520;
	DROP TABLE

IF EXISTS tmp_events_exception_20150520;
	DROP TABLE

IF EXISTS tmp_events_exception_kwargs_20150520;
	CREATE TABLE tmp_json_events_20150520 AS

SELECT dt,
	input__file__name FILE,
	a.json_notification json_notification
FROM neha8380.original_st_notifications a
WHERE
	-- dt = ${hivevar:curr_dt_utc}
	dt = '20150520';

CREATE TABLE neha8380.tmp_notifications_20150520 AS

SELECT *
FROM neha8380.tmp_json_events_20150520 a lateral VIEW json_tuple(a.json_notification, 'cell', 'region', 'notification') b AS cell,
	region,
	notification;

DROP TABLE

IF EXISTS tmp_json_events_20150520;
	CREATE TABLE neha8380.tmp_events_20150520 AS

SELECT *
FROM neha8380.tmp_notifications_20150520 a lateral VIEW json_tuple(a.notification, 'message_id', '_context_request_id', 'timestamp', 'event_type', 'tenant_id', '_context_project_id', 'publisher_id', 'payload', 'exception', 'instance', 'priority') b AS message_id,
	message,
	context_request_id,
	TIMESTAMP,
	event_type,
	tenant_id,
	context_project_id,
	publisher_id,
	payload,
	exception,
	instance,
	priority;

DROP TABLE

IF EXISTS tmp_notifications_20150520;
	CREATE TABLE tmp_events_payload_20150520 AS

SELECT *
FROM tmp_events_20150520 a lateral VIEW json_tuple(a.payload, 'tenant_id', 'message', 'instance_uuid', 'instance_type', 'instance_id', 'user_id', 'instance_flavor_id', 'instance_type_id', 'os_type', 'memory_mb', 'root_gb', 'disk_gb', 'ephemeral_gb', 'vcpus', 'audit_period_beginning', 'audit_period_ending', 'state', 
		'old_state', 'state_description', 'launched_at', 'deleted_at', 'terminated_at', 'display_name', 'image_meta', 'terminated_at_utc', 'publisher_id', 'cell_name', 'new_task_state', 'old_task_state', 'progress', 'image_ref_url') b AS tenant_id_1,
	message,
	instance_uuid,
	instance_type,
	instance_id,
	user_id,
	instance_flavor_id,
	instance_type_id,
	os_type,
	memory_mb,
	root_gb,
	disk_gb,
	ephemeral_gb,
	vcpus,
	audit_period_beginning,
	audit_period_ending,
	STATE,
	old_state,
	state_description,
	launched_at,
	deleted_at,
	terminated_at,
	display_name,
	image_meta,
	terminated_at_utc,
	publisher_id1,
	cell_name,
	new_task_state,
	old_task_state,
	progress,
	image_ref_url;

DROP TABLE

IF EXISTS tmp_events_20150520;
	CREATE TABLE tmp_events_payload_imagemeta_20150520 AS

SELECT *
FROM tmp_events_payload_20150520 a lateral VIEW json_tuple(a.image_meta, 'instance_type_name', 'instance_type_flavorid', 'instance_type_flavor_id', 'org.openstack__1__architecture', 'org.openstack__1__os_version', 'org.openstack__1__os_distro', 'com.rackspace__1__options', 'image_type') b AS instance_type_name,
	instance_type_flavorid,
	instance_type_flavor_id,
	org_openstack__1__architecture,
	org_openstack__1__os_version,
	org_openstack__1__os_distro,
	com_rackspace__1__options,
	image_type;

DROP TABLE tmp_events_payload_20150520;

CREATE TABLE tmp_events_instance_20150520 AS

SELECT *
FROM tmp_events_payload_imagemeta_20150520 a lateral VIEW json_tuple(a.instance, 'uuid') b AS uuid;

DROP TABLE tmp_events_payload_imagemeta_20150520;

CREATE TABLE tmp_events_exception_20150520 AS

SELECT *
FROM tmp_events_instance_20150520 a lateral VIEW json_tuple(a.exception, 'kwargs') b AS kwargs;

DROP TABLE tmp_events_instance_20150520;

CREATE TABLE tmp_events_exception_kwargs_20150520 AS

SELECT *
FROM tmp_events_exception_20150520 a lateral VIEW json_tuple(a.kwargs, 'code', 'uuid') b AS exe_code,
	exe_uuid_1;

DROP TABLE tmp_events_exception_20150520;

ALTER TABLE events

DROP

IF EXISTS PARTITION (dt = '20150520');
	ALTER TABLE events ADD PARTITION (dt = '20150520');

USE neha8380;
FROM

tmp_events_exception_kwargs_20150520

INSERT overwrite TABLE events PARTITION (dt = '20150520')
SELECT FILE
	--,   dt	
	,
	cell,
	region,
	cast(TIMESTAMP AS TIMESTAMP) dt_ts_utc,
	to_date(cast(TIMESTAMP AS TIMESTAMP)) dt_utc,
	TIMESTAMP AS notification_timestamp,
	event_type event_type,
	coalesce(tenant_id_1, tenant_id, context_project_id) tenant_id,
	user_id user_id,
	context_request_id request_id,
	message_id message_id,
	message message,
	coalesce(instance_uuid, instance_id, exe_uuid_1, uuid) instance_id,
	coalesce(publisher_id, publisher_id1) host,
	coalesce(publisher_id, publisher_id1) service,
	coalesce(instance_type, instance_type_name, instance_type_flavorid) instance_flavor,
	instance_flavor_id instance_flavor_id,
	instance_type_name instance_type_name,
	instance_type_flavorid instance_type_flavorid,
	instance_type_id instance_type_id,
	instance_type_flavor_id instance_type_flavor_id,
	os_type os_type,
	image_type image_type,
	memory_mb memory_mb,
	disk_gb disk_gb,
	root_gb root_gb,
	ephemeral_gb ephemeral_gb,
	vcpus vcpus,
	instance_type instance_type,
	STATE STATE,
	old_state,
	state_description state_description,
	org_openstack__1__architecture os_architecture,
	org_openstack__1__os_version os_version,
	org_openstack__1__os_distro os_distro,
	com_rackspace__1__options rax_options,
	launched_at launched_at_utc,
	deleted_at deleted_at_utc,
	display_name display_name,
	audit_period_beginning audit_period_beginning_utc,
	audit_period_ending audit_period_ending_utc,
	exe_code exception_code,
	terminated_at_utc,
	priority,
	coalesce(publisher_id, publisher_id1) publisher_id,
	cell_name,
	new_task_state,
	old_task_state,
	progress,
	image_ref_url;

DROP TABLE

IF EXISTS tmp_events_exception_kwargs_20150520;
