USE [Stage_Monitoring]
GO

/****** Object:  StoredProcedure [dbo].[sp_Get_Current_Lists]    Script Date: 10/18/2016 2:07:33 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





/*
-------------------------------------------------------------------------------------------
Created By: David Teniente
Created On: 6/18/2013
-------------------------------------------------------------------------------------------

Description:
--------------


Modifications:
--------------

-------------------------------------------------------------------------------------------
*/ 

ALTER procedure [dbo].[sp_Get_Current_Lists]

as

declare @process_check as int

set @process_check = (
						select
							count(*)	as total_count
						from
							stage_monitoring.dbo.ref_processed_file_list
						where
							iscomplete in ('in_progress','stage_complete','stage_complete_with_errors')
					  )
								
if @process_check = 0
begin

--STEP 1 - Get current files in FTP directoriesquit;

--clear out current list
truncate table stage_monitoring.dbo.ref_current_file_list

--Rackwatch
exec stage_monitoring.dbo.sp_GetListOfFileWithSize N'D:\FTP\monitoring\availability\rackwatch\hourly\'
--exec stage_monitoring.dbo.sp_GetListOfFileWithSize N'D:\FTP\monitoring\availability\rackwatch\daily\'
--exec stage_monitoring.dbo.sp_GetListOfFileWithSize N'D:\FTP\monitoring\availability\rackwatch\monthly\'

--Sitescope
exec stage_monitoring.dbo.sp_GetListOfFileWithSize N'D:\FTP\monitoring\availability\sitescope\hourly\'
--exec stage_monitoring.dbo.sp_GetListOfFileWithSize N'D:\FTP\monitoring\availability\sitescope\daily\'
--exec stage_monitoring.dbo.sp_GetListOfFileWithSize N'D:\FTP\monitoring\availability\sitescope\monthly\'

--MaaS
exec stage_monitoring.dbo.sp_GetListOfFileWithSize N'D:\FTP\monitoring\availability\maas\hourly\'
--exec stage_monitoring.dbo.sp_GetListOfFileWithSize N'D:\FTP\monitoring\availability\maas\daily\'
--exec stage_monitoring.dbo.sp_GetListOfFileWithSize N'D:\FTP\monitoring\availability\maas\monthly\'

--NewRelic
exec stage_monitoring.dbo.sp_GetListOfFileWithSize N'D:\FTP\monitoring\availability\newrelic\hourly\'
exec sp_delete_0_files

delete from stage_monitoring.dbo.ref_current_file_list
	where
		size_KB < 1.00
	


--STEP 2 - have any of these files been processed and need to be archived
--clear out current list
truncate table stage_monitoring.dbo.ref_current_file_list_archive

--ALL Archives
exec stage_monitoring.dbo.sp_GetListOfFileWithSize_Archive N'D:\FTP\monitoring\availability\archive\'


--GET ROW COUNTS FOR FILES
begin

	declare @file_location	varchar(1024)
	declare @file_name_txt	varchar(255)
	
	declare file_move cursor LOCAL fast_forward for
	select 
		file_location,
		file_name_txt	
	from 
		stage_monitoring.dbo.ref_current_file_list


	open file_move
	fetch next from file_move into @file_location, @file_name_txt

	while @@FETCH_STATUS=0
	begin
			
		exec dbo.sp_get_row_count @file_location, @file_name_txt
		
		fetch next from file_move into @file_location, @file_name_txt
	
	end
	
	close file_move
	deallocate  file_move

end


end


if @process_check <> 0
begin
	print 'Process is still running.  Please check progress of current process'
end



GO




USE [Stage_Monitoring]
GO

/****** Object:  StoredProcedure [dbo].[sp_stage_report_monitoring_hourly_table]    Script Date: 10/18/2016 2:10:26 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO















/*
-------------------------------------------------------------------------------------------
Created By: David Teniente
Created On: 6/18/2013
-------------------------------------------------------------------------------------------

Description:
--------------


Modifications:
--------------
Natasha Gajic 07/20/2015 Added Maas
-------------------------------------------------------------------------------------------
*/ 

ALTER procedure [dbo].[sp_stage_report_monitoring_hourly_table]

as

declare @insert_time	datetime
set @insert_time		= getdate()


truncate table stage_monitoring.dbo.stg_report_monitoring_availability_hourly


alter index IDX_ROWKEY on [dbo].[stg_monitoring_hourly] REBUILD
alter index IDX_ROWKEY on [dbo].[ref_row_key] REBUILD
alter index IDX_Instance on [dbo].[ref_row_key] REBUILD

update ref_row_key set Monitor_Silo='N/A' where monitor_silo='AGENT'

--update ref_row_key set Monitor_source_system='RADAR' where Monitor_source_system='SiteScope' 

insert into stage_monitoring.dbo.stg_report_monitoring_availability_hourly


select
	--start time
	b.time_key,
	b.time_full_date,
	b.Time_Month_Number,
	b.Time_Month_Desc,
	b.Time_Month_Abbr,
	b.Time_Year_Number,
	b.Time_Quarter_Number,
	datepart(hh,a.converted_start_time)		as HMS_Military_Hour_Number,
	a.converted_start_time					as Time_Full_Date_HMS,
	----end time
	c.time_key								as end_time_key,
	c.time_full_date						as end_time_full_date,
	c.Time_Month_Number						as end_time_month_number,
	c.Time_Month_Desc						as end_time_month_desc,
	c.Time_Month_Abbr						as end_time_month_abbr,
	c.Time_Year_Number						as end_time_year_number,
	c.Time_Quarter_Number					as end_time_quarter_number,
	c.time_quarter_desc						as end_time_quarter_desc,
	datepart(hh,dateadd(hour,1,a.converted_start_time))		as HMS_Military_Hour_Number,
	dateadd(hour,1,a.converted_start_time)	as Time_Full_Date_HMS,
	--ACCOUNT
	zz.account_name, 
	zz.account_number, 
	zz.account_type, 
	--INSTANCE
	zz.device_host_name, 
	d.instance_nk,
	zz.device_status,	
	--MONITOR
	case
		when e.monitor_type	= 'web' then 'URL'
	else	
		e.Monitor_Type
	end									as Monitor_type,
	e.monitor_name,
	e.Monitor_Silo,
	-----------------------------------------------------------------------------
	case
		--DEFINE PING
		when 
			d.MONITOR_TYPE = 'Ping' 
			OR d.MONITOR_TYPE = 'ICMP'  
			OR d.monitor_NK = 'PING'
			then 'PING'
		
		--DEFINE PORT
		when
		((d.MONITOR_NK IN ('FTP','ColdFusion','Cold Fusion','PostgreSQL','MS SQL Server','Webport','SQLServer','HTTP',
							'IMAP','POP3','Telnet','SMTP','Postgres','HTTPS','MySQL','DNS','SSH') 
		AND d.monitor_source_system = 'rackwatch')
		OR
		(d.monitor_type in ('Protocol','Ping','FTPGet','ColdFusion','DNS','Protocol')
		and d.monitor_source_system = 'sitescope')
		OR
		(d.monitor_type in ('TCP','SMTP','SMTP-BANNER','SSH','NETWORK','MYSQL','DNS','APACHE')
		and d.monitor_source_system = 'maas'))
			then 'PORT'

		--DEFINE URL
		when
		((d.monitor_type in ('http','web','URL')
		and d.monitor_source_system = 'sitescope')
		OR
		(d.monitor_type in ('URL')
		and d.monitor_source_system = 'MaaS'))
			then 'URL'

		else
			'other'
	end											as monitor_type_group,
	-----------------------------------------------------------------------------
	d.Monitor_Source_System						as source_system_name,
	--MEASURES
	a.Aggreed_Up_Time							as uptimeDuration,
	a.Interval_Down_Time						as downtimeDurationWithSupression,
	a.Total_Down_Time  							as downtimeDurationWithoutSupression,
	cast(a.Calculated_Percent as decimal(38,6))	as uptimePercentageWithSupression,
	cast(a.Total_Percent as decimal(38,6))		as uptimePercentageWithoutSupression,
	-----------------------------------------------------------------------------
	a.row_key									as row_key,
	d.Monitor_NK								as monitor_id,
	d.instance_source_system					as device_source,
	zz.account_source_system_name,	
	@insert_time,
	a.file_name_txt
from
	dbo.stg_monitoring_hourly a
	
	--start time
	inner join stage_three_dw.dbo.dim_time b
		on	b.time_full_date		=	cast(convert(varchar,a.converted_start_time, 101) as datetime)
		
	--end time
	inner join stage_three_dw.dbo.dim_time c
		on	c.time_full_date		= 	cast(convert(varchar,dateadd(hour,1,a.converted_start_time), 101) as datetime)
		
	--join with ref_row_key for instance_nk and monitor_nk
	left join stage_monitoring.dbo.ref_row_key d
		on	a.row_key				=	d.row_key
		
	--join with dim_monitor
	left join stage_three_dw.dbo.dim_monitor e
	     on d.row_key=e.Monitor_ID
		--on	d.Monitor_NK			=	e.monitor_id_nk	
		--and d.Monitor_silo =e.Monitor_Silo 
		--and d.Monitor_source_system=(case
		--								when e.Source_System_Name = 'RADAR' then 'sitescope'
		--								 else
		--									e.Source_System_Name
		--								 end)									
				
		and e.Current_Record_Flag	=	1
		
		
	left join stage_monitoring.dbo.report_account_device_daily zz
		on	d.instance_nk				= zz.device_number
		and d.instance_source_system	= zz.device_source_system_name



--UPDATE stage_monitoring.dbo.ref_processed_file_list 
update a
	set		a.process_into_reporting	=	@insert_time
		,	a.reporting_row_count		=	z.total_row_count
		,	a.reporting_duration		=	datediff(ss,@insert_time,getdate())
		,	a.iscomplete				=	'inserting_into_production'
		
from 
	stage_monitoring.dbo.ref_processed_file_list a
	
	inner join (
				select
					file_name_txt,
					count(*)	as total_row_count
				from
					stage_monitoring.dbo.stg_report_monitoring_availability_hourly
				group by
					file_name_txt
				) as z
			on a.file_name_txt = z.file_name_txt	
			
					

alter index IDX_ROWKEY on [dbo].[stg_monitoring_hourly] DISABLE

alter index IDX_ROWKEY on [dbo].[ref_row_key] DISABLE
alter index IDX_Instance on [dbo].[ref_row_key] DISABLE


GO



USE [Stage_Monitoring]
GO

/****** Object:  StoredProcedure [dbo].[sp_AGG_Daily_Monitoring_UTC]    Script Date: 10/18/2016 2:16:44 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




/*
-------------------------------------------------------------------------------------------
Created By: David Teniente
Created On: 7/25/2013
-------------------------------------------------------------------------------------------

Description:
--------------


Modifications:
--------------

Natasha Gajic 07/20/2015 added MaaS & Performance imporvement

-------------------------------------------------------------------------------------------
*/

ALTER procedure [dbo].[sp_AGG_Daily_Monitoring_UTC]

as

declare @begin_date		datetime
declare @end_date		datetime
declare @begin_date_last_hour		datetime
declare @etl_date		datetime
declare @sql			varchar(1000)
declare @run_id			int
declare @check_day		datetime
declare @timezone		varchar(10)
declare @min_id			bigint
declare @max_id         bigint
declare @min_maas_id			bigint
declare @max_maas_id         bigint

declare @min_sitescope_id			bigint
declare @max_sitescope_id         bigint

declare @min_rackwatch_id			bigint
declare @max_rackwatch_id         bigint

declare @min_newrelic_id			bigint
declare @max_newrelic_id         bigint

declare @max_date_in_hourly datetime

--Audit variables
declare @process_begin	datetime
declare @step_1			datetime
declare @step_2			datetime
declare	@step_3			datetime
declare @step_4			datetime


--GET UTC DAY
declare @utc_start_date	datetime
declare @tz_diff		int
--set @utc_start_date		= '2015-06-18 00:00:00.000'-- hard code date to run for missing days
--set @utc_start_date		= (
--							select
--								time_full_date
--							from
--								stage_three_dw.dbo.Dim_Time
--							where
--								Time_Month_Number	= MONTH(getdate()-1)
--							and Time_Year_Number	= YEAR(getdate()-1)
--							and Time_Day_Number		= DAY(getdate()-1)
--							)
							
set @utc_start_date		= (select DATEADD(day,1,max(Time_Full_Date_HMS)) from stage_three_dw.dbo.Report_Monitoring_Availability_Daily)
print @utc_start_date
--set @utc_start_date		= '2015-10-14 00:00:00.000'-- hard code date to run for missing days
set @max_date_in_hourly = (select DATEADD(day,1,max(Time_Full_Date_HMS)) from stage_three_dw.dbo.Report_Monitoring_Availability_Hourly where source_system_name ='MaaS')
print @max_date_in_hourly
while (@utc_start_date< cast(floor(cast(@max_date_in_hourly as float)) as datetime) )
begin
--set datetimes (Central Times for hourly)
set	@begin_date			=	stage_three_dw.dbo.udf_Timezone_Conversion_WITH_DST(@utc_start_date,'CT')
set @end_date			=	DATEADD(ss,86399,@begin_date)
set @begin_date_last_hour = DATEADD(ss,82800,@begin_date)
set @etl_date			=	GETDATE()

print @begin_date	
print @end_date
print @begin_date_last_hour

--set check day
set @check_day			= (select MAX(day_to_process) from stage_monitoring.dbo.ref_audit_daily_agg where timezone = 'UTC')
set @timezone			= 'UTC'
set @min_maas_id             = 	(select min(id) from stage_three_dw.dbo.Report_Monitoring_Availability_Hourly with   (FORCESEEK, INDEX (IDX_DATE_HMS)) where Time_Full_Date_HMS = @begin_date and source_system_name='MaaS')
set @max_maas_id             =	(select max(id) from stage_three_dw.dbo.Report_Monitoring_Availability_Hourly with   (FORCESEEK, INDEX (IDX_DATE_HMS)) where Time_Full_Date_HMS = @begin_date_last_hour and source_system_name='MaaS')
set @min_sitescope_id             = 	(select min(id) from stage_three_dw.dbo.Report_Monitoring_Availability_Hourly with   (FORCESEEK, INDEX (IDX_DATE_HMS)) where Time_Full_Date_HMS = @begin_date and source_system_name='SiteScope')
set @max_sitescope_id             =	(select max(id) from stage_three_dw.dbo.Report_Monitoring_Availability_Hourly with   (FORCESEEK, INDEX (IDX_DATE_HMS)) where Time_Full_Date_HMS = @begin_date_last_hour and source_system_name='SiteScope')

set @min_rackwatch_id             = 	(select min(id) from stage_three_dw.dbo.Report_Monitoring_Availability_Hourly with   (FORCESEEK, INDEX (IDX_DATE_HMS)) where Time_Full_Date_HMS = @begin_date and source_system_name='RackWatch')
set @max_rackwatch_id             =	(select max(id) from stage_three_dw.dbo.Report_Monitoring_Availability_Hourly with   (FORCESEEK, INDEX (IDX_DATE_HMS)) where Time_Full_Date_HMS = @begin_date_last_hour and source_system_name='RackWatch')

set @min_newrelic_id             = 	(select min(id) from stage_three_dw.dbo.Report_Monitoring_Availability_Hourly with   (FORCESEEK, INDEX (IDX_DATE_HMS)) where Time_Full_Date_HMS = @begin_date and source_system_name='NewRelic')
set @max_newrelic_id             =	(select max(id) from stage_three_dw.dbo.Report_Monitoring_Availability_Hourly with   (FORCESEEK, INDEX (IDX_DATE_HMS)) where Time_Full_Date_HMS = @begin_date_last_hour and source_system_name='NewRelic')


print @min_maas_id			
print @max_maas_id    

print @min_newrelic_id			
print @max_newrelic_id        

print @min_sitescope_id			
print @max_sitescope_id         

print @min_rackwatch_id			
print @max_rackwatch_id         

print @min_id
print @max_id

--if @check_day < @utc_start_date

begin

insert into stage_monitoring.dbo.ref_audit_daily_agg
(
	day_to_process,
	timezone,
	begin_process
)
select
	@utc_start_date,'UTC',@etl_date
	
	
	
-----------------------------------------------------------------------------
--step 1 stage the agg
-----------------------------------------------------------------------------
--set @sql = '
--truncate table stage_monitoring.dbo.stg_monitoring_daily_universal_time

--insert into stage_monitoring.dbo.stg_monitoring_daily_universal_time
--(
--	uptimeDuration,
--	downtimeDurationWithSupression,
--	downtimeDurationWithoutSupression,
--	uptimePercentageWithSupression,
--	uptimePercentageWithoutSupression,
--	row_key, device_number, device_source
--)
--SELECT 
--	sum(uptimeDuration),
--	sum(downtimeDurationWithSupression),
--	sum(downtimeDurationWithoutSupression),
--	avg(uptimePercentageWithSupression),
--	avg(uptimePercentageWithoutSupression),
--	row_key,device_number, device_source
--FROM 
--	stage_three_dw.dbo.Report_Monitoring_Availability_Hourly with (nolock)
--where
--	Source_System_Name in (''sitescope'',''rackwatch'',''maas'')
--and
--	Time_Full_Date_HMS >= ''' + convert(varchar, @begin_date) + '''
--and 
--	time_full_date_hms <= ''' + convert(varchar, @end_date) + '''
--group by
--	row_key, device_number, device_source
--'

--set @sql = '
truncate table stage_monitoring.dbo.stg_monitoring_daily_universal_time
print 'Loading MaaS data'

insert into stage_monitoring.dbo.stg_monitoring_daily_universal_time
(
	sum_uptimeDuration,
	sum_downtimeDurationWithSupression,
	sum_downtimeDurationWithoutSupression,
	avg_uptimePercentageWithSupression,
	avg_uptimePercentageWithoutSupression,
	row_key, device_number, device_source,
	[Account_Name],[Account_Number],[Account_Type],[Device_Host_Name],[Device_Status],[Monitor_Type],
    [Monitor_Name],[Monitor_Silo],[Source_System_Name],[Monitor_ID],[Account_Source], [Monitor_Type_Group]
)
SELECT 
	sum(uptimeDuration),
	sum(downtimeDurationWithSupression),
	sum(downtimeDurationWithoutSupression),
	avg(uptimePercentageWithSupression),
	avg(uptimePercentageWithoutSupression),
	row_key,device_number, device_source,
	[Account_Name],[Account_Number],[Account_Type],[Device_Host_Name],[Device_Status],[Monitor_Type],
    [Monitor_Name],[Monitor_Silo],[Source_System_Name],[Monitor_ID],[Account_Source],[Monitor_Type_Group]
FROM 
	stage_three_dw.dbo.Report_Monitoring_Availability_Hourly with (nolock) 
where
    (id between @min_maas_id and @max_maas_id )
	and (Time_Full_Date_HMS  between   @begin_date	and  @end_date) 
	and Source_System_Name = 'MaaS'
	and Account_Source='Salesforce'
group by
	row_key, device_number, device_source, 
	[Account_Name],[Account_Number],[Account_Type],[Device_Host_Name],[Device_Status],[Monitor_Type],
    [Monitor_Name],[Monitor_Silo],[Source_System_Name],[Monitor_ID],[Account_Source],[Monitor_Type_Group]

print 'Loading SiteScope data'

insert into stage_monitoring.dbo.stg_monitoring_daily_universal_time
(
	sum_uptimeDuration,
	sum_downtimeDurationWithSupression,
	sum_downtimeDurationWithoutSupression,
	avg_uptimePercentageWithSupression,
	avg_uptimePercentageWithoutSupression,
	row_key, device_number, device_source,
	[Account_Name],[Account_Number],[Account_Type],[Device_Host_Name],[Device_Status],[Monitor_Type],
    [Monitor_Name],[Monitor_Silo],[Source_System_Name],[Monitor_ID],[Account_Source], [Monitor_Type_Group]
)
SELECT 
	sum(uptimeDuration),
	sum(downtimeDurationWithSupression),
	sum(downtimeDurationWithoutSupression),
	avg(uptimePercentageWithSupression),
	avg(uptimePercentageWithoutSupression),
	row_key,device_number, device_source,
	[Account_Name],[Account_Number],[Account_Type],[Device_Host_Name],[Device_Status],[Monitor_Type],
    [Monitor_Name],[Monitor_Silo],[Source_System_Name],[Monitor_ID],[Account_Source],[Monitor_Type_Group]
FROM 
	stage_three_dw.dbo.Report_Monitoring_Availability_Hourly with (nolock) 
where
    (id between @min_sitescope_id and @max_sitescope_id )
	and (Time_Full_Date_HMS  between   @begin_date	and  @end_date) 
	and Source_System_Name = 'SiteScope'
	--(Time_Full_Date_HMS  between   @begin_date	and  @end_date) 
	and Account_Source='Salesforce'
group by
	row_key, device_number, device_source, 
	[Account_Name],[Account_Number],[Account_Type],[Device_Host_Name],[Device_Status],[Monitor_Type],
    [Monitor_Name],[Monitor_Silo],[Source_System_Name],[Monitor_ID],[Account_Source],[Monitor_Type_Group]


	print 'Loading SiteScope data'

insert into stage_monitoring.dbo.stg_monitoring_daily_universal_time
(
	sum_uptimeDuration,
	sum_downtimeDurationWithSupression,
	sum_downtimeDurationWithoutSupression,
	avg_uptimePercentageWithSupression,
	avg_uptimePercentageWithoutSupression,
	row_key, device_number, device_source,
	[Account_Name],[Account_Number],[Account_Type],[Device_Host_Name],[Device_Status],[Monitor_Type],
    [Monitor_Name],[Monitor_Silo],[Source_System_Name],[Monitor_ID],[Account_Source], [Monitor_Type_Group]
)
SELECT 
	sum(uptimeDuration),
	sum(downtimeDurationWithSupression),
	sum(downtimeDurationWithoutSupression),
	avg(uptimePercentageWithSupression),
	avg(uptimePercentageWithoutSupression),
	row_key,device_number, device_source,
	[Account_Name],[Account_Number],[Account_Type],[Device_Host_Name],[Device_Status],[Monitor_Type],
    [Monitor_Name],[Monitor_Silo],[Source_System_Name],[Monitor_ID],[Account_Source],[Monitor_Type_Group]
FROM 
	stage_three_dw.dbo.Report_Monitoring_Availability_Hourly with (nolock) 
where
    (id between @min_rackwatch_id and @max_rackwatch_id )
	and (Time_Full_Date_HMS  between   @begin_date	and  @end_date) 
	and Source_System_Name = 'RackWatch'
	--(Time_Full_Date_HMS  between   @begin_date	and  @end_date) 
	and Account_Source='Salesforce'
group by
	row_key, device_number, device_source, 
	[Account_Name],[Account_Number],[Account_Type],[Device_Host_Name],[Device_Status],[Monitor_Type],
    [Monitor_Name],[Monitor_Silo],[Source_System_Name],[Monitor_ID],[Account_Source],[Monitor_Type_Group]



print 'Loading NewRelic data'

insert into stage_monitoring.dbo.stg_monitoring_daily_universal_time
(
	sum_uptimeDuration,
	sum_downtimeDurationWithSupression,
	sum_downtimeDurationWithoutSupression,
	avg_uptimePercentageWithSupression,
	avg_uptimePercentageWithoutSupression,
	row_key, device_number, device_source,
	[Account_Name],[Account_Number],[Account_Type],[Device_Host_Name],[Device_Status],[Monitor_Type],
    [Monitor_Name],[Monitor_Silo],[Source_System_Name],[Monitor_ID],[Account_Source], [Monitor_Type_Group]
)
SELECT 
	sum(uptimeDuration),
	sum(downtimeDurationWithSupression),
	sum(downtimeDurationWithoutSupression),
	avg(uptimePercentageWithSupression),
	avg(uptimePercentageWithoutSupression),
	row_key,device_number, device_source,
	[Account_Name],[Account_Number],[Account_Type],[Device_Host_Name],[Device_Status],[Monitor_Type],
    [Monitor_Name],[Monitor_Silo],[Source_System_Name],[Monitor_ID],[Account_Source],[Monitor_Type_Group]
FROM 
	stage_three_dw.dbo.Report_Monitoring_Availability_Hourly with (nolock) 
where
    (id between @min_newrelic_id and @max_newrelic_id )
	and (Time_Full_Date_HMS  between   @begin_date	and  @end_date) 
	and Source_System_Name = 'NewRelic'
	and Account_Source='Salesforce'
group by
	row_key, device_number, device_source, 
	[Account_Name],[Account_Number],[Account_Type],[Device_Host_Name],[Device_Status],[Monitor_Type],
    [Monitor_Name],[Monitor_Silo],[Source_System_Name],[Monitor_ID],[Account_Source],[Monitor_Type_Group]
--return


update  a
set a.Monitor_Type=b.Monitor_type , a.Monitor_Name= b.Monitor_Name
from stage_monitoring.dbo.stg_monitoring_daily_universal_time a, stage_three_DW.dbo.dim_monitor b 
where a.Monitor_ID			=	b.monitor_id_nk	
		and a.Monitor_silo =b.Monitor_Silo 
		and a.source_system_name=(case
										when b.Source_System_Name = 'RADAR' then 'sitescope'
										 else
											b.Source_System_Name
										 end)									
				
		and b.Current_Record_Flag	=	1
--exec (@sql)

--alter index IDX_ROW_KEY  on stage_monitoring.dbo.stg_monitoring_daily_universal_time REBUILD	
--alter index IDX_Device  on stage_monitoring.dbo.stg_monitoring_daily_universal_time REBUILD	

set @run_id = (select max(id) from stage_monitoring.dbo.ref_audit_daily_agg)


update a
	set
		a.step_1_stage_agg = GETDATE()
from
	stage_monitoring.dbo.ref_audit_daily_agg a
where
	a.id = @run_id 
	

-----------------------------------------------------------------------------
--step 2 get row_keys
-----------------------------------------------------------------------------
--truncate table stage_monitoring.dbo.ref_row_key_hold_NULL

--insert into stage_monitoring.dbo.ref_row_key_hold_NULL
--(
--	row_key,
--	row_key_string
--)
--select
--	distinct 
--	row_key,
--	replace(replace(replace(row_key,']:[',','),'[',''),']','')
--from
--	stage_monitoring.dbo.stg_monitoring_daily_universal_time


--update a
--	set
--		a.step_2_get_row_keys = GETDATE()
--from
--	stage_monitoring.dbo.ref_audit_daily_agg a
--where
--	a.id = @run_id 

----------------------------------------------------
--row_key and row_key_string inserted into hold table
--process row_key_string / parse process to ref_row_key lookup table
----------------------------------------------------
--truncate table stage_monitoring.dbo.ref_row_key_NULL

--;with
--cteSplit AS
--(
--select
--	a.row_key,
--	row_number() over (partition by a.row_key order by t.N)	as row_N,
--	substring(',' + a.row_key_string, N+1, charindex(',', a.row_key_string + ',', N)-N) as SplitValue
--from
--	stage_monitoring.dbo.tally t

--	cross join stage_monitoring.dbo.ref_row_key_hold_NULL a
--WHERE 
--	t.N <= LEN(',' + a.row_key_string)
--AND SUBSTRING(',' + a.row_key_string, N, 1) = ','
--)
--insert into stage_monitoring.dbo.ref_row_key_NULL
--(
--	[row_key]
--	,[Instance_NK]
--	,[Instance_Source_System]
--	,[Monitor_NK]
--	,[Monitor_Type]
--	,[Monitor_Silo]
--	,[Monitor_Source_System]
--)
-- SELECT s.row_key,
--		MAX(CASE WHEN s.row_N = 1 THEN isnull(SplitValue,'N/A') END) AS instance_nk,
--		MAX(CASE WHEN s.row_N = 2 THEN isnull(SplitValue,'N/A') END) AS instance_source_system,
--		MAX(CASE WHEN s.row_N = 3 THEN isnull(SplitValue,'N/A') END) AS monitor_nk,
--		MAX(CASE WHEN s.row_N = 4 THEN isnull(SplitValue,'N/A') END) AS monitor_type,
--		MAX(CASE WHEN s.row_N = 5 THEN isnull(SplitValue,'N/A') END) AS monitor_silo,
--		MAX(CASE WHEN s.row_N = 6 THEN isnull(SplitValue,'N/A') END) AS monitor_source_system
--FROM 
--	cteSplit s
--GROUP BY 
--	s.row_key


update a
	set
		a.atep_3_parse_row_key = GETDATE()
from
	stage_monitoring.dbo.ref_audit_daily_agg a
where
	a.id = @run_id 
		
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
truncate table stage_monitoring.dbo.stg_report_monitoring_availability_daily_universal_time
insert into stage_monitoring.dbo.stg_report_monitoring_availability_daily_universal_time
(
	[Time_KEY]
	,[Time_Full_Date]
	,[Time_Month_Number]
	,[Time_Month_Desc]
	,[Time_Month_Abbr]
	,[Time_Year_Number]
	,[Time_Quarter_Number]
	,[Time_Full_Date_HMS]
	,[Account_Name]
	,[Account_Number]
	,[Account_Type]
	,[Device_Host_Name]
	,[Device_Number]
	,[Device_Status]
	,[Monitor_Type]
	,[Monitor_Name]
	,[Monitor_Silo]
	,[Source_System_Name]
	,[uptimeDuration]
	,[downtimeDurationWithSupression]
	,[downtimeDurationWithoutSupression]
	,[uptimePercentageWithSupression]
	,[uptimePercentageWithoutSupression]
	,[row_key]
	,[Monitor_ID]
	,[Device_Source]
	,[Account_Source]
	,[etl_date]
	,[Monitor_Type_Group]
)
select
	c.Time_KEY,
	c.Time_Full_Date,
	c.Time_Month_Number,
	c.Time_Month_Desc,
	c.Time_Month_Abbr,
	c.Time_Year_Number,
	c.Time_Quarter_Number,
	@utc_start_date,
	z.Account_Name,
	z.Account_Number,
	z.Account_Type,
	z.Device_Host_Name,
	z.Device_Number,
	z.Device_Status,
	z.monitor_type,
	z.Monitor_Name,
	z.Monitor_Silo,
	z.Source_System_Name,
	z.sum_uptimeDuration,
	z.sum_downtimeDurationWithSupression,
	z.sum_downtimeDurationWithoutSupression,
	z.avg_uptimePercentageWithSupression,
	z.avg_uptimePercentageWithoutSupression,
	z.row_key,
	z.Monitor_ID,					
	z.Device_Source,		
	z.account_source,
	@etl_Date,
	 z.Monitor_Type_Group
		

from	
	stage_monitoring.dbo.stg_monitoring_daily_universal_time z
	
	
	inner join stage_three_dw.dbo.Dim_Time c
		on c.Time_Full_Date		=	@utc_start_date

	
--insert into stage_monitoring.dbo.stg_report_monitoring_availability_daily_universal_time
--(
--	[Time_KEY]
--	,[Time_Full_Date]
--	,[Time_Month_Number]
--	,[Time_Month_Desc]
--	,[Time_Month_Abbr]
--	,[Time_Year_Number]
--	,[Time_Quarter_Number]
--	,[Time_Full_Date_HMS]
--	,[Account_Name]
--	,[Account_Number]
--	,[Account_Type]
--	,[Device_Host_Name]
--	,[Device_Number]
--	,[Device_Status]
--	,[Monitor_Type]
--	,[Monitor_Name]
--	,[Monitor_Silo]
--	,[Source_System_Name]
--	,[uptimeDuration]
--	,[downtimeDurationWithSupression]
--	,[downtimeDurationWithoutSupression]
--	,[uptimePercentageWithSupression]
--	,[uptimePercentageWithoutSupression]
--	,[row_key]
--	,[Monitor_ID]
--	,[Device_Source]
--	,[Account_Source]
--	,[etl_date]
--	,[Monitor_Type_Group]
--)
--select
--	c.Time_KEY,
--	c.Time_Full_Date,
--	c.Time_Month_Number,
--	c.Time_Month_Desc,
--	c.Time_Month_Abbr,
--	c.Time_Year_Number,
--	c.Time_Quarter_Number,
--	@utc_start_date,
--	z.Account_Name,
--	z.Account_Number,
--	z.Account_Type,
--	z.Device_Host_Name,
--	z.Device_Number,
--	z.Device_Status,
--	case
--		when x.monitor_type = 'web' then 'URL'
--		else
--			x.monitor_type
--	end				as monitor_type,
--	x.Monitor_Name,
--	x.Monitor_Silo,
--	x.Source_System_Name,
--	a.uptimeDuration,
--	a.downtimeDurationWithSupression,
--	a.downtimeDurationWithoutSupression,
--	a.uptimePercentageWithSupression,
--	a.uptimePercentageWithoutSupression,
--	a.row_key,
--	x.Monitor_ID_NK,					
--	a.Device_Source,		
--	z.account_source_system_name,
--	@etl_Date,
--	(
--	case
--		--DEFINE PING
--		when 
--				x.MONITOR_TYPE = 'Ping' 
--			OR 
--				x.MONITOR_TYPE = 'ICMP'  
--			--OR 
--			--	zzz.monitor_nk = 'PING'
--			then 'PING'
		
--		--DEFINE PORT
--		when
--		((x.Monitor_Type IN ('FTP','ColdFusion','Cold Fusion','PostgreSQL','MS SQL Server','Webport','SQLServer','HTTP',
--							'IMAP','POP3','Telnet','SMTP','Postgres','HTTPS','MySQL','DNS','SSH') 
--		AND x.source_system_name= 'rackwatch')
--		OR
--		(x.monitor_type in ('Protocol','Ping','FTPGet','ColdFusion','DNS','Protocol')
--		and x.source_system_name = 'sitescope')
--		OR
--		(x.monitor_type in ('TCP','SMTP','SMTP-BANNER','SSH','NETWORK','MYSQL','DNS','APACHE')
--		and x.source_system_name = 'maas'))
--			then 'PORT'

--		--DEFINE URL
--		when
--		((x.monitor_type in ('http','web','URL')
--		and x.source_system_name = 'sitescope')
--		OR
--		(x.monitor_type in ('URL')
--		and x.source_system_name = 'MaaS'))
--			then 'URL'

--		else
--			'other'
--	end	
--	)as Monitor_Type_Group
		

--from	
--	stage_monitoring.dbo.stg_monitoring_daily_universal_time a
	
--	inner join stage_monitoring.dbo.ref_row_key_NULL zzz
--		on a.row_key = zzz.row_key

--	inner join stage_three_dw.dbo.Dim_Time c
--		on c.Time_Full_Date		=	@utc_start_date

--	--join with dim_monitor
--	left join stage_three_dw.dbo.dim_monitor x
--		on	x.monitor_id_nk			=	zzz.monitor_nk
--		and x.monitor_silo			=	zzz.monitor_silo
--		and x.source_system_name    =	zzz.Monitor_Source_System --(case
--		--									when zzz.Monitor_Source_System = 'sitescope' then 'RADAR'
--		--								 else
--		--									zzz.Monitor_Source_System 
--		--								 end)	
--		--on a.row_key=x.monitor_id	
--		and x.Current_Record_Flag	=	1
		
		
--	left join stage_monitoring.dbo.report_account_device_daily z
--		on	a.device_number				= z.device_number
--		and a.device_source	= z.device_source_system_name




update a
	set
		a.step_4_stg_with_attributes = GETDATE()
from
	stage_monitoring.dbo.ref_audit_daily_agg a
where
	a.id = @run_id 


----------------------------------------------------------------------------------
----insert into central time daily
----------------------------------------------------------------------------------		
--insert into stage_three_dw.dbo.report_monitoring_availability_daily_universal_time
insert into stage_three_dw.dbo.report_monitoring_availability_daily
(
	--[id]
	--,
	[Time_KEY]
	,[Time_Full_Date]
	,[Time_Month_Number]
	,[Time_Month_Desc]
	,[Time_Month_Abbr]
	,[Time_Year_Number]
	,[Time_Quarter_Number]
	,[Time_Full_Date_HMS]
	,[Account_Name]
	,[Account_Number]
	,[Account_Type]
	,[Device_Host_Name]
	,[Device_Number]
	,[Device_Status]
	,[Monitor_Type]
	,[Monitor_Name]
	,[Monitor_Silo]
	,[Monitor_Type_Group]
	,[Source_System_Name]
	,[uptimeDuration]
	,[downtimeDurationWithSupression]
	,[downtimeDurationWithoutSupression]
	,[uptimePercentageWithSupression]
	,[uptimePercentageWithoutSupression]
	,[row_key]
	,[Monitor_ID]
	,[Device_Source]
	,[Account_Source]
	,[etl_date]
)
SELECT 
	[Time_KEY]
	,[Time_Full_Date]
	,[Time_Month_Number]
	,[Time_Month_Desc]
	,[Time_Month_Abbr]
	,[Time_Year_Number]
	,[Time_Quarter_Number]
	,[Time_Full_Date_HMS]
	,[Account_Name]
	,[Account_Number]
	,[Account_Type]
	,[Device_Host_Name]
	,[Device_Number]
	,[Device_Status]
	,[Monitor_Type]
	,[Monitor_Name]
	,[Monitor_Silo]
	,[Monitor_Type_Group]
	,[Source_System_Name]
	,[uptimeDuration]
	,[downtimeDurationWithSupression]
	,[downtimeDurationWithoutSupression]
	,[uptimePercentageWithSupression]
	,[uptimePercentageWithoutSupression]
	,[row_key]
	,[Monitor_ID]
	,[Device_Source]
	,[Account_Source]
	,[etl_date]
FROM 
	[Stage_Monitoring].[dbo].[stg_report_monitoring_availability_daily_universal_time]
	


update a
	set
		a.step_5_insert_to_production = GETDATE()
from
	stage_monitoring.dbo.ref_audit_daily_agg a
where
	a.id = @run_id 


----------------------------------------------------------------------------------
----calculate times
----------------------------------------------------------------------------------	

update a	
	set
		a.duration_1		=	DATEDIFF(ss,begin_process,step_1_stage_agg)
	,	a.duration_2		=	DATEDIFF(ss,step_1_stage_agg,step_2_get_row_keys )
	,	a.duration_3		=	DATEDIFF(SS,step_2_get_row_keys,atep_3_parse_row_key )
	,	a.duration_4		=	DATEDIFF(ss,atep_3_parse_row_key,step_4_stg_with_attributes )
	,	a.duration_5		=	DATEDIFF(ss,step_4_stg_with_attributes,step_5_insert_to_production )
	,	a.total_row_count	=	(select COUNT(*) from stage_monitoring.dbo.stg_report_monitoring_availability_daily_universal_time)
from
	stage_monitoring.dbo.ref_audit_daily_agg a
where
	a.id = @run_id 


--alter index IDX_ROW_KEY  on stage_monitoring.dbo.stg_monitoring_daily_universal_time  DISABLE
--alter index IDX_Device  on stage_monitoring.dbo.stg_monitoring_daily_universal_time DISABLE
end

set @utc_start_date		= (select DATEADD(day,1,@utc_start_date))
print @utc_start_date

end




GO


