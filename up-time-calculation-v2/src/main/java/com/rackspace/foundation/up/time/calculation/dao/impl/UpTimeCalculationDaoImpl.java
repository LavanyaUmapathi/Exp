package com.rackspace.foundation.up.time.calculation.dao.impl;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.spring.HectorTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import org.apache.log4j.Logger;

import com.rackspace.foundation.cassandra.service.ElasticCluster;
import com.rackspace.foundation.cassandra.service.ClusterBuilder;
import com.rackspace.foundation.cassandra.dao.query.vo.MultiGetSuperSliceQuery;
import com.rackspace.foundation.cassandra.dao.query.vo.RangeSliceQuery;
import com.rackspace.foundation.cassandra.dao.query.vo.SuperColumnRangeSliceQuery;
import com.rackspace.foundation.cassandra.dao.query.vo.SuperColumnRow;
import com.rackspace.foundation.cassandra.dao.query.vo.SuperColumnWithSuperColumnRow;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;

public class UpTimeCalculationDaoImpl {

	private static final String NEXT_START_TIME_STAMP = "\"NextStartTimeStamp\"";
	private static final String RUN_FLAG = "RunFlag";
	private static final String ENABLED_MONITORS_SUPER = "\"EnabledMonitorsSuper\"";
	private static final String POLL_STATE_CHANGES = "PollStateChanges";
	private static final String SUPRESSION_INTERVALS = "\"SupressionIntervals\"";
	private static final String POLL_STATE_ERRORS = "\"PollStateErrors\"";
	private static final String UP_TIME_CALCULATION_SUPER = "\"UpTimeCalculationSuper\"";
	private static final String UP_TIME_CALCULATION__DAILY_SUPER = "UpTimeCalculationDailySuper";
	private static final String STATUS = "Status";
	private static final String MONITOR_STATUS = "MonitorStatus";
	private static final String START_TIME_STAMP = "StartTimeStamp";
	private static final String START_TIME = "StartTime";
	private static final String SCHEDULING = "Scheduling";
	private static final String END_TIME = "EndTime";
	private static final String UP_TIME_CALC_TIME_STAMP = "\"UpTimeCalcTimeStamp\"";
	private static final String UP_TIME_CALC_DATA = "\"UpTimeCalcData\"";
	private static final String INSTANCE_STATUS_SUPER = "InstanceStatusSuper";
	private static final String KEY_COLUMN_NAME = "key";
	private static final String COLUMN1_NAME = "column1";
	private static final String COLUMN2_NAME = "column2";
	private static final String VALUE_NAME = "value";
	private static final String POLL_DURATION = "PollDuration";
	private static final String CATEGORY_NAME = "Category";
	private static final String ERROR_NAME = "Error";
	private static final String SUPRESSION_DURATION = "SupressionDuration";
	private static final String SUPRESSION_SOURCE = "SupressionSource";
	private static final int POLL_RECORDS_BATCH = 10000;
	private static final int SUPRESSION_RECORDS_BATCH = 5000;
	private static final String SUPRESSION_DEFAULT_SOURCE = "Default";
	private ElasticCluster cluster;
	private String clusterName;
	private List<String> seeds;

	private Cluster cqlCluster;

	private Session session;

	private static org.apache.log4j.Logger log = Logger
			.getLogger(UpTimeCalculationDaoImpl.class);

	public String getEndTimeStamp(String sourceSystem) {

		String cmd = "select " + this.END_TIME + " from "
				+ this.UP_TIME_CALC_DATA + "." + this.UP_TIME_CALC_TIME_STAMP
				+ " where key='" + sourceSystem + "'";
		List<Row> rSet = this.session.execute(cmd).all();
		return (rSet.get(0).getString(this.END_TIME));

		// HectorTemplate hTemplate = this.cluster
		// .getHectorTemplate(UP_TIME_CALC_DATA);
		// SliceQuery<String, String, String> getNextStartTSQuery = hTemplate
		// .createSliceQuery(StringSerializer.get(),
		// StringSerializer.get(), StringSerializer.get());
		//
		// getNextStartTSQuery.setColumnFamily(UP_TIME_CALC_TIME_STAMP);
		// getNextStartTSQuery.setKey(sourceSystem);
		// getNextStartTSQuery.setColumnNames(END_TIME);
		// QueryResult<ColumnSlice<String, String>> nextStartTS =
		// getNextStartTSQuery
		// .execute();
		// ColumnSlice<String, String> columnSlice = nextStartTS.get();
		//
		// HColumn<String, String> startTimeColumn = columnSlice
		// .getColumnByName(END_TIME);
		//
		// return startTimeColumn.getValue();
	}

	// public String getSchedulingFlag(String processName) {
	//
	// HectorTemplate hTemplate = this.cluster
	// .getHectorTemplate(UP_TIME_CALC_DATA);
	// SliceQuery<String, String, String> getNextStartTSQuery = hTemplate
	// .createSliceQuery(StringSerializer.get(),
	// StringSerializer.get(), StringSerializer.get());
	//
	// getNextStartTSQuery.setColumnFamily(SCHEDULING);
	// getNextStartTSQuery.setKey(processName);
	// getNextStartTSQuery.setColumnNames(RUN_FLAG);
	// QueryResult<ColumnSlice<String, String>> nextStartTS =
	// getNextStartTSQuery
	// .execute();
	// ColumnSlice<String, String> columnSlice = nextStartTS.get();
	//
	// HColumn<String, String> runFlagColumn = columnSlice
	// .getColumnByName(RUN_FLAG);
	// return (runFlagColumn.getValue());
	//
	// }
	//
	// public void setRunFlag(String processName, String flagValue) {
	//
	// ThriftColumnFamilyTemplate<String, String> schedulingTemplate =
	// this.cluster
	// .getStandardColumnFamilyTemplate(UP_TIME_CALC_DATA, SCHEDULING);
	// Mutator<String> updateSchedulingFlag = schedulingTemplate
	// .createMutator();
	//
	// updateSchedulingFlag.addInsertion(processName, SCHEDULING,
	// HFactory.createStringColumn(RUN_FLAG, flagValue));
	// updateSchedulingFlag.execute();
	// return;
	//
	// }

	public void setNextStartTime(String nextStartTime, String sourceSystem) {
		log.debug("Updating next time stamp to:" + nextStartTime);
		String cmd = "insert into " + this.UP_TIME_CALC_DATA + "."
				+ this.NEXT_START_TIME_STAMP + " (" + this.KEY_COLUMN_NAME
				+ "," + this.START_TIME + ") values ('" + sourceSystem + "', '"
				+ nextStartTime + "')";

		log.info(cmd);
		this.session.execute(cmd);

		// ThriftColumnFamilyTemplate<String, String> nextTimeStampTemplate =
		// this.cluster
		// .getStandardColumnFamilyTemplate(UP_TIME_CALC_DATA,
		// NEXT_START_TIME_STAMP);
		// Mutator<String> updateNextTime =
		// nextTimeStampTemplate.createMutator();
		//
		// updateNextTime.addInsertion(sourceSystem, NEXT_START_TIME_STAMP,
		// HFactory.createStringColumn(START_TIME, nextStartTime));
		// updateNextTime.execute();
		// log.debug("Time stamp updated");

	}

	public void setNextUpTimeCalcTimeStamp(String nextTimeStamp,
			String sourceSystem) {

		String cmd = "insert into " + this.UP_TIME_CALC_DATA + "."
				+ this.UP_TIME_CALC_TIME_STAMP + " (" + this.KEY_COLUMN_NAME
				+ "," + this.END_TIME + ") values ('" + sourceSystem + "', '"
				+ nextTimeStamp + "')";

		log.info(cmd);
		this.session.execute(cmd);

		//
		// ThriftColumnFamilyTemplate<String, String> nextTimeStampTemplate =
		// this.cluster
		// .getStandardColumnFamilyTemplate(UP_TIME_CALC_DATA,
		// UP_TIME_CALC_TIME_STAMP);
		// Mutator<String> updateNextTime =
		// nextTimeStampTemplate.createMutator();
		//
		// updateNextTime.addInsertion(sourceSystem, UP_TIME_CALC_TIME_STAMP,
		// HFactory.createStringColumn(END_TIME, nextTimeStamp));
		// updateNextTime.execute();
		return;

	}

	public String getNextStartTimeStamp(String sourceSystem) {
		String cmd = "select " + this.START_TIME + " from "
				+ this.UP_TIME_CALC_DATA + "." + this.NEXT_START_TIME_STAMP
				+ " where key='" + sourceSystem + "'";
		List<Row> rSet = this.session.execute(cmd).all();
		return (rSet.get(0).getString(this.START_TIME));

		// HectorTemplate hTemplate = this.cluster
		// .getHectorTemplate(UP_TIME_CALC_DATA);
		// SliceQuery<String, String, String> getNextStartTSQuery = hTemplate
		// .createSliceQuery(StringSerializer.get(),
		// StringSerializer.get(), StringSerializer.get());
		//
		// getNextStartTSQuery.setColumnFamily(NEXT_START_TIME_STAMP);
		// getNextStartTSQuery.setKey(sourceSystem);
		// getNextStartTSQuery.setColumnNames(START_TIME);
		// QueryResult<ColumnSlice<String, String>> nextStartTS =
		// getNextStartTSQuery
		// .execute();
		// ColumnSlice<String, String> columnSlice = nextStartTS.get();
		//
		// HColumn<String, String> startTimeColumn = columnSlice
		// .getColumnByName(START_TIME);
		// String startTime = startTimeColumn.getValue();
		// if (startTime.compareTo("0") == 0) {
		// Calendar c = Calendar.getInstance();
		//
		// startTime = String.valueOf(c.getTimeInMillis() / 1000);
		// }
		//
		// return startTime;
	}

	// public List<String> getAllPollStateChangeKeys() {
	//
	// RangeSliceQuery rangeSliceQuery = new RangeSliceQuery();
	// rangeSliceQuery.setElasticCluster(this.cluster);
	// rangeSliceQuery.setColumnFamilyName(POLL_STATE_CHANGES);
	// rangeSliceQuery.setKeyspaceName(UP_TIME_CALC_DATA);
	//
	// return rangeSliceQuery.getKeysOnly();
	// }

	// public List<String> getAllPollStateErrorsKeys() {
	//
	// RangeSliceQuery rangeSliceQuery = new RangeSliceQuery();
	// rangeSliceQuery.setElasticCluster(this.cluster);
	// rangeSliceQuery.setColumnFamilyName(POLL_STATE_ERRORS);
	// rangeSliceQuery.setKeyspaceName(UP_TIME_CALC_DATA);
	//
	// return rangeSliceQuery.getKeysOnly();
	// }
	public List<String> getAllPollStateErrorsKeys() {
		List<String> errorKeys = new ArrayList<String>();

		String sqlCmd = " SELECT distinct " + this.KEY_COLUMN_NAME + " from "
				+ this.UP_TIME_CALC_DATA + "." + this.POLL_STATE_ERRORS;
		log.info(sqlCmd);
		ResultSet rSet = this.session.execute(sqlCmd);

		Iterator<Row> rSetIterator = rSet.iterator();
		int i = 0;
		while (rSetIterator.hasNext()) {

			errorKeys.add(rSetIterator.next().getString(this.KEY_COLUMN_NAME));

		}

		log.debug("getAppPollStateErrorsKey returning result size:"
				+ errorKeys.size());
		return errorKeys;
	}

	// public List<String> getAllUpTimeCalcKeys() {
	//
	// RangeSliceQuery rangeSliceQuery = new RangeSliceQuery();
	// rangeSliceQuery.setElasticCluster(this.cluster);
	// rangeSliceQuery.setColumnFamilyName(UP_TIME_CALCULATION_SUPER);
	// rangeSliceQuery.setKeyspaceName(UP_TIME_CALC_DATA);
	//
	// return rangeSliceQuery.getKeysOnly();
	// }

	// public List<String> getLastEnabledMonitors(String sourceSystem)
	// throws Exception {
	//
	// log.debug("In get Last Enabled Monitors");
	//
	// SuperColumnRangeSliceQuery query = new SuperColumnRangeSliceQuery();
	// query.setElasticCluster(this.cluster);
	// query.setColumnFamilyName(ENABLED_MONITORS_SUPER);
	// query.setKeyspaceName(UP_TIME_CALC_DATA);
	//
	// query.setOneKey(sourceSystem);
	// query.setReverseResultOrder(true);
	// query.setStartSuperColumn(String.valueOf(Calendar.getInstance()
	// .getTimeInMillis() / 1000));
	// query.setEndSuperColumn(String.valueOf(0));
	//
	// Map<String, SuperColumnWithSuperColumnRow> queryResult = query
	// .getPreviousSupercolumn().getQueryResult();
	// long currentTimeStamp = 0;
	// List<String> allEnabledMonitors = new ArrayList<String>();
	//
	// if (!queryResult.isEmpty()) {
	// Map<String, SuperColumnRow> oneSuperColumnResult = queryResult.get(
	// sourceSystem).getSuperColumnWithSuperColumnRow();
	// for (String superColumn : oneSuperColumnResult.keySet()) {
	// long currentStartTime = Long.valueOf(superColumn).longValue();
	// if (currentStartTime > currentTimeStamp) {
	// allEnabledMonitors = null;
	// allEnabledMonitors = new ArrayList<String>();
	// allEnabledMonitors.addAll(oneSuperColumnResult
	// .get(superColumn).getRowColumns().keySet());
	// currentTimeStamp = currentStartTime;
	// }
	// }
	// }
	//
	// log.debug("Last Enabled monitors returning result:"
	// + allEnabledMonitors.size());
	// return allEnabledMonitors;
	//
	// }

	// public List<String> getLastEnabledMonitors(String startTS,
	// String sourceSystem) throws Exception {
	// log.info("In get Last Enabled Monitors. Start ts : " + startTS);
	//
	// SuperColumnRangeSliceQuery query = new SuperColumnRangeSliceQuery();
	// query.setElasticCluster(this.cluster);
	// query.setColumnFamilyName(ENABLED_MONITORS_SUPER);
	// query.setKeyspaceName(UP_TIME_CALC_DATA);
	//
	// query.setOneKey(sourceSystem);
	// query.setReverseResultOrder(true);
	// query.setStartSuperColumn(startTS);
	// query.setEndSuperColumn(String.valueOf(0));
	//
	// Map<String, SuperColumnWithSuperColumnRow> queryResult = query
	// .getPreviousSupercolumn().getQueryResult();
	// long currentTimeStamp = 0;
	// List<String> allEnabledMonitors = new ArrayList<String>();
	//
	// if (!queryResult.isEmpty()) {
	// Map<String, SuperColumnRow> oneSuperColumnResult = queryResult.get(
	// sourceSystem).getSuperColumnWithSuperColumnRow();
	// for (String superColumn : oneSuperColumnResult.keySet()) {
	// long currentStartTime = Long.valueOf(superColumn).longValue();
	// if (currentStartTime > currentTimeStamp) {
	// allEnabledMonitors = null;
	// allEnabledMonitors = new ArrayList<String>();
	// allEnabledMonitors.addAll(oneSuperColumnResult
	// .get(superColumn).getRowColumns().keySet());
	// currentTimeStamp = currentStartTime;
	// }
	// }
	// }
	//
	// log.debug("Last Enabled monitors returning result:" + startTS + ": "
	// + allEnabledMonitors.size());
	// return allEnabledMonitors;
	//
	// }

	public List<String> getLastEnabledMonitors(String startTS,
			String sourceSystem) throws Exception {
		log.info("In get Last Enabled Monitors. Start ts : " + startTS);
		List<String> allEnabledMonitors = new ArrayList<String>();

		String sqlCmd = " SELECT " + this.COLUMN1_NAME + " from "
				+ this.UP_TIME_CALC_DATA + "." + this.ENABLED_MONITORS_SUPER
				+ " where " + this.KEY_COLUMN_NAME + "='" + sourceSystem
				+ "' and " + this.COLUMN1_NAME + " < '" + startTS
				+ "' ORDER BY column1 DESC LIMIT 1";

		ResultSet rSet = this.session.execute(sqlCmd);
		if (rSet.iterator().hasNext()) {
			String maxTs = rSet.iterator().next().getString("column1");
			sqlCmd = "SELECT * from " + this.UP_TIME_CALC_DATA + "."
					+ this.ENABLED_MONITORS_SUPER + " where "
					+ this.KEY_COLUMN_NAME + " ='" + sourceSystem + "' and "
					+ this.COLUMN1_NAME + "='" + maxTs + "'";
			rSet = this.session.execute(sqlCmd);

			Iterator<Row> rSetIterator = rSet.iterator();
			while (rSetIterator.hasNext()) {
				allEnabledMonitors.add(rSetIterator.next().getString(
						this.COLUMN2_NAME));
			}
		}

		log.debug("Last Enabled monitors returning result:" + startTS + ": "
				+ allEnabledMonitors.size());
		return allEnabledMonitors;

	}

	public List<String> getLastEnabledMonitors(String sourceSystem)
			throws Exception {
		log.info("In get Last Enabled Monitors. ");
		List<String> allEnabledMonitors = new ArrayList<String>();

		String sqlCmd = " SELECT " + this.COLUMN1_NAME + " from "
				+ this.UP_TIME_CALC_DATA + "." + this.ENABLED_MONITORS_SUPER
				+ " where key='" + sourceSystem
				+ "' ORDER BY column1 DESC LIMIT 1";
		log.info(sqlCmd);
		ResultSet rSet = this.session.execute(sqlCmd);
		if (rSet.iterator().hasNext()) {
			String maxTs = rSet.iterator().next().getString(this.COLUMN1_NAME);
			sqlCmd = "SELECT * from " + this.UP_TIME_CALC_DATA + "."
					+ this.ENABLED_MONITORS_SUPER + " where key='"
					+ sourceSystem + "' and " + this.COLUMN1_NAME + " = '"
					+ maxTs + "'";
			log.info(sqlCmd);
			rSet = this.session.execute(sqlCmd);

			Iterator<Row> rSetIterator = rSet.iterator();
			while (rSetIterator.hasNext()) {
				allEnabledMonitors.add(rSetIterator.next().getString(
						this.COLUMN2_NAME));
			}
		}

		log.debug("Last Enabled monitors returning result:"
				+ allEnabledMonitors.size());
		return allEnabledMonitors;

	}

	// public List<String> getTimeFrameEnabledMonitors(String startTS,
	// String endTS, String sourceSystem) throws Exception {
	// log.info("In get TimeFrame  Enabled Monitors. Start ts : " + startTS
	// + " " + endTS + " " + sourceSystem);
	//
	// Set<String> uniqueMonitors = new TreeSet<String>();
	// List<String> allEnabledMonitors = new ArrayList<String>();
	// SuperColumnRangeSliceQuery query = new SuperColumnRangeSliceQuery();
	// query.setElasticCluster(this.cluster);
	// query.setColumnFamilyName(ENABLED_MONITORS_SUPER);
	// query.setKeyspaceName(UP_TIME_CALC_DATA);
	//
	// query.setOneKey(sourceSystem);
	// query.setReverseResultOrder(false);
	// query.setStartSuperColumn(startTS);
	// query.setEndSuperColumn(endTS);
	// Map<String, SuperColumnRow> queryResult = query.executeQuery()
	// .getQueryResult().get(sourceSystem)
	// .getSuperColumnWithSuperColumnRow();
	// if (!queryResult.isEmpty()) {
	//
	// Iterator<Entry<String, SuperColumnRow>> enabledMonitorsIterator =
	// queryResult
	// .entrySet().iterator();
	//
	// while (enabledMonitorsIterator.hasNext())
	//
	// {
	// uniqueMonitors.addAll(enabledMonitorsIterator.next().getValue()
	// .getRowColumns().keySet());
	//
	// }
	// }
	//
	// allEnabledMonitors.addAll(uniqueMonitors);
	// log.debug("Returning Last Enabled Monitors. "
	// + allEnabledMonitors.size());
	// return allEnabledMonitors;
	//
	// }

//	public Map<String, String> getPreviousSupressionInterval(String instanceId,
//			long startTimeStamp, long endTimeStamp) {
//		log.debug("Executing getPreviousSupressionInterval");
//
//		Map<String, String> supressionIntervalsResult = new LinkedHashMap<String, String>();
//
//		SuperColumnRangeSliceQuery query = new SuperColumnRangeSliceQuery();
//		query.setElasticCluster(this.cluster);
//		query.setColumnFamilyName(SUPRESSION_INTERVALS);
//		query.setKeyspaceName(UP_TIME_CALC_DATA);
//
//		query.setOneKey(instanceId);
//		query.setReverseResultOrder(true);
//		query.setStartSuperColumn(String.valueOf(startTimeStamp));
//		query.setEndSuperColumn(String.valueOf(endTimeStamp));
//
//		Map<String, SuperColumnWithSuperColumnRow> queryResult = query
//				.getPreviousSupercolumn().getQueryResult();
//		long lastTimeStamp = 0;
//		new ArrayList<String>();
//
//		if (!queryResult.isEmpty()) {
//			Map<String, SuperColumnRow> oneSuperColumnResult = queryResult.get(
//					instanceId).getSuperColumnWithSuperColumnRow();
//			for (String superColumn : oneSuperColumnResult.keySet()) {
//				long currentStartTime = Long.valueOf(superColumn).longValue();
//				if (currentStartTime > lastTimeStamp) {
//
//					Map<String, String> oneSupressionIntervalResult = new HashMap<String, String>();
//					oneSupressionIntervalResult.put(START_TIME_STAMP,
//							superColumn);
//
//					oneSupressionIntervalResult.putAll(oneSuperColumnResult
//							.get(superColumn).getRowColumns());
//
//					supressionIntervalsResult
//							.putAll(oneSupressionIntervalResult);
//					lastTimeStamp = currentStartTime;
//				}
//			}
//		}
//
//		return supressionIntervalsResult;
//
//	}

//	public Map<String, String> getPreviousPollStateError(String instanceId,
//			long startTimeStamp, long endTimeStamp)
//			throws InterruptedException, ExecutionException {
//		log.debug("Executing getPreviousPollStateError 2");
//
//		SuperColumnRangeSliceQuery query = new SuperColumnRangeSliceQuery();
//		query.setElasticCluster(cluster);
//		// query.setColumnFamilyName(POLL_STATE_ERRORS);
//		// query.setKeyspaceName(UP_TIME_CALC_DATA);
//		query.setColumnFamilyName("PollStateErrors");
//		query.setKeyspaceName("UpTimeCalcData");
//
//		query.setOneKey(instanceId);
//		query.setReverseResultOrder(true);
//		query.setStartSuperColumn(String.valueOf(startTimeStamp));
//		query.setEndSuperColumn(String.valueOf(endTimeStamp));
//
//		Map<String, SuperColumnWithSuperColumnRow> queryResult = query
//				.getPreviousSupercolumn().getQueryResult();
//		long lastTimeStamp = 0;
//		Map<String, String> previousPollStateErrorResult = new LinkedHashMap<String, String>();
//
//		if (!queryResult.isEmpty()) {
//			Map<String, SuperColumnRow> pollStateChanges = queryResult.get(
//					instanceId).getSuperColumnWithSuperColumnRow();
//			for (String oneSuperColumn : pollStateChanges.keySet()) {
//				long currentStartTime = Long.valueOf(oneSuperColumn)
//						.longValue();
//				if (currentStartTime > lastTimeStamp) {
//					Map<String, String> onePollStateError = new HashMap<String, String>();
//					onePollStateError.put(START_TIME_STAMP, oneSuperColumn);
//					onePollStateError.putAll(pollStateChanges.get(
//							oneSuperColumn).getRowColumns());
//					previousPollStateErrorResult.putAll(onePollStateError);
//					lastTimeStamp = currentStartTime;
//				}
//			}
//		}
//		return previousPollStateErrorResult;
//
//	}

	// public Map<String, String> getMonitorEnabledDisabledIntervals(
	// String monitorId) throws Exception {
	// log.debug("Executing getMonitorEnabledDisabledIntervals");
	//
	// SuperColumnRangeSliceQuery query = new SuperColumnRangeSliceQuery();
	//
	// query.setElasticCluster(this.cluster);
	// query.setColumnFamilyName(MONITOR_STATUS);
	// query.setKeyspaceName(UP_TIME_CALC_DATA);
	//
	// query.setOneKey(monitorId);
	// query.setReverseResultOrder(false);
	// query.setStartSuperColumn(String.valueOf(0));
	// query.setEndSuperColumn("9999999999");
	//
	// Map<String, SuperColumnWithSuperColumnRow> resultSets = query
	// .executeQuery().getQueryResult();
	//
	// SortedMap<String, String> monitorEnabledDisabledIntervals = new
	// TreeMap<String, String>();
	//
	// if (!resultSets.isEmpty()) {
	// Map<String, SuperColumnRow> monitorStatus = resultSets.get(
	// monitorId).getSuperColumnWithSuperColumnRow();
	// for (String superColumn : monitorStatus.keySet()) {
	// monitorEnabledDisabledIntervals.put(superColumn, monitorStatus
	// .get(superColumn).getRowColumns().get(STATUS));
	// }
	//
	// }
	//
	// return monitorEnabledDisabledIntervals;
	//
	// }

	// @SuppressWarnings("static-access")
	// public List<Map<String, String>> getDailyUpTimeCalc(String monitorId,
	// String startTimeStamp, String endTimeStamp)
	// throws InterruptedException, ExecutionException, Exception {
	// log.info("Get Daily Up time calc for :" + monitorId + " "
	// + startTimeStamp + " " + endTimeStamp);
	// List<Map<String, String>> upTimeCalcResult = new ArrayList<Map<String,
	// String>>();
	//
	// SuperColumnRangeSliceQuery query = new SuperColumnRangeSliceQuery();
	// query.setElasticCluster(cluster);
	// query.setColumnFamilyName(this.UP_TIME_CALCULATION__DAILY_SUPER);
	// query.setKeyspaceName(UP_TIME_CALC_DATA);
	//
	// query.setOneKey(monitorId);
	// query.setReverseResultOrder(false);
	// query.setStartSuperColumn(startTimeStamp);
	// query.setEndSuperColumn(endTimeStamp);
	// Map<String, SuperColumnWithSuperColumnRow> resultSet = query
	// .executeQuery().getQueryResult();
	//
	// if (!resultSet.isEmpty()) {
	// Map<String, SuperColumnRow> upTimeCalc = resultSet.get(monitorId)
	// .getSuperColumnWithSuperColumnRow();
	//
	// for (String oneSuperColumn : upTimeCalc.keySet()) {
	// Map<String, String> oneUpTimeCalcResult = new HashMap<String, String>();
	// oneUpTimeCalcResult.put(START_TIME_STAMP, oneSuperColumn);
	//
	// oneUpTimeCalcResult.putAll(upTimeCalc.get(oneSuperColumn)
	// .getRowColumns());
	// upTimeCalcResult.add(oneUpTimeCalcResult);
	// oneUpTimeCalcResult = null;
	//
	// }
	// }
	//
	// return upTimeCalcResult;
	// }

	// public List<Map<String, String>> getUpTimeCalc(String monitorId,
	// String startTimeStamp, String endTimeStamp)
	// throws InterruptedException, ExecutionException, Exception {
	// log.info("Get Up time calc for :" + monitorId + " " + startTimeStamp
	// + " " + endTimeStamp);
	// List<Map<String, String>> upTimeCalcResult = new ArrayList<Map<String,
	// String>>();
	//
	// SuperColumnRangeSliceQuery query = new SuperColumnRangeSliceQuery();
	// query.setElasticCluster(cluster);
	// query.setColumnFamilyName(UP_TIME_CALCULATION_SUPER);
	// query.setKeyspaceName(UP_TIME_CALC_DATA);
	//
	// query.setOneKey(monitorId);
	// query.setReverseResultOrder(false);
	// query.setStartSuperColumn(startTimeStamp);
	// query.setEndSuperColumn(endTimeStamp);
	// Map<String, SuperColumnWithSuperColumnRow> resultSet = query
	// .executeQuery().getQueryResult();
	//
	// if (!resultSet.isEmpty()) {
	// Map<String, SuperColumnRow> oneSuperColumnResult = resultSet.get(
	// monitorId).getSuperColumnWithSuperColumnRow();
	//
	// for (String oneSuperColumn : resultSet.keySet())
	//
	// {
	// Map<String, String> oneUpTimeCalcResult = new HashMap<String, String>();
	// oneUpTimeCalcResult.put(START_TIME_STAMP, oneSuperColumn);
	//
	// oneUpTimeCalcResult.putAll(oneSuperColumnResult.get(
	// oneSuperColumn).getRowColumns());
	// upTimeCalcResult.add(oneUpTimeCalcResult);
	// oneUpTimeCalcResult = null;
	//
	// }
	// }
	//
	// return upTimeCalcResult;
	// }

	public List<Map<String, String>> getPollStateErrors(String instanceId,
			long startTimeStamp, long endTimeStamp)
			throws InterruptedException, ExecutionException {

		String cmd = "Select * from " + this.UP_TIME_CALC_DATA + "."
				+ this.POLL_STATE_ERRORS + " where key ='" + instanceId
				+ "' and " + this.COLUMN1_NAME + ">'" + startTimeStamp
				+ "' and " + this.COLUMN1_NAME + "<'" + endTimeStamp + "'";
		log.info(cmd);
		List<Map<String, String>> pollStateErrorsResult = new ArrayList<Map<String, String>>();

		ResultSet rSet = this.session.execute(cmd);

		Iterator<Row> rSetIterator = rSet.iterator();

		String currentTs = "";
		String previousTs = "";
		Map<String, String> errorDetails = new HashMap<String, String>();
		while (rSetIterator.hasNext()) {
			Row oneRow = rSetIterator.next();
			currentTs = oneRow.getString(this.COLUMN1_NAME);
			if (currentTs.compareTo(previousTs) != 0) {
				if (!previousTs.isEmpty()) {
					pollStateErrorsResult.add(errorDetails);
					errorDetails = new HashMap<String, String>();
				}
				previousTs = currentTs;

			}
			errorDetails.put(START_TIME_STAMP, currentTs);
			errorDetails.put(oneRow.getString(this.COLUMN2_NAME),
					oneRow.getString(this.VALUE_NAME));

		}

		return pollStateErrorsResult;
	}

//	public List<Map<String, String>> getSupressionIntervals(String instanceId,
//			long startTimeStamp, long endTimeStamp)
//			throws InterruptedException, ExecutionException {
//
//		SuperColumnRangeSliceQuery query = new SuperColumnRangeSliceQuery();
//		query.setElasticCluster(this.cluster);
//		query.setColumnFamilyName(SUPRESSION_INTERVALS);
//		query.setKeyspaceName(UP_TIME_CALC_DATA);
//
//		query.setOneKey(instanceId);
//		query.setReverseResultOrder(false);
//		query.setStartSuperColumn(String.valueOf(startTimeStamp));
//		query.setEndSuperColumn(String.valueOf(endTimeStamp));
//		Map<String, SuperColumnWithSuperColumnRow> resultSet = query
//				.executeQuery().getQueryResult();
//
//		List<Map<String, String>> supressionIntervalsResult = new ArrayList<Map<String, String>>();
//
//		if (!resultSet.isEmpty()) {
//			Map<String, SuperColumnRow> supressionIntervals = resultSet.get(
//					instanceId).getSuperColumnWithSuperColumnRow();
//
//			for (String oneSuperColumn : supressionIntervals.keySet()) {
//				Map<String, String> oneSupressionIntervalResult = new HashMap<String, String>();
//				oneSupressionIntervalResult.put(START_TIME_STAMP,
//						oneSuperColumn);
//
//				oneSupressionIntervalResult.putAll(supressionIntervals.get(
//						oneSuperColumn).getRowColumns());
//				supressionIntervalsResult.add(oneSupressionIntervalResult);
//				oneSupressionIntervalResult = null;
//
//			}
//		}

//		return supressionIntervalsResult;
//	}

	// public Map<String, Map<String, Map<String, String>>> getAllSupression(
	// List<String> allDevices, long startTs, long endTs) throws Exception {
	//
	// MultiGetSuperSliceQuery query = new MultiGetSuperSliceQuery();
	// query.setElasticCluster(this.cluster);
	// query.setColumnFamilyName(SUPRESSION_INTERVALS);
	// query.setKeyspaceName(UP_TIME_CALC_DATA);
	// query.setKeys(allDevices);
	// query.setReverseResultOrder(false);
	// query.setStartSuperColumn(String.valueOf(startTs));
	// query.setEndSuperColumn(String.valueOf(endTs));
	//
	// Map<String, Map<String, Map<String, String>>> allSupressionsResult =
	// query
	// .executeQuery();
	//
	// log.debug("Collecting previus supressions. Current result size :"
	// + allSupressionsResult.size());
	//
	// MultiGetSuperSliceQuery query1 = new MultiGetSuperSliceQuery();
	// query1.setElasticCluster(cluster);
	// query1.setColumnFamilyName(SUPRESSION_INTERVALS);
	// query1.setKeyspaceName(UP_TIME_CALC_DATA);
	//
	// query1.setKeys(allDevices);
	// query1.setReverseResultOrder(true);
	// query1.setStartSuperColumn(String.valueOf(startTs - 1));
	// query1.setEndSuperColumn("0");
	// Map<String, Map<String, Map<String, String>>>
	// allPreviousSupressionsResult = query1
	// .getPreviousSuperColumn();
	//
	// log.debug("previous collected ");
	// Map<String, Map<String, Map<String, String>>>
	// validPreviousSupressionsResult = new LinkedHashMap<String, Map<String,
	// Map<String, String>>>();
	//
	// Iterator<Entry<String, Map<String, Map<String, String>>>>
	// allPreviousSupressionsResultIterator = allPreviousSupressionsResult
	// .entrySet().iterator();
	//
	// while (allPreviousSupressionsResultIterator.hasNext()) {
	// Entry<String, Map<String, Map<String, String>>> onePreviousRecord =
	// allPreviousSupressionsResultIterator
	// .next();
	//
	// String previousStratTime = onePreviousRecord.getValue().keySet()
	// .iterator().next();
	// String previousEndTime = onePreviousRecord.getValue()
	// .get(previousStratTime).get(END_TIME);
	//
	// if (Long.valueOf(previousEndTime) <= startTs
	// && Long.valueOf(previousEndTime) > 0) {
	//
	// } else {
	// if (Long.valueOf(previousStratTime) >= endTs) {
	//
	// } else {
	// validPreviousSupressionsResult.put(
	// onePreviousRecord.getKey(),
	// onePreviousRecord.getValue());
	//
	// }
	// }
	// }
	// log.debug("Valid size:" + validPreviousSupressionsResult.size());
	// Iterator<Entry<String, Map<String, Map<String, String>>>>
	// validPreviousSupressionsResultIterator = validPreviousSupressionsResult
	// .entrySet().iterator();
	//
	// while (validPreviousSupressionsResultIterator.hasNext()) {
	// Entry<String, Map<String, Map<String, String>>> onePrevious =
	// validPreviousSupressionsResultIterator
	// .next();
	//
	// String previousRec = onePrevious.getKey();
	// Map<String, Map<String, String>> supressionDetails = onePrevious
	// .getValue();
	//
	// Iterator<Entry<String, Map<String, Map<String, String>>>>
	// supressionResultIterator = allSupressionsResult
	// .entrySet().iterator();
	//
	// boolean hasErrors = false;
	// while (supressionResultIterator.hasNext()) {
	// Entry<String, Map<String, Map<String, String>>> oneSupressionEntry =
	// supressionResultIterator
	// .next();
	//
	// if (oneSupressionEntry.getKey().compareTo(previousRec) == 0) {
	// supressionResultIterator.remove();
	// Map<String, Map<String, String>> oneMonitorAllSupression = new
	// LinkedHashMap<String, Map<String, String>>();
	// oneMonitorAllSupression.putAll(supressionDetails);
	//
	// oneMonitorAllSupression.putAll(oneSupressionEntry
	// .getValue());
	//
	// Map<String, Map<String, Map<String, String>>> completeSupression = new
	// HashMap<String, Map<String, Map<String, String>>>();
	// completeSupression
	// .put(previousRec, oneMonitorAllSupression);
	//
	// allSupressionsResult.putAll(completeSupression);
	// hasErrors = true;
	// break;
	// }
	//
	// }
	// if (!hasErrors) {
	// allSupressionsResult.put(previousRec, supressionDetails);
	// }
	// }
	// log.debug("all previous collected. Result size is: "
	// + allSupressionsResult.size());
	//
	// return allSupressionsResult;
	//
	// }
	//
	// public Map<String, Map<String, Map<String, String>>> getAllSupression(
	// List<String> allDevices, long startTs, long endTs) throws Exception {
	//
	//
	// Map<String, Map<String, Map<String, String>>> allSupressions = new
	// HashMap<String, Map<String, Map<String, String>>>();
	//
	// StringBuilder sqlGetSuppressions = new StringBuilder();
	// sqlGetSuppressions.append("Select * from " + this.UP_TIME_CALC_DATA
	// + "." + this.SUPRESSION_INTERVALS + " where "
	// + this.KEY_COLUMN_NAME + " in (");
	// int i = 0;
	// for (String oneKey : allDevices) {
	// if (i > 0) {
	// sqlGetSuppressions.append(",");
	//
	// }
	// sqlGetSuppressions.append("'");
	// sqlGetSuppressions.append(oneKey);
	// sqlGetSuppressions.append("'");
	//
	//
	// i++;
	// if (i == this.SUPRESSION_RECORDS_BATCH) {
	// String previousSQL = sqlGetSuppressions.toString()
	// + ") and column1 < '" + startTs + "'";
	// sqlGetSuppressions.append(") and column1 >='" + startTs
	// + "' and column1 < '" + endTs + "'");
	// Map<String, Map<String, Map<String, String>>> currentSupressions = this
	// .getCompositeQueryResult(sqlGetSuppressions.toString());
	//
	// Map<String, Map<String, Map<String, String>>> previousSupressions = this
	// .findValidPreviousSupressions(previousSQL, startTs);
	//
	//
	// log.warn(this.SUPRESSION_RECORDS_BATCH+" records processed current supressions:"+currentSupressions.size()+" valid previous errors: "+previousSupressions.size());
	//
	// allSupressions.putAll(previousSupressions);
	// allSupressions.putAll(currentSupressions);
	//
	// sqlGetSuppressions = new StringBuilder();
	// sqlGetSuppressions.append("Select * from "
	// + this.UP_TIME_CALC_DATA + "." + this.SUPRESSION_INTERVALS
	// + " where key in (");
	// i = 0;
	// }
	// }
	// String previousSQL = sqlGetSuppressions.toString() + ") and column1 < '"
	// + startTs+"'";
	//
	// sqlGetSuppressions.append(") and column1 >='" + startTs
	// + "' and column1 < '" + endTs + "'");
	// Map<String, Map<String, Map<String, String>>> currentSupressions = this
	// .getCompositeQueryResult(sqlGetSuppressions.toString());
	//
	// Map<String, Map<String, Map<String, String>>> previousSupressions = this
	// .findValidPreviousSupressions(previousSQL, startTs);
	//
	// log.warn(this.SUPRESSION_INTERVALS+" records processed current supressions :"+currentSupressions.size()+" valid previous errors: "+previousSupressions.size());
	//
	//
	// allSupressions.putAll(currentSupressions);
	// allSupressions.putAll(currentSupressions);
	// log.warn("total errors:"+allSupressions.size());
	//
	// return allSupressions;
	//
	// }
	public Map<String, Map<String, Map<String, String>>> getAllSupression(
			List<String> allDevices, long startTs, long endTs) throws Exception {

		Map<String, Map<String, Map<String, String>>> allSupressions = new HashMap<String, Map<String, Map<String, String>>>();

		StringBuilder sqlGetSuppressions = new StringBuilder();
		sqlGetSuppressions.append("Select * from " + this.UP_TIME_CALC_DATA
				+ "." + this.SUPRESSION_INTERVALS + " where "
				+ this.KEY_COLUMN_NAME + " in (");
		int i = 0;
		for (String oneKey : allDevices) {
			if (i > 0) {
				sqlGetSuppressions.append(",");

			}
			sqlGetSuppressions.append("'");
			sqlGetSuppressions.append(oneKey);
			sqlGetSuppressions.append("'");

			i++;
			// //if (i == this.SUPRESSION_RECORDS_BATCH) {
			// String previousSQL = sqlGetSuppressions.toString()
			// + ") and column1 < '" + startTs + "'";
			// sqlGetSuppressions.append(") and column1 >='" + startTs
			// + "' and column1 < '" + endTs + "'");
			// Map<String, Map<String, Map<String, String>>> currentSupressions
			// = this
			// .getCompositeQueryResult(sqlGetSuppressions.toString());
			//
			// Map<String, Map<String, Map<String, String>>> previousSupressions
			// = this
			// .findValidPreviousSupressions(previousSQL, startTs);
			//
			//
			// log.warn(this.SUPRESSION_RECORDS_BATCH+" records processed current supressions:"+currentSupressions.size()+" valid previous errors: "+previousSupressions.size());
			//
			// allSupressions.putAll(previousSupressions);
			// allSupressions.putAll(currentSupressions);
			//
			// sqlGetSuppressions = new StringBuilder();
			// sqlGetSuppressions.append("Select * from "
			// + this.UP_TIME_CALC_DATA + "." + this.SUPRESSION_INTERVALS
			// + " where key in (");
			// i = 0;
			// }
		}
		// String previousSQL = sqlGetSuppressions.toString() +
		// ") and column1 < '"
		// + startTs+"'";

		sqlGetSuppressions.append(") and column1 >='" + startTs
				+ "' and column1 < '" + endTs + "'");
		Map<String, Map<String, Map<String, String>>> currentSupressions = this
				.getCompositeQueryResult(sqlGetSuppressions.toString());

		Map<String, Map<String, Map<String, String>>> previousSupressions = this
				.findValidPreviousSupressions(allDevices, startTs);

		log.warn(this.SUPRESSION_INTERVALS
				+ " records processed current supressions :"
				+ currentSupressions.size() + " valid previous errors: "
				+ previousSupressions.size());

		allSupressions.putAll(previousSupressions);
		allSupressions.putAll(currentSupressions);
		log.warn("total errors:" + allSupressions.size());

		return allSupressions;

	}

	public Map<String, Map<String, Map<String, String>>> getAllPollStateErrors(
			List<String> allActiveMonitors, long startTs, long endTs) {
		log.warn("Collecting all Errors for monticor count:"
				+ allActiveMonitors.size());
		int i = 0;
		Map<String, Map<String, Map<String, String>>> allPollStateErrorsResult = new HashMap<String, Map<String, Map<String, String>>>();
		List<String> getPrevious = new ArrayList<String>();
		StringBuilder sqlGetPollState = new StringBuilder();
		sqlGetPollState.append("Select * from " + this.UP_TIME_CALC_DATA + "."
				+ this.POLL_STATE_ERRORS + " where key in (");

		for (String oneKey : allActiveMonitors) {

			if (i > 0) {
				sqlGetPollState.append(",");

			}
			sqlGetPollState.append("'");
			sqlGetPollState.append(oneKey);
			sqlGetPollState.append("'");
			getPrevious.add(oneKey);
			i++;
			if (i == this.POLL_RECORDS_BATCH) {
				String previousSQL = sqlGetPollState.toString()
						+ ") and column1 < '" + startTs + "'";
				sqlGetPollState.append(") and column1 >='" + startTs
						+ "' and column1 < '" + endTs + "'");
				Map<String, Map<String, Map<String, String>>> currentPollErrors = this
						.getCompositeQueryResult(sqlGetPollState.toString());

				// Map<String, Map<String, Map<String, String>>>
				// previousPollErrors = this
				// .findValidPreviousErrors(previousSQL, startTs);
				Map<String, Map<String, Map<String, String>>> previousPollErrors = this
						.findValidPreviousErrors(getPrevious, startTs);

				log.warn(this.POLL_RECORDS_BATCH
						+ " records processed current Errors:"
						+ currentPollErrors.size() + " valid previous errors: "
						+ previousPollErrors.size());

				allPollStateErrorsResult.putAll(previousPollErrors);
				allPollStateErrorsResult.putAll(currentPollErrors);

				sqlGetPollState = new StringBuilder();
				sqlGetPollState.append("Select * from "
						+ this.UP_TIME_CALC_DATA + "." + this.POLL_STATE_ERRORS
						+ " where key in (");
				log.warn("Reset the list of monitors for previous errors");
				getPrevious = new ArrayList<String>();
				i = 0;
			}
		}
		String previousSQL = sqlGetPollState.toString() + ") and column1 < '"
				+ startTs + "'";

		sqlGetPollState.append(") and column1 >='" + startTs
				+ "' and column1 < '" + endTs + "'");
		Map<String, Map<String, Map<String, String>>> currentPollErrors = this
				.getCompositeQueryResult(sqlGetPollState.toString());

		Map<String, Map<String, Map<String, String>>> previousPollErrors = this
				.findValidPreviousErrors(getPrevious, startTs);

		log.warn(this.POLL_RECORDS_BATCH + " records processed current Errors:"
				+ currentPollErrors.size() + " valid previous errors: "
				+ previousPollErrors.size());

		allPollStateErrorsResult.putAll(previousPollErrors);
		allPollStateErrorsResult.putAll(currentPollErrors);
		log.warn("total errors:" + allPollStateErrorsResult.size());
		return allPollStateErrorsResult;
	}

	// public Map<String, Map<String, Map<String, String>>>
	// getAllPollStateErrors(
	// List<String> allActiveMonitors, long startTs, long endTs)
	// throws Exception {
	// log.debug("Inside get ALL Errors");
	//
	// MultiGetSuperSliceQuery query = new MultiGetSuperSliceQuery();
	//
	// query.setElasticCluster(cluster);
	// query.setColumnFamilyName(POLL_STATE_ERRORS);
	// query.setKeyspaceName(UP_TIME_CALC_DATA);
	// query.setColumnFamilyName("pollstateerrors");
	// query.setKeyspaceName("uptimecalcdata");
	//
	//
	// query.setKeys(allActiveMonitors);
	// query.setReverseResultOrder(false);
	// query.setStartSuperColumn(String.valueOf(startTs));
	// query.setEndSuperColumn(String.valueOf(endTs));
	// // query.setRowCount(allActiveMonitors.size());
	//
	// log.debug("Get all poll state errors query completed");
	// Map<String, Map<String, Map<String, String>>> allPollStateErrorsResult =
	// query
	// .executeQuery();
	//
	// log.debug("Collecting previus errors. Current result size :"
	// + allPollStateErrorsResult.size());
	//
	// MultiGetSuperSliceQuery query1 = new MultiGetSuperSliceQuery();
	// query1.setElasticCluster(cluster);
	// query1.setColumnFamilyName(POLL_STATE_ERRORS);
	// query1.setKeyspaceName(UP_TIME_CALC_DATA);
	//
	// query1.setKeys(allActiveMonitors);
	// query1.setReverseResultOrder(true);
	// query1.setStartSuperColumn(String.valueOf(startTs - 1));
	//
	// query1.setEndSuperColumn("0");
	//
	// Map<String, Map<String, Map<String, String>>>
	// allPreviousPollStateErrorsResult = query1
	// .getPreviousSuperColumn();
	//
	// Map<String, Map<String, Map<String, String>>>
	// validPreviousPollStateErrorsResult = new LinkedHashMap<String,
	// Map<String, Map<String, String>>>();
	// Iterator<Entry<String, Map<String, Map<String, String>>>>
	// allPreviousIterator = allPreviousPollStateErrorsResult
	// .entrySet().iterator();
	// while (allPreviousIterator.hasNext()) {
	// Entry<String, Map<String, Map<String, String>>> previousErrorPoll =
	// allPreviousIterator
	// .next();
	//
	// String previousStratTime = previousErrorPoll.getValue().keySet()
	// .iterator().next();
	// String previousEndTime = previousErrorPoll.getValue()
	// .get(previousStratTime).get(END_TIME);
	//
	// if (Long.valueOf(previousEndTime) <= startTs
	// && Long.valueOf(previousEndTime) > 0) {
	//
	// } else {
	// if (Long.valueOf(previousStratTime) >= endTs) {
	//
	// } else {
	// validPreviousPollStateErrorsResult.put(
	// previousErrorPoll.getKey(),
	// previousErrorPoll.getValue());
	//
	// }
	//
	// }
	// }
	// log.debug("Valid size:" + validPreviousPollStateErrorsResult.size());
	// Iterator<Entry<String, Map<String, Map<String, String>>>>
	// validPreviousPollStateErrorsResultIterator =
	// validPreviousPollStateErrorsResult
	// .entrySet().iterator();
	// while (validPreviousPollStateErrorsResultIterator.hasNext()) {
	// Entry<String, Map<String, Map<String, String>>> onePrevious =
	// validPreviousPollStateErrorsResultIterator
	// .next();
	//
	// String previousRec = onePrevious.getKey();
	// Iterator<Entry<String, Map<String, Map<String, String>>>>
	// pollStateErrorsResultIterator = allPollStateErrorsResult
	// .entrySet().iterator();
	// boolean hasErrors = false;
	// while (pollStateErrorsResultIterator.hasNext()) {
	// Entry<String, Map<String, Map<String, String>>> oneErrorPoll =
	// pollStateErrorsResultIterator
	// .next();
	// if (oneErrorPoll.getKey().compareTo(previousRec) == 0) {
	//
	// pollStateErrorsResultIterator.remove();
	// Map<String, Map<String, String>> oneMonitorAllErrors = new
	// LinkedHashMap<String, Map<String, String>>();
	// oneMonitorAllErrors.putAll(onePrevious.getValue());
	//
	// oneMonitorAllErrors.putAll(oneErrorPoll.getValue());
	//
	// Map<String, Map<String, Map<String, String>>> completeError = new
	// LinkedHashMap<String, Map<String, Map<String, String>>>();
	// completeError.put(previousRec, oneMonitorAllErrors);
	//
	// allPollStateErrorsResult.putAll(completeError);
	// hasErrors = true;
	// break;
	// }
	//
	// }
	// if (!hasErrors) {
	// allPollStateErrorsResult.put(previousRec,
	// onePrevious.getValue());
	// }
	// }
	// log.debug("All previous collected. Result size is: "
	// + allPollStateErrorsResult.size());
	// return allPollStateErrorsResult;
	// }
	private Map<String, Map<String, Map<String, String>>> findValidPreviousErrors(
			List<String> rowKeys, long startTs) {
		Map<String, Map<String, Map<String, String>>> previousErrorsResult = new HashMap<String, Map<String, Map<String, String>>>();
		String cmd = " select * from " + this.UP_TIME_CALC_DATA + "."
				+ this.POLL_STATE_ERRORS + " where key='$rowKey' order by "
				+ this.COLUMN1_NAME + " desc limit 1";
		log.warn("Getting previous errors for :" + rowKeys.size()
				+ " row keys with stmt: " + cmd);

		for (String oneKey : rowKeys) {
			ResultSet rSet1 = this.session.execute(cmd.replace("$rowKey",
					oneKey));
			if (rSet1.iterator().hasNext()) {
				Row oneRow = rSet1.iterator().next();
				if (oneRow.getString(this.VALUE_NAME).compareTo("0") == 0) {
					Map<String, String> previousErrorDetails = new HashMap<String, String>();
					previousErrorDetails.put(this.POLL_DURATION,
							String.valueOf(0));
					previousErrorDetails.put(this.END_TIME, String.valueOf(0));
					previousErrorDetails.put(this.CATEGORY_NAME,
							this.ERROR_NAME);

					Map<String, Map<String, String>> previousError = new HashMap<String, Map<String, String>>();
					previousError
							.put(String.valueOf(oneRow
									.getString(this.COLUMN1_NAME)),
									previousErrorDetails);

					previousErrorsResult.put(
							oneRow.getString(this.KEY_COLUMN_NAME),
							previousError);

				} else {
					if (Long.valueOf(oneRow.getString(this.COLUMN1_NAME))
							+ Long.valueOf(oneRow.getString(this.VALUE_NAME)) > startTs) {
						Map<String, String> previousErrorDetails = new HashMap<String, String>();
						previousErrorDetails.put(this.POLL_DURATION,
								oneRow.getString(this.VALUE_NAME));
						previousErrorDetails.put(this.END_TIME, String
								.valueOf(Long.valueOf(oneRow
										.getString(this.COLUMN1_NAME))
										+ Long.valueOf(oneRow
												.getString(this.VALUE_NAME))));
						previousErrorDetails.put(this.CATEGORY_NAME,
								this.ERROR_NAME);

						Map<String, Map<String, String>> previousError = new HashMap<String, Map<String, String>>();
						previousError.put(String.valueOf(oneRow
								.getString(this.COLUMN1_NAME)),
								previousErrorDetails);

						previousErrorsResult.put(
								oneRow.getString(this.KEY_COLUMN_NAME),
								previousError);
					}
				}

			}

		}
		return previousErrorsResult;
	}

	private Map<String, Map<String, Map<String, String>>> findValidPreviousErrors(
			String cmd, long startTs) {
		Map<String, Map<String, Map<String, String>>> previousErrorsResult = new HashMap<String, Map<String, Map<String, String>>>();

		ResultSet rSet1 = this.session.execute(cmd);

		Iterator<Row> previousPollStateIterator = rSet1.iterator();
		String previousKey = "";
		int i = 0;
		Map<String, Map<String, String>> previousError = new HashMap<String, Map<String, String>>();
		// boolean keyProcessed = false;
		while (previousPollStateIterator.hasNext()) {
			Row oneRow = previousPollStateIterator.next();
			String currentKey = oneRow.getString(this.KEY_COLUMN_NAME);
			if (i == 0) {
				previousKey = currentKey;
				i++;
			}
			if (currentKey.compareTo(previousKey) == 0) {

				// if (!keyProcessed) {
				if (oneRow.getString(this.COLUMN2_NAME)
						.compareTo(this.END_TIME) == 0) {

					long pollEndTime = Long.valueOf(oneRow
							.getString(this.VALUE_NAME));
					long pollStartTime = Long.valueOf(oneRow
							.getString(this.COLUMN1_NAME));
					if (pollEndTime == 0 || pollEndTime > startTs) {
						Map<String, String> previousErrorDetails = new HashMap<String, String>();
						if (pollEndTime == 0) {
							previousErrorDetails.put(this.POLL_DURATION,
									String.valueOf(0));
							previousErrorDetails.put(this.END_TIME,
									String.valueOf(0));
							previousErrorDetails.put(this.CATEGORY_NAME,
									this.ERROR_NAME);

							// keyProcessed = true;
						} else {
							previousErrorDetails
									.put(this.POLL_DURATION,
											String.valueOf(pollEndTime
													- pollStartTime));
							previousErrorDetails.put(this.END_TIME,
									String.valueOf(0));
							previousErrorDetails.put(this.CATEGORY_NAME,
									this.ERROR_NAME);
						}

						previousError.put(String.valueOf(pollStartTime),
								previousErrorDetails);

					}
					// if (pollEndTime <= startTs) {
					// keyProcessed = true;
					// }

				}
				// }

			}

			else {
				if (!previousError.isEmpty()) {
					previousErrorsResult.put(previousKey, previousError);
				}
				previousKey = currentKey;
				previousError = new HashMap<String, Map<String, String>>();
				// keyProcessed = false;

			}
		}
		return previousErrorsResult;

	}

	private Map<String, Map<String, Map<String, String>>> findValidPreviousSupressions(
			List<String> devices, long startTs) {
		Map<String, Map<String, Map<String, String>>> previousSupressions = new HashMap<String, Map<String, Map<String, String>>>();
		for (String oneDevice : devices) {
			String cmd = "select * from "
					+ this.UP_TIME_CALC_DATA
					+ "."
					+ this.SUPRESSION_INTERVALS
					+ " where key = '$rowkey' and column1 < '$startTime' order by column1 desc limit 2";

			cmd = cmd.replace("$rowkey", oneDevice).replace("$startTime",
					String.valueOf(startTs));
			ResultSet rSet1 = this.session.execute(cmd.replace("$rowKey",
					oneDevice).replace("$startTime", String.valueOf(startTs)));
			List<Row> allRows = rSet1.all();
			if (allRows.size() == 2) {
				Row suppresionDurationRow = allRows.get(1);
				if (suppresionDurationRow.getString(this.VALUE_NAME).compareTo(
						"0") == 0) {
					Map<String, String> supressionDetails = new HashMap<String, String>();
					Map<String, Map<String, String>> onePreviousSupressions = new HashMap<String, Map<String, String>>();
					supressionDetails.put(this.SUPRESSION_DURATION,
							String.valueOf(0));
					supressionDetails.put(this.END_TIME, String.valueOf(0));
					supressionDetails.put(this.SUPRESSION_SOURCE,
							this.SUPRESSION_DEFAULT_SOURCE);
					onePreviousSupressions.put(String
							.valueOf(suppresionDurationRow
									.getString(this.COLUMN1_NAME)),
							supressionDetails);
					previousSupressions.put(oneDevice, onePreviousSupressions);

				} else if (Long.valueOf(suppresionDurationRow
						.getString(this.COLUMN1_NAME))
						+ Long.valueOf(suppresionDurationRow
								.getString(this.VALUE_NAME)) > startTs) {
					Map<String, String> supressionDetails = new HashMap<String, String>();
					Map<String, Map<String, String>> onePreviousSupressions = new HashMap<String, Map<String, String>>();
					supressionDetails.put(this.SUPRESSION_DURATION,
							suppresionDurationRow.getString(this.VALUE_NAME));
					supressionDetails.put(this.END_TIME, String.valueOf(Long
							.valueOf(suppresionDurationRow
									.getString(this.COLUMN1_NAME))
							+ Long.valueOf(suppresionDurationRow
									.getString(this.VALUE_NAME))));
					supressionDetails.put(this.SUPRESSION_SOURCE,
							this.SUPRESSION_DEFAULT_SOURCE);
					onePreviousSupressions.put(String
							.valueOf(suppresionDurationRow
									.getString(this.COLUMN1_NAME)),
							supressionDetails);
					previousSupressions.put(oneDevice, onePreviousSupressions);
				}
			}
		}

		return previousSupressions;

	}

	private Map<String, Map<String, Map<String, String>>> findValidPreviousSupressions(
			String cmd, long startTs) {
		Map<String, Map<String, Map<String, String>>> previousErrorsResult = new HashMap<String, Map<String, Map<String, String>>>();

		ResultSet rSet1 = this.session.execute(cmd);

		Iterator<Row> previousSupressionIterator = rSet1.iterator();
		String previousKey = "";
		int i = 0;
		int p = 0;
		Map<String, Map<String, String>> previousSupressions = new HashMap<String, Map<String, String>>();
		// boolean keyProcessed = false;
		while (previousSupressionIterator.hasNext()) {
			Row oneRow = previousSupressionIterator.next();
			String currentKey = oneRow.getString(this.KEY_COLUMN_NAME);
			
			if (i == 0) {
				previousKey = currentKey;
				i++;
			}
			p++;
			if (currentKey.compareTo(previousKey) == 0) {

				// if (!keyProcessed) {
				if (oneRow.getString(this.COLUMN2_NAME)
						.compareTo(this.END_TIME) == 0) {

					long supressionEndTime = Long.valueOf(oneRow
							.getString(this.VALUE_NAME));
					long supressionStartName = Long.valueOf(oneRow
							.getString(this.COLUMN1_NAME));
					if (supressionEndTime == 0 || supressionEndTime > startTs) {
						Map<String, String> supressionDetails = new HashMap<String, String>();
						if (supressionEndTime == 0) {
							supressionDetails.put(this.SUPRESSION_DURATION,
									String.valueOf(0));
							supressionDetails.put(this.END_TIME,
									String.valueOf(0));
							supressionDetails.put(this.SUPRESSION_SOURCE,
									this.SUPRESSION_DEFAULT_SOURCE);

							// keyProcessed = true;
						} else {
							supressionDetails.put(
									this.SUPRESSION_DURATION,
									String.valueOf(supressionEndTime
											- supressionStartName));
							supressionDetails.put(this.END_TIME,
									String.valueOf(0));
							supressionDetails.put(this.SUPRESSION_SOURCE,
									this.SUPRESSION_DEFAULT_SOURCE);
						}

						previousSupressions.put(
								String.valueOf(supressionStartName),
								supressionDetails);

					}

				}

			}

			else {
				if (!previousSupressions.isEmpty()) {
					previousErrorsResult.put(previousKey, previousSupressions);
				}
				previousKey = currentKey;
				previousSupressions = new HashMap<String, Map<String, String>>();
				if (oneRow.getString(this.COLUMN2_NAME)
						.compareTo(this.END_TIME) == 0) {

					long supressionEndTime = Long.valueOf(oneRow
							.getString(this.VALUE_NAME));
					long supressionStartName = Long.valueOf(oneRow
							.getString(this.COLUMN1_NAME));
					if (supressionEndTime == 0 || supressionEndTime > startTs) {
						Map<String, String> supressionDetails = new HashMap<String, String>();
						if (supressionEndTime == 0) {
							supressionDetails.put(this.SUPRESSION_DURATION,
									String.valueOf(0));
							supressionDetails.put(this.END_TIME,
									String.valueOf(0));
							supressionDetails.put(this.SUPRESSION_SOURCE,
									this.SUPRESSION_DEFAULT_SOURCE);

							// keyProcessed = true;
						} else {
							supressionDetails.put(
									this.SUPRESSION_DURATION,
									String.valueOf(supressionEndTime
											- supressionStartName));
							supressionDetails.put(this.END_TIME,
									String.valueOf(0));
							supressionDetails.put(this.SUPRESSION_SOURCE,
									this.SUPRESSION_DEFAULT_SOURCE);
						}

						previousSupressions.put(
								String.valueOf(supressionStartName),
								supressionDetails);

					}
				}
				p = 0;

			}
		}
		return previousErrorsResult;

	}

	public Map<String, Map<String, Map<String, String>>> getAllHourlyUpTimeCalc(
			List<String> allActiveMonitors, long startTs, long endTs)
			throws Exception {
		log.warn("In get Hourly monitor size :"+allActiveMonitors.size());
		StringBuilder sqlGetCalculation = new StringBuilder();
		sqlGetCalculation.append("Select * from " + this.UP_TIME_CALC_DATA
				+ "." + this.UP_TIME_CALCULATION_SUPER + " where "
				+ this.KEY_COLUMN_NAME + " in (");
		int i = 0;
		for (String oneKey : allActiveMonitors) {
			if (i > 0) {
				sqlGetCalculation.append(",");

			}
			sqlGetCalculation.append("'");
			sqlGetCalculation.append(oneKey);
			sqlGetCalculation.append("'");
			i++;
		}
		sqlGetCalculation
				.append(") and " + this.COLUMN1_NAME + " >='" + startTs
						+ "' and " + this.COLUMN1_NAME + " < '" + endTs + "'");
		log.warn(sqlGetCalculation.toString());
		log.warn("Calling get CompositeQueryResult");

		Map<String, Map<String, Map<String, String>>> allHourlyUpTimeResult = this
				.getCompositeQueryResult(sqlGetCalculation.toString());
		log.warn("Got up time calc result");

		// ResultSet rSet = this.session.execute(sqlGetCalculation.toString());
		//
		// Iterator<Row> rSetIterator = rSet.iterator();
		// String currentKey = "";
		// String currentTs = "";
		// Map<String, String> fieldsMap = new HashMap<String, String>();
		// Map<String, Map<String, String>> superColumnMap = new HashMap<String,
		// Map<String, String>>();
		// i = 0;
		// while (rSetIterator.hasNext()) {
		// Row oneRow = rSetIterator.next();
		// String key = oneRow.getString(this.KEY_COLUMN_NAME);
		// String ts = oneRow.getString(this.COLUMN1_NAME);
		// if (i == 0) {
		// currentKey = key;
		// currentTs = ts;
		// fieldsMap.put(oneRow.getString(this.COLUMN2_NAME),
		// oneRow.getString(this.VALUE_NAME));
		// i++;
		// } else {
		// if (key.compareTo(currentKey) == 0
		// && ts.compareTo(currentTs) == 0) {
		// fieldsMap.put(oneRow.getString(this.COLUMN2_NAME),
		// oneRow.getString(this.VALUE_NAME));
		// } else {
		// if (key.compareTo(currentKey) == 0
		// && ts.compareTo(currentTs) != 0) {
		// superColumnMap.put(currentTs, fieldsMap);
		// currentTs = ts;
		// fieldsMap = new HashMap<String, String>();
		// fieldsMap.put(oneRow.getString(this.COLUMN2_NAME),
		// oneRow.getString(this.VALUE_NAME));
		// } else {
		// if (key.compareTo(currentKey) != 0) {
		// superColumnMap.put(currentTs, fieldsMap);
		// allHourlyUpTimeResult
		// .put(currentKey, superColumnMap);
		// currentTs = ts;
		// currentKey = key;
		// fieldsMap = new HashMap<String, String>();
		// fieldsMap.put(oneRow.getString(this.COLUMN2_NAME),
		// oneRow.getString(this.VALUE_NAME));
		// superColumnMap = new HashMap<String, Map<String, String>>();
		//
		// }
		// }
		// }
		// }
		//
		// }
		// if (currentKey.length() > 0) {
		// superColumnMap.put(currentTs, fieldsMap);
		// allHourlyUpTimeResult.put(currentKey, superColumnMap);
		// }

		return allHourlyUpTimeResult;
	}

	// public Map<String, Map<String, Map<String, String>>>
	// getAllDailyUpTimeCalc(
	// List<String> allActiveMonitors, long startTs, long endTs)
	// throws Exception {
	//
	// MultiGetSuperSliceQuery query = new MultiGetSuperSliceQuery();
	//
	// query.setElasticCluster(cluster);
	// query.setColumnFamilyName(UP_TIME_CALCULATION__DAILY_SUPER);
	// query.setKeyspaceName(UP_TIME_CALC_DATA);
	//
	// query.setKeys(allActiveMonitors);
	// query.setReverseResultOrder(false);
	// query.setStartSuperColumn(String.valueOf(startTs));
	// query.setEndSuperColumn(String.valueOf(endTs));
	//
	// log.warn("Get daily up time calc query completed");
	// Map<String, Map<String, Map<String, String>>> allDailyUpTimeResult =
	// query
	// .executeQuery();
	//
	// log.warn("Collected Daily. Current result size :"
	// + allDailyUpTimeResult.size());
	//
	// return allDailyUpTimeResult;
	// }

	private void createElasticCluster() {
		if (this.seeds != null && !this.seeds.isEmpty()
				&& this.clusterName != null) {
			for (String oneSeed : this.seeds) {
				this.cluster = ClusterBuilder.getCluster(clusterName, oneSeed);
				if (this.cluster != null) {
					return;
				}
			}
		} else {
			throw new IllegalStateException(
					"Cluster name and seeds list have to be specified");
		}

	}

	private void createConnection() {

		this.cqlCluster = Cluster.builder().addContactPoint(this.seeds.get(0))
				.build();
		final Metadata metadata = this.cqlCluster.getMetadata();

		log.info("Connected to cluster: " + metadata.getClusterName());
		for (final Host host : metadata.getAllHosts()) {
			log.info("Datacenter: " + host.getDatacenter() + " Host:"
					+ host.getAddress() + "Rack: " + host.getRack());
		}

		session = this.cqlCluster.connect();

	}

	public void close() {
		this.session.close();
		
		this.cqlCluster.close();
		
	}

//	public String getInstanceFirstStatusTimeStamp(String instanceKey) {
//
//		// Map<String, ElasticCluster> clusters = cassandraElasticService
//		// .getDataCluster(UP_TIME_CALC_DATA, INSTANCE_STATUS_SUPER);
//
//		SuperColumnRangeSliceQuery query = new SuperColumnRangeSliceQuery();
//		query.setElasticCluster(cluster);
//		query.setColumnFamilyName(INSTANCE_STATUS_SUPER);
//		query.setKeyspaceName(UP_TIME_CALC_DATA);
//
//		query.setOneKey(instanceKey);
//		query.setReverseResultOrder(false);
//
//		long startTS = 0;
//
//		Map<String, SuperColumnWithSuperColumnRow> resultSet = query
//				.executeQuery().getQueryResult();
//
//		if (!resultSet.isEmpty()) {
//			Map<String, SuperColumnRow> instanceStatus = resultSet.get(
//					instanceKey).getSuperColumnWithSuperColumnRow();
//
//			for (String statusTs : instanceStatus.keySet()) {
//				if (startTS == 0) {
//					startTS = Long.valueOf(statusTs);
//				} else {
//					if (Long.valueOf(statusTs) < startTS) {
//						startTS = Long.valueOf(statusTs);
//					}
//				}
//
//			}
//
//		}
//		return (String.valueOf(startTS));
//
//	}

//	public Map<String, String> getInstanceLastStatus(String instanceKey) {
//
//		SuperColumnRangeSliceQuery query = new SuperColumnRangeSliceQuery();
//		query.setElasticCluster(cluster);
//		query.setColumnFamilyName(INSTANCE_STATUS_SUPER);
//		query.setKeyspaceName(UP_TIME_CALC_DATA);
//
//		query.setOneKey(instanceKey);
//		query.setReverseResultOrder(true);
//		query.setStartSuperColumn(String.valueOf(Calendar.getInstance()
//				.getTimeInMillis() / 1000));
//		query.setEndSuperColumn(String.valueOf(0));
//
//		Map<String, String> lastStatus = new LinkedHashMap<String, String>();
//
//		long startTS = 0;
//
//		Map<String, SuperColumnWithSuperColumnRow> resultSet = query
//				.executeQuery().getQueryResult();
//
//		if (!resultSet.isEmpty()) {
//			Map<String, SuperColumnRow> instanceStatus = resultSet.get(
//					instanceKey).getSuperColumnWithSuperColumnRow();
//
//			for (String startTime : instanceStatus.keySet()) {
//				if (lastStatus.isEmpty()) {
//					lastStatus.put(START_TIME_STAMP, startTime);
//					startTS = Long.valueOf(startTime);
//					lastStatus.putAll(instanceStatus.get(startTime)
//							.getRowColumns());
//				} else {
//					if (Long.valueOf(startTime) > startTS) {
//						lastStatus.clear();
//						lastStatus.put(START_TIME_STAMP, startTime);
//						startTS = Long.valueOf(startTime);
//						lastStatus.putAll(instanceStatus.get(startTime)
//								.getRowColumns());
//					}
//				}
//
//			}
//
//		}
//		return lastStatus;
//
//	}

	public UpTimeCalculationDaoImpl(String clusterName, List<String> seeds) {
		super();
		this.clusterName = clusterName;
		this.seeds = seeds;
		// this.createElasticCluster();
		this.createConnection();
	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public List<String> getSeeds() {
		return seeds;
	}

	public void setSeeds(List<String> seeds) {
		this.seeds = seeds;
	}

	public Session getSession() {
		return session;
	}

	private Map<String, Map<String, Map<String, String>>> getCompositeQueryResult(
			String query) {
		Map<String, Map<String, Map<String, String>>> compositeResult = new HashMap<String, Map<String, Map<String, String>>>();

		ResultSet rSet = this.session.execute(query);

		Iterator<Row> rSetIterator = rSet.iterator();
		String currentKey = "";
		String currentTs = "";
		Map<String, String> fieldsMap = new HashMap<String, String>();
		Map<String, Map<String, String>> superColumnMap = new HashMap<String, Map<String, String>>();
		int i = 0;
		while (rSetIterator.hasNext()) {
			Row oneRow = rSetIterator.next();
			String key = oneRow.getString(this.KEY_COLUMN_NAME);
			String ts = oneRow.getString(this.COLUMN1_NAME);
			if (i == 0) {
				currentKey = key;
				currentTs = ts;
				fieldsMap.put(oneRow.getString(this.COLUMN2_NAME),
						oneRow.getString(this.VALUE_NAME));
				i++;
			} else {
				if (key.compareTo(currentKey) == 0
						&& ts.compareTo(currentTs) == 0) {
					fieldsMap.put(oneRow.getString(this.COLUMN2_NAME),
							oneRow.getString(this.VALUE_NAME));
				} else {
					if (key.compareTo(currentKey) == 0
							&& ts.compareTo(currentTs) != 0) {
						superColumnMap.put(currentTs, fieldsMap);
						currentTs = ts;
						fieldsMap = new HashMap<String, String>();
						fieldsMap.put(oneRow.getString(this.COLUMN2_NAME),
								oneRow.getString(this.VALUE_NAME));

					} else {
						if (key.compareTo(currentKey) != 0) {
							superColumnMap.put(currentTs, fieldsMap);
							compositeResult.put(currentKey, superColumnMap);
							currentTs = ts;
							currentKey = key;
							fieldsMap = new HashMap<String, String>();
							fieldsMap.put(oneRow.getString(this.COLUMN2_NAME),
									oneRow.getString(this.VALUE_NAME));
							superColumnMap = new HashMap<String, Map<String, String>>();

						}
					}
				}
			}

		}
		if (currentKey.length() > 0) {
			superColumnMap.put(currentTs, fieldsMap);
			compositeResult.put(currentKey, superColumnMap);
		}

		return compositeResult;
	}
}
