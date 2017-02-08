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

public class UpTimeCalculationDaoImpl {

	private static final String NEXT_START_TIME_STAMP = "NextStartTimeStamp";
	private static final String RUN_FLAG = "RunFlag";
	private static final String ENABLED_MONITORS_SUPER = "EnabledMonitorsSuper";
	private static final String POLL_STATE_CHANGES = "PollStateChanges";
	private static final String SUPRESSION_INTERVALS = "SupressionIntervals";
	private static final String POLL_STATE_ERRORS = "PollStateErrors";
	private static final String UP_TIME_CALCULATION_SUPER = "UpTimeCalculationSuper";
	private static final String UP_TIME_CALCULATION__DAILY_SUPER = "UpTimeCalculationDailySuper";
	private static final String STATUS = "Status";
	private static final String MONITOR_STATUS = "MonitorStatus";
	private static final String START_TIME_STAMP = "StartTimeStamp";
	private static final String START_TIME = "StartTime";
	private static final String SCHEDULING = "Scheduling";
	private static final String END_TIME = "EndTime";
	private static final String UP_TIME_CALC_TIME_STAMP = "UpTimeCalcTimeStamp";
	private static final String UP_TIME_CALC_DATA = "UpTimeCalcData";
	private static final String INSTANCE_STATUS_SUPER = "InstanceStatusSuper";
	private ElasticCluster cluster;
	private String clusterName;
	private List<String> seeds;

	private static org.apache.log4j.Logger log = Logger
			.getLogger(UpTimeCalculationDaoImpl.class);

	public String getEndTimeStamp(String sourceSystem) {

		HectorTemplate hTemplate = this.cluster
				.getHectorTemplate(UP_TIME_CALC_DATA);
		SliceQuery<String, String, String> getNextStartTSQuery = hTemplate
				.createSliceQuery(StringSerializer.get(),
						StringSerializer.get(), StringSerializer.get());

		getNextStartTSQuery.setColumnFamily(UP_TIME_CALC_TIME_STAMP);
		getNextStartTSQuery.setKey(sourceSystem);
		getNextStartTSQuery.setColumnNames(END_TIME);
		QueryResult<ColumnSlice<String, String>> nextStartTS = getNextStartTSQuery
				.execute();
		ColumnSlice<String, String> columnSlice = nextStartTS.get();

		HColumn<String, String> startTimeColumn = columnSlice
				.getColumnByName(END_TIME);

		return startTimeColumn.getValue();
	}

	public String getSchedulingFlag(String processName) {

		HectorTemplate hTemplate = this.cluster
				.getHectorTemplate(UP_TIME_CALC_DATA);
		SliceQuery<String, String, String> getNextStartTSQuery = hTemplate
				.createSliceQuery(StringSerializer.get(),
						StringSerializer.get(), StringSerializer.get());

		getNextStartTSQuery.setColumnFamily(SCHEDULING);
		getNextStartTSQuery.setKey(processName);
		getNextStartTSQuery.setColumnNames(RUN_FLAG);
		QueryResult<ColumnSlice<String, String>> nextStartTS = getNextStartTSQuery
				.execute();
		ColumnSlice<String, String> columnSlice = nextStartTS.get();

		HColumn<String, String> runFlagColumn = columnSlice
				.getColumnByName(RUN_FLAG);
		return (runFlagColumn.getValue());

	}

	public void setRunFlag(String processName, String flagValue) {

		ThriftColumnFamilyTemplate<String, String> schedulingTemplate = this.cluster
				.getStandardColumnFamilyTemplate(UP_TIME_CALC_DATA, SCHEDULING);
		Mutator<String> updateSchedulingFlag = schedulingTemplate
				.createMutator();

		updateSchedulingFlag.addInsertion(processName, SCHEDULING,
				HFactory.createStringColumn(RUN_FLAG, flagValue));
		updateSchedulingFlag.execute();
		return;

	}

	public void setNextStartTime(String nextStartTime, String sourceSystem) {
		log.debug("Updating next time stamp to:" + nextStartTime);
		ThriftColumnFamilyTemplate<String, String> nextTimeStampTemplate = this.cluster
				.getStandardColumnFamilyTemplate(UP_TIME_CALC_DATA,
						NEXT_START_TIME_STAMP);
		Mutator<String> updateNextTime = nextTimeStampTemplate.createMutator();

		updateNextTime.addInsertion(sourceSystem, NEXT_START_TIME_STAMP,
				HFactory.createStringColumn(START_TIME, nextStartTime));
		updateNextTime.execute();
		log.debug("Time stamp updated");

	}

	public void setNextUpTimeCalcTimeStamp(String nextTimeStamp,
			String sourceSystem) {

		ThriftColumnFamilyTemplate<String, String> nextTimeStampTemplate = this.cluster
				.getStandardColumnFamilyTemplate(UP_TIME_CALC_DATA,
						UP_TIME_CALC_TIME_STAMP);
		Mutator<String> updateNextTime = nextTimeStampTemplate.createMutator();

		updateNextTime.addInsertion(sourceSystem, UP_TIME_CALC_TIME_STAMP,
				HFactory.createStringColumn(END_TIME, nextTimeStamp));
		updateNextTime.execute();
		return;

	}

	public String getNextStartTimeStamp(String sourceSystem) {

		HectorTemplate hTemplate = this.cluster
				.getHectorTemplate(UP_TIME_CALC_DATA);
		SliceQuery<String, String, String> getNextStartTSQuery = hTemplate
				.createSliceQuery(StringSerializer.get(),
						StringSerializer.get(), StringSerializer.get());

		getNextStartTSQuery.setColumnFamily(NEXT_START_TIME_STAMP);
		getNextStartTSQuery.setKey(sourceSystem);
		getNextStartTSQuery.setColumnNames(START_TIME);
		QueryResult<ColumnSlice<String, String>> nextStartTS = getNextStartTSQuery
				.execute();
		ColumnSlice<String, String> columnSlice = nextStartTS.get();

		HColumn<String, String> startTimeColumn = columnSlice
				.getColumnByName(START_TIME);
		String startTime = startTimeColumn.getValue();
		if (startTime.compareTo("0") == 0) {
			Calendar c = Calendar.getInstance();

			startTime = String.valueOf(c.getTimeInMillis() / 1000);
		}

		return startTime;
	}

	public List<String> getAllPollStateChangeKeys() {

		RangeSliceQuery rangeSliceQuery = new RangeSliceQuery();
		rangeSliceQuery.setElasticCluster(this.cluster);
		rangeSliceQuery.setColumnFamilyName(POLL_STATE_CHANGES);
		rangeSliceQuery.setKeyspaceName(UP_TIME_CALC_DATA);

		return rangeSliceQuery.getKeysOnly();
	}

	public List<String> getAllPollStateErrorsKeys() {

		RangeSliceQuery rangeSliceQuery = new RangeSliceQuery();
		rangeSliceQuery.setElasticCluster(this.cluster);
		rangeSliceQuery.setColumnFamilyName(POLL_STATE_ERRORS);
		rangeSliceQuery.setKeyspaceName(UP_TIME_CALC_DATA);

		return rangeSliceQuery.getKeysOnly();
	}

	public List<String> getAllUpTimeCalcKeys() {

		RangeSliceQuery rangeSliceQuery = new RangeSliceQuery();
		rangeSliceQuery.setElasticCluster(this.cluster);
		rangeSliceQuery.setColumnFamilyName(UP_TIME_CALCULATION_SUPER);
		rangeSliceQuery.setKeyspaceName(UP_TIME_CALC_DATA);

		return rangeSliceQuery.getKeysOnly();
	}

	public List<String> getLastEnabledMonitors(String sourceSystem)
			throws Exception {

		log.debug("In get Last Enabled Monitors");

		SuperColumnRangeSliceQuery query = new SuperColumnRangeSliceQuery();
		query.setElasticCluster(this.cluster);
		query.setColumnFamilyName(ENABLED_MONITORS_SUPER);
		query.setKeyspaceName(UP_TIME_CALC_DATA);

		query.setOneKey(sourceSystem);
		query.setReverseResultOrder(true);
		query.setStartSuperColumn(String.valueOf(Calendar.getInstance()
				.getTimeInMillis() / 1000));
		query.setEndSuperColumn(String.valueOf(0));

		Map<String, SuperColumnWithSuperColumnRow> queryResult = query
				.getPreviousSupercolumn().getQueryResult();
		long currentTimeStamp = 0;
		List<String> allEnabledMonitors = new ArrayList<String>();

		if (!queryResult.isEmpty()) {
			Map<String, SuperColumnRow> oneSuperColumnResult = queryResult.get(
					sourceSystem).getSuperColumnWithSuperColumnRow();
			for (String superColumn : oneSuperColumnResult.keySet()) {
				long currentStartTime = Long.valueOf(superColumn).longValue();
				if (currentStartTime > currentTimeStamp) {
					allEnabledMonitors = null;
					allEnabledMonitors = new ArrayList<String>();
					allEnabledMonitors.addAll(oneSuperColumnResult
							.get(superColumn).getRowColumns().keySet());
					currentTimeStamp = currentStartTime;
				}
			}
		}

		log.debug("Last Enabled monitors returning result:"
				+ allEnabledMonitors.size());
		return allEnabledMonitors;

	}

	public List<String> getLastEnabledMonitors(String startTS,
			String sourceSystem) throws Exception {
		log.info("In get Last Enabled Monitors. Start ts : " + startTS);

		SuperColumnRangeSliceQuery query = new SuperColumnRangeSliceQuery();
		query.setElasticCluster(this.cluster);
		query.setColumnFamilyName(ENABLED_MONITORS_SUPER);
		query.setKeyspaceName(UP_TIME_CALC_DATA);

		query.setOneKey(sourceSystem);
		query.setReverseResultOrder(true);
		query.setStartSuperColumn(startTS);
		query.setEndSuperColumn(String.valueOf(0));

		Map<String, SuperColumnWithSuperColumnRow> queryResult = query
				.getPreviousSupercolumn().getQueryResult();
		long currentTimeStamp = 0;
		List<String> allEnabledMonitors = new ArrayList<String>();

		if (!queryResult.isEmpty()) {
			Map<String, SuperColumnRow> oneSuperColumnResult = queryResult.get(
					sourceSystem).getSuperColumnWithSuperColumnRow();
			for (String superColumn : oneSuperColumnResult.keySet()) {
				long currentStartTime = Long.valueOf(superColumn).longValue();
				if (currentStartTime > currentTimeStamp) {
					allEnabledMonitors = null;
					allEnabledMonitors = new ArrayList<String>();
					allEnabledMonitors.addAll(oneSuperColumnResult
							.get(superColumn).getRowColumns().keySet());
					currentTimeStamp = currentStartTime;
				}
			}
		}

		log.debug("Last Enabled monitors returning result:" + startTS + ": "
				+ allEnabledMonitors.size());
		return allEnabledMonitors;

	}

	public List<String> getTimeFrameEnabledMonitors(String startTS,
			String endTS, String sourceSystem) throws Exception {
		log.info("In get TimeFrame  Enabled Monitors. Start ts : " + startTS
				+ " " + endTS + " " + sourceSystem);

		Set<String> uniqueMonitors = new TreeSet<String>();
		List<String> allEnabledMonitors = new ArrayList<String>();
		SuperColumnRangeSliceQuery query = new SuperColumnRangeSliceQuery();
		query.setElasticCluster(this.cluster);
		query.setColumnFamilyName(ENABLED_MONITORS_SUPER);
		query.setKeyspaceName(UP_TIME_CALC_DATA);

		query.setOneKey(sourceSystem);
		query.setReverseResultOrder(false);
		query.setStartSuperColumn(startTS);
		query.setEndSuperColumn(endTS);
		Map<String, SuperColumnRow> queryResult = query.executeQuery()
				.getQueryResult().get(sourceSystem)
				.getSuperColumnWithSuperColumnRow();
		if (!queryResult.isEmpty()) {

			Iterator<Entry<String, SuperColumnRow>> enabledMonitorsIterator = queryResult
					.entrySet().iterator();

			while (enabledMonitorsIterator.hasNext())

			{
				uniqueMonitors.addAll(enabledMonitorsIterator.next().getValue()
						.getRowColumns().keySet());

			}
		}

		allEnabledMonitors.addAll(uniqueMonitors);
		log.debug("Returning Last Enabled Monitors. "
				+ allEnabledMonitors.size());
		return allEnabledMonitors;

	}

	public Map<String, String> getPreviousSupressionInterval(String instanceId,
			long startTimeStamp, long endTimeStamp) {
		log.debug("Executing getPreviousSupressionInterval");

		Map<String, String> supressionIntervalsResult = new LinkedHashMap<String, String>();

		SuperColumnRangeSliceQuery query = new SuperColumnRangeSliceQuery();
		query.setElasticCluster(this.cluster);
		query.setColumnFamilyName(SUPRESSION_INTERVALS);
		query.setKeyspaceName(UP_TIME_CALC_DATA);

		query.setOneKey(instanceId);
		query.setReverseResultOrder(true);
		query.setStartSuperColumn(String.valueOf(startTimeStamp));
		query.setEndSuperColumn(String.valueOf(endTimeStamp));

		Map<String, SuperColumnWithSuperColumnRow> queryResult = query
				.getPreviousSupercolumn().getQueryResult();
		long lastTimeStamp = 0;
		new ArrayList<String>();

		if (!queryResult.isEmpty()) {
			Map<String, SuperColumnRow> oneSuperColumnResult = queryResult.get(
					instanceId).getSuperColumnWithSuperColumnRow();
			for (String superColumn : oneSuperColumnResult.keySet()) {
				long currentStartTime = Long.valueOf(superColumn).longValue();
				if (currentStartTime > lastTimeStamp) {

					Map<String, String> oneSupressionIntervalResult = new HashMap<String, String>();
					oneSupressionIntervalResult.put(START_TIME_STAMP,
							superColumn);

					oneSupressionIntervalResult.putAll(oneSuperColumnResult
							.get(superColumn).getRowColumns());

					supressionIntervalsResult
							.putAll(oneSupressionIntervalResult);
					lastTimeStamp = currentStartTime;
				}
			}
		}

		return supressionIntervalsResult;

	}

	public Map<String, String> getPreviousPollStateError(String instanceId,
			long startTimeStamp, long endTimeStamp)
			throws InterruptedException, ExecutionException {
		log.debug("Executing getPreviousPollStateError 2");

		SuperColumnRangeSliceQuery query = new SuperColumnRangeSliceQuery();
		query.setElasticCluster(cluster);
		query.setColumnFamilyName(POLL_STATE_ERRORS);
		query.setKeyspaceName(UP_TIME_CALC_DATA);

		query.setOneKey(instanceId);
		query.setReverseResultOrder(true);
		query.setStartSuperColumn(String.valueOf(startTimeStamp));
		query.setEndSuperColumn(String.valueOf(endTimeStamp));

		Map<String, SuperColumnWithSuperColumnRow> queryResult = query
				.getPreviousSupercolumn().getQueryResult();
		long lastTimeStamp = 0;
		Map<String, String> previousPollStateErrorResult = new LinkedHashMap<String, String>();

		if (!queryResult.isEmpty()) {
			Map<String, SuperColumnRow> pollStateChanges = queryResult.get(
					instanceId).getSuperColumnWithSuperColumnRow();
			for (String oneSuperColumn : pollStateChanges.keySet()) {
				long currentStartTime = Long.valueOf(oneSuperColumn)
						.longValue();
				if (currentStartTime > lastTimeStamp) {
					Map<String, String> onePollStateError = new HashMap<String, String>();
					onePollStateError.put(START_TIME_STAMP, oneSuperColumn);
					onePollStateError.putAll(pollStateChanges.get(
							oneSuperColumn).getRowColumns());
					previousPollStateErrorResult.putAll(onePollStateError);
					lastTimeStamp = currentStartTime;
				}
			}
		}
		return previousPollStateErrorResult;

	}

	public Map<String, String> getMonitorEnabledDisabledIntervals(
			String monitorId) throws Exception {
		log.debug("Executing getMonitorEnabledDisabledIntervals");

		SuperColumnRangeSliceQuery query = new SuperColumnRangeSliceQuery();

		query.setElasticCluster(this.cluster);
		query.setColumnFamilyName(MONITOR_STATUS);
		query.setKeyspaceName(UP_TIME_CALC_DATA);

		query.setOneKey(monitorId);
		query.setReverseResultOrder(false);
		query.setStartSuperColumn(String.valueOf(0));
		query.setEndSuperColumn("9999999999");

		Map<String, SuperColumnWithSuperColumnRow> resultSets = query
				.executeQuery().getQueryResult();

		SortedMap<String, String> monitorEnabledDisabledIntervals = new TreeMap<String, String>();

		if (!resultSets.isEmpty()) {
			Map<String, SuperColumnRow> monitorStatus = resultSets.get(
					monitorId).getSuperColumnWithSuperColumnRow();
			for (String superColumn : monitorStatus.keySet()) {
				monitorEnabledDisabledIntervals.put(superColumn, monitorStatus
						.get(superColumn).getRowColumns().get(STATUS));
			}

		}

		return monitorEnabledDisabledIntervals;

	}

	@SuppressWarnings("static-access")
	public List<Map<String, String>> getDailyUpTimeCalc(String monitorId,
			String startTimeStamp, String endTimeStamp)
			throws InterruptedException, ExecutionException, Exception {
		log.info("Get Daily Up time calc for :" + monitorId + " "
				+ startTimeStamp + " " + endTimeStamp);
		List<Map<String, String>> upTimeCalcResult = new ArrayList<Map<String, String>>();

		SuperColumnRangeSliceQuery query = new SuperColumnRangeSliceQuery();
		query.setElasticCluster(cluster);
		query.setColumnFamilyName(this.UP_TIME_CALCULATION__DAILY_SUPER);
		query.setKeyspaceName(UP_TIME_CALC_DATA);

		query.setOneKey(monitorId);
		query.setReverseResultOrder(false);
		query.setStartSuperColumn(startTimeStamp);
		query.setEndSuperColumn(endTimeStamp);
		Map<String, SuperColumnWithSuperColumnRow> resultSet = query
				.executeQuery().getQueryResult();

		if (!resultSet.isEmpty()) {
			Map<String, SuperColumnRow> upTimeCalc = resultSet.get(monitorId)
					.getSuperColumnWithSuperColumnRow();

			for (String oneSuperColumn : upTimeCalc.keySet()) {
				Map<String, String> oneUpTimeCalcResult = new HashMap<String, String>();
				oneUpTimeCalcResult.put(START_TIME_STAMP, oneSuperColumn);

				oneUpTimeCalcResult.putAll(upTimeCalc.get(oneSuperColumn)
						.getRowColumns());
				upTimeCalcResult.add(oneUpTimeCalcResult);
				oneUpTimeCalcResult = null;

			}
		}

		return upTimeCalcResult;
	}

	public List<Map<String, String>> getUpTimeCalc(String monitorId,
			String startTimeStamp, String endTimeStamp)
			throws InterruptedException, ExecutionException, Exception {
		log.info("Get Up time calc for :" + monitorId + " " + startTimeStamp
				+ " " + endTimeStamp);
		List<Map<String, String>> upTimeCalcResult = new ArrayList<Map<String, String>>();

		SuperColumnRangeSliceQuery query = new SuperColumnRangeSliceQuery();
		query.setElasticCluster(cluster);
		query.setColumnFamilyName(UP_TIME_CALCULATION_SUPER);
		query.setKeyspaceName(UP_TIME_CALC_DATA);

		query.setOneKey(monitorId);
		query.setReverseResultOrder(false);
		query.setStartSuperColumn(startTimeStamp);
		query.setEndSuperColumn(endTimeStamp);
		Map<String, SuperColumnWithSuperColumnRow> resultSet = query
				.executeQuery().getQueryResult();

		if (!resultSet.isEmpty()) {
			Map<String, SuperColumnRow> oneSuperColumnResult = resultSet.get(
					monitorId).getSuperColumnWithSuperColumnRow();

			for (String oneSuperColumn : resultSet.keySet())

			{
				Map<String, String> oneUpTimeCalcResult = new HashMap<String, String>();
				oneUpTimeCalcResult.put(START_TIME_STAMP, oneSuperColumn);

				oneUpTimeCalcResult.putAll(oneSuperColumnResult.get(
						oneSuperColumn).getRowColumns());
				upTimeCalcResult.add(oneUpTimeCalcResult);
				oneUpTimeCalcResult = null;

			}
		}

		return upTimeCalcResult;
	}

	public List<Map<String, String>> getPollStateErrors(String instanceId,
			long startTimeStamp, long endTimeStamp)
			throws InterruptedException, ExecutionException {

		SuperColumnRangeSliceQuery query = new SuperColumnRangeSliceQuery();
		query.setElasticCluster(cluster);
		query.setColumnFamilyName(POLL_STATE_ERRORS);
		query.setKeyspaceName(UP_TIME_CALC_DATA);

		query.setOneKey(instanceId);
		query.setReverseResultOrder(false);
		query.setStartSuperColumn(String.valueOf(startTimeStamp));
		query.setEndSuperColumn(String.valueOf(endTimeStamp));
		Map<String, SuperColumnWithSuperColumnRow> resultSet = query
				.executeQuery().getQueryResult();

		List<Map<String, String>> pollStateErrorsResult = new ArrayList<Map<String, String>>();

		if (!resultSet.isEmpty()) {
			Map<String, SuperColumnRow> pollStateErrors = resultSet.get(
					instanceId).getSuperColumnWithSuperColumnRow();

			for (String oneSuperColumn : pollStateErrors.keySet()) {
				Map<String, String> onePollStateErrorResult = new HashMap<String, String>();
				onePollStateErrorResult.put(START_TIME_STAMP, oneSuperColumn);

				onePollStateErrorResult.putAll(pollStateErrors.get(
						oneSuperColumn).getRowColumns());
				pollStateErrorsResult.add(onePollStateErrorResult);
				onePollStateErrorResult = null;

			}
		}

		return pollStateErrorsResult;
	}

	public List<Map<String, String>> getSupressionIntervals(String instanceId,
			long startTimeStamp, long endTimeStamp)
			throws InterruptedException, ExecutionException {

		SuperColumnRangeSliceQuery query = new SuperColumnRangeSliceQuery();
		query.setElasticCluster(this.cluster);
		query.setColumnFamilyName(SUPRESSION_INTERVALS);
		query.setKeyspaceName(UP_TIME_CALC_DATA);

		query.setOneKey(instanceId);
		query.setReverseResultOrder(false);
		query.setStartSuperColumn(String.valueOf(startTimeStamp));
		query.setEndSuperColumn(String.valueOf(endTimeStamp));
		Map<String, SuperColumnWithSuperColumnRow> resultSet = query
				.executeQuery().getQueryResult();

		List<Map<String, String>> supressionIntervalsResult = new ArrayList<Map<String, String>>();

		if (!resultSet.isEmpty()) {
			Map<String, SuperColumnRow> supressionIntervals = resultSet.get(
					instanceId).getSuperColumnWithSuperColumnRow();

			for (String oneSuperColumn : supressionIntervals.keySet()) {
				Map<String, String> oneSupressionIntervalResult = new HashMap<String, String>();
				oneSupressionIntervalResult.put(START_TIME_STAMP,
						oneSuperColumn);

				oneSupressionIntervalResult.putAll(supressionIntervals.get(
						oneSuperColumn).getRowColumns());
				supressionIntervalsResult.add(oneSupressionIntervalResult);
				oneSupressionIntervalResult = null;

			}
		}

		return supressionIntervalsResult;
	}

	public Map<String, Map<String, Map<String, String>>> getAllSupression(
			List<String> allDevices, long startTs, long endTs) throws Exception {

		MultiGetSuperSliceQuery query = new MultiGetSuperSliceQuery();
		query.setElasticCluster(this.cluster);
		query.setColumnFamilyName(SUPRESSION_INTERVALS);
		query.setKeyspaceName(UP_TIME_CALC_DATA);
		query.setKeys(allDevices);
		query.setReverseResultOrder(false);
		query.setStartSuperColumn(String.valueOf(startTs));
		query.setEndSuperColumn(String.valueOf(endTs));

		Map<String, Map<String, Map<String, String>>> allSupressionsResult = query
				.executeQuery();

		log.debug("Collecting previus supressions. Current result size :"
				+ allSupressionsResult.size());

		MultiGetSuperSliceQuery query1 = new MultiGetSuperSliceQuery();
		query1.setElasticCluster(cluster);
		query1.setColumnFamilyName(SUPRESSION_INTERVALS);
		query1.setKeyspaceName(UP_TIME_CALC_DATA);

		query1.setKeys(allDevices);
		query1.setReverseResultOrder(true);
		query1.setStartSuperColumn(String.valueOf(startTs - 1));
		query1.setEndSuperColumn("0");
		Map<String, Map<String, Map<String, String>>> allPreviousSupressionsResult = query1
				.getPreviousSuperColumn();

		log.debug("previous collected ");
		Map<String, Map<String, Map<String, String>>> validPreviousSupressionsResult = new LinkedHashMap<String, Map<String, Map<String, String>>>();

		Iterator<Entry<String, Map<String, Map<String, String>>>> allPreviousSupressionsResultIterator = allPreviousSupressionsResult
				.entrySet().iterator();

		while (allPreviousSupressionsResultIterator.hasNext()) {
			Entry<String, Map<String, Map<String, String>>> onePreviousRecord = allPreviousSupressionsResultIterator
					.next();

			String previousStratTime = onePreviousRecord.getValue().keySet()
					.iterator().next();
			String previousEndTime = onePreviousRecord.getValue()
					.get(previousStratTime).get(END_TIME);

			if (Long.valueOf(previousEndTime) <= startTs
					&& Long.valueOf(previousEndTime) > 0) {

			} else {
				if (Long.valueOf(previousStratTime) >= endTs) {

				} else {
					validPreviousSupressionsResult.put(
							onePreviousRecord.getKey(),
							onePreviousRecord.getValue());

				}
			}
		}
		log.debug("Valid size:" + validPreviousSupressionsResult.size());
		Iterator<Entry<String, Map<String, Map<String, String>>>> validPreviousSupressionsResultIterator = validPreviousSupressionsResult
				.entrySet().iterator();

		while (validPreviousSupressionsResultIterator.hasNext()) {
			Entry<String, Map<String, Map<String, String>>> onePrevious = validPreviousSupressionsResultIterator
					.next();

			String previousRec = onePrevious.getKey();
			Map<String, Map<String, String>> supressionDetails = onePrevious
					.getValue();

			Iterator<Entry<String, Map<String, Map<String, String>>>> supressionResultIterator = allSupressionsResult
					.entrySet().iterator();

			boolean hasErrors = false;
			while (supressionResultIterator.hasNext()) {
				Entry<String, Map<String, Map<String, String>>> oneSupressionEntry = supressionResultIterator
						.next();

				if (oneSupressionEntry.getKey().compareTo(previousRec) == 0) {
					supressionResultIterator.remove();
					Map<String, Map<String, String>> oneMonitorAllSupression = new LinkedHashMap<String, Map<String, String>>();
					oneMonitorAllSupression.putAll(supressionDetails);

					oneMonitorAllSupression.putAll(oneSupressionEntry
							.getValue());

					Map<String, Map<String, Map<String, String>>> completeSupression = new HashMap<String, Map<String, Map<String, String>>>();
					completeSupression
							.put(previousRec, oneMonitorAllSupression);

					allSupressionsResult.putAll(completeSupression);
					hasErrors = true;
					break;
				}

			}
			if (!hasErrors) {
				allSupressionsResult.put(previousRec, supressionDetails);
			}
		}
		log.debug("all previous collected. Result size is: "
				+ allSupressionsResult.size());

		return allSupressionsResult;

	}

	public Map<String, Map<String, Map<String, String>>> getAllPollStateErrors(
			List<String> allActiveMonitors, long startTs, long endTs)
			throws Exception {
		log.debug("Inside get ALL Errors");

		MultiGetSuperSliceQuery query = new MultiGetSuperSliceQuery();

		query.setElasticCluster(cluster);
		query.setColumnFamilyName(POLL_STATE_ERRORS);
		query.setKeyspaceName(UP_TIME_CALC_DATA);

		query.setKeys(allActiveMonitors);
		query.setReverseResultOrder(false);
		query.setStartSuperColumn(String.valueOf(startTs));
		query.setEndSuperColumn(String.valueOf(endTs));
		// query.setRowCount(allActiveMonitors.size());

		log.debug("Get all poll state errors query completed");
		Map<String, Map<String, Map<String, String>>> allPollStateErrorsResult = query
				.executeQuery();

		log.debug("Collecting previus errors. Current result size :"
				+ allPollStateErrorsResult.size());

		MultiGetSuperSliceQuery query1 = new MultiGetSuperSliceQuery();
		query1.setElasticCluster(cluster);
		query1.setColumnFamilyName(POLL_STATE_ERRORS);
		query1.setKeyspaceName(UP_TIME_CALC_DATA);

		query1.setKeys(allActiveMonitors);
		query1.setReverseResultOrder(true);
		query1.setStartSuperColumn(String.valueOf(startTs - 1));

		query1.setEndSuperColumn("0");

		Map<String, Map<String, Map<String, String>>> allPreviousPollStateErrorsResult = query1
				.getPreviousSuperColumn();

		Map<String, Map<String, Map<String, String>>> validPreviousPollStateErrorsResult = new LinkedHashMap<String, Map<String, Map<String, String>>>();
		Iterator<Entry<String, Map<String, Map<String, String>>>> allPreviousIterator = allPreviousPollStateErrorsResult
				.entrySet().iterator();
		while (allPreviousIterator.hasNext()) {
			Entry<String, Map<String, Map<String, String>>> previousErrorPoll = allPreviousIterator
					.next();

			String previousStratTime = previousErrorPoll.getValue().keySet()
					.iterator().next();
			String previousEndTime = previousErrorPoll.getValue()
					.get(previousStratTime).get(END_TIME);

			if (Long.valueOf(previousEndTime) <= startTs
					&& Long.valueOf(previousEndTime) > 0) {

			} else {
				if (Long.valueOf(previousStratTime) >= endTs) {

				} else {
					validPreviousPollStateErrorsResult.put(
							previousErrorPoll.getKey(),
							previousErrorPoll.getValue());

				}

			}
		}
		log.debug("Valid size:" + validPreviousPollStateErrorsResult.size());
		Iterator<Entry<String, Map<String, Map<String, String>>>> validPreviousPollStateErrorsResultIterator = validPreviousPollStateErrorsResult
				.entrySet().iterator();
		while (validPreviousPollStateErrorsResultIterator.hasNext()) {
			Entry<String, Map<String, Map<String, String>>> onePrevious = validPreviousPollStateErrorsResultIterator
					.next();

			String previousRec = onePrevious.getKey();
			Iterator<Entry<String, Map<String, Map<String, String>>>> pollStateErrorsResultIterator = allPollStateErrorsResult
					.entrySet().iterator();
			boolean hasErrors = false;
			while (pollStateErrorsResultIterator.hasNext()) {
				Entry<String, Map<String, Map<String, String>>> oneErrorPoll = pollStateErrorsResultIterator
						.next();
				if (oneErrorPoll.getKey().compareTo(previousRec) == 0) {

					pollStateErrorsResultIterator.remove();
					Map<String, Map<String, String>> oneMonitorAllErrors = new LinkedHashMap<String, Map<String, String>>();
					oneMonitorAllErrors.putAll(onePrevious.getValue());

					oneMonitorAllErrors.putAll(oneErrorPoll.getValue());

					Map<String, Map<String, Map<String, String>>> completeError = new LinkedHashMap<String, Map<String, Map<String, String>>>();
					completeError.put(previousRec, oneMonitorAllErrors);

					allPollStateErrorsResult.putAll(completeError);
					hasErrors = true;
					break;
				}

			}
			if (!hasErrors) {
				allPollStateErrorsResult.put(previousRec,
						onePrevious.getValue());
			}
		}
		log.debug("All previous collected. Result size is: "
				+ allPollStateErrorsResult.size());
		return allPollStateErrorsResult;
	}

	public Map<String, Map<String, Map<String, String>>> getAllHourlyUpTimeCalc(
			List<String> allActiveMonitors, long startTs, long endTs)
			throws Exception {

		MultiGetSuperSliceQuery query = new MultiGetSuperSliceQuery();

		query.setElasticCluster(cluster);
		query.setColumnFamilyName(UP_TIME_CALCULATION_SUPER);
		query.setKeyspaceName(UP_TIME_CALC_DATA);

		query.setKeys(allActiveMonitors);
		query.setReverseResultOrder(false);
		query.setStartSuperColumn(String.valueOf(startTs));
		query.setEndSuperColumn(String.valueOf(endTs));

		log.warn("Get hourly up time calc query completed");
		Map<String, Map<String, Map<String, String>>> allHourlyUpTimeResult = query
				.executeQuery();
		if (allHourlyUpTimeResult != null) {
			log.warn("Collected Hourly. Current result size :"
					+ allHourlyUpTimeResult.size());
		}

		return allHourlyUpTimeResult;
	}

	public Map<String, Map<String, Map<String, String>>> getAllDailyUpTimeCalc(
			List<String> allActiveMonitors, long startTs, long endTs)
			throws Exception {

		MultiGetSuperSliceQuery query = new MultiGetSuperSliceQuery();

		query.setElasticCluster(cluster);
		query.setColumnFamilyName(UP_TIME_CALCULATION__DAILY_SUPER);
		query.setKeyspaceName(UP_TIME_CALC_DATA);

		query.setKeys(allActiveMonitors);
		query.setReverseResultOrder(false);
		query.setStartSuperColumn(String.valueOf(startTs));
		query.setEndSuperColumn(String.valueOf(endTs));

		log.warn("Get daily up time calc query completed");
		Map<String, Map<String, Map<String, String>>> allDailyUpTimeResult = query
				.executeQuery();

		log.warn("Collected Daily. Current result size :"
				+ allDailyUpTimeResult.size());

		return allDailyUpTimeResult;
	}

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

	public String getInstanceFirstStatusTimeStamp(String instanceKey) {

		// Map<String, ElasticCluster> clusters = cassandraElasticService
		// .getDataCluster(UP_TIME_CALC_DATA, INSTANCE_STATUS_SUPER);

		SuperColumnRangeSliceQuery query = new SuperColumnRangeSliceQuery();
		query.setElasticCluster(cluster);
		query.setColumnFamilyName(INSTANCE_STATUS_SUPER);
		query.setKeyspaceName(UP_TIME_CALC_DATA);

		query.setOneKey(instanceKey);
		query.setReverseResultOrder(false);

		long startTS = 0;

		Map<String, SuperColumnWithSuperColumnRow> resultSet = query
				.executeQuery().getQueryResult();

		if (!resultSet.isEmpty()) {
			Map<String, SuperColumnRow> instanceStatus = resultSet.get(
					instanceKey).getSuperColumnWithSuperColumnRow();

			for (String statusTs : instanceStatus.keySet()) {
				if (startTS == 0) {
					startTS = Long.valueOf(statusTs);
				} else {
					if (Long.valueOf(statusTs) < startTS) {
						startTS = Long.valueOf(statusTs);
					}
				}

			}

		}
		return (String.valueOf(startTS));

	}

	public Map<String, String> getInstanceLastStatus(String instanceKey) {

		SuperColumnRangeSliceQuery query = new SuperColumnRangeSliceQuery();
		query.setElasticCluster(cluster);
		query.setColumnFamilyName(INSTANCE_STATUS_SUPER);
		query.setKeyspaceName(UP_TIME_CALC_DATA);

		query.setOneKey(instanceKey);
		query.setReverseResultOrder(true);
		query.setStartSuperColumn(String.valueOf(Calendar.getInstance()
				.getTimeInMillis() / 1000));
		query.setEndSuperColumn(String.valueOf(0));

		Map<String, String> lastStatus = new LinkedHashMap<String, String>();

		long startTS = 0;

		Map<String, SuperColumnWithSuperColumnRow> resultSet = query
				.executeQuery().getQueryResult();

		if (!resultSet.isEmpty()) {
			Map<String, SuperColumnRow> instanceStatus = resultSet.get(
					instanceKey).getSuperColumnWithSuperColumnRow();

			for (String startTime : instanceStatus.keySet()) {
				if (lastStatus.isEmpty()) {
					lastStatus.put(START_TIME_STAMP, startTime);
					startTS = Long.valueOf(startTime);
					lastStatus.putAll(instanceStatus.get(startTime)
							.getRowColumns());
				} else {
					if (Long.valueOf(startTime) > startTS) {
						lastStatus.clear();
						lastStatus.put(START_TIME_STAMP, startTime);
						startTS = Long.valueOf(startTime);
						lastStatus.putAll(instanceStatus.get(startTime)
								.getRowColumns());
					}
				}

			}

		}
		return lastStatus;

	}

	public UpTimeCalculationDaoImpl(String clusterName, List<String> seeds) {
		super();
		this.clusterName = clusterName;
		this.seeds = seeds;
		this.createElasticCluster();
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

}
