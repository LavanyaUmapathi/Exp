package com.rackspace.foundation.up.time.calculation;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

import com.rackspace.foundation.informatica.cassandra.direct.loader.api.impl.InformaticaCassandraBulkLoader;
import com.rackspace.foundation.up.time.calculation.dao.impl.UpTimeCalculationDaoImpl;

public class UpTimeCalculationExec {

	private UpTimeCalculationDaoImpl upTimeCalculationDaoImpl;

	

	
	private String clusterName;
	
	private String rootIP;
	
	private String csvDir;
	private boolean writeToFile;
	private FileWriter writer;

	private FileWriter dailyWriter;
	private FileWriter monthlyWriter;

	private String deviceID;

	private String previousDeviceID;

	private String sourceSystemName;

	private InformaticaCassandraBulkLoader informaticaCassandraLoader;

	
	private InformaticaCassandraBulkLoader informaticaCassandraLoaderDaily;

	
	private InformaticaCassandraBulkLoader informaticaCassandraLoaderMonthly;

	

	private Map<String, Map<String, Map<String, String>>> allPollStateErrors;
	private Map<String, Map<String, Map<String, String>>> allSupressions;



	private List<String> allEanbledMonitorsKeys;

	private int fileRowCount = 0;

	private int dailyFileRowCount = 0;

	private int monthlyFileRowCount = 0;

	private long hourlyEndTimeStamp;

	private long dailyEndTimeStamp;

	private List<Map<String, String>> intervalSupressions;

	private static final String START_TIME_STAMP = "StartTimeStamp";
	private static final String END_TIME = "EndTime";
	private static final String POLL_DURATION = "PollDuration";
	private static final String STATUS = "Status";
	private static final String TOTAL_DOWN_TIME = "TotalDownTime";
	private static final String CALCULATED_DOWN_TIME = "CalculatedDownTime";
	private static final String AGREED_UP_TIME = "AgreedUpTime";
	private static final String TOTAL_AVAILABILITY_PERCENT = "TotalAvailabilityPercent";
	private static final String CALCULATED_AVAILABILITY_PERCENT = "CalculatedAvailabilityPercent";
	private static final String NUMBER_OF_ERROR_POLLS = "NumberOfErrorPolls";
	private static final String SUPRESSION_DURATION = "SupressionDuration";
	private static final String UP_TIME_DURATION = "UpTimeDuration";

	private static final String COMPUTER_NO_LONGER_ACTIVE = "Computer No Longer Active";

	private static final String KEYSPACE_NAME = "UpTimeCalcData";
	private static final String UP_TIME_CALCULATION_CF = "UpTimeCalculationSuper";
	private static final String UP_TIME_CALCULATION_DAILY_CF = "UpTimeCalculationDailySuper";
	private static final String UP_TIME_CALCULATION_MONTHLY_CF = "UpTimeCalculationMonthlySuper";
	private static final String POLL_STATE_ERRORS_CF = "PollStateErrors";
	private static final int MONITOR_BATCH_SIZE = 40;

	private static final int FILE_ROW_COUNT_LIMIT = 3000000;

	private static final long NUMBER_OF_SECONDS_IN_DAY = 86400;
	private static final long NUMBER_OF_SECONDS_IN_HOUR = 3600;
	private static final long NUMBER_OF_SECONDS_IN_MINUTE = 60;

	private static final String INCREMENTAL_CALCULATION = "incremental";
	private static final String INITIAL_CALCULATION = "initial";

	private String calculationType = INCREMENTAL_CALCULATION;

	private static org.apache.log4j.Logger log = Logger
			.getLogger(UpTimeCalculationExec.class);

//	private void executeCalculation(String oneKey, String startTs, String endTs)
//			throws Exception {
//	
//		long startTsLong = Long.valueOf(startTs);
//		long endTsLong = Long.valueOf(endTs);
//
//		
//		log.info("Get Errors for monitor:" + oneKey);
//		List<Map<String, String>> pollStateErrors = upTimeCalculationDaoImpl
//				.getPollStateErrors(oneKey, startTsLong, endTsLong);
//		
//		log.info("Got ERRORS for monitor:" + oneKey + " "
//				+ pollStateErrors.size());
//		log.info("Get PREVIOUS Errors for monitor:" + oneKey);
//		Map<String, String> previousPollStateErrors = upTimeCalculationDaoImpl
//				.getPreviousPollStateError(oneKey, startTsLong - 1, 0);
//		log.info("Got PREVIOUS Errors for monitor:" + oneKey + " "
//				+ previousPollStateErrors.size());
//		if (!previousPollStateErrors.isEmpty()) {
//			if ((Long.valueOf(previousPollStateErrors.get(START_TIME_STAMP))
//					+ Long.valueOf(previousPollStateErrors.get(POLL_DURATION))
//							.longValue() > startTsLong)
//					|| Long.valueOf(previousPollStateErrors.get(POLL_DURATION))
//							.longValue() == 0) {
//				pollStateErrors.add(0, previousPollStateErrors);
//			}
//
//		}
//
//		if (!pollStateErrors.isEmpty()) {
//
//			if (this.deviceID.compareTo(this.previousDeviceID) != 0) {
//				this.intervalSupressions = new ArrayList<Map<String, String>>();
//				this.previousDeviceID = this.deviceID;
//				log.info("Get Supression for device:" + deviceID);
//				intervalSupressions = upTimeCalculationDaoImpl
//						.getSupressionIntervals(deviceID, startTsLong,
//								endTsLong);
//				log.info("Got Supression for device:" + deviceID + ":"
//						+ intervalSupressions.size());
//
//				log.info("Get PREVIOUS Supression for device:" + deviceID);
//				Map<String, String> previousSupression = upTimeCalculationDaoImpl
//						.getPreviousSupressionInterval(deviceID,
//								startTsLong - 1, 0);
//				log.info("Got PREVIOUS Supression for device:" + deviceID + ":"
//						+ previousSupression.size());
//				if (!previousSupression.isEmpty()) {
//					if ((Long.valueOf(previousSupression.get(START_TIME_STAMP))
//							+ Long.valueOf(
//									previousSupression.get(SUPRESSION_DURATION))
//									.longValue() > startTsLong)
//							|| Long.valueOf(
//									previousSupression.get(SUPRESSION_DURATION))
//									.longValue() == 0) {
//						intervalSupressions.add(0, previousSupression);
//					}
//				}
//			} else {
//				log.info("Same device no need to get supressions");
//			}
//
//		}
//
//		calculateOneMonitorDevice(oneKey, startTsLong, endTsLong,
//				pollStateErrors, intervalSupressions);
//	}

	@SuppressWarnings("static-access")
	private void calculateOneMonitorDevice(String monitorDeviceKey,
			long startTsLong, long endTsLong,
			List<Map<String, String>> pollStateErrors,
			List<Map<String, String>> intervalSupressions) throws Exception {

		log.info(monitorDeviceKey + " error number: " + pollStateErrors.size()
				+ " supression number :" + intervalSupressions.size());

		long currentStartTime = startTsLong;
		long currentEndTime = currentStartTime + 3600;
		long numberOfErrors = 0;

		long dailyIntervalDownTime = 0;
		long dailyTotalDownTime = 0;

		long monthlyIntervalDownTime = 0;
		long monthlyTotalDownTime = 0;

		long dailyAgreedUpTime = 0;
		long monthlyAgreedUpTime = 0;

		float dailyTotalAvailabilityPercent = 0;
		float dailyCalculatedAvailabilityPercent = 0;

		float monthlyTotalAvailabilityPercent = 0;
		float monthlyCalculatedAvailabilityPercent = 0;

		long dailyStartTimeStamp = currentStartTime;
		long monthlyStartTimeStamp = currentStartTime;

		Calendar monthlyCalendar = Calendar.getInstance();

		Calendar dailyCalendar = Calendar.getInstance();

		monthlyCalendar.setTimeInMillis(monthlyStartTimeStamp * 1000);

		dailyCalendar.setTimeInMillis(dailyStartTimeStamp * 1000);

		if (monthlyCalendar.get(Calendar.DAY_OF_MONTH) != 1
				|| monthlyCalendar.get(Calendar.HOUR_OF_DAY) != 0
				|| monthlyCalendar.get(Calendar.MINUTE) != 0
				|| monthlyCalendar.get(Calendar.SECOND) != 0)

		{
			monthlyCalendar.set(Calendar.DAY_OF_MONTH, 1);
			monthlyCalendar.set(Calendar.HOUR_OF_DAY, 0);
			monthlyCalendar.set(Calendar.MINUTE, 0);
			monthlyCalendar.set(Calendar.SECOND, 0);
			monthlyStartTimeStamp = monthlyCalendar.getTimeInMillis() / 1000;
		}

		if (dailyCalendar.get(Calendar.HOUR_OF_DAY) != 0
				|| dailyCalendar.get(Calendar.MINUTE) != 0
				|| dailyCalendar.get(Calendar.SECOND) != 0)

		{

			dailyCalendar.set(Calendar.HOUR_OF_DAY, 0);
			dailyCalendar.set(Calendar.MINUTE, 0);
			dailyCalendar.set(Calendar.SECOND, 0);
			dailyStartTimeStamp = dailyCalendar.getTimeInMillis() / 1000;
		}

		long dailyNumberOfErrors = 0;
		long monthlyNumberOfErrors = 0;
		long numberOfSecondsInMonth = 0;
		long numberOfSecondsInDay = 0;

		boolean firstRecord = true;

	

		while (currentStartTime < endTsLong) {
			numberOfErrors = 0;
			log.info(currentStartTime);
			log.info(currentEndTime);
			long aggreedTimeDecrement = 0;
			if (intervalSupressions.size() > 0) {
				aggreedTimeDecrement = getSupressionDecrement(
						intervalSupressions, currentStartTime, currentEndTime);
			}

			long intervalDownTime = 0;
			long totalDownTime = 0;

			for (Map<String, String> oneError : pollStateErrors) {
				long pollStartTime = Long.valueOf(
						oneError.get(START_TIME_STAMP)).longValue();
				long pollEndTime = Long.valueOf(oneError.get(END_TIME))
						.longValue();
				long pollDuration = Long.valueOf(oneError.get(POLL_DURATION))
						.longValue();
				if (pollDuration >= 0) {
					if (pollEndTime == 0) {
						pollEndTime = Calendar.getInstance().getTimeInMillis() / 1000;
						
					}
					if (pollStartTime >= currentEndTime
							|| pollEndTime <= currentStartTime) {

					} else {
						numberOfErrors++;

						if (pollDuration == 0
								&& Long.valueOf(oneError.get(END_TIME))
										.longValue() == 0) {
							pollDuration = 3600;
						}
						if (pollStartTime <= currentStartTime
								&& pollEndTime >= currentEndTime) {
							totalDownTime = 3600;
							intervalDownTime = 3600;
							long supresionDecrement = 0;
							if (intervalSupressions.size() > 0) {
								supresionDecrement = getSupressionDecrement(
										intervalSupressions, currentStartTime,
										currentEndTime);

								intervalDownTime = 3600 - supresionDecrement;
							}
							break;
						} else {
							if (pollStartTime >= currentStartTime
									&& pollEndTime <= currentEndTime) {
								totalDownTime = totalDownTime + pollDuration;
								intervalDownTime = intervalDownTime
										+ pollDuration;
								long supresionDecrement = 0;
								if (intervalSupressions.size() > 0) {
									supresionDecrement = getSupressionDecrement(
											intervalSupressions, pollStartTime,
											pollEndTime);

									intervalDownTime = intervalDownTime
											- supresionDecrement;
								}
							} else {
								if (pollStartTime <= currentStartTime
										&& pollEndTime <= currentEndTime
										&& pollEndTime >= currentStartTime) {
									totalDownTime = totalDownTime
											+ (pollEndTime - currentStartTime);
									intervalDownTime = intervalDownTime
											+ (pollEndTime - currentStartTime);

									long supresionDecrement = 0;
									if (intervalSupressions.size() > 0) {

										supresionDecrement = getSupressionDecrement(
												intervalSupressions,
												currentStartTime, pollEndTime);

										intervalDownTime = intervalDownTime
												- supresionDecrement;
									}
								} else {
									if (pollStartTime >= currentStartTime
											&& pollEndTime >= currentEndTime
											&& pollStartTime <= currentEndTime) {
										totalDownTime = totalDownTime
												+ (currentEndTime - pollStartTime);
										intervalDownTime = intervalDownTime
												+ (currentEndTime - pollStartTime);
										long supresionDecrement = 0;
										if (intervalSupressions.size() > 0) {
											supresionDecrement = getSupressionDecrement(
													intervalSupressions,
													pollStartTime,
													currentEndTime);

											intervalDownTime = intervalDownTime
													- supresionDecrement;
										}
									}
								}
							}
						}

					}
				} else {
					log.warn("NEGATIVE POLL DURATION: " + monitorDeviceKey);
				}
			}
			// Get agreed up time
			long agreedUpTime = 3600 - aggreedTimeDecrement;
			if (agreedUpTime < 0) {
				agreedUpTime = 0;
			}
			if (agreedUpTime > 3600) {
				agreedUpTime = 3600;
			}
			if (totalDownTime > 3600) {
				totalDownTime = 3600;
			}
			if (intervalDownTime > 3600) {
				intervalDownTime = 3600;
			}

			if (totalDownTime < 0) {
				totalDownTime = 0;
			}
			if (intervalDownTime < 0) {
				intervalDownTime = 0;
			}
			float totalAvailabilityPercent = (float) 1
					- ((float) totalDownTime / (float) 3600);
			float calculatedAvailabilityPercent = 1;
			if (agreedUpTime > 0) {
				calculatedAvailabilityPercent = (float) 1
						- ((float) intervalDownTime / (float) agreedUpTime);
			}
			if (totalAvailabilityPercent < 0.001) {
				log.info("Rounding totalAvailabilityPercent "+String.valueOf(totalAvailabilityPercent)+" to 0");
				totalAvailabilityPercent = 0;
			}
			if (calculatedAvailabilityPercent < 0.001) {
				log.info("Rounding calculatedAvailabilityPercent "+String.valueOf(calculatedAvailabilityPercent)+" to 0");

				calculatedAvailabilityPercent = 0;
			}
			// Load row
			Map<String, String> oneSuperColumn = new LinkedHashMap<String, String>();
			oneSuperColumn.put(TOTAL_DOWN_TIME, String.valueOf(totalDownTime));
			oneSuperColumn.put(CALCULATED_DOWN_TIME,
					String.valueOf(intervalDownTime));
			oneSuperColumn.put(AGREED_UP_TIME, String.valueOf(agreedUpTime));
			oneSuperColumn.put(TOTAL_AVAILABILITY_PERCENT,
					String.valueOf(totalAvailabilityPercent));
			oneSuperColumn.put(CALCULATED_AVAILABILITY_PERCENT,
					String.valueOf(calculatedAvailabilityPercent));
			oneSuperColumn.put(NUMBER_OF_ERROR_POLLS,
					String.valueOf(numberOfErrors));
		
			log.info("Loading row for:" + monitorDeviceKey + " SUPERCOLUMN:"
					+ String.valueOf(currentStartTime));
			informaticaCassandraLoader.loadRow(monitorDeviceKey,
					String.valueOf(currentStartTime), Long.valueOf(0),
					oneSuperColumn);
			if (this.writeToFile) {
				writer.append(monitorDeviceKey);
				writer.append(",");
				writer.append(String.valueOf(currentStartTime));
				writer.append(",");
				writer.append(String.valueOf(this.NUMBER_OF_SECONDS_IN_HOUR));
				writer.append(",");
				writer.append(String.valueOf(totalDownTime));
				writer.append(",");
				writer.append(String.valueOf(intervalDownTime));
				writer.append(",");
				writer.append(String.valueOf(agreedUpTime));
				writer.append(",");
				writer.append(String.valueOf(totalAvailabilityPercent));
				writer.append(",");
				writer.append(String.valueOf(calculatedAvailabilityPercent));
				writer.append("\n");
				writer.flush();
				this.setFileRowCount(this.getFileRowCount() + 1);
				if (this.getFileRowCount() > FILE_ROW_COUNT_LIMIT) {
					writer.close();
					Calendar c = Calendar.getInstance();
					String baseFileName = System
							.getenv("CASSANDRA_BULK_LOAD_DIR")
							+ "/"
							+ this.csvDir
							+ "/UpTimeCalc_Hourly_"
							+ this.getSourceSystemName()
							+ String.valueOf(c.getTimeInMillis() / 1000)
							+ ".csv";
					this.setWriter(new FileWriter(baseFileName, true));
					this.setFileRowCount(0);

				}

			}

			currentStartTime = currentEndTime;

			currentEndTime = currentEndTime + 3600;
//
//			if (this.getCalculationType().compareTo(this.INITIAL_CALCULATION) == 0) {
//				numberOfSecondsInMonth = numberOfSecondsInMonth
//						+ this.NUMBER_OF_SECONDS_IN_HOUR;
//				numberOfSecondsInDay = numberOfSecondsInDay
//						+ this.NUMBER_OF_SECONDS_IN_HOUR;
//				dailyTotalDownTime = dailyTotalDownTime + totalDownTime;
//				monthlyTotalDownTime = monthlyTotalDownTime + totalDownTime;
//
//				dailyIntervalDownTime = dailyIntervalDownTime
//						+ intervalDownTime;
//				monthlyIntervalDownTime = monthlyIntervalDownTime
//						+ intervalDownTime;
//
//				dailyAgreedUpTime = dailyAgreedUpTime + agreedUpTime;
//				monthlyAgreedUpTime = monthlyAgreedUpTime + agreedUpTime;
//
//				dailyNumberOfErrors = dailyNumberOfErrors + numberOfErrors;
//				monthlyNumberOfErrors = monthlyNumberOfErrors + numberOfErrors;
//
//				Calendar c = Calendar.getInstance();
//
//				c.setTimeInMillis(currentStartTime * 1000);
//
//				if (c.get(Calendar.HOUR_OF_DAY) == 0 && !firstRecord) {
//					if (numberOfSecondsInDay > this.NUMBER_OF_SECONDS_IN_DAY) {
//						numberOfSecondsInDay = this.NUMBER_OF_SECONDS_IN_DAY;
//					}
//					if (dailyTotalDownTime > this.NUMBER_OF_SECONDS_IN_DAY) {
//						dailyTotalDownTime = this.NUMBER_OF_SECONDS_IN_DAY;
//					}
//
//					if (dailyAgreedUpTime > this.NUMBER_OF_SECONDS_IN_DAY) {
//						dailyAgreedUpTime = this.NUMBER_OF_SECONDS_IN_DAY;
//					}
//					if (dailyIntervalDownTime > this.NUMBER_OF_SECONDS_IN_DAY) {
//						dailyIntervalDownTime = this.NUMBER_OF_SECONDS_IN_DAY;
//					}
//					dailyTotalAvailabilityPercent = (float) 1
//							- ((float) dailyTotalDownTime / (float) numberOfSecondsInDay);
//					dailyCalculatedAvailabilityPercent = 1;
//					if (dailyAgreedUpTime > 0) {
//						dailyCalculatedAvailabilityPercent = (float) 1
//								- ((float) dailyIntervalDownTime / (float) dailyAgreedUpTime);
//					}
//					if (dailyTotalAvailabilityPercent < 0.0001) {
//						dailyTotalAvailabilityPercent = 0;
//					}
//					if (dailyCalculatedAvailabilityPercent < 0.0001) {
//						dailyCalculatedAvailabilityPercent = 0;
//					}
//					// Load row
//					Map<String, String> oneDailySuperColumn = new LinkedHashMap<String, String>();
//					oneDailySuperColumn.put(this.UP_TIME_DURATION,
//							String.valueOf(numberOfSecondsInDay));
//					oneDailySuperColumn.put(TOTAL_DOWN_TIME,
//							String.valueOf(dailyTotalDownTime));
//					oneDailySuperColumn.put(CALCULATED_DOWN_TIME,
//							String.valueOf(dailyIntervalDownTime));
//					oneDailySuperColumn.put(AGREED_UP_TIME,
//							String.valueOf(dailyAgreedUpTime));
//					oneDailySuperColumn.put(TOTAL_AVAILABILITY_PERCENT,
//							String.valueOf(dailyTotalAvailabilityPercent));
//					oneDailySuperColumn.put(CALCULATED_AVAILABILITY_PERCENT,
//							String.valueOf(dailyCalculatedAvailabilityPercent));
//					oneDailySuperColumn.put(NUMBER_OF_ERROR_POLLS,
//							String.valueOf(dailyNumberOfErrors));
//				
//					log.info("Loading DAILY row for:" + monitorDeviceKey
//							+ " SUPERCOLUMN:"
//							+ String.valueOf(dailyStartTimeStamp));
//					this.informaticaCassandraLoaderDaily.loadRow(
//							monitorDeviceKey,
//							String.valueOf(dailyStartTimeStamp),
//							Long.valueOf(0), oneDailySuperColumn);
//					if (this.writeToFile) {
//						this.dailyWriter.append(monitorDeviceKey);
//						this.dailyWriter.append(",");
//						this.dailyWriter.append(String
//								.valueOf(dailyStartTimeStamp));
//						this.dailyWriter.append(",");
//						this.dailyWriter.append(String
//								.valueOf(numberOfSecondsInDay));
//						this.dailyWriter.append(",");
//						this.dailyWriter.append(String
//								.valueOf(dailyTotalDownTime));
//						this.dailyWriter.append(",");
//						this.dailyWriter.append(String
//								.valueOf(dailyIntervalDownTime));
//						this.dailyWriter.append(",");
//						this.dailyWriter.append(String
//								.valueOf(dailyAgreedUpTime));
//						this.dailyWriter.append(",");
//						this.dailyWriter.append(String
//								.valueOf(dailyTotalAvailabilityPercent));
//						this.dailyWriter.append(",");
//						this.dailyWriter.append(String
//								.valueOf(dailyCalculatedAvailabilityPercent));
//						this.dailyWriter.append("\n");
//						this.dailyWriter.flush();
//						this.setDailyFileRowCount(this.getDailyFileRowCount() + 1);
//						if (this.getDailyFileRowCount() > FILE_ROW_COUNT_LIMIT) {
//							this.dailyWriter.close();
//
//							String baseFileName = System
//									.getenv("CASSANDRA_BULK_LOAD_DIR")
//									+ "/"
//									+ this.csvDir
//									+ "/UpTimeCalc_Daily_"
//									+ this.getSourceSystemName()
//									+ String.valueOf(Calendar.getInstance()
//											.getTimeInMillis() / 1000) + ".csv";
//							this.dailyWriter = new FileWriter(baseFileName,
//									true);
//							this.setDailyFileRowCount(0);
//
//						}
//					}
//					dailyTotalDownTime = 0;
//
//					dailyIntervalDownTime = 0;
//					dailyNumberOfErrors = 0;
//					dailyAgreedUpTime = 0;
//					dailyTotalAvailabilityPercent = 0;
//					dailyCalculatedAvailabilityPercent = 0;
//					numberOfSecondsInDay = 0;
//					dailyStartTimeStamp = dailyStartTimeStamp
//							+ this.NUMBER_OF_SECONDS_IN_DAY;
//
//					Calendar checkDailyStatHour = Calendar.getInstance();
//					checkDailyStatHour
//							.setTimeInMillis(dailyStartTimeStamp * 1000);
//					if (checkDailyStatHour.get(Calendar.HOUR_OF_DAY) == 23) {
//						checkDailyStatHour.add(Calendar.HOUR, 1);
//						dailyStartTimeStamp = checkDailyStatHour
//								.getTimeInMillis() / 1000;
//					}
//
//					if (c.get(Calendar.DAY_OF_MONTH) == 1) {
//
//						Calendar checkSec = Calendar.getInstance();
//						checkSec.setTimeInMillis(monthlyStartTimeStamp * 1000);
//						checkSec.add(Calendar.MONTH, 1);
//						checkSec.add(Calendar.HOUR, -6);
//						long numOfSecInMonth = checkSec
//								.get(Calendar.DAY_OF_MONTH)
//								* this.NUMBER_OF_SECONDS_IN_DAY;
//
//						if (monthlyTotalDownTime > numOfSecInMonth) {
//							monthlyTotalDownTime = numOfSecInMonth;
//						}
//						if (numberOfSecondsInMonth > numOfSecInMonth) {
//							numberOfSecondsInMonth = numOfSecInMonth;
//						}
//						if (monthlyAgreedUpTime > numOfSecInMonth) {
//							monthlyAgreedUpTime = numOfSecInMonth;
//						}
//
//						if (monthlyIntervalDownTime > numOfSecInMonth) {
//							monthlyIntervalDownTime = numOfSecInMonth;
//						}
//
//						monthlyTotalAvailabilityPercent = (float) 1
//								- ((float) monthlyTotalDownTime / (float) numberOfSecondsInMonth);
//						monthlyCalculatedAvailabilityPercent = 1;
//						if (monthlyAgreedUpTime > 0) {
//							monthlyCalculatedAvailabilityPercent = (float) 1
//									- ((float) monthlyIntervalDownTime / (float) monthlyAgreedUpTime);
//						}
//						if (monthlyTotalAvailabilityPercent < 0.0001) {
//							monthlyTotalAvailabilityPercent = 0;
//						}
//						if (monthlyCalculatedAvailabilityPercent < 0.0001) {
//							monthlyCalculatedAvailabilityPercent = 0;
//						}
//
//						Map<String, String> oneMonthlySuperColumn = new LinkedHashMap<String, String>();
//						oneMonthlySuperColumn.put(this.UP_TIME_DURATION,
//								String.valueOf(numberOfSecondsInMonth));
//						oneMonthlySuperColumn.put(TOTAL_DOWN_TIME,
//								String.valueOf(monthlyTotalDownTime));
//						oneMonthlySuperColumn.put(CALCULATED_DOWN_TIME,
//								String.valueOf(monthlyIntervalDownTime));
//						oneMonthlySuperColumn.put(AGREED_UP_TIME,
//								String.valueOf(monthlyAgreedUpTime));
//						oneMonthlySuperColumn
//								.put(TOTAL_AVAILABILITY_PERCENT,
//										String.valueOf(monthlyTotalAvailabilityPercent));
//						oneMonthlySuperColumn
//								.put(CALCULATED_AVAILABILITY_PERCENT,
//										String.valueOf(monthlyCalculatedAvailabilityPercent));
//						oneMonthlySuperColumn.put(NUMBER_OF_ERROR_POLLS,
//								String.valueOf(monthlyNumberOfErrors));
//					
//						log.info("Loading MONTHLY row for:" + monitorDeviceKey
//								+ " SUPERCOLUMN:"
//								+ String.valueOf(monthlyStartTimeStamp));
//						this.informaticaCassandraLoaderMonthly.loadRow(
//								monitorDeviceKey,
//								String.valueOf(monthlyStartTimeStamp),
//								Long.valueOf(0), oneMonthlySuperColumn);
//						if (this.writeToFile) {
//							this.monthlyWriter.append(monitorDeviceKey);
//							this.monthlyWriter.append(",");
//							this.monthlyWriter.append(String
//									.valueOf(monthlyStartTimeStamp));
//							this.monthlyWriter.append(",");
//
//							this.monthlyWriter.append(String
//									.valueOf(numberOfSecondsInMonth));
//							this.monthlyWriter.append(",");
//							this.monthlyWriter.append(String
//									.valueOf(monthlyTotalDownTime));
//							this.monthlyWriter.append(",");
//							this.monthlyWriter.append(String
//									.valueOf(monthlyIntervalDownTime));
//							this.monthlyWriter.append(",");
//							this.monthlyWriter.append(String
//									.valueOf(monthlyAgreedUpTime));
//							this.monthlyWriter.append(",");
//							this.monthlyWriter.append(String
//									.valueOf(monthlyTotalAvailabilityPercent));
//							this.monthlyWriter.append(",");
//							this.monthlyWriter
//									.append(String
//											.valueOf(monthlyCalculatedAvailabilityPercent));
//							this.monthlyWriter.append("\n");
//							monthlyWriter.flush();
//							this.setMonthlyFileRowCount(this
//									.getMonthlyFileRowCount() + 1);
//							if (this.getMonthlyFileRowCount() > FILE_ROW_COUNT_LIMIT) {
//								this.monthlyWriter.close();
//
//								String baseFileName = System
//										.getenv("CASSANDRA_BULK_LOAD_DIR")
//										+ "/"
//										+ this.csvDir
//										+ "/UpTimeCalc_Monthly_"
//										+ this.getSourceSystemName()
//										+ String.valueOf(Calendar.getInstance()
//												.getTimeInMillis() / 1000)
//										+ ".csv";
//								this.monthlyWriter = new FileWriter(
//										baseFileName, true);
//								this.setMonthlyFileRowCount(0);
//
//							}
//						}
//						monthlyTotalDownTime = 0;
//
//						monthlyIntervalDownTime = 0;
//						monthlyNumberOfErrors = 0;
//						monthlyAgreedUpTime = 0;
//						monthlyTotalAvailabilityPercent = 0;
//						monthlyCalculatedAvailabilityPercent = 0;
//						monthlyStartTimeStamp = monthlyStartTimeStamp
//								+ numOfSecInMonth;
//
//						Calendar checkMonthlyStatHour = Calendar.getInstance();
//						checkMonthlyStatHour
//								.setTimeInMillis(monthlyStartTimeStamp * 1000);
//						log.warn(monitorDeviceKey);
//						log.warn(checkMonthlyStatHour.getTimeInMillis() / 1000);
//						log.warn(checkMonthlyStatHour.getTimeZone()
//								.getDisplayName());
//						log.warn(checkMonthlyStatHour
//								.get(Calendar.DAY_OF_MONTH));
//						log.warn(checkMonthlyStatHour.get(Calendar.MONTH));
//						log.warn(checkMonthlyStatHour.get(Calendar.HOUR_OF_DAY));
//						if (checkMonthlyStatHour.get(Calendar.HOUR_OF_DAY) == 23) {
//							log.warn("Time Change Month");
//							checkMonthlyStatHour.add(Calendar.HOUR, 1);
//							monthlyStartTimeStamp = checkMonthlyStatHour
//									.getTimeInMillis() / 1000;
//						}
//						numberOfSecondsInMonth = 0;
//
//					}
//
//				} else {
//					firstRecord = false;
//				}
//
//			}

		}


	}

	private long getSupressionDecrement(
			List<Map<String, String>> intervalSupressions, long startTime,
			long endTime) {

		long supressionDecrement = 0;
		for (Map<String, String> oneSupression : intervalSupressions) {
			long supressionStartTime = Long.valueOf(
					oneSupression.get(START_TIME_STAMP)).longValue();
			long supressionEndTime = Long.valueOf(oneSupression.get(END_TIME))
					.longValue();
			if (supressionEndTime == 0) {
				supressionEndTime = Calendar.getInstance().getTimeInMillis() / 1000;
			}

			if (supressionStartTime >= endTime
					|| supressionEndTime <= startTime) {
				log.info("Case 2");
				
			} else {

				long supressionDuration = Long.valueOf(
						oneSupression.get(SUPRESSION_DURATION)).longValue();
				if (supressionDuration == 0
						&& Long.valueOf(oneSupression.get(END_TIME))
								.longValue() == 0) {
					supressionDuration = 3600;
				}
				if (supressionStartTime <= startTime
						&& supressionEndTime >= endTime) {
					log.info("Case 1");
					supressionDecrement = endTime - startTime;
					break;
				} else {
					if (supressionStartTime >= startTime
							&& supressionEndTime <= endTime) {
						log.info("Case 3");
						supressionDecrement = supressionDecrement
								+ supressionDuration;
					} else {
						if (supressionStartTime <= startTime
								&& supressionEndTime <= endTime
								&& supressionEndTime >= startTime) {
							log.info("Case 4");
							supressionDecrement = supressionDecrement
									+ (supressionEndTime - startTime);

						} else {
							if (supressionStartTime >= startTime
									&& supressionEndTime >= endTime
									&& supressionStartTime <= endTime) {
								log.info("Case 5");
								supressionDecrement = supressionDecrement
										+ (endTime - supressionStartTime);

							}
						}

					}
				}

			}
		}
		log.info("Exit:" + supressionDecrement);
		return supressionDecrement;
	}

	

	@SuppressWarnings("static-access")
//	public void initialCalculation() throws Exception {
//
//		
//
//		this.previousDeviceID = "";
//		try {
//			System.out.println(this.clusterName);
//			System.out.println(this.rootIP);
//
//			
//			String startTs = upTimeCalculationDaoImpl.getNextStartTimeStamp(
//					 this.sourceSystemName);
//			String endTs = upTimeCalculationDaoImpl.getEndTimeStamp(
//					 this.sourceSystemName);
//			
//			
//			
//			
//
//			long startTsMod = Long.valueOf(startTs)
//					% this.NUMBER_OF_SECONDS_IN_HOUR;
//			startTs = String.valueOf(Long.valueOf(startTs) - startTsMod);
//
//			long endTSLong = Long.valueOf(endTs).longValue();
//			long endTsMod = endTSLong % this.NUMBER_OF_SECONDS_IN_DAY;
//			endTs = String.valueOf(endTSLong - endTsMod);
//			long dailyEndTS = endTSLong - endTsMod;
//
//			Calendar c1 = Calendar.getInstance();
//
//			c1.setTimeInMillis(dailyEndTS * 1000);
//			int dayNumber = c1.get(Calendar.DAY_OF_MONTH);
//
//			log.warn("Daily end TS:" + dailyEndTS);
//			log.warn("Daily Day of month:" + dayNumber);
//
//			long monthlyEndTs = dailyEndTS
//					- (Long.valueOf(dayNumber - 1) * Long
//							.valueOf(this.NUMBER_OF_SECONDS_IN_DAY));
//			log.warn("Calculated month end TS:" + monthlyEndTs);
//			this.setHourlyEndTimeStamp(Long.valueOf(endTs));
//
//			log.warn("Start time :" + startTs);
//			log.warn("End time :" + endTs);
//			
//
//			
//			informaticaCassandraLoader = new InformaticaCassandraBulkLoader(clusterName, rootIP, KEYSPACE_NAME, UP_TIME_CALCULATION_CF,
//					true);
//			
//
//			
////			informaticaCassandraLoaderDaily = new InformaticaCassandraBulkLoader(clusterName, rootIP, KEYSPACE_NAME,
////					UP_TIME_CALCULATION_DAILY_CF, true);
////			
////
////			
////			informaticaCassandraLoaderMonthly = new InformaticaCassandraBulkLoader(clusterName, rootIP, KEYSPACE_NAME,
////					UP_TIME_CALCULATION_MONTHLY_CF, true);
////					
//
//			if (this.isWriteToFile()) {
//				Calendar c = Calendar.getInstance();
//				String baseFileName = System.getenv("CASSANDRA_BULK_LOAD_DIR")
//						+ "/" + this.csvDir + "/UpTimeCalc_Daily_"
//						+ this.sourceSystemName
//						+ String.valueOf(c.getTimeInMillis() / 1000) + ".csv";
//				this.dailyWriter = new FileWriter(baseFileName, true);
//
//				baseFileName = System.getenv("CASSANDRA_BULK_LOAD_DIR") + "/"
//						+ this.csvDir + "/UpTimeCalc_Monthly_"
//						+ this.sourceSystemName
//						+ String.valueOf(c.getTimeInMillis() / 1000) + ".csv";
//				this.monthlyWriter = new FileWriter(baseFileName, true);
//			}
//
//			log.info(new Date());
//			log.warn("getting all keys");
//
//			List<String> allKeys = upTimeCalculationDaoImpl
//					.getLastEnabledMonitors(startTs, getSourceSystemName());
//			log.warn("Keys collected");
//			log.info("Concurrent run");
//
//			int i = 0;
//
//
//			this.previousDeviceID = "";
//			this.intervalSupressions = new ArrayList<Map<String, String>>();
//			for (String oneKey : allKeys) {
//
//				log.info("One calc started:" + oneKey);
//				StringTokenizer tokenizer = new StringTokenizer(oneKey, ":");
//				List<String> keyTokens = new ArrayList<String>();
//				while (tokenizer.hasMoreTokens()) {
//					keyTokens.add(tokenizer.nextToken());
//				}
//
//				tokenizer = null;
//
//				this.deviceID = keyTokens.get(0) + ":" + keyTokens.get(1);
//
//				log.info(this.deviceID);
//				String instanceFirstTS = upTimeCalculationDaoImpl
//						.getInstanceFirstStatusTimeStamp(this.deviceID);
//				Map<String, String> lastStatus = upTimeCalculationDaoImpl
//						.getInstanceLastStatus(this.deviceID);
//				String instanceStartTS = startTs;
//				String instanceEndTS = endTs;
//				if (instanceFirstTS.length() > 0 && !lastStatus.isEmpty()) {
//					if (Long.valueOf(instanceFirstTS) > Long
//							.valueOf(instanceStartTS)) {
//
//						long instanceStartTsMod = Long.valueOf(instanceFirstTS)
//								% this.NUMBER_OF_SECONDS_IN_HOUR;
//						instanceFirstTS = String.valueOf(Long
//								.valueOf(instanceFirstTS) - instanceStartTsMod);
//
//						instanceStartTS = instanceFirstTS;
//						log.warn("new Start Time for :" + this.deviceID + " "
//								+ instanceStartTS);
//
//					}
//					if (Long.valueOf(lastStatus.get(START_TIME_STAMP)) < Long
//							.valueOf(instanceEndTS)
//							&& lastStatus.get(STATUS).compareTo(
//									COMPUTER_NO_LONGER_ACTIVE) == 0) {
//						log.warn("Computer no longer active on :"
//								+ this.deviceID + " "
//								+ lastStatus.get(START_TIME_STAMP));
//						instanceEndTS = lastStatus.get(START_TIME_STAMP);
//
//						long instanceEndTsMod = Long.valueOf(instanceEndTS)
//								% this.NUMBER_OF_SECONDS_IN_HOUR;
//						instanceEndTS = String.valueOf(Long
//								.valueOf(instanceEndTS) - instanceEndTsMod);
//						log.warn("new End Time for :" + this.deviceID + " "
//								+ instanceEndTS);
//
//					}
//				}
//
//				if (Long.valueOf(instanceEndTS) > Long.valueOf(instanceStartTS)) {
//
//					executeCalculation(oneKey, instanceStartTS, instanceEndTS);
//				} else {
//					log.warn("Will not calc :"
//							+ this.deviceID
//							+ " computer no longer active before the start date");
//				}
//
//				i++;
//
//				if (i % 1000 == 0) {
//					log.warn("Another 1000:" + i + ":" + new Date());
//				}
//
//			}
//
//			log.warn("files created");
//
//			log.warn(new Date());
//			informaticaCassandraLoader.loadData();
//			this.informaticaCassandraLoaderDaily.loadData();
//			this.informaticaCassandraLoaderMonthly.loadData();
//
//			log.warn(new Date());
//			log.warn("setting endTS");
//			log.warn("Hourly:" + endTs);
//			upTimeCalculationDaoImpl.setNextStartTime( endTs,
//					this.sourceSystemName);
//			if (this.getCalculationType().compareTo(this.INITIAL_CALCULATION) == 0) {
//
//				log.warn("Daily:" + String.valueOf(dailyEndTS));
//				log.warn("Monthly:" + String.valueOf(monthlyEndTs));
//
//				upTimeCalculationDaoImpl.setNextStartTime(
//						String.valueOf(dailyEndTS), this.sourceSystemName
//								+ ":Daily");
//				upTimeCalculationDaoImpl.setNextStartTime(
//						String.valueOf(monthlyEndTs), this.sourceSystemName
//								+ ":Monthly");
//
//			}
//
//			if (writeToFile) {
//				writer.flush();
//				writer.close();
//				this.dailyWriter.flush();
//				this.dailyWriter.close();
//				this.monthlyWriter.flush();
//				this.monthlyWriter.close();
//			}
//
//		} catch (Exception e) {
//			log.error(e.getMessage());
//			e.printStackTrace();
//			throw e;
//		}
//		
//	}

	private void executeCalculation(long startTs, long endTs) throws Exception {
		int i = 0;

		for (String oneMonitorDevKey : this.allEanbledMonitorsKeys) {
			log.info("Next to calc: " + oneMonitorDevKey);
			if (i % 1000 == 0) {
				log.warn("Incremental, another 1000: " + i);
			}
			i++;
			List<Map<String, String>> oneKeyPollStateErrors = new ArrayList<Map<String, String>>();
			List<Map<String, String>> oneKeySupressions = new ArrayList<Map<String, String>>();

			StringTokenizer tokenizer = new StringTokenizer(oneMonitorDevKey,
					":");
			List<String> keyTokens = new ArrayList<String>();

			while (tokenizer.hasMoreTokens()) {
				keyTokens.add(tokenizer.nextToken());
			}

			tokenizer = null;

			this.deviceID = keyTokens.get(0) + ":" + keyTokens.get(1);

			Iterator<Entry<String, Map<String, Map<String, String>>>> allPollStateErrorsIterator = this.allPollStateErrors
					.entrySet().iterator();
			while (allPollStateErrorsIterator.hasNext()) {
				Entry<String, Map<String, Map<String, String>>> oneMonitorErrors = allPollStateErrorsIterator
						.next();


				if (oneMonitorErrors.getKey().compareTo(oneMonitorDevKey) == 0) {

					log.info("Error found: " + oneMonitorDevKey);

					Iterator<Entry<String, Map<String, String>>> oneKeyErrorItrator = oneMonitorErrors
							.getValue().entrySet().iterator();

					while (oneKeyErrorItrator.hasNext()) {

						Entry<String, Map<String, String>> oneError = oneKeyErrorItrator
								.next();
						String pollStartTS = oneError.getKey();

						
						if (Long.valueOf(oneError.getValue().get(POLL_DURATION))
								.longValue() < 0) {
							log.warn("Negative Error Poll Duration for:"
									+ oneMonitorDevKey);

						} else {
							Map<String, String> oneErrorDetail = new HashMap<String, String>();
							oneErrorDetail.put(START_TIME_STAMP, pollStartTS);
							log.info("loop2");
							oneErrorDetail.putAll(oneError.getValue());
							oneKeyPollStateErrors.add(oneErrorDetail);
						}
					

					}
				}
			}

			Iterator<Entry<String, Map<String, Map<String, String>>>> allSupressionsIterator = this.allSupressions
					.entrySet().iterator();

			while (allSupressionsIterator.hasNext()) {
				Entry<String, Map<String, Map<String, String>>> oneDeviceSupression = allSupressionsIterator
						.next();
				if (oneDeviceSupression.getKey().compareTo(this.deviceID) == 0) {
					log.info("Supression found: " + this.deviceID);
					Iterator<Entry<String, Map<String, String>>> oneDeviceSupressionItrator = oneDeviceSupression
							.getValue().entrySet().iterator();

					while (oneDeviceSupressionItrator.hasNext()) {

						Entry<String, Map<String, String>> oneDeviceSupressionEntry = oneDeviceSupressionItrator
								.next();

						Map<String, String> oneSupressionDetail = new HashMap<String, String>();
						oneSupressionDetail.put(START_TIME_STAMP,
								oneDeviceSupressionEntry.getKey());

						oneSupressionDetail.putAll(oneDeviceSupressionEntry
								.getValue());

						oneKeySupressions.add(oneSupressionDetail);

					}

				}
			}
			log.info("calling calc for:" + oneMonitorDevKey + " " + startTs
					+ " " + endTs);
			calculateOneMonitorDevice(oneMonitorDevKey, startTs, endTs,
					oneKeyPollStateErrors, oneKeySupressions);
			log.info("calling done:" + oneMonitorDevKey + " " + startTs + " "
					+ endTs);
		}

	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void incrementalCalculation() throws Exception {
		log.warn("Incremental calc");
		UpTimeCalculationDaoImpl upTimeCalculationDaoImpl=null;
		if (this.clusterName == null || this.rootIP == null){
			throw new IllegalStateException ("Cassandra cluster name and IP has to be specified as input argument");
		}
		else
		{
			List<String> seeds = new ArrayList<String>();
			seeds.add(rootIP);
			upTimeCalculationDaoImpl= new UpTimeCalculationDaoImpl(this.clusterName, seeds);
		}
		
		
		
		
		this.previousDeviceID = "";
		try {

			System.out.println(this.clusterName);
			System.out.println(this.rootIP);

			

			String startTs = upTimeCalculationDaoImpl.getNextStartTimeStamp(
					 this.sourceSystemName);
			String endTs = upTimeCalculationDaoImpl.getEndTimeStamp(
					 this.sourceSystemName);
			long endTSLong = Long.valueOf(endTs).longValue();
			long endTsMod = endTSLong % 3600;
			endTs = String.valueOf(endTSLong - endTsMod);
			this.setHourlyEndTimeStamp(Long.valueOf(endTs));

			log.warn("Start time :" + startTs);
			log.warn("End time :" + endTs);
			if (Long.valueOf(endTs).longValue() <= Long.valueOf(startTs)
					.longValue()) {
				log.warn("Calculation End Time stamp is before the start time stamp will not run");
				return;
			}
			Calendar c = Calendar.getInstance();



			int cstOffset = c.getTimeZone().getOffset(0);

			log.warn("Current calendar time:" + c.getTimeInMillis() / 1000);
			log.warn("Current offset:" + cstOffset);

		
			long newTime = c.getTimeInMillis() / 1000;

			log.warn("Current time is:" + newTime);
			if (newTime < Long.valueOf(startTs) + 3600) {
				log.warn("Prevous hour calc already completed will not run");
				return;
			}
			if (Long.valueOf(endTs) < Long.valueOf(startTs) + 3600) {
				log.warn("Time of the start of the last extraction has to be at least one hour after the start of the calculation will not run");
				return;
			}

//			if (Long.valueOf(endTs) - Long.valueOf(startTs) > 3600 * 72) {
//				log.warn("Time difference more than 72 hours will run it as initial");
//				this.initialCalculation();
//				return;
//			}

			log.warn(new Date());
			log.warn("getting all keys");

			this.allEanbledMonitorsKeys = upTimeCalculationDaoImpl
					.getLastEnabledMonitors(this.getSourceSystemName());

			log.warn("Keys collected:" + this.allEanbledMonitorsKeys.size());

			this.allPollStateErrors = new HashMap<String, Map<String, Map<String, String>>>();
			this.allSupressions = new HashMap<String, Map<String, Map<String, String>>>();

			List<String> allDevices = new ArrayList<String>();

			log.warn("getting all errors++++");
			
			
			this.allPollStateErrors = upTimeCalculationDaoImpl
					.getAllPollStateErrors(allEanbledMonitorsKeys, Long
							.valueOf(startTs).longValue(), Long.valueOf(endTs)
							.longValue());
			log.warn(new Date());
			log.warn("Errors collected :" + this.allPollStateErrors.size());

			log.warn("Creating list of devices");
			log.info(new Date());
			Iterator<Entry<String, Map<String, Map<String, String>>>> allPollStateErrorsIterator = this.allPollStateErrors
					.entrySet().iterator();
			while (allPollStateErrorsIterator.hasNext()) {

				Entry<String, Map<String, Map<String, String>>> onePollStateError = allPollStateErrorsIterator
						.next();
				String oneKey = onePollStateError.getKey();
				log.info("One key for supression:" + oneKey);
				StringTokenizer tokenizer = new StringTokenizer(oneKey, ":");
				List<String> keyTokens = new ArrayList<String>();
				while (tokenizer.hasMoreTokens()) {
					keyTokens.add(tokenizer.nextToken());
				}

				tokenizer = null;

				this.deviceID = keyTokens.get(0) + ":" + keyTokens.get(1);
				allDevices.add(deviceID);

			}

			log.warn("Creating list of unique devices");
			Set uniqueDevices = new LinkedHashSet(allDevices);
			List<String> allUniqueDevices = new ArrayList<String>();

			allUniqueDevices.addAll(uniqueDevices);

			log.warn("List of Devices created getting all supressions:"
					+ allUniqueDevices.size() );

			log.warn(new Date());
			this.allSupressions = upTimeCalculationDaoImpl.getAllSupression(
					allUniqueDevices, Long.valueOf(startTs),
					Long.valueOf(endTs));
			log.warn("Supressions selected :" + this.allSupressions.size());
			log.warn(new Date());

			log.warn("Creating loader");
			

			
			informaticaCassandraLoader = new InformaticaCassandraBulkLoader(clusterName, rootIP, KEYSPACE_NAME, UP_TIME_CALCULATION_CF,
					true);
			

			log.warn("Created loader");
			
			log.warn("Concurrent run-- Incremental load");

			this.executeCalculation(Long.valueOf(startTs).longValue(), Long
					.valueOf(endTs).longValue());

			log.warn("files created");

			log.warn(new Date());
			informaticaCassandraLoader.loadData();
			log.warn(new Date());
			log.warn("Round next start time to full hour");
			long endTsLong = Long.valueOf(endTs).longValue();
			long modTs = endTsLong % 3600;
			long roundedTs = endTsLong - modTs;
			log.warn("Updating timestamp to :" + roundedTs);
			upTimeCalculationDaoImpl.setNextStartTime(
					String.valueOf(roundedTs), this.sourceSystemName);
			log.warn("Time stamp updated");
			if (writeToFile) {
				writer.flush();
				writer.close();
			}

		} catch (Exception e) {
			log.error(e.getMessage());
			System.out.println("Exception");
			upTimeCalculationDaoImpl.close();
			throw e;
		}
		log.warn("Closing DB Connections");
		upTimeCalculationDaoImpl.close();
		log.warn("DB closed");
		

	}

//	@SuppressWarnings("static-access")
//	public void dailyUpTimeCalc() throws Exception {
//
//		log.info("In Calculating daily");
//		if (this.isWriteToFile()) {
//			log.info("Will write to the file");
//
//			String baseFileName = System.getenv("CASSANDRA_BULK_LOAD_DIR")
//					+ "/"
//					+ this.csvDir
//					+ "/UpTimeCalc_Daily_"
//					+ this.getSourceSystemName()
//					+ String.valueOf(Calendar.getInstance().getTimeInMillis() / 1000)
//					+ ".csv";
//			this.setWriter(new FileWriter(baseFileName, true));
//			this.setFileRowCount(0);
//
//		}
//		
//		String startTs = upTimeCalculationDaoImpl.getNextStartTimeStamp(
//				 this.sourceSystemName + ":Daily");
//		Calendar c = Calendar.getInstance();
//		if (Long.valueOf(startTs) + this.NUMBER_OF_SECONDS_IN_DAY > c
//				.getTimeInMillis() / 1000) {
//			log.warn("Daily for " + this.sourceSystemName + " and date: "
//					+ startTs + " already processed - will not run");
//			return;
//		}
//		log.warn("Daily for " + this.sourceSystemName + " and date: " + startTs
//				+ " will run");
//
//		String endTs = upTimeCalculationDaoImpl.getEndTimeStamp(
//				this.sourceSystemName);
//		Calendar endCalendar = Calendar.getInstance();
//		endCalendar.setTimeInMillis(Long.valueOf(endTs)*1000);
//		long numOfSecondsInCurrentDay =  endCalendar.get(Calendar.HOUR_OF_DAY)
//				* this.NUMBER_OF_SECONDS_IN_HOUR + endCalendar.get(Calendar.MINUTE)
//				* this.NUMBER_OF_SECONDS_IN_MINUTE + endCalendar.get(Calendar.SECOND);
//		
//		
//		
//
//		log.warn("Old end TS: " + endTs);
//
//		endTs = String.valueOf(Long.valueOf(endTs) - numOfSecondsInCurrentDay);
//
//		log.warn("New end TS: " + endTs);
//
//		
//		log.warn("Daily for " + this.sourceSystemName + " and date: " + startTs
//				+ " will run unitl date " + endTs);
//
//		log.info("Creating loader");
//		
//		informaticaCassandraLoader = new InformaticaCassandraBulkLoader(clusterName, rootIP, KEYSPACE_NAME, UP_TIME_CALCULATION_CF,
//				true);
//		
//		log.info("Loader created");
//
//		while (Long.valueOf(startTs) < Long.valueOf(endTs)) {
//
//			List<String> enabledMonitors = upTimeCalculationDaoImpl
//					.getTimeFrameEnabledMonitors(startTs, startTs,
//							this.sourceSystemName + ":Daily");
//			log.warn("Retreived enabled Monitors for: " + startTs + " "
//					+ this.sourceSystemName + " Number of records:"
//					+ enabledMonitors.size());
//
//			if (enabledMonitors.size() == 0) {
//				log.warn("No monitors for timestamp: " + startTs + " "
//						+ this.sourceSystemName + " trying previous monitors");
//
//				enabledMonitors = upTimeCalculationDaoImpl
//						.getLastEnabledMonitors(startTs, this.sourceSystemName
//								+ ":Daily");
//
//				log.warn("Retreived previous enabled Monitors for: " + startTs
//						+ " " + this.sourceSystemName + " Number of records:"
//						+ enabledMonitors.size());
//
//			}
//			for (String oneMonitor : enabledMonitors) {
//				long dailyUpTime = 0;
//				long dailyDownTime = 0;
//				long dailyAgreedUpTime = 0;
//				long dailyCalculatedDownTime = 0;
//				long dailyNumberOfErrors = 0;
//
//				List<Map<String, String>> hourlyCalculation = upTimeCalculationDaoImpl
//						.getUpTimeCalc(
//								oneMonitor,
//								startTs,
//								String.valueOf(Long.valueOf(startTs)
//										+ this.NUMBER_OF_SECONDS_IN_DAY - 1));
//				if (hourlyCalculation.size() > 0) {
//					for (Map<String, String> oneCalc : hourlyCalculation) {
//						if (dailyUpTime <= this.NUMBER_OF_SECONDS_IN_DAY) {
//							dailyUpTime = dailyUpTime
//									+ NUMBER_OF_SECONDS_IN_HOUR;
//							dailyDownTime = dailyDownTime
//									+ Long.valueOf(oneCalc
//											.get(this.TOTAL_DOWN_TIME));
//							dailyAgreedUpTime = dailyAgreedUpTime
//									+ Long.valueOf(oneCalc
//											.get(this.AGREED_UP_TIME));
//							dailyCalculatedDownTime = dailyCalculatedDownTime
//									+ Long.valueOf(oneCalc
//											.get(this.CALCULATED_DOWN_TIME));
//							dailyNumberOfErrors = dailyNumberOfErrors
//									+ Long.valueOf(oneCalc
//											.get(this.NUMBER_OF_ERROR_POLLS));
//						}
//					}
//					if (dailyUpTime > this.NUMBER_OF_SECONDS_IN_DAY) {
//						log.warn("dailyUpTime greater than number of sec in a day for: "
//								+ oneMonitor);
//					}
//					float totalAvailabilityPercent = 1;
//					if (dailyUpTime > 0) {
//						totalAvailabilityPercent = (float) 1
//
//						- ((float) dailyDownTime / (float) dailyUpTime);
//					}
//					float calculatedAvailabilityPercent = 1;
//					if (dailyAgreedUpTime > 0) {
//						calculatedAvailabilityPercent = (float) 1
//								- ((float) dailyCalculatedDownTime / (float) dailyAgreedUpTime);
//					}
//
//					if (totalAvailabilityPercent < 0.0001) {
//						totalAvailabilityPercent = 0;
//					}
//					if (calculatedAvailabilityPercent < 0.0001) {
//						calculatedAvailabilityPercent = 0;
//					}
//
//					Map<String, String> oneSuperColumn = new LinkedHashMap<String, String>();
//					oneSuperColumn.put(this.UP_TIME_DURATION,
//							String.valueOf(dailyUpTime));
//					oneSuperColumn.put(TOTAL_DOWN_TIME,
//							String.valueOf(dailyDownTime));
//					oneSuperColumn.put(CALCULATED_DOWN_TIME,
//							String.valueOf(dailyCalculatedDownTime));
//					oneSuperColumn.put(AGREED_UP_TIME,
//							String.valueOf(dailyAgreedUpTime));
//					oneSuperColumn.put(TOTAL_AVAILABILITY_PERCENT,
//							String.valueOf(totalAvailabilityPercent));
//					oneSuperColumn.put(CALCULATED_AVAILABILITY_PERCENT,
//							String.valueOf(calculatedAvailabilityPercent));
//					oneSuperColumn.put(NUMBER_OF_ERROR_POLLS,
//							String.valueOf(dailyNumberOfErrors)); //
//			
//					log.debug("Loading row for:" + oneMonitor + " SUPERCOLUMN:"
//							+ String.valueOf(startTs));
//					informaticaCassandraLoader.loadRow(oneMonitor,
//							String.valueOf(startTs), Long.valueOf(0),
//							oneSuperColumn);
//					if (this.writeToFile) {
//						writer.append(oneMonitor);
//						writer.append(",");
//						writer.append(String.valueOf(startTs));
//						writer.append(",");
//						writer.append(String.valueOf(dailyUpTime));
//						writer.append(",");
//						writer.append(String.valueOf(dailyDownTime));
//						writer.append(",");
//						writer.append(String.valueOf(dailyCalculatedDownTime));
//						writer.append(",");
//						writer.append(String.valueOf(dailyAgreedUpTime));
//						writer.append(",");
//						writer.append(String.valueOf(totalAvailabilityPercent));
//						writer.append(",");
//						writer.append(String
//								.valueOf(calculatedAvailabilityPercent));
//						writer.append("\n");
//						writer.flush();
//						this.setFileRowCount(this.getFileRowCount() + 1);
//						if (this.getFileRowCount() > FILE_ROW_COUNT_LIMIT) {
//							writer.close();
//
//							String baseFileName = System
//									.getenv("CASSANDRA_BULK_LOAD_DIR")
//									+ "/"
//									+ this.csvDir
//									+ "/UpTimeCalc_Daily_"
//									+ this.getSourceSystemName()
//									+ String.valueOf(Calendar.getInstance()
//											.getTimeInMillis() / 1000) + ".csv";
//							this.setWriter(new FileWriter(baseFileName, true));
//							this.setFileRowCount(0);
//
//						}
//
//					}
//
//				}
//			}
//			startTs = String.valueOf(Long.valueOf(startTs)
//					+ this.NUMBER_OF_SECONDS_IN_DAY);
//
//			Calendar checkDailyStatHour = Calendar.getInstance();
//			checkDailyStatHour.setTimeInMillis(Long.valueOf(startTs) * 1000);
//			if (checkDailyStatHour.get(Calendar.HOUR_OF_DAY) == 23) {
//				checkDailyStatHour.add(Calendar.HOUR, 1);
//				startTs = String
//						.valueOf(checkDailyStatHour.getTimeInMillis() / 1000);
//			}
//
//		}
//
//		log.warn("files created");
//
//		log.warn(new Date());
//		informaticaCassandraLoader.loadData();
//		log.warn(new Date());
//
//		log.warn("calculation completed for :" + this.sourceSystemName);
//		log.warn("Set new time stamp");
//		upTimeCalculationDaoImpl.setNextStartTime(
//				String.valueOf(startTs), this.sourceSystemName + ":Daily");
//		return;
//
//	}
//
//	@SuppressWarnings("static-access")
//	public void dailyUpTimeCalcBatch() throws Exception {
//
//		log.warn("In Calculating daily");
//		log.warn(this.clusterName + " " + this.rootIP + " "
//				+ this.sourceSystemName);
//
//		
//		if (this.isWriteToFile()) {
//			log.info("Will write to the file");
//
//			String baseFileName = System.getenv("CASSANDRA_BULK_LOAD_DIR")
//					+ "/"
//					+ this.csvDir
//					+ "/UpTimeCalc_Daily_"
//					+ this.getSourceSystemName()
//					+ String.valueOf(Calendar.getInstance().getTimeInMillis() / 1000)
//					+ ".csv";
//			this.setWriter(new FileWriter(baseFileName, true));
//			this.setFileRowCount(0);
//
//		}
//		
//		String startTs = upTimeCalculationDaoImpl.getNextStartTimeStamp(
//				 this.sourceSystemName + ":Daily");
//		Calendar c = Calendar.getInstance();
//		if (Long.valueOf(startTs) + this.NUMBER_OF_SECONDS_IN_DAY > c
//				.getTimeInMillis() / 1000) {
//			log.warn("Daily for " + this.sourceSystemName + " and date: "
//					+ startTs + " already processed - will not run");
//			return;
//		}
//		log.warn("Daily for " + this.sourceSystemName + " and date: " + startTs
//				+ " will run");
//
//		String endTs = upTimeCalculationDaoImpl.getEndTimeStamp(
//				this.sourceSystemName);
//		Calendar endCalendar = Calendar.getInstance();
//		endCalendar.setTimeInMillis(Long.valueOf(endTs)*1000);
//		
//		long numOfSecondsInCurrentDay =  endCalendar.get(Calendar.HOUR_OF_DAY)
//				* this.NUMBER_OF_SECONDS_IN_HOUR + endCalendar.get(Calendar.MINUTE)
//				* this.NUMBER_OF_SECONDS_IN_MINUTE + endCalendar.get(Calendar.SECOND);
//		
//		
//		
//		
//		
//		log.warn("Old end TS: " + endTs);
//
//		endTs = String.valueOf(Long.valueOf(endTs) - numOfSecondsInCurrentDay);
//
//		log.warn("New end TS: " + endTs);
//
//		
//
//		log.warn("Daily for " + this.sourceSystemName + " and date: " + startTs
//				+ " will run unitl date " + endTs);
//
//		log.info("Creating loader");
//		
//		informaticaCassandraLoader = new InformaticaCassandraBulkLoader(clusterName, rootIP, KEYSPACE_NAME, UP_TIME_CALCULATION_CF,
//				true);
//		
//		log.info("Loader created");
//
//	
//
//		while (Long.valueOf(startTs) < Long.valueOf(endTs)) {
//
//			List<String> enabledMonitors = upTimeCalculationDaoImpl
//					.getTimeFrameEnabledMonitors(startTs, startTs,
//							this.sourceSystemName + ":Daily");
//			log.warn("Retreived enabled Monitors for: " + startTs + " "
//					+ this.sourceSystemName + " Number of records:"
//					+ enabledMonitors.size());
//
//			if (enabledMonitors.size() == 0) {
//				log.warn("No monitors for timestamp: " + startTs + " "
//						+ this.sourceSystemName + " trying previous monitors");
//
//				enabledMonitors = upTimeCalculationDaoImpl
//						.getLastEnabledMonitors(startTs, this.sourceSystemName
//								+ ":Daily");
//
//				log.warn("Retreived previous enabled Monitors for: " + startTs
//						+ " " + this.sourceSystemName + " Number of records:"
//						+ enabledMonitors.size());
//
//			}
//
//			List<String> enabledMonitorsSet = new ArrayList<String>();
//
//			for (int z = 0; z < enabledMonitors.size(); z++)
//
//			{
//				enabledMonitorsSet.add(enabledMonitors.get(z));
//				if ((z > 0 && z % MONITOR_BATCH_SIZE == 0) || z==enabledMonitors.size()-1) {
//
//					log.warn("Retreiving up time calc batch for index: " + z);
//					Map<String, Map<String, Map<String, String>>> hourUpTime = upTimeCalculationDaoImpl
//							.getAllHourlyUpTimeCalc(enabledMonitorsSet,
//									Long.valueOf(startTs),
//									Long.valueOf(startTs)
//											+ this.NUMBER_OF_SECONDS_IN_DAY - 1);
//					log.warn("Retreived up time calc batch for index: " + z);
//					enabledMonitorsSet.clear();
//
//					Iterator<Entry<String, Map<String, Map<String, String>>>> hourlyCalcIterator = hourUpTime
//							.entrySet().iterator();
//					while (hourlyCalcIterator.hasNext()) {
//						Entry<String, Map<String, Map<String, String>>> oneRowKeyCalc = hourlyCalcIterator
//								.next();
//						String oneMonitor = oneRowKeyCalc.getKey();
//						Iterator<Entry<String, Map<String, String>>> oneRowKeyHourlyCalcIterator = oneRowKeyCalc
//								.getValue().entrySet().iterator();
//						List<Map<String, String>> oneRowKeyResults = new ArrayList<Map<String, String>>();
//
//						while (oneRowKeyHourlyCalcIterator.hasNext()) {
//							oneRowKeyResults.add(oneRowKeyHourlyCalcIterator
//									.next().getValue());
//						}
//
//		
//
//						long dailyUpTime = 0;
//						long dailyDownTime = 0;
//						long dailyAgreedUpTime = 0;
//						long dailyCalculatedDownTime = 0;
//						long dailyNumberOfErrors = 0;
//
//						if (oneRowKeyResults.size() > 0) {
//							for (Map<String, String> oneCalc : oneRowKeyResults) {
//								if (dailyUpTime <= this.NUMBER_OF_SECONDS_IN_DAY) {
//									dailyUpTime = dailyUpTime
//											+ NUMBER_OF_SECONDS_IN_HOUR;
//									dailyDownTime = dailyDownTime
//											+ Long.valueOf(oneCalc
//													.get(this.TOTAL_DOWN_TIME));
//									dailyAgreedUpTime = dailyAgreedUpTime
//											+ Long.valueOf(oneCalc
//													.get(this.AGREED_UP_TIME));
//									dailyCalculatedDownTime = dailyCalculatedDownTime
//											+ Long.valueOf(oneCalc
//													.get(this.CALCULATED_DOWN_TIME));
//									dailyNumberOfErrors = dailyNumberOfErrors
//											+ Long.valueOf(oneCalc
//													.get(this.NUMBER_OF_ERROR_POLLS));
//								}
//							}
//							if (dailyUpTime > this.NUMBER_OF_SECONDS_IN_DAY) {
//								log.warn("dailyUpTime greater than number of sec in a day for: "
//										+ oneMonitor);
//							}
//							float totalAvailabilityPercent = 1;
//							if (dailyUpTime > 0) {
//								totalAvailabilityPercent = (float) 1
//
//								- ((float) dailyDownTime / (float) dailyUpTime);
//							}
//							float calculatedAvailabilityPercent = 1;
//							if (dailyAgreedUpTime > 0) {
//								calculatedAvailabilityPercent = (float) 1
//										- ((float) dailyCalculatedDownTime / (float) dailyAgreedUpTime);
//							}
//
//							if (totalAvailabilityPercent < 0.0001) {
//								totalAvailabilityPercent = 0;
//							}
//							if (calculatedAvailabilityPercent < 0.0001) {
//								calculatedAvailabilityPercent = 0;
//							}
//
//							Map<String, String> oneSuperColumn = new LinkedHashMap<String, String>();
//							oneSuperColumn.put(this.UP_TIME_DURATION,
//									String.valueOf(dailyUpTime));
//							oneSuperColumn.put(TOTAL_DOWN_TIME,
//									String.valueOf(dailyDownTime));
//							oneSuperColumn.put(CALCULATED_DOWN_TIME,
//									String.valueOf(dailyCalculatedDownTime));
//							oneSuperColumn.put(AGREED_UP_TIME,
//									String.valueOf(dailyAgreedUpTime));
//							oneSuperColumn.put(TOTAL_AVAILABILITY_PERCENT,
//									String.valueOf(totalAvailabilityPercent));
//							oneSuperColumn
//									.put(CALCULATED_AVAILABILITY_PERCENT,
//											String.valueOf(calculatedAvailabilityPercent));
//							oneSuperColumn.put(NUMBER_OF_ERROR_POLLS,
//									String.valueOf(dailyNumberOfErrors)); //
//
//							log.debug("Loading row for:" + oneMonitor
//									+ " SUPERCOLUMN:" + String.valueOf(startTs));
//							informaticaCassandraLoader.loadRow(oneMonitor,
//									String.valueOf(startTs), Long.valueOf(0),
//									oneSuperColumn);
//							if (this.writeToFile) {
//								writer.append(oneMonitor);
//								writer.append(",");
//								writer.append(String.valueOf(startTs));
//								writer.append(",");
//								writer.append(String.valueOf(dailyUpTime));
//								writer.append(",");
//								writer.append(String.valueOf(dailyDownTime));
//								writer.append(",");
//								writer.append(String
//										.valueOf(dailyCalculatedDownTime));
//								writer.append(",");
//								writer.append(String.valueOf(dailyAgreedUpTime));
//								writer.append(",");
//								writer.append(String
//										.valueOf(totalAvailabilityPercent));
//								writer.append(",");
//								writer.append(String
//										.valueOf(calculatedAvailabilityPercent));
//								writer.append("\n");
//								writer.flush();
//								this.setFileRowCount(this.getFileRowCount() + 1);
//								if (this.getFileRowCount() > FILE_ROW_COUNT_LIMIT) {
//									writer.close();
//
//									String baseFileName = System
//											.getenv("CASSANDRA_BULK_LOAD_DIR")
//											+ "/"
//											+ this.csvDir
//											+ "/UpTimeCalc_Daily_"
//											+ this.getSourceSystemName()
//											+ String.valueOf(Calendar
//													.getInstance()
//													.getTimeInMillis() / 1000)
//											+ ".csv";
//									this.setWriter(new FileWriter(baseFileName,
//											true));
//									this.setFileRowCount(0);
//
//								}
//
//							}
//
//						}
//					}
//
//				}
//
//			}
//
//			startTs = String.valueOf(Long.valueOf(startTs)
//					+ this.NUMBER_OF_SECONDS_IN_DAY);
//
//			Calendar checkDailyStatHour = Calendar.getInstance();
//			checkDailyStatHour.setTimeInMillis(Long.valueOf(startTs) * 1000);
//			if (checkDailyStatHour.get(Calendar.HOUR_OF_DAY) == 23) {
//				checkDailyStatHour.add(Calendar.HOUR, 1);
//				startTs = String
//						.valueOf(checkDailyStatHour.getTimeInMillis() / 1000);
//			}
//
//		}
//
//		log.warn("files created");
//
//		log.warn(new Date());
//		informaticaCassandraLoader.loadData();
//		log.warn(new Date());
//
//		log.warn("calculation completed for :" + this.sourceSystemName);
//		log.warn("Set new time stamp");
//		upTimeCalculationDaoImpl.setNextStartTime(
//				String.valueOf(startTs), this.sourceSystemName + ":Daily");
//		return;
//
//	}

//	@SuppressWarnings("static-access")
//	public void monthlyUpTimeCalc() throws Exception {
//
//		log.info("In Calculating monthly");
//		if (this.isWriteToFile()) {
//			log.info("Will write to the file");
//
//			String baseFileName = System.getenv("CASSANDRA_BULK_LOAD_DIR")
//					+ "/"
//					+ this.csvDir
//					+ "/UpTimeCalc_Monthly_"
//					+ this.getSourceSystemName()
//					+ String.valueOf(Calendar.getInstance().getTimeInMillis() / 1000)
//					+ ".csv";
//			this.setWriter(new FileWriter(baseFileName, true));
//			this.setFileRowCount(0);
//
//		}
//
//		
//
//		String startTs = upTimeCalculationDaoImpl.getNextStartTimeStamp(
//				 this.sourceSystemName + ":Monthly");
//		
//
//		Calendar c1 = Calendar.getInstance();
//		c1.setTimeInMillis(Long.valueOf(startTs) * 1000);
//
//		String endTs = upTimeCalculationDaoImpl.getEndTimeStamp(
//				this.sourceSystemName);
//		log.warn("Monthly Start TS:" + startTs);
//		log.warn("Monthly End Ts:" + endTs);
//		Calendar c2 = Calendar.getInstance();
//		c2.setTimeInMillis(Long.valueOf(endTs) * 1000);
//
//		log.warn("Monthly Start Month:" + c1.get(Calendar.MONTH));
//		log.warn("Monthly End Month:" + c2.get(Calendar.MONTH));
//
//		if (c1.get(Calendar.MONTH) == c2.get(Calendar.MONTH)) {
//			log.warn("Monthly for " + this.sourceSystemName + " and date: "
//					+ startTs + " already processed - will not run");
//			return;
//		}
//		log.warn("Monthly for " + this.sourceSystemName + " and date: "
//				+ startTs + " will run");
//
//		long numberOfSecondsInCurrentMonth = c2.get(Calendar.DAY_OF_MONTH)
//				* this.NUMBER_OF_SECONDS_IN_DAY + c2.get(Calendar.HOUR_OF_DAY)
//				* this.NUMBER_OF_SECONDS_IN_HOUR + c2.get(Calendar.MINUTE)
//				* this.NUMBER_OF_SECONDS_IN_MINUTE + c2.get(Calendar.SECOND);
//		endTs = String.valueOf((c2.getTimeInMillis()/1000)
//				- numberOfSecondsInCurrentMonth);
//		log.warn("NEW End Ts:" + endTs);
//		
//		log.warn("Monthly for " + this.sourceSystemName + " and date: "
//				+ startTs + " will run unitl date " + endTs);
//
//		log.info("Creating loader");
//		
//		informaticaCassandraLoader = new InformaticaCassandraBulkLoader(clusterName, rootIP, KEYSPACE_NAME, UP_TIME_CALCULATION_CF,
//				true);
//		
//		log.info("Loader created");
//
//		while (Long.valueOf(startTs) < Long.valueOf(endTs)) {
//
//			List<String> enabledMonitors = upTimeCalculationDaoImpl
//					.getTimeFrameEnabledMonitors(startTs, startTs,
//							this.sourceSystemName + ":Monthly");
//			log.info("Retreived enabled Monitors for: " + startTs + " "
//					+ this.sourceSystemName + " Number of records:"
//					+ enabledMonitors.size());
//
//			if (enabledMonitors.size() == 0) {
//				log.info("No monitors for timestamp: " + startTs + " "
//						+ this.sourceSystemName + " trying previous monitors");
//
//				enabledMonitors = upTimeCalculationDaoImpl
//						.getLastEnabledMonitors(startTs, this.sourceSystemName
//								+ ":Monthly");
//
//				log.info("Retreived previous enabled Monitors for: " + startTs
//						+ " " + this.sourceSystemName + " Number of records:"
//						+ enabledMonitors.size());
//
//			}
//
//			c1.setTimeInMillis(Long.valueOf(startTs) * 1000);
//
//			Calendar cGetNSec = Calendar.getInstance();
//
//			cGetNSec.setTimeInMillis(Long.valueOf(startTs) * 1000);
//			cGetNSec.add(Calendar.MONTH, 1);
//			cGetNSec.add(Calendar.HOUR, -6);
//
//			long numOfSecInMonth = cGetNSec.get(Calendar.DAY_OF_MONTH)
//					* this.NUMBER_OF_SECONDS_IN_DAY;
//
//			c1.add(Calendar.MONTH, 1);
//
//			log.info("Calculation up time for period: " + startTs + " to "
//					+ c1.getTimeInMillis() / 1000);
//			for (String oneMonitor : enabledMonitors) {
//				long monthlyUpTime = 0;
//				long monthlyDownTime = 0;
//				long monthlyAgreedUpTime = 0;
//				long monthlyCalculatedDownTime = 0;
//				long monthlyNumberOfErrors = 0;
//
//				List<Map<String, String>> monthlyCalculation = upTimeCalculationDaoImpl
//						.getDailyUpTimeCalc(oneMonitor, startTs, String
//								.valueOf((c1.getTimeInMillis() / 1000) - 1));
//				if (monthlyCalculation.size() > 0) {
//					for (Map<String, String> oneCalc : monthlyCalculation) {
//						if (monthlyUpTime < numOfSecInMonth) {
//							monthlyUpTime = monthlyUpTime
//									+ NUMBER_OF_SECONDS_IN_DAY;
//							monthlyDownTime = monthlyDownTime
//									+ Long.valueOf(oneCalc
//											.get(this.TOTAL_DOWN_TIME));
//							monthlyAgreedUpTime = monthlyAgreedUpTime
//									+ Long.valueOf(oneCalc
//											.get(this.AGREED_UP_TIME));
//							monthlyCalculatedDownTime = monthlyCalculatedDownTime
//									+ Long.valueOf(oneCalc
//											.get(this.CALCULATED_DOWN_TIME));
//							monthlyNumberOfErrors = monthlyNumberOfErrors
//									+ Long.valueOf(oneCalc
//											.get(this.NUMBER_OF_ERROR_POLLS));
//						}
//					}
//					if (monthlyUpTime > 0) {
//						float totalAvailabilityPercent = 1;
//						if (monthlyUpTime > 0) {
//							totalAvailabilityPercent = (float) 1
//									- ((float) monthlyDownTime / (float) monthlyUpTime);
//						}
//						float calculatedAvailabilityPercent = 1;
//						if (monthlyAgreedUpTime > 0) {
//							calculatedAvailabilityPercent = (float) 1
//									- ((float) monthlyCalculatedDownTime / (float) monthlyAgreedUpTime);
//						}
//
//						if (totalAvailabilityPercent < 0.0001) {
//							totalAvailabilityPercent = 0;
//						}
//						if (calculatedAvailabilityPercent < 0.0001) {
//							calculatedAvailabilityPercent = 0;
//						}
//
//						Map<String, String> oneSuperColumn = new LinkedHashMap<String, String>();
//						oneSuperColumn.put(this.UP_TIME_DURATION,
//								String.valueOf(monthlyUpTime));
//						oneSuperColumn.put(TOTAL_DOWN_TIME,
//								String.valueOf(monthlyDownTime));
//						oneSuperColumn.put(CALCULATED_DOWN_TIME,
//								String.valueOf(monthlyCalculatedDownTime));
//						oneSuperColumn.put(AGREED_UP_TIME,
//								String.valueOf(monthlyAgreedUpTime));
//						oneSuperColumn.put(TOTAL_AVAILABILITY_PERCENT,
//								String.valueOf(totalAvailabilityPercent));
//						oneSuperColumn.put(CALCULATED_AVAILABILITY_PERCENT,
//								String.valueOf(calculatedAvailabilityPercent));
//						oneSuperColumn.put(NUMBER_OF_ERROR_POLLS,
//								String.valueOf(monthlyNumberOfErrors)); //
//						// System.out.println(currentStartTime);
//						log.debug("Loading row for:" + oneMonitor
//								+ " SUPERCOLUMN:" + String.valueOf(startTs));
//						informaticaCassandraLoader.loadRow(oneMonitor,
//								String.valueOf(startTs), Long.valueOf(0),
//								oneSuperColumn);
//						if (this.writeToFile) {
//							writer.append(oneMonitor);
//							writer.append(",");
//							writer.append(String.valueOf(startTs));
//							writer.append(",");
//							writer.append(String.valueOf(monthlyUpTime));
//							//writer.append(",");
//							//writer.append(String.valueOf(c1.getTimeInMillis() / 1000));
//							writer.append(",");
//							writer.append(String.valueOf(monthlyDownTime));
//							writer.append(",");
//							writer.append(String
//									.valueOf(monthlyCalculatedDownTime));
//							writer.append(",");
//							writer.append(String.valueOf(monthlyAgreedUpTime));
//							writer.append(",");
//							writer.append(String
//									.valueOf(totalAvailabilityPercent));
//							writer.append(",");
//							writer.append(String
//									.valueOf(calculatedAvailabilityPercent));
//							writer.append("\n");
//							writer.flush();
//							this.setFileRowCount(this.getFileRowCount() + 1);
//							if (this.getFileRowCount() > FILE_ROW_COUNT_LIMIT) {
//								writer.close();
//
//								String baseFileName = System
//										.getenv("CASSANDRA_BULK_LOAD_DIR")
//										+ "/"
//										+ this.csvDir
//										+ "/UpTimeCalc_Monthly_"
//										+ this.getSourceSystemName()
//										+ String.valueOf(Calendar.getInstance()
//												.getTimeInMillis() / 1000)
//										+ ".csv";
//								this.setWriter(new FileWriter(baseFileName,
//										true));
//								this.setFileRowCount(0);
//
//							}
//
//						}
//					}
//
//				}
//			}
//			startTs = String.valueOf(c1.getTimeInMillis() / 1000);
//
//			Calendar checkMonthlyStatHour = Calendar.getInstance();
//			checkMonthlyStatHour.setTimeInMillis(Long.valueOf(startTs) * 1000);
//			if (checkMonthlyStatHour.get(Calendar.HOUR_OF_DAY) == 23) {
//				checkMonthlyStatHour.add(Calendar.HOUR, 1);
//				startTs = String
//						.valueOf(checkMonthlyStatHour.getTimeInMillis() / 1000);
//			}
//
//		}
//
//		log.warn("files created");
//
//		log.warn(new Date());
//		informaticaCassandraLoader.loadData();
//		log.warn(new Date());
//
//		log.warn("Monthly calculation completed for :" + this.sourceSystemName);
//		log.warn("Set new time stamp");
//		upTimeCalculationDaoImpl.setNextStartTime(
//				String.valueOf(String.valueOf(c1.getTimeInMillis() / 1000)),
//				this.sourceSystemName + ":Monthly");
//		return;
//
//	}

//	@SuppressWarnings("static-access")
//	public void monthlyUpTimeCalcBatch() throws Exception {
//		
//		
//
//		log.warn("In Calculating monthly");
//		log.warn(this.clusterName + " " + this.rootIP + " "
//				+ this.sourceSystemName);
//
//		
//		if (this.isWriteToFile()) {
//			log.info("Will write to the file");
//
//			String baseFileName = System.getenv("CASSANDRA_BULK_LOAD_DIR")
//					+ "/"
//					+ this.csvDir
//					+ "/UpTimeCalc_Monthly_"
//					+ this.getSourceSystemName()
//					+ String.valueOf(Calendar.getInstance().getTimeInMillis() / 1000)
//					+ ".csv";
//			this.setWriter(new FileWriter(baseFileName, true));
//			this.setFileRowCount(0);
//
//		}
//		
//		
//
//		String startTs = upTimeCalculationDaoImpl.getNextStartTimeStamp(
//				 this.sourceSystemName + ":Monthly");
//		Calendar c = Calendar.getInstance();
//
//		Calendar c1 = Calendar.getInstance();
//		c1.setTimeInMillis(Long.valueOf(startTs) * 1000);
//
//		if (c1.get(Calendar.MONTH) == c.get(Calendar.MONTH)) {
//			log.warn("Monthly for " + this.sourceSystemName + " and date: "
//					+ startTs + " already processed - will not run");
//			return;
//		}
//		log.warn("Monthly for " + this.sourceSystemName + " and date: "
//				+ startTs + " will run");
//
//		String endTs = upTimeCalculationDaoImpl.getEndTimeStamp(
//				this.sourceSystemName);
//		log.warn("End Ts:" + endTs);
//		Calendar c2 = Calendar.getInstance();
//		c2.setTimeInMillis(Long.valueOf(endTs) * 1000);
//
//		long numberOfSecondsInCurrentMonth = c2.get(Calendar.DAY_OF_MONTH)-1
//				* this.NUMBER_OF_SECONDS_IN_DAY + c2.get(Calendar.HOUR_OF_DAY)
//				* this.NUMBER_OF_SECONDS_IN_HOUR + c2.get(Calendar.MINUTE)
//				* this.NUMBER_OF_SECONDS_IN_MINUTE + c2.get(Calendar.SECOND);
//		endTs = String.valueOf((c2.getTimeInMillis()/1000)
//				- numberOfSecondsInCurrentMonth);
//		
//		log.warn("NEW End Ts:" + endTs);
//		
//		log.warn("Monthly for " + this.sourceSystemName + " and date: "
//				+ startTs + " will run unitl date " + endTs);
//
//		log.info("Creating loader");
//		
//		informaticaCassandraLoader = new InformaticaCassandraBulkLoader(clusterName, rootIP, KEYSPACE_NAME, UP_TIME_CALCULATION_CF,
//				true);
//		
//		log.info("Loader created");
//
//		
//
//		while (Long.valueOf(startTs) < Long.valueOf(endTs)) {
//
//			List<String> enabledMonitors = upTimeCalculationDaoImpl
//					.getTimeFrameEnabledMonitors(startTs, startTs,
//							this.sourceSystemName + ":Monthly");
//			log.info("Retreived enabled Monitors for: " + startTs + " "
//					+ this.sourceSystemName + " Number of records:"
//					+ enabledMonitors.size());
//
//			if (enabledMonitors.size() == 0) {
//				log.info("No monitors for timestamp: " + startTs + " "
//						+ this.sourceSystemName + " trying previous monitors");
//
//				enabledMonitors = upTimeCalculationDaoImpl
//						.getLastEnabledMonitors(startTs, this.sourceSystemName
//								+ ":Monthly");
//
//				log.info("Retreived previous enabled Monitors for: " + startTs
//						+ " " + this.sourceSystemName + " Number of records:"
//						+ enabledMonitors.size());
//
//			}
//
//			c1.setTimeInMillis(Long.valueOf(startTs) * 1000);
//
//			Calendar cGetNSec = Calendar.getInstance();
//
//			cGetNSec.setTimeInMillis(Long.valueOf(startTs) * 1000);
//			cGetNSec.add(Calendar.MONTH, 1);
//			cGetNSec.add(Calendar.HOUR, -6);
//
//			long numOfSecInMonth = cGetNSec.get(Calendar.DAY_OF_MONTH)
//					* this.NUMBER_OF_SECONDS_IN_DAY;
//
//			c1.add(Calendar.MONTH, 1);
//
//			log.warn("Monthly Calculation up time for period: " + startTs + " to "
//					+ c1.getTimeInMillis() / 1000);
//			
//			log.warn("Monthly Calculation up time call arguments: " + startTs + " to "
//					+ ((c1.getTimeInMillis() / 1000)-1));
//
//
//			List<String> enabledMonitorsSet = new ArrayList<String>();
//
//			for (int z = 0; z < enabledMonitors.size(); z++)
//
//			{
//				enabledMonitorsSet.add(enabledMonitors.get(z));
//				if ((z > 0 && z % MONITOR_BATCH_SIZE == 0) || z==enabledMonitors.size()-1) {
//
//					log.warn("Retreiving daily up time calc batch for index: "
//							+ z);
//					Map<String, Map<String, Map<String, String>>> hourUpTime = upTimeCalculationDaoImpl
//							.getAllDailyUpTimeCalc(enabledMonitorsSet,
//									Long.valueOf(startTs),
//									(c1.getTimeInMillis() / 1000) - 1);
//					log.warn("Retreived daily up time calc batch for index: "
//							+ z);
//					enabledMonitorsSet.clear();
//
//					Iterator<Entry<String, Map<String, Map<String, String>>>> dailyCalcIterator = hourUpTime
//							.entrySet().iterator();
//					while (dailyCalcIterator.hasNext()) {
//						Entry<String, Map<String, Map<String, String>>> oneRowKeyCalc = dailyCalcIterator
//								.next();
//						String oneMonitor = oneRowKeyCalc.getKey();
//						Iterator<Entry<String, Map<String, String>>> oneRowKeyDailyCalcIterator = oneRowKeyCalc
//								.getValue().entrySet().iterator();
//						List<Map<String, String>> oneRowKeyResults = new ArrayList<Map<String, String>>();
//
//						while (oneRowKeyDailyCalcIterator.hasNext()) {
//							oneRowKeyResults.add(oneRowKeyDailyCalcIterator
//									.next().getValue());
//						}
//
//						// Created calc list
//
//						long monthlyUpTime = 0;
//						long monthlyDownTime = 0;
//						long monthlyAgreedUpTime = 0;
//						long monthlyCalculatedDownTime = 0;
//						long monthlyNumberOfErrors = 0;
//
//						if (oneRowKeyResults.size() > 0) {
//							for (Map<String, String> oneCalc : oneRowKeyResults) {
//								if (monthlyUpTime < numOfSecInMonth) {
//									monthlyUpTime = monthlyUpTime
//											+ NUMBER_OF_SECONDS_IN_DAY;
//									monthlyDownTime = monthlyDownTime
//											+ Long.valueOf(oneCalc
//													.get(this.TOTAL_DOWN_TIME));
//									monthlyAgreedUpTime = monthlyAgreedUpTime
//											+ Long.valueOf(oneCalc
//													.get(this.AGREED_UP_TIME));
//									monthlyCalculatedDownTime = monthlyCalculatedDownTime
//											+ Long.valueOf(oneCalc
//													.get(this.CALCULATED_DOWN_TIME));
//									monthlyNumberOfErrors = monthlyNumberOfErrors
//											+ Long.valueOf(oneCalc
//													.get(this.NUMBER_OF_ERROR_POLLS));
//								}
//							}
//							if (monthlyUpTime > 0) {
//								float totalAvailabilityPercent = 1;
//								if (monthlyUpTime > 0) {
//									totalAvailabilityPercent = (float) 1
//											- ((float) monthlyDownTime / (float) monthlyUpTime);
//								}
//								float calculatedAvailabilityPercent = 1;
//								if (monthlyAgreedUpTime > 0) {
//									calculatedAvailabilityPercent = (float) 1
//											- ((float) monthlyCalculatedDownTime / (float) monthlyAgreedUpTime);
//								}
//
//								if (totalAvailabilityPercent < 0.0001) {
//									totalAvailabilityPercent = 0;
//								}
//								if (calculatedAvailabilityPercent < 0.0001) {
//									calculatedAvailabilityPercent = 0;
//								}
//
//								Map<String, String> oneSuperColumn = new LinkedHashMap<String, String>();
//								oneSuperColumn.put(this.UP_TIME_DURATION,
//										String.valueOf(monthlyUpTime));
//								oneSuperColumn.put(TOTAL_DOWN_TIME,
//										String.valueOf(monthlyDownTime));
//								oneSuperColumn.put(CALCULATED_DOWN_TIME, String
//										.valueOf(monthlyCalculatedDownTime));
//								oneSuperColumn.put(AGREED_UP_TIME,
//										String.valueOf(monthlyAgreedUpTime));
//								oneSuperColumn
//										.put(TOTAL_AVAILABILITY_PERCENT,
//												String.valueOf(totalAvailabilityPercent));
//								oneSuperColumn
//										.put(CALCULATED_AVAILABILITY_PERCENT,
//												String.valueOf(calculatedAvailabilityPercent));
//								oneSuperColumn.put(NUMBER_OF_ERROR_POLLS,
//										String.valueOf(monthlyNumberOfErrors)); //
//								// System.out.println(currentStartTime);
//								log.debug("Loading row for:" + oneMonitor
//										+ " SUPERCOLUMN:"
//										+ String.valueOf(startTs));
//								informaticaCassandraLoader.loadRow(oneMonitor,
//										String.valueOf(startTs),
//										Long.valueOf(0), oneSuperColumn);
//								if (this.writeToFile) {
//									writer.append(oneMonitor);
//									writer.append(",");
//									writer.append(String.valueOf(startTs));
//									writer.append(",");
//									writer.append(String.valueOf(monthlyUpTime));
//									writer.append(",");
//							
//									writer.append(String
//											.valueOf(monthlyDownTime));
//									writer.append(",");
//									writer.append(String
//											.valueOf(monthlyCalculatedDownTime));
//									writer.append(",");
//									writer.append(String
//											.valueOf(monthlyAgreedUpTime));
//									writer.append(",");
//									writer.append(String
//											.valueOf(totalAvailabilityPercent));
//									writer.append(",");
//									writer.append(String
//											.valueOf(calculatedAvailabilityPercent));
//									writer.append("\n");
//									writer.flush();
//									this.setFileRowCount(this.getFileRowCount() + 1);
//									if (this.getFileRowCount() > FILE_ROW_COUNT_LIMIT) {
//										writer.close();
//
//										String baseFileName = System
//												.getenv("CASSANDRA_BULK_LOAD_DIR")
//												+ "/"
//												+ this.csvDir
//												+ "/UpTimeCalc_Monthly_"
//												+ this.getSourceSystemName()
//												+ String.valueOf(Calendar
//														.getInstance()
//														.getTimeInMillis() / 1000)
//												+ ".csv";
//										this.setWriter(new FileWriter(
//												baseFileName, true));
//										this.setFileRowCount(0);
//
//									}
//
//								}
//							}
//
//						}
//					}
//				}
//			}
//			startTs = String.valueOf(c1.getTimeInMillis() / 1000);
//
//			Calendar checkMonthlyStatHour = Calendar.getInstance();
//			checkMonthlyStatHour.setTimeInMillis(Long.valueOf(startTs) * 1000);
//			if (checkMonthlyStatHour.get(Calendar.HOUR_OF_DAY) == 23) {
//				checkMonthlyStatHour.add(Calendar.HOUR, 1);
//				startTs = String
//						.valueOf(checkMonthlyStatHour.getTimeInMillis() / 1000);
//			}
//
//		}
//
//		log.warn("files created");
//
//		log.warn(new Date());
//		informaticaCassandraLoader.loadData();
//		log.warn(new Date());
//
//		log.warn("Monthly calculation completed for :" + this.sourceSystemName);
//		log.warn("Set new time stamp");
//		upTimeCalculationDaoImpl.setNextStartTime(
//				String.valueOf(String.valueOf(c1.getTimeInMillis() / 1000)),
//				this.sourceSystemName + ":Monthly");
//		return;
//
//	}

	public static void main(String[] args) {
		try {
			final org.apache.log4j.Logger log = Logger
					.getLogger("up Time Calc main");
			log.warn("In upTimeCalc main");

			

			
			UpTimeCalculationExec upTimeCalculationExec = new UpTimeCalculationExec();
			String calcType = args[0];
			String writeToFile = args[1];
			String sourceSystem = args[2];
			String cassandraClusterName = args[3];
			String cassandraClusterIp = args[4];
			String csvDir = args[5];
			upTimeCalculationExec.setSourceSystemName(sourceSystem);
			upTimeCalculationExec.setRootIP(cassandraClusterIp);
			upTimeCalculationExec.setClusterName(cassandraClusterName);
			upTimeCalculationExec.setCsvDir(csvDir);
			
			log.warn("Input arguments: calculation type "+ calcType+", write to file  "+ writeToFile+", source system "+sourceSystem+" cluster name "+cassandraClusterName+"cluster IP "+cassandraClusterIp+", csv dir "+ csvDir);
			if (writeToFile.compareTo("true") == 0) {
				upTimeCalculationExec.setWriteToFile(true);
				Calendar c = Calendar.getInstance();
				String baseFileName = System
						.getenv("CASSANDRA_BULK_LOAD_DIR")
						+ "/"
						+ upTimeCalculationExec.getCsvDir()
						+ "/UpTimeCalc_Hourly_"
						+ sourceSystem
						+ String.valueOf(c.getTimeInMillis() / 1000)
						+ ".csv";
				upTimeCalculationExec.setWriter(new FileWriter(
						baseFileName, true));
				upTimeCalculationExec.setFileRowCount(0);
				

			}
			
			
			if (args[0].compareTo("incremental") == 0) {
				
					
					upTimeCalculationExec.incrementalCalculation();
/*					log.info("Hourly calculated will attempt daily");
					upTimeCalculationExec.dailyUpTimeCalcBatch();
					log.warn("Daily run - will check for monthly");
					upTimeCalculationExec.monthlyUpTimeCalcBatch();
*/
				
			} else {
				
					log.warn("Input argument is NOT Incremental - Inital Calculation");
//					upTimeCalculationExec.setCalculationType("initial");
//					upTimeCalculationExec.initialCalculation();
//					log.info("Initial calculation performed daily and monthly ");
					throw new IllegalStateException ("Calculation type has to be incremental");
					
				}
			

			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
			log.error(e.getMessage());
			log.error(e);

			System.exit(1);
		}
		
	}

	public UpTimeCalculationExec() throws Exception {
		super();

		

		// TODO Auto-generated constructor stub
	}

	public boolean isWriteToFile() {
		return writeToFile;
	}

	public void setWriteToFile(boolean writeToFile) {
		this.writeToFile = writeToFile;
	}

	public FileWriter getWriter() {
		return writer;
	}

	public void setWriter(FileWriter writer) {
		this.writer = writer;
	}

	public String getSourceSystemName() {
		return sourceSystemName;
	}

	public void setSourceSystemName(String sourceSystemName) {
		this.sourceSystemName = sourceSystemName;
	}

	public int getFileRowCount() {
		return fileRowCount;
	}

	public void setFileRowCount(int fileRowCount) {
		this.fileRowCount = fileRowCount;
	}

	public long getHourlyEndTimeStamp() {
		return hourlyEndTimeStamp;
	}

	public void setHourlyEndTimeStamp(long hourlyEndTimeStamp) {
		this.hourlyEndTimeStamp = hourlyEndTimeStamp;
	}

	public long getDailyEndTimeStamp() {
		return dailyEndTimeStamp;
	}

	public void setDailyEndTimeStamp(long dailyEndTimeStamp) {
		this.dailyEndTimeStamp = dailyEndTimeStamp;
	}

	public String getCalculationType() {
		return calculationType;
	}

	public void setCalculationType(String calculationType) {
		this.calculationType = calculationType;
	}

	public int getDailyFileRowCount() {
		return dailyFileRowCount;
	}

	public void setDailyFileRowCount(int dailyFileRowCount) {
		this.dailyFileRowCount = dailyFileRowCount;
	}

	public int getMonthlyFileRowCount() {
		return monthlyFileRowCount;
	}

	public void setMonthlyFileRowCount(int monthlyFileRowCount) {
		this.monthlyFileRowCount = monthlyFileRowCount;
	}

	public String getCsvDir() {
		return csvDir;
	}

	public void setCsvDir(String csvDir) {
		this.csvDir = csvDir;
	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public String getRootIP() {
		return rootIP;
	}

	public void setRootIP(String rootIP) {
		this.rootIP = rootIP;
	}

}
