package com.rackspace.foundation.device.status.to.supression;

import java.util.HashMap;
import java.util.Map;

public class DeviceStatusToSupression {
	private String previousDeviceNumber;
	private int previousStartStatus;
	private int previouEndStatus;
	
	private long previousTimeStamp;
	private static final int ONLINE=12;
	private static final int MIGRATION=15;
	private static final int SYSTEM_COMPROMISED=42;
	private static final int APP_COMPROMISED=44;
	
	public Map<String, Map<String, Map<String, String>>> determineSupressionIterval(
			String deviceNumber, int startStatus, 
			int endStatus,  long transitionTimeStamp) {
		Map<String, String> oneSupressionIntervalDetails = new HashMap<String, String>();
		Map<String, Map<String, String>> oneSupressionIntervalResult = new HashMap<String, Map<String, String>>();
		Map<String, Map<String, Map<String, String>>> oneSupressionIntervalFullResult = new HashMap<String, Map<String, Map<String, String>>>();
		// System.out.println("In the calc");
		// System.out.println(deviceNumber);
		// System.out.println(startStatus);
		// System.out.println(startStatusFlag);
		// System.out.println(endStatus);
		// System.out.println(endStatusFlag);
		// System.out.println(transitionTimeStamp);
		// System.out.println("Previous device");
		// System.out.println(this.previousDeviceNumber);

		if (deviceNumber.compareTo(previousDeviceNumber) == 0) {
			// System.out.println("Same device");
			if (this.previouEndStatus!= this.APP_COMPROMISED &&
					this.previouEndStatus!=this.SYSTEM_COMPROMISED &&
					this.previouEndStatus!=this.MIGRATION &&
					this.previouEndStatus!=this.ONLINE) {
				// System.out.println("previous status to supression");
				// System.out.println("EndTimeStamp:"+
				// String.valueOf(transitionTimeStamp));
				// System.out.println("Duration:"+String.valueOf(transitionTimeStamp-this.previousTimeStamp));
				// System.out.println("Start TS:"+String.valueOf(this.previousTimeStamp));
				oneSupressionIntervalDetails.put("EndTime",
						String.valueOf(transitionTimeStamp));
				oneSupressionIntervalDetails.put(
						"SupressionDuration",
						String.valueOf(transitionTimeStamp
								- this.previousTimeStamp));
				oneSupressionIntervalDetails.put("SupressionSource",
						"DeviceStatus");
				oneSupressionIntervalResult.put(
						String.valueOf(this.previousTimeStamp),
						oneSupressionIntervalDetails);
				oneSupressionIntervalFullResult.put(deviceNumber,
						oneSupressionIntervalResult);
				// System.out.println("Output created");

			}
			
		} else {
			// System.out.println("Devices different");
			if (this.previousDeviceNumber.length() > 0) {
				// System.out.println("Not the first record");
				if (this.previouEndStatus!=this.APP_COMPROMISED &&
						this.previouEndStatus!=this.SYSTEM_COMPROMISED &&
						this.previouEndStatus!=this.ONLINE &&
						this.previouEndStatus!=this.MIGRATION) {
					// System.out.println("previous End was supression- create with end 0");
					oneSupressionIntervalDetails.put("EndTime", "0");
					oneSupressionIntervalDetails.put("SupressionDuration", "0");
					oneSupressionIntervalDetails.put("SupressionSource",
							"DeviceStatus");
					oneSupressionIntervalResult.put(
							String.valueOf(this.previousTimeStamp),
							oneSupressionIntervalDetails);
					oneSupressionIntervalFullResult.put(
							this.previousDeviceNumber,
							oneSupressionIntervalResult);

				} 
				}
		
		}
		// System.out.println("Set previous");
		this.previousDeviceNumber = deviceNumber;
		this.previousStartStatus = startStatus;
		
		this.previouEndStatus = endStatus;
		
		this.previousTimeStamp = transitionTimeStamp;

		return oneSupressionIntervalFullResult;
	}

	public String getPreviousDeviceNumber() {
		return previousDeviceNumber;
	}

	public void setPreviousDeviceNumber(String previousDeviceNumber) {
		this.previousDeviceNumber = previousDeviceNumber;
	}

	public int getPreviousStartStatus() {
		return previousStartStatus;
	}

	public void setPreviousStartStatus(int previousStartStatus) {
		this.previousStartStatus = previousStartStatus;
	}

	public int getPreviouEndStatus() {
		return previouEndStatus;
	}

	public void setPreviouEndStatus(int previouEndStatus) {
		this.previouEndStatus = previouEndStatus;
	}

	

	public long getPreviousTimeStamp() {
		return previousTimeStamp;
	}

	public void setPreviousTimeStamp(long previousTimeStamp) {
		this.previousTimeStamp = previousTimeStamp;
	}

	public DeviceStatusToSupression() {
		super();
		this.previouEndStatus = 0;
		this.previousDeviceNumber = "";
		
		this.previousStartStatus = 0;
		this.previousTimeStamp = 0;
		// TODO Auto-generated constructor stub
	}

}
