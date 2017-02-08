package com.rackspace.foundation.up.time.load;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

import com.google.common.base.Charsets;
import com.google.common.io.Files;


public class LoadUpTimeCalcCF {
	protected String ipAddress;
	protected String clusterName;
	public LoadUpTimeCalcCF() {
		super();
		// TODO Auto-generated constructor stub
	}
	public LoadUpTimeCalcCF(String ipAddress, String clusterName) {
		super();
		this.ipAddress = ipAddress;
		this.clusterName = clusterName;
	}
	protected String moveDirectory;
	protected String sourceDirectory;
	private org.apache.log4j.Logger log = Logger
			.getLogger(LoadUpTimeCalcCF.class);
	public String getIpAddress() {
		return ipAddress;
	}
	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}
	public String getClusterName() {
		return clusterName;
	}
	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}
	public String getMoveDirectory() {
		return moveDirectory;
	}
	public void setMoveDirectory(String moveDirectory) {
		this.moveDirectory = moveDirectory;
	}
	public String getSourceDirectory() {
		return sourceDirectory;
	}
	public void setSourceDirectory(String sourceDirectory) {
		this.sourceDirectory = sourceDirectory;
	}
	protected boolean stopExecution() throws IOException{
		File controlFile = new File("runLoadControlFlag");
		if (controlFile.exists()) {
			String stopFlag = Files.toString(controlFile, Charsets.UTF_8);
			if (stopFlag.compareTo("1")==0){
				return true;
			}else{
				return false;
			}

		} else{
			log.error("runLoadControlFlag does not exist!");
			throw new IllegalStateException("runLoadControlFlag does not exist!");
		}
	}

}
