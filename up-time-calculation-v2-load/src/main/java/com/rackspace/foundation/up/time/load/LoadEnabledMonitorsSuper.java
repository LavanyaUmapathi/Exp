package com.rackspace.foundation.up.time.load;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.log4j.Logger;

import com.rackspace.foundation.informatica.cassandra.direct.loader.api.impl.InformaticaCassandraBulkLoader;



public class LoadEnabledMonitorsSuper extends LoadUpTimeCalcCF  {
	private final String KEYSPACE_NAME = "UpTimeCalcData";
	private final String CF_NAME = "EnabledMonitorsSuper";
	private boolean runLoadCassandraOnly=false;
	
	private org.apache.log4j.Logger log = Logger
			.getLogger(LoadEnabledMonitorsSuper.class);

	public void load() throws Exception{
		
		List<String> processedFiles = new ArrayList<String>();
		
		InformaticaCassandraBulkLoader informaticaLoader = 
		new InformaticaCassandraBulkLoader(this.clusterName, this.ipAddress,
				this.KEYSPACE_NAME, this.CF_NAME, true);
		
		File folder = new File(this.sourceDirectory);
		if (!runLoadCassandraOnly){
		
		for (File file : folder.listFiles()){
			if (!file.isDirectory()){
			FileInputStream fileStream=null;
			Scanner sc = null;
			log.warn("processing file: "+file.getAbsolutePath());
			try{
			fileStream=new FileInputStream(file);
			sc = new Scanner (fileStream,"UTF-8");
			int rowCount =0;
			Map<String,String> enabledMonitors= new HashMap<String,String>();
			
			String superColumn = null;
			String previousRowKey="";
			while (sc.hasNextLine()){
				
				String[] allFields = sc.nextLine().split(",");
				String rowKey = allFields[0];
				if (rowKey.compareTo(previousRowKey)!=0)
				{
					if (rowCount ==0){
					previousRowKey=allFields[0];
					superColumn=allFields[1];
					rowCount++;
					
					} else{
						log.warn("About to load row: "+previousRowKey+" "+superColumn+" "+enabledMonitors.size());
						
						informaticaLoader.loadRow(previousRowKey, superColumn, Long.valueOf(0), enabledMonitors);
						enabledMonitors= new HashMap<String,String>();
						previousRowKey=allFields[0];
						superColumn=allFields[1];
						
					}
				}
				enabledMonitors.put(allFields[2], "-1");
		
			}
			
			log.warn("About to load row: "+previousRowKey+" "+superColumn+" "+enabledMonitors.size());
			
			informaticaLoader.loadRow(previousRowKey, superColumn, Long.valueOf(0), enabledMonitors);
			processedFiles.add(file.getName());
			
			
			
			} finally {
				if (fileStream != null){
					fileStream.close();
				}
				if (sc != null){
					sc.close();
				}
			}
			}
			
		}
		}
		log.warn("About to load data into cassandra");
		informaticaLoader.loadData();
		File moveDir= new File(this.moveDirectory);
		if (!moveDir.exists()){
			moveDir.mkdir();
		}
		for (String oneFileName : processedFiles){
			File oneFile = new File (this.sourceDirectory+"/"+oneFileName);
			oneFile.renameTo(new File(this.moveDirectory+"/"+oneFileName));
		}
			
		
	}
public static void main(String[] args) throws Exception {
		
	LoadEnabledMonitorsSuper ee = new LoadEnabledMonitorsSuper();
		ee.setClusterName(args[0]);
		ee.setIpAddress(args[1]);
		ee.setSourceDirectory(args[2]);
		ee.setMoveDirectory(args[3]);
		ee.setRunLoadCassandraOnly(Boolean.valueOf(args[4]).booleanValue());
		final org.apache.log4j.Logger log = Logger
				.getLogger("LoadEnabledMonitorsSuper main");
		log.warn("LoadEnabledMonitorsSuper input arguments "+ ee.getClusterName()+" "+ee.getIpAddress()+" "+ee.getSourceDirectory()+" "+ee.getMoveDirectory() +" "+ee.isRunLoadCassandraOnly());

		ee.load();

	}
public boolean isRunLoadCassandraOnly() {
	return runLoadCassandraOnly;
}
public void setRunLoadCassandraOnly(boolean runLoadCassandraOnly) {
	this.runLoadCassandraOnly = runLoadCassandraOnly;
}

}
