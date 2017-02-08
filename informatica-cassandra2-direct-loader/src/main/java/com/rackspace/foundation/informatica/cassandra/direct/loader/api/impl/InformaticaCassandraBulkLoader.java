package com.rackspace.foundation.informatica.cassandra.direct.loader.api.impl;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.apache.cassandra.utils.UUIDGen.decompose;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

//import me.prettyprint.cassandra.serializers.AsciiSerializer;




import org.apache.cassandra.db.BufferCell;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.io.sstable.SSTableSimpleUnsortedWriter;
import org.apache.cassandra.io.sstable.SSTableSimpleWriter;
//import org.apache.commons.lang.StringUtils;
import org.apache.commons.io.IOUtils;

//import com.rackspace.foundation.informatica.cassandra.direct.loader.helper.CassandraBulkLoader;
import org.apache.log4j.Logger;

import com.google.common.collect.Table.Cell;
import com.rackspace.foundation.apache.cassandra.tools.BulkLoader;



public class InformaticaCassandraBulkLoader  {

	private SSTableSimpleUnsortedWriter informaticaCassandraWritter;
	//private SSTableSimpleWriter informaticaCassandraWritter;
	
	private File csvFile;
	private Writer csvWriter;

	private String previousKey = "";

	private String rootClusterName;
	private String rootURL;

	private String keyspaceName;
	private String columnFamilyName;

	private boolean superColumnFamily;

	private String bulkLoadDir;

	private int rowCount;
	
	private boolean createCSVOnly;
	private String csvDir;
	
	private org.apache.log4j.Logger log = Logger
			.getLogger(InformaticaCassandraBulkLoader.class);
	
	CompositeType compType;

	public void loadRow(String CASSANDRA_KEY, String SUPERCOLUMN,
			Long TIMESTAMP, Map<String, String> rowMap) throws IOException // throws Exception
	{
if (!this.createCSVOnly){
		String Attribute_Name = "";
		String Attribute_Value = "";
		ByteBuffer uuid;
		long timestamp;
		this.rowCount++;

		try {


			log.info("BUILDING NEW COMPOSITE  JUST ONE KEY!!!!!!!!!!!!!!!!!!!!!!");
			this.informaticaCassandraWritter
			.newRow(bytes(CASSANDRA_KEY));

			;
			if (TIMESTAMP == 0) {
				timestamp = System.currentTimeMillis();
			} else {
				timestamp = TIMESTAMP;
			}
			log.info("Current timestamp: "+ timestamp);
		
			Iterator<Entry<String,String>> columnIterator = rowMap.entrySet().iterator();
			int cc=0;
			while (columnIterator.hasNext()) {

				
				Entry<String,String> m = columnIterator.next();
				
				

				Attribute_Name =  m.getKey();

				Attribute_Value =  m.getValue();
				if (cc%1000==0){
					log.info(Attribute_Name +" "+Attribute_Value);
				}
				
			
				this.informaticaCassandraWritter.addColumn(
						 this.compType.builder().add( bytes( SUPERCOLUMN ) ).add( bytes( Attribute_Name ) ).build(),  bytes(Attribute_Value),
						System.currentTimeMillis()*1000L);
				cc++;

		
			}
		} catch (Exception e) {
			log.warn(e.getMessage());
			for (StackTraceElement sT : e.getStackTrace()) {
				log.warn(sT.toString());
			}
		}
} else{
	this.csvWriter.write(CASSANDRA_KEY+","+SUPERCOLUMN);
	Iterator<String> valuesIterator= rowMap.values().iterator();
	while (valuesIterator.hasNext()){
		this.csvWriter.write(","+valuesIterator.next());
	}
	this.csvWriter.write("\n");
	
}
	}

	public void loadData() throws Exception {
		if (!this.createCSVOnly){

		log.warn("Closing cassandra writter");
		if (this.informaticaCassandraWritter!= null)
		this.informaticaCassandraWritter.close();

		log.warn("Closed cassandra writter");
		log.warn(this.bulkLoadDir);
		log.warn(this.rootURL);
		String[] loadDir = new String[2];
		loadDir[0] = this.bulkLoadDir;
		loadDir[1] = "-d " + this.rootURL;
		log.warn(loadDir[0]);
		log.warn(loadDir[1]);

		try {

			log.warn("About to start cassandra data load - not system call");
			log.warn("Creating com.rackspace.foundation.apache.cassandra.tools.BulkLoader");
			
			BulkLoader cassandraBulkLoader = new BulkLoader();
			cassandraBulkLoader.mainLoad(loadDir);

			log.warn("Cassandra data load completed");

		} catch (Exception e) {
			log.warn(e.getMessage());

		}
		
		File[] files = (new File(loadDir[0])).listFiles();
		for (File file : files) {
			file.delete();

		}
		log.warn("Temporary loader files deleted");
		} else{
			this.csvWriter.close();
			
		}
		
	}

	public String getRootClusterName() {
		return rootClusterName;
	}

	public void setRootClusterName(String rootClusterName) {
		this.rootClusterName = rootClusterName;
	}

	public String getRootURL() {
		return rootURL;
	}

	public void setRootURL(String rootURL) {
		this.rootURL = rootURL;
	}

	public String getKeyspaceName() {
		return keyspaceName;
	}

	public void setKeyspaceName(String keyspaceName) {
		this.keyspaceName = keyspaceName;
	}

	public String getColumnFamilyName() {
		return columnFamilyName;
	}

	public void setColumnFamilyName(String columnFamilyName) {
		this.columnFamilyName = columnFamilyName;
	}

	public InformaticaCassandraBulkLoader() {
		super();
		// System.out.print("Constructor");
		// TODO Auto-generated constructor stub
	}

	public boolean isSuperColumnFamily() {
		return superColumnFamily;
	}

	public void setSuperColumnFamily(boolean superColumnFamily) {
		this.superColumnFamily = superColumnFamily;
	}

	private void maintainBulkLoadDir() {

		// System.out.print("get bulk dir");
		String baseFileName = "";
		String baseCSVDirName="";
		if (this.bulkLoadDir == null) {
			// System.out.print("get bulk dir it is null");
			File cassandraLoadDir;

			if (System.getenv("CASSANDRA_BULK_LOAD_DIR") == null) {
				// System.out.print("get bulk dir env is null");
				baseFileName = "/home/informatica/infa_shared/TgtFiles";
				// System.out.print(baseFileName);

			} else {
				baseFileName = System.getenv("CASSANDRA_BULK_LOAD_DIR");
				// System.out.print(baseFileName);

			}
			if (this.createCSVOnly){
				baseCSVDirName=baseFileName+"/csv";
				File csvCreateDir= new File(baseCSVDirName);
				if (!csvCreateDir.exists()){
					csvCreateDir.mkdir();
				}
				String cfCSVBaseFileName = baseCSVDirName+ "/" + this.keyspaceName;
				csvCreateDir= new File(cfCSVBaseFileName);
				if (!csvCreateDir.exists()){
					csvCreateDir.mkdir();
				}
				String kCSVBaseFileName = cfCSVBaseFileName+"/"+this.columnFamilyName;
				csvCreateDir= new File(kCSVBaseFileName);
				if (!csvCreateDir.exists()){
					csvCreateDir.mkdir();
				}
				this.csvDir=csvCreateDir.getAbsolutePath();
				log.info("Create CSV files only load");
				log.info("Csv Dir is: "+this.csvDir);
				
			}
			log.info("BaseFileName:" + baseFileName);
			cassandraLoadDir = new File(baseFileName);
			if (!cassandraLoadDir.exists()) {
				// System.out.print("get base bulk dir create dir");
				cassandraLoadDir.mkdir();
			}

			// String cfBaseFileName = baseFileName+"/"+this.columnFamilyName;
			String cfBaseFileName = baseFileName + "/" + this.keyspaceName;
			// System.out.print(cfBaseFileName);
			cassandraLoadDir = new File(cfBaseFileName);
			if (!cassandraLoadDir.exists()) {
				// System.out.print("get cfbase bulk dir create dir");
				cassandraLoadDir.mkdir();
			}
			// String ksBaseFileName= cfBaseFileName+"/"+this.keyspaceName;
			String ksBaseFileName = cfBaseFileName + "/"
					+ this.columnFamilyName;
			// System.out.print(ksBaseFileName);
			cassandraLoadDir = new File(ksBaseFileName);
			if (!cassandraLoadDir.exists()) {
				// System.out.print("get ksbase bulk dir create dir");
				cassandraLoadDir.mkdir();
			}

			this.bulkLoadDir = cassandraLoadDir.getAbsolutePath();
		}
		System.out.print("get bulk dir" + this.bulkLoadDir);
		return;
	}

	private void maintainInformaticaCassandraWritter() throws Exception {
		if (!this.createCSVOnly){
		try {
			if (this.informaticaCassandraWritter == null) {

				log.warn(this.rootClusterName);
				log.warn(this.rootURL);

				FileInputStream fIs = new FileInputStream(
						System.getenv("CASSANDRA_CONF")
								+ "/cassandraTemplate.yaml");
				String yamlFileContent = IOUtils.toString(fIs);

				String newYamlFileContent = yamlFileContent.replaceAll(
						"#seedString#", this.rootURL).replaceAll(
						"#clusterName#", this.rootClusterName);

				String newYamlFileName = System.getenv("CASSANDRA_CONF")
						+ "/cassandra.yaml";
				String newYamlFileConfigName = "file:" + newYamlFileName;

				FileOutputStream fOs = new FileOutputStream(newYamlFileName);
				IOUtils.write(newYamlFileContent, fOs);
				IOUtils.closeQuietly(fIs);
				IOUtils.closeQuietly(fOs);

				System.setProperty("cassandra.config", newYamlFileConfigName);

				if (superColumnFamily) {
					log.info("TWO VALUE IN COMPOSITE Creating supercolumn loader WITH COMPOSITE !!!!!!!!!!!!!!!!!");
					final List<AbstractType<?>> compositeTypes = new ArrayList<>();
			        compositeTypes.add(AsciiType.instance);
			        compositeTypes.add(AsciiType.instance);
			        
			       compType =
			        		CompositeType.getInstance(compositeTypes);
			        
			        
					
					
					
					
//					this.informaticaCassandraWritter = new SSTableSimpleUnsortedWriter(
//							new File(this.bulkLoadDir),
//							new Murmur3Partitioner(), this.keyspaceName,
//							this.columnFamilyName, AsciiType.instance,
//							AsciiType.instance, 64); 
					this.informaticaCassandraWritter = new SSTableSimpleUnsortedWriter(
							new File(this.bulkLoadDir),
							new Murmur3Partitioner(), this.keyspaceName,
							this.columnFamilyName, compType,
							null, 64); 
					log.info("using Murmur3Partitioner & SSTableSimpleUnsortedWriter");
//					this.informaticaCassandraWritter = new SSTableSimpleWriter(
//							new File(this.bulkLoadDir),
//							new Murmur3Partitio
//					:q!ner(), this.keyspaceName,
//							this.columnFamilyName, AsciiType.instance,
//							AsciiType.instance);
				} else {
					this.informaticaCassandraWritter = new SSTableSimpleUnsortedWriter(
							new File(this.bulkLoadDir),
							new Murmur3Partitioner(), this.keyspaceName,
							this.columnFamilyName, AsciiType.instance, null, 64);
//					this.informaticaCassandraWritter = new SSTableSimpleWriter(
//							new File(this.bulkLoadDir),
//							new Murmur3Partitioner(), this.keyspaceName,
//							this.columnFamilyName, AsciiType.instance, null);
				}

			}
		} catch (Exception e) {
			log.warn("manage writter exception");
			log.warn(e.getMessage());
			for (StackTraceElement sT : e.getStackTrace()) {
				log.warn(sT.toString());
			}
			throw e;
		}
		} else{
			String fileName = this.keyspaceName+"_"+this.columnFamilyName+"_"+Calendar.getInstance().getTimeInMillis()+".txt";
			this.csvFile= new File(this.csvDir+"/"+fileName);
			this.csvWriter = new BufferedWriter(new FileWriter(this.csvFile));
			
			
			
		}
		return;
	}

	public InformaticaCassandraBulkLoader(String rootClusterName,
			String rootURL, String keyspaceName, String columnFamilyName,
			boolean superColumnFamily) throws Exception {
		super();
		this.rootClusterName = rootClusterName;
		this.rootURL = rootURL;
		this.keyspaceName = keyspaceName;
		this.columnFamilyName = columnFamilyName;
		this.superColumnFamily = superColumnFamily;
		this.maintainBulkLoadDir();
		this.maintainInformaticaCassandraWritter();
		this.rowCount = 0;
	}

	public boolean isCreateCSVOnly() {
		return createCSVOnly;
	}

	public void setCreateCSVOnly(boolean createCSVOnly) {
		this.createCSVOnly = createCSVOnly;
	}

}
