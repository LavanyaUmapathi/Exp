package com.rackspace.foundation.informatica.cassandra.direct.loader.api.impl;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.apache.cassandra.utils.UUIDGen.decompose;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.io.sstable.SSTableSimpleUnsortedWriter;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.io.IOUtils;

import com.rackspace.foundation.informatica.cassandra.direct.loader.helper.CassandraBulkLoader;

public class InformaticaCassandraBulkLoader  {

	private SSTableSimpleUnsortedWriter informaticaCassandraWritter;


	private String previousKey = "";

	private String rootClusterName;
	private String rootURL;

	private String keyspaceName;
	private String columnFamilyName;

	private boolean superColumnFamily;

	private String bulkLoadDir;

	private int rowCount;

	public void loadRow(String CASSANDRA_KEY, String SUPERCOLUMN,
			Long TIMESTAMP, Map<String, String> rowMap) // throws Exception
	{

		String Attribute_Name = "";
		String Attribute_Value = "";
		ByteBuffer uuid;
		long timestamp;
		this.rowCount++;

		try {

			if (!StringUtils.isBlank(CASSANDRA_KEY)
					&& !StringUtils.isEmpty(CASSANDRA_KEY)) {
				if (previousKey.compareTo(CASSANDRA_KEY) != 0) {
					this.informaticaCassandraWritter
							.newRow(bytes(CASSANDRA_KEY));
					previousKey = CASSANDRA_KEY;
					this.rowCount = 1;

				}
			} else {

				uuid = ByteBuffer.wrap(decompose(UUID.randomUUID()));
				this.informaticaCassandraWritter.newRow(uuid);

			}

			if (this.rowCount % 5000 == 0) {
				this.informaticaCassandraWritter.newRow(bytes(CASSANDRA_KEY));
			}

			if (!StringUtils.isBlank(SUPERCOLUMN)
					&& !StringUtils.isEmpty(SUPERCOLUMN)) {

				this.informaticaCassandraWritter
						.newSuperColumn(bytes(SUPERCOLUMN));

			}
			;
			if (TIMESTAMP == 0) {
				timestamp = System.currentTimeMillis() * 1000;
			} else {
				timestamp = TIMESTAMP;
			}

			@SuppressWarnings("rawtypes")
			Iterator columnIterator = rowMap.entrySet().iterator();
			while (columnIterator.hasNext()) {

				@SuppressWarnings("rawtypes")
				Map.Entry m = (Map.Entry) columnIterator.next();

				Attribute_Name = (String) m.getKey();

				Attribute_Value = (String) m.getValue();

				this.informaticaCassandraWritter.addColumn(
						bytes(Attribute_Name), bytes(Attribute_Value),
						timestamp);

		
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
			for (StackTraceElement sT : e.getStackTrace()) {
				System.out.println(sT.toString());
			}
		}
	}

	public void loadData() throws Exception {

		System.out.println("Closing cassandra writter");

		this.informaticaCassandraWritter.close();

		System.out.println("Closed cassandra writter");
		System.out.println(this.bulkLoadDir);
		System.out.println(this.rootURL);
		String[] loadDir = new String[2];
		loadDir[0] = this.bulkLoadDir;
		loadDir[1] = "-d " + this.rootURL;
		System.out.println(loadDir[0]);
		System.out.println(loadDir[1]);

		try {

			System.out
					.print("About to start cassandra data load - not system call");
			CassandraBulkLoader cassandraBulkLoader = new CassandraBulkLoader();
			cassandraBulkLoader.mainLoad(loadDir);

			System.out.print("Cassandra data load completed");

		} catch (Exception e) {
			System.out.println(e.getMessage());

		}
		System.out.print("Deleting temporary loader files");
		File[] files = (new File(loadDir[0])).listFiles();
		for (File file : files) {
			file.delete();

		}
		System.out.print("Temporary loader files deleted");
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
		try {
			if (this.informaticaCassandraWritter == null) {

				System.out.println(this.rootClusterName);
				System.out.println(this.rootURL);

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

					this.informaticaCassandraWritter = new SSTableSimpleUnsortedWriter(
							new File(this.bulkLoadDir),
							new RandomPartitioner(), this.keyspaceName,
							this.columnFamilyName, AsciiType.instance,
							AsciiType.instance, 64);
				} else {
					this.informaticaCassandraWritter = new SSTableSimpleUnsortedWriter(
							new File(this.bulkLoadDir),
							new RandomPartitioner(), this.keyspaceName,
							this.columnFamilyName, AsciiType.instance, null, 64);
				}

			}
		} catch (Exception e) {
			System.out.println("manage writter exception");
			System.out.println(e.getMessage());
			for (StackTraceElement sT : e.getStackTrace()) {
				System.out.println(sT.toString());
			}
			throw e;
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

}
