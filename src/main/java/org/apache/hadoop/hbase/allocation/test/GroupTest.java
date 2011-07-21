package org.apache.hadoop.hbase.allocation.test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.allocation.CheckMeta;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.IOUtils;


public class GroupTest {
	private static HTableDescriptor[] listTables() throws IOException {
		final TreeSet<HTableDescriptor> uniqueTables = new TreeSet<HTableDescriptor>();
		MetaScannerVisitor visitor = new MetaScannerVisitor() {
			public boolean processRow(Result result) throws IOException {
				try {
					byte[] value = result.getValue(HConstants.CATALOG_FAMILY,
							HConstants.REGIONINFO_QUALIFIER);
					HRegionInfo info = null;
					if (value != null) {
						info = Writables.getHRegionInfo(value);
					}
					// Only examine the rows where the startKey is zero length
					if (info != null && info.getStartKey().length == 0) {
						uniqueTables.add(info.getTableDesc());
					}
					return true;
				} catch (RuntimeException e) {
					throw e;
				}
			}
		};
		MetaScanner.metaScan(HBaseConfiguration.create(), visitor);

		return uniqueTables.toArray(new HTableDescriptor[uniqueTables.size()]);
	}
	final static Configuration c = HBaseConfiguration.create();
	public static void main(String args[]) throws IOException {
		Configuration c = HBaseConfiguration.create();
		HBaseAdmin d = new HBaseAdmin(c);
		// d.disableTable("testPri");
		// d.deleteTable("testPri");
		HServerInfo s = null;
		for (HServerInfo server : d.getClusterStatus().getServerInfo()) {
			s = server;
			System.out.println(server);
		}
		HTableDescriptor[] tables = listTables();
		for (HTableDescriptor des : tables) {
			
				HashMap<HRegionInfo, HServerAddress> map = CheckMeta
						.getRegionAddress(des.getNameAsString());
				for (Entry<HRegionInfo, HServerAddress> e : map.entrySet())
					System.out.println(e.getKey().getRegionNameAsString()+ ":"
							+ e.getValue());
		}
		String conffile = "/home/liujia_l.pt/test.group";
		FileOutputStream fwriter = new FileOutputStream(new File(conffile));
		String ss = "0;dw79.kgb.sqa.cm4,60027;:1;dw79.kgb.sqa.cm4,60033;dw79.kgb.sqa.cm4,60024:";
		fwriter.write(Bytes.toBytes(ss));
		fwriter.close();
		File localfile = new File(conffile);
		FileSystem fs = FileSystem.get(c);

		long localsize = localfile.length();
		Path p=new Path(FSUtils.getRootDir(c)+ "groupinformation.conf");
		Path inputpath= new Path("/disk1/hbasedata2/groupinformation.conf");
		if (localsize > 0) {
			InputStream inst = new BufferedInputStream(new FileInputStream(
					conffile));		
			OutputStream outst = fs.create(inputpath);
			IOUtils.copyBytes(inst, outst, (int) localsize, true);
		}

	}

}
