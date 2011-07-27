/**
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.allocation.group;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;

/**
 * Read group configuration from hdfs and write back to hdfs
 * <p>
 * Also provide region and table information on regionserver
 */
public class ServerWithGroup {
	static final Log LOG = LogFactory.getLog(ServerWithGroup.class);

	final static String DEFAULT_GROUP = "0";

	private static boolean isprocess = false;
	private static boolean ismoveregion = false;
	private static boolean isbalance = false;
	private static boolean ischangetable = false;
	private static String currentserver = "";
	private static boolean doMoveconf = false;
	private static boolean iserror = false;
	private static String errormsg = "";

	private static Set<MoveGroupPlan> movegroupPlanset = new HashSet<MoveGroupPlan>();

	/**
	 * Used for prevent other task when there is an task processing in system
	 * 
	 * This method is used for identify different tasks.
	 * 
	 * @return true if task is processing , false if not.
	 */
	public static boolean isIsprocess() {
		return isprocess;
	}

	/**
	 * Set process flag
	 * 
	 * @param current
	 *            process flag
	 */
	public static void setIsprocess(boolean isprocess) {
		ServerWithGroup.isprocess = isprocess;
	}

	/**
	 * Used for get current processing regionserver when change regionserver
	 * group
	 * 
	 * @return current processing regionserver name
	 */
	public static String getCurrentserver() {
		return currentserver;
	}

	/**
	 * Set current processing regionserver name
	 * 
	 * @param currentserver
	 *            regionserver name
	 */
	public static void setCurrentserver(String currentserver) {
		ServerWithGroup.currentserver = currentserver;
	}

	/**
	 * Used for identify whether regionserver is updating configuration and jars
	 * 
	 * @return true if system is move regionserver configuration ,else false
	 */
	public static boolean isDoMoveconf() {
		return doMoveconf;
	}

	/**
	 * Set regionserver is move configuration and lib flag
	 * 
	 * @param doMoveconf
	 *            if system is updating regionserver configuration and library
	 */
	public static void setDoMoveconf(boolean doMoveconf) {
		ServerWithGroup.doMoveconf = doMoveconf;
	}

	/**
	 * Used for identify whether regionserver is moving regions
	 * 
	 * @return true if system is moving regions between regionservers , else
	 *         false
	 */
	public static boolean isIsmoveregion() {
		return ismoveregion;
	}

	/**
	 * Set regionserver is move region flag
	 * 
	 * @param ismoveregion
	 *            if system is moving regions
	 */
	public static void setIsmoveregion(boolean ismoveregion) {
		ServerWithGroup.ismoveregion = ismoveregion;
	}

	/**
	 * Used for identify whether system is balancing group or table
	 * 
	 * @return true if system is balancing , false if not.
	 */
	public static boolean isIsbalance() {
		return isbalance;
	}

	/**
	 * Set system is balance group or table
	 * 
	 * @param isbalance
	 *            if system is balancing table or group
	 */
	public static void setIsbalance(boolean isbalance) {
		ServerWithGroup.isbalance = isbalance;
	}

	/**
	 * Used for identify whether system is change table priority or groups
	 * 
	 * @return true if system is changing talbe, false if not.
	 */
	public static boolean isIschangetable() {
		return ischangetable;
	}

	/**
	 * Set system is changing table group or priority
	 * 
	 * @param ischangetable
	 *            if system is change table priority or groups
	 */
	public static void setIschangetable(boolean ischangetable) {
		ServerWithGroup.ischangetable = ischangetable;
	}

	/**
	 * Used for identify whether system is has an error, this will shown in
	 * front-end
	 * 
	 * @return true if there is error occured ,else fasle
	 */
	public static boolean isIserror() {
		return iserror;
	}

	/**
	 * Set system is encount an error when processing
	 * 
	 * @param iserror
	 *            if system occured error
	 */
	public static void setIserror(boolean iserror) {
		ServerWithGroup.iserror = iserror;
	}

	/**
	 * Used for show error message content , this will shown in front-end
	 * 
	 * @return msg , error message content
	 */
	public static String getErrormsg() {
		return errormsg;
	}

	/**
	 * Set error message content
	 * 
	 * @param errormsg
	 *            set error message
	 */
	public static void setErrormsg(String errormsg) {
		ServerWithGroup.errormsg = errormsg;
	}

	/**
	 * Used for change regionserver group , {@link MoveGroupPlan} will be done
	 * by a thread {@link ProcessMove} , it will be used in front-end
	 * 
	 * @return current processing regionserver plans movegroupPlanset
	 */
	public static Set<MoveGroupPlan> getMovegroupPlanset() {
		return movegroupPlanset;
	}

	/**
	 * Set current moveplan {@link MoveGroupPlan}
	 * 
	 * @param movegroupPlanset
	 *            current will moved regionservers
	 */
	public static void setMovegroupPlanset(Set<MoveGroupPlan> movegroupPlanset) {
		ServerWithGroup.movegroupPlanset = movegroupPlanset;
	}

	/**
	 * Read regionserver information form hdfs to a String line
	 * <p>
	 * Configuration file is stored in
	 * hdfs://${hbase.rootdir}/groupinformation.conf Format :
	 * groupname1;isSpecial;regionservername1;regionservername2:
	 * groupname2;isSpecial;regionservername3
	 * 
	 * @param master
	 *            HMaster interface
	 * @param confpath
	 *            groupinformation file Path in hdfs
	 * @return line Configuration File content
	 * @throws IOException
	 *             IOException ocurred in hdfs
	 */
	public static String readGroupInfo(HMaster master, final String confpath)
			throws IOException {
		FileSystem fs = master.getMasterFileSystem().getFileSystem();
		URI hdfsuri = master.getMasterFileSystem().getRootDir().toUri();
		Path inputpath = new Path(hdfsuri.toString() + "/" + confpath);
		String outline = "";
		boolean isexist;
		try {
			isexist = fs.exists(inputpath);
		} catch (IOException e) {
			return outline;
		}
		if (isexist) {
			FSDataInputStream hdfsInStream = fs.open(inputpath);
			byte[] ioBuffer = new byte[1024];
			int readLen = hdfsInStream.read(ioBuffer);
			while (-1 != readLen) {
				String line = Bytes.toStringBinary(ioBuffer, 0, readLen);
				outline += line;
				readLen = hdfsInStream.read(ioBuffer);
			}
			hdfsInStream.close();
		}
		LOG.info("Read groupinformation.conf from  hdfs://${hbase.rootdir}/groupinformation.conf");
		return outline;
	}

	/**
	 * Initialize groupmap for regionserver
	 * <p>
	 * System will delete offline regionserver from configuration file and add
	 * new online regionserver into DEFAULT_GROUP
	 * 
	 * @param master
	 *            HMaster interface
	 * @param confline
	 *            Configuration content
	 * @return groupmap Map< String, List< String >> key:groupname , value:
	 *         regionserver name list
	 */
	public static Map<String, List<String>> initGroupMap(HMaster master,
			String confline) {

		ClusterStatus status = master.getClusterStatus();
		Collection<HServerInfo> serverInfo = status.getServerInfo();
		Set<String> onlineserver = new HashSet<String>();
		for (HServerInfo info : serverInfo) {
			String servername = info.getHostname() + ","
					+ info.getServerAddress().getPort();
			onlineserver.add(servername);
		}

		Map<String, List<String>> groupmap = new HashMap<String, List<String>>();
		Set<String> finalserverset = new HashSet<String>();
		if (confline.length() > 0) {
			String[] grouplines = confline.split(":");
			for (String line : grouplines) {
				if (line == null || line.length() <= 0)
					continue;
				String[] serverlines = line.split(";");
				if (serverlines.length <= 1)
					continue;
				String groupname = serverlines[0];
				// add servers of this group
				List<String> rglist = new ArrayList<String>();
				for (int i = 2; i < serverlines.length; i++) {
					if (onlineserver.contains(serverlines[i])) {
						rglist.add(serverlines[i]);
						finalserverset.add(serverlines[i]);
					}
				}
				groupmap.put(groupname, rglist);
			}
		}
		// add new online server
		for (HServerInfo info : serverInfo) {
			String servername = info.getHostname() + ","
					+ info.getServerAddress().getPort();
			if (!finalserverset.contains(servername)) {
				// add to default group
				if (!groupmap.containsKey(DEFAULT_GROUP)) {
					groupmap.put(DEFAULT_GROUP, new ArrayList<String>());
				}
				groupmap.get(DEFAULT_GROUP).add(servername);
			}
		}
		return groupmap;
	}

	/**
	 * Get group property grouppropertymap from configuration line ,if this
	 * group is special ,it will be set true , and when move regionserver groups
	 * ,it will force update new configuration and jars
	 * 
	 * @param master
	 *            HMaster interface
	 * @param confline
	 *            Configuration content
	 * @return grouppropertymap Map< String, Boolean > , key : groupname ,value:
	 *         isSpecialConfiguration
	 */
	public static Map<String, Boolean> initGroupPropertyMap(HMaster master,
			String confline) {
		Map<String, Boolean> grouppropertymap = new HashMap<String, Boolean>();
		if (confline.length() > 0) {
			String[] grouplines = confline.split(":");
			for (String line : grouplines) {
				if (line == null || line.length() <= 0)
					continue;
				String[] serverlines = line.split(";");
				if (serverlines.length <= 1)
					continue;
				String groupname = serverlines[0];
				String groupproperty = serverlines[1];
				grouppropertymap.put(groupname, Boolean.valueOf(groupproperty));
			}
		}
		if (!grouppropertymap.keySet().contains(DEFAULT_GROUP)) {
			grouppropertymap.put(DEFAULT_GROUP, false);
		} else {
			if (grouppropertymap.get(DEFAULT_GROUP) == true) {
				grouppropertymap.remove(DEFAULT_GROUP);
				grouppropertymap.put(DEFAULT_GROUP, false);
			}
		}
		return grouppropertymap;
	}

	/**
	 * Write regionserver groupinformation back to hdfs
	 * 
	 * @param master
	 *            HMaster interface
	 * @param confpath
	 *            Configuration file Path in hdfs
	 * @param groupmap
	 *            Map< String, List< String >> key:groupname , value:
	 *            regionserver name list
	 * @param grouppropertymap
	 *            Map< String, Boolean > , key : groupname ,value:
	 *            isSpecialConfiguration
	 * @throws IOException
	 *             IOException occured in hdfs
	 */
	public static void writeGroupInfo(HMaster master, final String confpath,
			Map<String, List<String>> groupmap,
			Map<String, Boolean> grouppropertymap) throws IOException {
		// write back to hdfs
		FileSystem fs = master.getMasterFileSystem().getFileSystem();
		URI hdfsuri = master.getMasterFileSystem().getRootDir().toUri();
		Path outputpath = new Path(hdfsuri.toString() + "/" + confpath);
		FSDataOutputStream outputStream = fs.create(outputpath);
		for (Map.Entry<String, List<String>> entry : groupmap.entrySet()) {
			String group = entry.getKey();
			outputStream.write(Bytes.toBytes(group + ";"));
			String isspecial = grouppropertymap.get(group).toString();
			outputStream.write(Bytes.toBytes(isspecial + ";"));
			List<String> serverlist = entry.getValue();
			for (String server : serverlist) {
				outputStream.write(Bytes.toBytes(server + ";"));
			}
			outputStream.write(Bytes.toBytes(":"));
		}
		outputStream.close();
		LOG.info("Write groupinformation back to  hdfs://${hbase.rootdir}/groupinformation.conf");
	}

	/**
	 * Read region information of regionserver from .META. region
	 * 
	 * @param hmaster
	 *            HMaster interface
	 * @param servername
	 *            regionserver name
	 * @return regionlist List< HRegionInfo > , List of regions this
	 *         regionserver hold
	 */
	public static List<HRegionInfo> listRegionsInServerFromMeta(
			HMaster hmaster, String servername) {
		List<HRegionInfo> regionlist = new ArrayList<HRegionInfo>();
		try {
			HTable metatable = new HTable(hmaster.getConfiguration(),
					HConstants.META_TABLE_NAME);
			Scan scan = new Scan();
			scan.addColumn(HConstants.CATALOG_FAMILY,
					HConstants.REGIONINFO_QUALIFIER);
			scan.addColumn(HConstants.CATALOG_FAMILY,
					HConstants.SERVER_QUALIFIER);
			ResultScanner rs = metatable.getScanner(scan);
			Result rt = null;
			while ((rt = rs.next()) != null) {
				byte[] value = rt.getValue(HConstants.CATALOG_FAMILY,
						HConstants.REGIONINFO_QUALIFIER);
				if (value == null || value.length == 0)
					continue;
				HRegionInfo region = null;
				try {
					region = Writables.getHRegionInfo(value);
				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}
				byte[] address = rt.getValue(HConstants.CATALOG_FAMILY,
						HConstants.SERVER_QUALIFIER);
				if (address == null || address.length == 0)
					continue;
				if (region.isOffline() || region.isSplit())
					continue;
				if (!regionlist.contains(region)) {
					HServerAddress addr = new HServerAddress(
							Bytes.toString(address));
					String name = addr.getHostname() + "," + addr.getPort();
					if (name.equals(servername)) {
						regionlist.add(region);
					}
				}
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		return regionlist;
	}

	/**
	 * Read region information stored in regionserver , we get this information
	 * directly from {@link HRegionInterface}
	 * 
	 * @param hmaster
	 *            HMaster interface
	 * @param servername
	 *            regionserver name
	 * @return regionlist List< HRegionInfo > , List of regions this
	 *         regionserver hold
	 * @throws MasterNotRunningException
	 *             master not running
	 * @throws ZooKeeperConnectionException
	 *             zookeeper connection exception
	 */
	public static List<HRegionInfo> listRegionOnRegionServer(HMaster master,
			String servername) throws MasterNotRunningException,
			ZooKeeperConnectionException {
		List<HRegionInfo> regionlist = new ArrayList<HRegionInfo>();
		HBaseAdmin admin = new HBaseAdmin(master.getConfiguration());
		HConnection coon = admin.getConnection();
		Collection<HServerInfo> servercollect = master.getClusterStatus()
				.getServerInfo();
		HServerAddress addr = null;
		for (HServerInfo info : servercollect) {
			String name = info.getHostname() + ","
					+ info.getServerAddress().getPort();
			if (name.equals(servername)) {
				addr = info.getServerAddress();
				break;
			}
		}
		if (addr == null)
			return regionlist;
		HRegionInterface hri = null;
		try {
			hri = coon.getHRegionConnection(addr);
			regionlist = hri.getOnlineRegions();
		} catch (Throwable  e) {
			e.printStackTrace();
		}
		return regionlist;
	}

	/**
	 * List all online tables stored on regionserver
	 * 
	 * @param master
	 *            HMaster interface
	 * @param servername
	 *            regionserver name
	 * @return tablelist List< HTableDescriptor > , all tables hold on this
	 *         regionserver
	 * @throws MasterNotRunningException
	 *             master not running
	 * @throws ZooKeeperConnectionException
	 *             zookeeper connection exception
	 */
	public static List<HTableDescriptor> listTableOnRegionServer(
			HMaster master, String servername)
			throws MasterNotRunningException, ZooKeeperConnectionException {
		List<HRegionInfo> regionlist = listRegionOnRegionServer(master,
				servername);
		List<HTableDescriptor> tablelist = new ArrayList<HTableDescriptor>();
		for (HRegionInfo info : regionlist) {
			HTableDescriptor htabledesc = info.getTableDesc();
			if (!tablelist.contains(htabledesc)) {
				tablelist.add(htabledesc);
			}
		}
		return tablelist;
	}

	/**
	 * List all tables in hbase system
	 * 
	 * @param hmaster
	 *            HMaster interface
	 * @return tablelist List< HTableDescriptor > , all tables hold on this
	 *         regionserver
	 */
	public static List<HTableDescriptor> listAllTables(HMaster hmaster) {
		List<HTableDescriptor> tablelist = new ArrayList<HTableDescriptor>();
		try {
			HTable metatable = new HTable(hmaster.getConfiguration(),
					HConstants.META_TABLE_NAME);
			Scan scan = new Scan();
			scan.addColumn(HConstants.CATALOG_FAMILY,
					HConstants.REGIONINFO_QUALIFIER);
			ResultScanner rs = metatable.getScanner(scan);
			Result rt = null;
			while ((rt = rs.next()) != null) {
				byte[] value = rt.getValue(HConstants.CATALOG_FAMILY,
						HConstants.REGIONINFO_QUALIFIER);
				if (value == null || value.length == 0)
					continue;
				HRegionInfo region = Writables.getHRegionInfo(value);
				if (region.isOffline() || region.isSplit())
					continue;
				HTableDescriptor table = region.getTableDesc();
				if (!tablelist.contains(table)) {
					tablelist.add(table);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return tablelist;
	}

	/**
	 * Get region number of a group
	 * 
	 * @param hmaster
	 *            HMaster interface
	 * @param groupname
	 *            groupname
	 * @param groupmap
	 *            Map< String, List< String >> ,key:groupname ,value
	 *            regionserver list
	 * @return totalregions , total region number hold on this group
	 */
	public static int getRegionNumOnGroup(HMaster hmaster, String groupname,
			Map<String, List<String>> groupmap) {
		int totalregions = 0;
		if (groupmap == null || groupmap.size() <= 0)
			return totalregions;
		if (!groupmap.keySet().contains(groupname))
			return totalregions;
		List<String> serverlist = groupmap.get(groupname);
		if (serverlist.size() <= 0)
			return totalregions;
		for (String server : serverlist) {
			try {
				List<HRegionInfo> regions = listRegionOnRegionServer(hmaster,
						server);
				totalregions += regions.size();
			} catch (MasterNotRunningException e) {
				e.printStackTrace();
			} catch (ZooKeeperConnectionException e) {
				e.printStackTrace();
			}
		}
		return totalregions;
	}

	/**
	 * Get region number of a table on a regionserver
	 * 
	 * @param hmaster
	 *            HMaster interface
	 * @param table
	 *            tablename
	 * @param server
	 *            regionserver
	 * @return totalregions , total region number hold on this table of this
	 *         regionserver
	 */
	public static int getRegionOfTableOnServer(HMaster hmaster, String table,
			HServerInfo server) {
		int size = 0;
		String servername = server.getHostname() + ","
				+ server.getServerAddress().getPort();
		try {
			List<HRegionInfo> regionlist = listRegionOnRegionServer(hmaster,
					servername);
			for (HRegionInfo info : regionlist) {
				String tablename = info.getTableDesc().getNameAsString();
				if (table.equals(tablename)) {
					size++;
				}
			}
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		}
		return size;
	}
}