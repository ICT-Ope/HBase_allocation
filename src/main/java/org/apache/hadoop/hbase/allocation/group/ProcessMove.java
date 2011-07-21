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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.master.GroupAssignmentManager;

/**
 * This class is used for change regionserver group property
 * <p>
 * It will move all regions off a regionserver ,and then move into other
 * regionserver left on this group.
 */
public class ProcessMove implements Runnable {
	static final Log LOG = LogFactory.getLog(ProcessMove.class);
	final static String DEFAULT_GROUP = "0";
	final String GROUP_INFORMATION = "groupinformation.conf";

	private Set<MoveGroupPlan> movegroupPlanset = new HashSet<MoveGroupPlan>();
	private Map<String, List<String>> groupmap = new HashMap<String, List<String>>();
	private Map<String, Boolean> grouppropertymap = new HashMap<String, Boolean>();
	private HMaster master = null;
	private String currentdir = null;

	/**
	 * Construct method for thread
	 * 
	 * @param movegroupPlanset
	 *            Set<{@link MoveGroupPlan}> , Will change regionserver plan set
	 * @param groupmap
	 * 			  Map < String, List< String >> , key:groupname , value: regionserver name list 
	 * @param grouppropertymap
	 * 			  Map< String, Boolean > , key : groupname ,value : isSpecialConfiguration
	 * @param master 
	 * 			 Master Interface 
	 * @param currentdir
	 * 			 current context dir path
	 */
	public ProcessMove(Set<MoveGroupPlan> movegroupPlanset,
			Map<String, List<String>> groupmap,
			Map<String, Boolean> grouppropertymap, HMaster master,
			String currentdir) {
		super();
		this.movegroupPlanset = movegroupPlanset;
		this.groupmap = groupmap;
		this.grouppropertymap = grouppropertymap;
		this.master = master;
		this.currentdir = currentdir;
	}

	/**
	 * Move all regions and update configuration and librarys
	 * 
	 */
	public void run() {
		List<String> willmoveservers = new ArrayList<String>();
		ServerWithGroup.setMovegroupPlanset(movegroupPlanset);
		ServerWithGroup.setIserror(false);

		for (MoveGroupPlan plan : movegroupPlanset) {
			String servername = plan.getServername();
			willmoveservers.add(servername);
		}

		for (MoveGroupPlan plan : movegroupPlanset) {
			String servername = plan.getServername();
			String originalgp = plan.getOriginalgroup();
			String targetgp = plan.getTargetgroup();
			ServerWithGroup.setCurrentserver(servername);
			LOG.info("Move " + servername + " form group " + originalgp
					+ " to group " + targetgp);
			if (originalgp.equals(targetgp)) {
				willmoveservers.remove(servername);
				continue;
			}
			List<HRegionInfo> regionlist = new ArrayList<HRegionInfo>();
			try {
				regionlist = ServerWithGroup.listRegionOnRegionServer(master,
						servername);
			} catch (MasterNotRunningException e) {
				e.printStackTrace();
			} catch (ZooKeeperConnectionException e) {
				e.printStackTrace();
			}
			if (regionlist != null) {
				LOG.info("We have " + regionlist.size()
						+ " regions to move ...");
				// move region
				while (true) {
					// assign regions on this server this region's table other
					// servers
					if (regionlist == null || regionlist.size() <= 0)
						break;
					for (HRegionInfo hri : regionlist) {
						String tablename = hri.getTableDesc().getNameAsString();
						List<HServerInfo> availalbeserver = GroupAssignmentManager
								.getAvailableServer(tablename);
						List<String> availservernames = new ArrayList<String>();
						for (HServerInfo hsr : availalbeserver) {
							String name = hsr.getHostname() + ","
									+ hsr.getServerAddress().getPort();
							// cancel will delete server
							if (!willmoveservers.contains(name)) {
								availservernames.add(name);
								LOG.info("Available server " + name
										+ "for table " + tablename);
							}
						}
						LOG.info("Available server  length "
								+ availservernames.size());
						if (availservernames.size() <= 0) {
							LOG.info("No available regionservers left for  table"
									+ tablename
									+ " to assign,and it will be changed to group 0 !!");
							continue;
							// think carefully
						}

						Random r = new Random();
						int index = r.nextInt(availservernames.size());
						String targertservername = availservernames.get(index);
						HServerInfo targetserver = null;
						LOG.info("Choose Server  targert" + servername
								+ "to moved this region to...");
						Collection<HServerInfo> serverInfo = master
								.getClusterStatus().getServerInfo();
						for (HServerInfo sinfo : serverInfo) {
							String thisservername = sinfo.getHostname() + ","
									+ sinfo.getServerAddress().getPort();
							if (thisservername.equals(targertservername)) {
								targetserver = sinfo;
								break;
							}
						}
						LOG.info("Choose Server " + targertservername);
						if (targetserver == null) {
							LOG.info("RegionServer Missing ...");
							ServerWithGroup.setIserror(true);
							ServerWithGroup
									.setErrormsg("Region server missing when move servergroup");
							synchronized (ServerWithGroup.class) {
								ServerWithGroup.setIsprocess(false);
							}
							return;
						}
						String regionName = Bytes.toStringBinary(hri
								.getRegionName());
						String encodeRegionName = regionName.substring(
								(regionName.lastIndexOf(".",
										regionName.length() - 2)) + 1,
								regionName.length() - 1);
						try {
							if (!hri.isOffline() && !hri.isSplit()) {
								synchronized (GroupAssignmentManager.class) {
									master.move(
											Bytes.toBytes(encodeRegionName),
											Bytes.toBytes(targetserver
													.getServerName()));
								}
								LOG.info("Move region " + encodeRegionName
										+ " from server " + servername
										+ " to server "
										+ targetserver.getServerName());
							}
						} catch (UnknownRegionException e) {
							e.printStackTrace();
						}
					}
					try {
						regionlist = ServerWithGroup.listRegionOnRegionServer(
								master, servername);
					} catch (MasterNotRunningException e) {
						e.printStackTrace();
					} catch (ZooKeeperConnectionException e) {
						e.printStackTrace();
					}
					if (regionlist.size() > 0) {
						LOG.info("Can't move all regions once ,do it again!");
						try {
							Thread.sleep(500);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
			}
			if (grouppropertymap.get(targetgp)
					|| grouppropertymap.get(originalgp)) {
				// update configuration and library
				List<String> rglist = groupmap.get(targetgp);
				if (rglist == null || rglist.size() <= 0) {
					LOG.info("Don't Need to Update Conf");
				} else {
					String sourceserver = rglist.get(0);
					String sourceaddress = sourceserver.substring(0,
							sourceserver.indexOf(","));
					String targetaddress = servername.substring(0,
							servername.indexOf(","));
					try {
						boolean result = true;
						ServerWithGroup.setDoMoveconf(true);
						LOG.info("before move conf ");
						MoveConfImpl moveimpl = new MoveConfImpl(currentdir);
						LOG.info("after move conf ");
						result = moveimpl.ScpConf(sourceaddress, "get");
						if (!result)
							throw new Exception("get conf exception");
						result = moveimpl.ImplRegionServer(targetaddress,
								"stop");
						if (!result)
							throw new Exception("stop server exception");
						result = moveimpl.ScpConf(targetaddress, "put");
						if (!result)
							throw new Exception("put conf exception");
						result = moveimpl.ImplRegionServer(targetaddress,
								"start");
						if (!result)
							throw new Exception("start server exception");
						ServerWithGroup.setDoMoveconf(false);
					} catch (Exception e) {
						e.printStackTrace();
						ServerWithGroup.setIserror(true);
						ServerWithGroup
								.setErrormsg("Error occured when update regionserver configuration and jars ...");
						synchronized (ServerWithGroup.class) {
							ServerWithGroup.setIsprocess(false);
						}
						return;
					}
				}
			}
			willmoveservers.remove(servername);
			groupmap.get(originalgp).remove(servername);
			groupmap.get(targetgp).add(servername);
			LOG.info("update groupmap successfully");
			try {
				ServerWithGroup.writeGroupInfo(master, GROUP_INFORMATION,
						groupmap, grouppropertymap);
				LOG.info("Write conf back ");
				GroupAssignmentManager.initValue(false);
				LOG.info("GroupAssignmentManager initValue ");
			} catch (IOException e) {
				LOG.info(ProcessMove.class, e);
			}
			plan.setStatus(true);
			ServerWithGroup.setMovegroupPlanset(movegroupPlanset);
		}
		synchronized (ServerWithGroup.class) {
			ServerWithGroup.setIsprocess(false);
			ServerWithGroup.setIsmoveregion(false);
		}
		LOG.info("Finished update group information!");
	}

}
