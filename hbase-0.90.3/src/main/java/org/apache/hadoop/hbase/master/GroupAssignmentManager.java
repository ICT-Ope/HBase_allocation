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
package org.apache.hadoop.hbase.master;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.allocation.CheckMeta;
import org.apache.hadoop.hbase.allocation.group.AutoBalance;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.executor.RegionTransitionData;
import org.apache.hadoop.hbase.ipc.ScheduleHBaseServer;
import org.apache.hadoop.hbase.master.AssignmentManager.GeneralBulkAssigner;
import org.apache.hadoop.hbase.master.AssignmentManager.RegionState;
import org.apache.hadoop.hbase.master.LoadBalancer.RegionPlan;
import org.apache.hadoop.hbase.master.handler.ClosedRegionHandler;
import org.apache.hadoop.hbase.master.handler.OpenedRegionHandler;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZKTable;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.io.Writable;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

/**
 * 
 * manager assignment of region according to the group information
 * 
 */
public class GroupAssignmentManager extends AssignmentManager {
  // contains the table group information
  private static HashMap<String, List<String>> tableGroup = new HashMap<String, List<String>>();
  // contains the server group infomation
  private static HashMap<String, HashSet<HServerInfo>> groupServers = new HashMap<String, HashSet<HServerInfo>>();
  // contains the
  private static HashMap<String, HashSet<String>> groupServersString = new HashMap<String, HashSet<String>>();

  // used only when refresh configuration
  private static HashMap<String, Boolean> groupDifConf = new HashMap<String, Boolean>();

  static HMaster master;
  static private Configuration conf = HBaseConfiguration.create();

  // the default group which servers and tables belong to
  public static final String DEFAULT_GROUP = "0";

  // group information key in table descriptor.
  public static final byte[] GROUP_KEY = Bytes.toBytes("group");
  public static final String GROUP_SPLITER = ",";
  private static ServerManager serverManager;
  private static HBaseAdmin admin;
  private static final Log LOG = LogFactory
      .getLog(GroupAssignmentManager.class);
  // TimeoutMonitor timeoutMonitor;
  static CatalogTracker catalogTracker;
  private ZKTable zkTableIn;
  static final int refreshInterver = 500000;
  private ExecutorService executorServiceIn = null;
  private static final boolean AssignToMetaServer=conf.getBoolean("hbase.group.assigntometaServer", true);

  // the gap between the maximum and minimum when do the balance
  public static final int div = 2;

  /**
   * initiate the refresh thread which check the wrong assignment and load the
   * group infomation
   */
  static {

    try {
      long period = conf.getLong("hbase.balancer.period", 300000);
      AutoBalance balancer = new AutoBalance(period);
      balancer.startBalance();
    } catch (Exception e1) {
      LOG.error("start balancer error" + e1.getMessage());
      e1.printStackTrace();
    }
    Thread refresher = new Thread() {
      public void run() {
        while (true) {
          try {
            try {
              sleep(refreshInterver);
            } catch (Exception e1) {

              e1.printStackTrace();
            }
            if (GroupAssignmentManager.master != null
                && GroupAssignmentManager.master.isAlive()
                && !master.isStopped()) {
              synchronized (GroupAssignmentManager.class) {
                HTableDescriptor[] dess = GroupAssignmentManager.initValue();
                maintainGroup(dess);
              }
              LOG.debug("groupDifConf:" + groupDifConf);
              LOG.debug("server groups:" + groupServers);
              LOG.debug("table groups:" + tableGroup);

            } else if (GroupAssignmentManager.master == null) {

              try {
                LOG.debug("server groups:" + groupServers);
                LOG.debug("table groups:" + tableGroup);
                sleep(refreshInterver);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }

            } else {
              break;
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }
    };
    refresher.start();
  }

  private static boolean haveGroup(String tableName) {
    HTableDescriptor t = null;
    if (admin == null) {
      try {
        admin = new HBaseAdmin(conf);
      } catch (MasterNotRunningException e) {
        e.printStackTrace();
      } catch (ZooKeeperConnectionException e) {
        e.printStackTrace();
      }
    }
    try {
      t = admin.getTableDescriptor(Bytes.toBytes(tableName));
    } catch (Exception e) {
      e.printStackTrace();
    }
    if (t != null) {
      byte[] groupsb = t.getValue(GROUP_KEY);
      if (groupsb != null) {
        return true;
      }
    }
    return false;
  }

  private static String[] getTableGroupsInner(String tableName) {
    String[] groups = new String[] { "0" };
    if (admin == null) {
      try {
        admin = new HBaseAdmin(conf);
      } catch (MasterNotRunningException e) {
        e.printStackTrace();
      } catch (ZooKeeperConnectionException e) {
        e.printStackTrace();
      }
    }
    HTableDescriptor t = null;
    try {
      t = admin.getTableDescriptor(Bytes.toBytes(tableName));
    } catch (IOException e) {
      e.printStackTrace();
    }
    if (t != null) {
      byte[] groupsb = t.getValue(GROUP_KEY);
      if (groupsb != null) {
        groups = Bytes.toString(t.getValue(GROUP_KEY)).split(GROUP_SPLITER);
      }
    }
    return groups;

  }

  @SuppressWarnings("unused")
  private HServerAddress serveringServer(HRegionInfo info) {
    Map<String, HServerInfo> map = serverManager.getOnlineServers();
    for (HServerInfo server : map.values()) {
      try {
        if (admin.getConnection()
            .getHRegionConnection(server.getServerAddress())
            .getRegionInfo(info.getEncodedNameAsBytes()) != null) {
          return server.getServerAddress();
        }
      } catch (NotServingRegionException e) {
        continue;
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return null;

  }

  // /**
  // * Gets the online regions of the specified table. This method looks at
  // the
  // * in-memory state. It does not go to <code>.META.</code>. Only returns
  // * <em>online</em> regions. If a region on this table has been closed
  // during
  // * a disable, etc., it will be included in the returned list. So, the
  // * returned list may not necessarily be ALL regions in this table, its all
  // * the ONLINE regions in the table.
  // *
  // * @param tableName
  // * @return Online regions from <code>tableName</code>
  // */
  //
  // public List<HRegionInfo> getRegionsOfTable(byte[] tableName) {
  // if (true)
  // return super.getRegionsOfTable(tableName);
  // // System.out.println("In my method ............................");
  // if (this.getZKTable().isDisablingTable(Bytes.toString(tableName))
  // || this.getZKTable().isDisabledTable(Bytes.toString(tableName))) {
  // System.out.println("Table disabled ............................");
  // final List<HRegionInfo> l = super.getRegionsOfTable(tableName);
  // System.out.println("there are  " + l.size()
  // + " regions not disabled ............................");
  // Thread[] threads = new Thread[l.size()];
  // HashMap<HRegionInfo, HServerAddress> regionMaps = null;
  // try {
  // regionMaps = CheckMeta.getRegionAddress(Bytes
  // .toString(tableName), CheckMeta.getAllRegionInfo());
  // } catch (IOException e1) {
  // // TODO Auto-generated catch block
  // e1.printStackTrace();
  // } catch (InterruptedException e1) {
  // // TODO Auto-generated catch block
  // e1.printStackTrace();
  // }
  // if (regionMaps == null) {
  //
  // } else {
  // l.addAll(regionMaps.keySet());
  // }
  // boolean allOffLine = false;
  // while (l.size() != 0) {
  // allOffLine = true;
  // for (HRegionInfo info2 : l) {
  // if (info2.isSplit() || (!info2.isOffline())) {
  // allOffLine = false;
  // }
  // }
  // if (allOffLine) {
  // break;
  // }
  // for (int i = 0; i < l.size(); i++) {
  // final HRegionInfo info = l.get(i);
  // threads[i] = new Thread() {
  // public void run() {
  // HServerAddress address = serveringServer(info);
  // if (address == null) {
  // System.out
  // .println("disabled table have regions "
  // + info.getRegionNameAsString());
  // unassign(info, true);
  // clearRegionFromTransition(info);
  // l.remove(info);
  // } else {
  // System.out
  // .println("disabled table have regions "
  // + info.getRegionNameAsString()
  // + "servering at " + address);
  // unassign(info, true);
  // }
  // }
  // };
  // threads[i].start();
  // }
  // for (int i = 0; i < l.size(); i++) {
  // try {
  // threads[i].join();
  // } catch (InterruptedException e) {
  // // TODO Auto-generated catch block
  // e.printStackTrace();
  // }
  // }
  // }
  //
  // }
  // return super.getRegionsOfTable(tableName);
  // }

  private static String getTableGroup(HTableDescriptor dess) {
    byte[] group = dess.getValue(GROUP_KEY);
    if (group != null) {
      return Bytes.toString(group);
    }
    return DEFAULT_GROUP;
  }

  private static void maintainGroup(HTableDescriptor[] dess) {
    try {
      HTableDescriptor t[];
      if (dess == null) {
        t = listTables();
      } else {
        t = dess;
      }
      HashMap<HRegionInfo, HServerAddress> maps = null;
      try {
        maps = CheckMeta.getAllRegionInfo();
      } catch (InterruptedException e1) {
        e1.printStackTrace();
      }

      if (maps == null)
        return;
      for (HTableDescriptor des : t) {
        try {
          Map<HRegionInfo, HServerAddress> map = CheckMeta.getRegionAddress(
              des.getNameAsString(), maps);

          for (HRegionInfo hri : map.keySet()) {
            if (hri.isOffline() || hri.isMetaRegion() || hri.isRootRegion()
                || hri.isSplit() || hri.isSplitParent())
              continue;
            if (!getTableGroup(hri.getTableDesc()).equals(getTableGroup(des))) {
              LOG.error(" error region group not right: region:"
                  + hri.getRegionNameAsString());
              LOG.error(" region desc:" + hri.getTableDesc());
              LOG.error(" table desc:" + des);
              hri.setTableDesc(des);
              MetaEditor.updateRegionInfo(catalogTracker, hri);
              master.getMasterFileSystem().updateRegionInfo(hri);
            }
          }
          List<HServerInfo> servers = getAvailableServer(des.getNameAsString());
          if (servers.size() == 0) {
            continue;
          }
          for (HRegionInfo info : map.keySet()) {
            boolean shouldMove = true;
            for (HServerInfo server : servers) {
              if (server.getServerAddress().getHostname()
                  .equals(map.get(info).getHostname())
                  && (server.getServerAddress().getPort() == map.get(info)
                      .getPort())) {
                shouldMove = false;
                break;
              }
            }

            if (shouldMove) {
              try {

                HServerInfo s = servers.get(RANDOM.nextInt(servers.size()));
                LOG.info("table is:" + des);
                LOG.info("table group is:"
                    + getTableGroups(des.getNameAsString()));
                LOG.info("move region:" + info.getEncodedName() + " to "
                    + s.getServerName() + " from :"
                    + map.get(info).getHostname() + ":"
                    + map.get(info).getPort());
                LOG.info("the available servers are:" + servers);
                master.move(info.getEncodedNameAsBytes(),
                    Bytes.toBytes(s.getServerName()));
              } catch (Exception e) {
                e.printStackTrace();
              }
            }

          }
        } catch (Exception e) {

          LOG.info("The table can't get region info" + des);
          e.printStackTrace();
        }

      }
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  /**
   * add a new group,only refresh the memory group information,use should change
   * the group information file first
   * 
   * @param groupName
   * @param servers
   *          the servers belong to the group
   */
  public static void newGroup(String groupName, List<HServerInfo> servers) {
    HashSet<HServerInfo> serverSet = groupServers.get(groupName);
    if (serverSet == null) {
      serverSet = new HashSet<HServerInfo>();
      groupServers.put(groupName, serverSet);
    }
    for (HServerInfo info : servers) {
      serverSet.add(info);
    }

  }

  private static boolean readConfig() {
    try {

      FSDataInputStream input = null;
      try {
        input = master.getMasterFileSystem().getFileSystem()
            .open(new Path(FSUtils.getRootDir(conf), "groupinformation.conf"));
      } catch (Exception e1) {
        e1.printStackTrace();
      }
      if (input != null) {
        BufferedInputStream binput = new BufferedInputStream(input);
        int size = binput.available();
        byte[] result = new byte[size];

        binput.read(result);
        String groups = Bytes.toString(result);
        String[] grouplines = groups.split(":");
        binput.close();
        System.out.println("config is" + Bytes.toString(result));

        for (String line : grouplines) {
          if (line == null || line.length() <= 0)
            continue;
          String[] serverlines = line.split(";");

          // System.out.println("Group" + serverlines[0]);
          String groupName = serverlines[0];
          HashSet<String> set = new HashSet<String>();
          String groupDifConfigV = serverlines[1];
          try {
            groupDifConf.put(groupName, Boolean.parseBoolean(groupDifConfigV));
          } catch (Exception e) {
            groupDifConf.put(groupName, false);
            e.printStackTrace();
          }
          for (int i = 2; i < serverlines.length; i++) {
            set.add(serverlines[i]);
          }
          groupServersString.put(groupName, set);
        }
      }

    } catch (Exception e) {
      // TODO Auto-generated catch block

      e.printStackTrace();
      if (groupServersString.get(DEFAULT_GROUP) == null) {
        groupServersString.put(DEFAULT_GROUP, new HashSet<String>());
        LOG.error("there is no server in group 0 ");
        return false;
      }

    }
    if (groupServersString.get(DEFAULT_GROUP) == null) {
      groupServersString.put(DEFAULT_GROUP, new HashSet<String>());
      LOG.error("there is no server in group 0 ");
      return false;
    }
    return true;

  }

  private static void initServerInfo() {
    List<HServerInfo> list = serverManager.getOnlineServersList();
    HashMap<String, HServerInfo> serverMap = new HashMap<String, HServerInfo>();
    for (HServerInfo info : list) {
      serverMap.put(info.getServerAddress().getHostname() + ","
          + info.getServerAddress().getPort(), info);
    }
    for (String group : groupServersString.keySet()) {
      HashSet<HServerInfo> set = new HashSet<HServerInfo>();
      for (String address : groupServersString.get(group)) {
        if (serverMap.get(address) != null) {
          set.add(serverMap.get(address));
          list.remove(serverMap.get(address));
        }
      }
      groupServers.put(group, set);

    }

    for (String group : groupServers.keySet()) {
      if (groupServersString.get(group) == null) {
        groupServers.remove(group);
      }
    }
    for (String group : groupDifConf.keySet()) {
      if (groupServersString.get(group) == null) {
        groupDifConf.remove(group);
      }
    }
    for (HServerInfo server : list) {
      groupDifConf.put(DEFAULT_GROUP, false);
      groupServers.get(DEFAULT_GROUP).add(server);
    }
    for (String s : groupServers.keySet()) {
      if (groupDifConf.get(s) == null) {
        groupDifConf.put(DEFAULT_GROUP, false);
      }
    }

  }

  private static HTableDescriptor[] listTables() throws IOException {
    return CheckMeta.getTables();
    // final TreeSet<HTableDescriptor> uniqueTables = new
    // TreeSet<HTableDescriptor>();
    // MetaScannerVisitor visitor = new MetaScannerVisitor() {
    // public boolean processRow(Result result) throws IOException {
    // try {
    // byte[] value = result.getValue(HConstants.CATALOG_FAMILY,
    // HConstants.REGIONINFO_QUALIFIER);
    // HRegionInfo info = null;
    // if (value != null) {
    // info = Writables.getHRegionInfo(value);
    // }
    // // Only examine the rows where the startKey is zero length
    // if (info != null && info.getStartKey().length == 0) {
    // uniqueTables.add(info.getTableDesc());
    // }
    // return true;
    // } catch (RuntimeException e) {
    // throw e;
    // }
    // }
    // };
    // MetaScanner.metaScan(conf, visitor);
    //
    // return uniqueTables.toArray(new HTableDescriptor[uniqueTables.size()]);
  }

  /**
   * set the table priority,because disable sometimes failed,so we disable one
   * table twice.
   * 
   * @param priority
   *          priority String
   * @param table
   *          table name
   */
  @SuppressWarnings("static-access")
  public static void setPriority(String priority, String table) {

    try {
      Integer.parseInt(priority);
    } catch (NumberFormatException e) {
      e.printStackTrace();
      return;
    }
    try {
      HBaseAdmin admin = new HBaseAdmin(conf);
      HTableDescriptor des = admin.getTableDescriptor(Bytes.toBytes(table));
      des.setValue(Bytes.toBytes(ScheduleHBaseServer.pri_string),
          Bytes.toBytes(priority));
      LOG.info("disable table start .............");
      try {
        admin.disableTable(table);
        Thread.currentThread().sleep(3000);
        admin.disableTable(table);
      } catch (Exception e1) {
        e1.printStackTrace();
      }
      try {
        Thread.currentThread().sleep(10000);
      } catch (InterruptedException e1) {
        e1.printStackTrace();
      }
      LOG.info(".....disable finished .............");

      try {
        admin.modifyTable(des.getName(), des);
        LOG.info(".....modify priority finished .............");
      } catch (Exception e) {
        e.printStackTrace();
      }
      try {
        admin.enableTable(table);
        LOG.info(".....enable table finished .............");
      } catch (Exception e) {
        e.printStackTrace();
      }
    } catch (MasterNotRunningException e) {
      e.printStackTrace();
    } catch (ZooKeeperConnectionException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  /**
   * set the table group,because disable sometimes failed,so we disable one
   * table twice.
   * 
   * @param groups
   *          groups the table belong to
   * @param table
   *          table name
   */
  @SuppressWarnings("static-access")
  public static void setGroup(String[] groups, String table) {

    try {
      HBaseAdmin admin = new HBaseAdmin(conf);

      HTableDescriptor des = admin.getTableDescriptor(Bytes.toBytes(table));
      String group = "";
      for (String s : groups) {
        group += (s + ",");
      }
      LOG.info("disable table :" + table + " start");
      des.setValue(GROUP_KEY, Bytes.toBytes(group));
      try {
        admin.disableTable(table);
        Thread.currentThread().sleep(5000);
        admin.disableTable(table);
      } catch (Exception e1) {
        e1.printStackTrace();
      }
      try {
        Thread.currentThread().sleep(10000);
      } catch (InterruptedException e1) {
        e1.printStackTrace();
      }
      LOG.info("disable table :" + table + "finished");
      try {
        admin.modifyTable(des.getName(), des);
      } catch (Exception e) {
        e.printStackTrace();
      }
      try {
        Thread.currentThread().sleep(5000);
        admin.modifyTable(des.getName(), des);
      } catch (InterruptedException e1) {
        e1.printStackTrace();
      }
      LOG.info("modify table :" + table + " finished");
      try {
        GroupAssignmentManager.initValue();
      } catch (Exception e) {
        e.printStackTrace();
      }
      LOG.info("intivalue  :" + table + " finished");
      try {
        admin.enableTable(table);
      } catch (Exception e) {
        e.printStackTrace();
      }
      LOG.info("enable :" + table + " finished");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static boolean first = true;

  /**
   * not used
   * 
   * @param first
   */
  public static void initValue(boolean first) {
    initValue();
  }

  /**
   * initiate the group information
   * 
   * @return the tables belong to the cluster
   */
  public static HTableDescriptor[] initValue() {

    boolean fileExist = readConfig();

    if (!first && !fileExist) {
      return null;
    }
    first = false;
    HTableDescriptor[] dess = null;
    try {
      dess = listTables();
      for (HTableDescriptor des : dess) {
        String[] groups = new String[] { "0" };
        byte[] groupsb = des.getValue(GROUP_KEY);
        if (groupsb != null) {
          groups = Bytes.toString(des.getValue(GROUP_KEY)).split(GROUP_SPLITER);
        }
        ArrayList<String> list = new ArrayList<String>();
        for (String group : groups)
          list.add(group);
        tableGroup.put(des.getNameAsString(), list);

      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    initServerInfo();
    return dess;

  }

  public GroupAssignmentManager(Server master, ServerManager serverManager,
      CatalogTracker catalogTracker, ExecutorService service)
      throws KeeperException {
    super(master, serverManager, catalogTracker, service);
    GroupAssignmentManager.master = (HMaster) master;
    GroupAssignmentManager.master.getZooKeeper().registerListenerFirst(this);
    GroupAssignmentManager.master.balanceSwitch(false);
    GroupAssignmentManager.conf = GroupAssignmentManager.master
        .getConfiguration();
    GroupAssignmentManager.serverManager = serverManager;
    GroupAssignmentManager.catalogTracker = catalogTracker;
    try {
      Field f = AssignmentManager.class.getDeclaredField("zkTable");
      f.setAccessible(true);
      this.zkTableIn = (ZKTable) f.get(this);
    } catch (Exception e) {
      e.printStackTrace();
    }

    try {
      Field f = AssignmentManager.class.getDeclaredField("executorService");
      f.setAccessible(true);
      this.executorServiceIn = (ExecutorService) f.get(this);
    } catch (SecurityException e) {

      e.printStackTrace();
    } catch (NoSuchFieldException e) {
      e.printStackTrace();
    } catch (IllegalArgumentException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }

  }

  private boolean regionAndServerIsSameGroup(HRegionInfo regionInfo,
      HServerInfo serverInfo) {

    List<String> groups = new ArrayList<String>();
    String[] groupss = getTableGroups(regionInfo.getTableDesc()
        .getNameAsString());
    for (String s : groupss)
      groups.add(s);

    boolean inGroup = false;
    for (String group : groups) {
      if (groupServers.get(group) == null)
        continue;
      if (groupServers.get(group).contains(serverInfo)) {
        inGroup = true;
        break;
      }
    }
    return inGroup;

  }

  @Override
  void balance(final RegionPlan plan) {
    if (!regionAndServerIsSameGroup(plan.getRegionInfo(), plan.getDestination())) {
      return;
    }
    synchronized (this.regionPlans) {
      this.regionPlans.put(plan.getRegionName(), plan);
    }
    unassign(plan.getRegionInfo());
  }

  /**
   * balance regions belong to one table
   * 
   * @param table
   *          the table you want to balance
   */
  public static void balanceTable(String table) {
    String groups[] = getTableGroups(table);
    HashSet<HServerInfo> servers = new HashSet<HServerInfo>();

    for (String group : groups) {
      System.out.println("table belong to :" + group);
      if (groupServers.get(group) != null) {
        servers.addAll(groupServers.get(group));
      }
    }

    HashMap<HRegionInfo, HServerAddress> maps = CheckMeta
        .getRegionAddress(table);
    HashMap<HRegionInfo, HServerInfo> map = new HashMap<HRegionInfo, HServerInfo>();
    for (Entry<HRegionInfo, HServerAddress> e : maps.entrySet()) {
      for (HServerInfo info : servers) {
        if (CheckMeta.isThisAddress(info, e.getValue())) {
          map.put(e.getKey(), info);
        }
      }
    }
    doBalance(servers, map);

  }

  /**
   * do the balance job according to the region map and the available server
   * list;
   * 
   * @param servers
   * @param map
   */
  private static void doBalance(HashSet<HServerInfo> servers,
      HashMap<HRegionInfo, HServerInfo> map) {
    HashMap<HRegionInfo, HServerInfo> moves = new HashMap<HRegionInfo, HServerInfo>();
    HashMap<HServerInfo, List<HRegionInfo>> serverList = new HashMap<HServerInfo, List<HRegionInfo>>();
    for (Entry<HRegionInfo, HServerInfo> e : map.entrySet()) {
      if (serverList.get(e.getValue()) == null) {
        serverList.put(e.getValue(), new ArrayList<HRegionInfo>());
      }
      serverList.get(e.getValue()).add(e.getKey());
    }
    for (HServerInfo s : servers) {
      if (serverList.get(s) == null) {
        serverList.put(s, new ArrayList<HRegionInfo>());
      }
    }

    int div = 10;
    while (div > GroupAssignmentManager.div) {
      HServerInfo maxLoad = null, minLoad = null;
      int maxLoadN = Integer.MIN_VALUE, minLoadN = Integer.MAX_VALUE;
      for (Entry<HServerInfo, List<HRegionInfo>> e : serverList.entrySet()) {
        if (e.getValue().size() >= maxLoadN) {
          maxLoadN = e.getValue().size();
          maxLoad = e.getKey();
        }
        if (e.getValue().size() <= minLoadN) {
          minLoadN = e.getValue().size();
          minLoad = e.getKey();
        }
      }
      if (maxLoad == null || minLoad == null)
        break;
      if (serverList.get(maxLoad).size() == 0)
        break;
      else {
        div = Math.abs(maxLoadN - minLoadN);
        int index = RANDOM.nextInt(serverList.get(maxLoad).size());
        moves.put(serverList.get(maxLoad).get(index), minLoad);
        serverList.get(minLoad).add(serverList.get(maxLoad).get(index));
        serverList.get(maxLoad).remove(index);
      }

    }

    for (Entry<HRegionInfo, HServerInfo> e : moves.entrySet()) {
      try {
        LOG.info("move :" + e.getKey().getEncodedName() + " regions to "
            + e.getValue());
        master.move(e.getKey().getEncodedNameAsBytes(),
            Bytes.toBytes(e.getValue().getServerName()));
      } catch (Exception e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      }
    }
  }

  /**
   * balance regions which in the same group
   * 
   * @param group
   *          the group you want to balance
   */
  public static void balanceGroup(String group) {
    HashSet<HServerInfo> servers = groupServers.get(group);
    if (servers == null || servers.size() < 2)
      return;
    HServerInfo[] infos = new HServerInfo[servers.size()];
    servers.toArray(infos);
    HashMap<HRegionInfo, HServerInfo> map = CheckMeta.getServerRegions(infos,
        true);
    if (map == null || map.size() < 2)
      return;

    doBalance(servers, map);

  }

  private static String[] getTableGroups(String tableName) {
    String groups[] = null;
    if (haveGroup(tableName)) {
      groups = getTableGroupsInner(tableName);
    } else {
      ArrayList<String> l = new ArrayList<String>();
      for (String s : groupDifConf.keySet()) {
        if (groupDifConf.get(s) == false) {
          l.add(s);
        }
      }
      groups = new String[l.size()];
      l.toArray(groups);
    }
    if (groups == null)
      groups = new String[] { DEFAULT_GROUP };
    return groups;
  }

  private static List<HServerInfo> filterRootServer(List<HServerInfo> servers) {
    if(AssignToMetaServer)
    {
      return servers;
    }
    try {
      if (servers.size() <= 1) {
        return servers;
      }
      HServerAddress rootAddress = catalogTracker.getRootLocation();
      if (rootAddress != null) {
        for (HServerInfo server : servers) {
          if (server.getServerAddress().equals(rootAddress)) {
            servers.remove(server);
            return servers;
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return servers;
  }

  /**
   * Get servers which this table can use according to group information
   * 
   * @param tableName
   * @return available server list
   */
  public static List<HServerInfo> getAvailableServer(String tableName) {
    String groups[] = null;

    groups = getTableGroups(tableName);
    List<HServerInfo> servers = serverManager.getOnlineServersList();

    List<HServerInfo> ret = new ArrayList<HServerInfo>();
    for (HServerInfo server : servers) {
      for (String group : groups) {
        if (groupServers.get(group) == null) {
          continue;
        }
        if (groupServers.get(group).contains(server)) {
          ret.add(server);
          break;
        }
      }
    }
    if (ret == null || ret.size() == 0) {
      ret = new ArrayList<HServerInfo>();
      ret.addAll(groupServers.get(DEFAULT_GROUP));
    }

    return filterRootServer(ret);

  }

  private List<HServerInfo> getDefaultServer() {
    List<HServerInfo> ret = new ArrayList<HServerInfo>();
    ret.addAll(groupServers.get(DEFAULT_GROUP));
    return ret;
  }

  private List<HServerInfo> getAvailableServer(List<HServerInfo> servers,
      String table) {

    List<String> groups = new ArrayList<String>();
    String groupsStr[] = getTableGroups(table);
    for (String s : groupsStr) {
      groups.add(s);
    }
    List<HServerInfo> ret = new ArrayList<HServerInfo>();
    for (HServerInfo server : servers) {

      for (String group : groups) {
        if (groupServers.get(group) == null) {
          continue;
        }
        if (groupServers.get(group).contains(server)) {
          ret.add(server);
          break;
        }
      }
    }
    if (ret.size() == 0) {
      if (groupServers.get(DEFAULT_GROUP) == null) {
        initValue();
      }
      ret.addAll(groupServers.get(DEFAULT_GROUP));
    }
    return filterRootServer(ret);
  }

  @Override
  RegionPlan getRegionPlan(final RegionState state,
      final HServerInfo serverToExclude, final boolean forceNewPlan) {
    List<HServerInfo> servers = serverManager.getOnlineServersList();
    boolean rootOrMeta = false;
    // if it's root or meta regions,use super getRegionPlan
    if (state.getRegion().isMetaRegion() || state.getRegion().isRootRegion()) {
      rootOrMeta = true;
      if (groupServers.get(DEFAULT_GROUP) == null) {
        try {
          readConfig();
          initServerInfo();
        } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }

      HServerAddress address = null;
      if (state.getRegion().isMetaRegion()) {
        try {
          address = catalogTracker.getRootLocation();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        if (address != null) {
          HServerInfo rootServer = null;
          for (HServerInfo serverInfo : servers) {
            if (serverInfo.getServerAddress().equals(address)) {
              rootServer = serverInfo;
              break;
            }
          }
          if (rootServer != null) {
            return new RegionPlan(state.getRegion(), null, rootServer);
          }

        }
      }
      return super.getRegionPlan(state, serverToExclude, forceNewPlan);
    } else {
      if (first)
        initValue();
    }
    String encodedName = state.getRegion().getEncodedName();

    // The remove below hinges on the fact that the call to
    // serverManager.getOnlineServersList() returns a copy
    if (serverToExclude != null)
      servers.remove(serverToExclude);
    if (servers.isEmpty())
      return null;
    if (rootOrMeta) {
      servers = this.getDefaultServer();
    } else {
      servers = this.getAvailableServer(servers, state.getRegion()
          .getTableDesc().getNameAsString());
    }

    // get available servers according to the group information

    RegionPlan randomPlan = new RegionPlan(state.getRegion(), null,
        LoadBalancer.randomAssignment(servers));
    boolean newPlan = false;
    RegionPlan existingPlan = null;
    synchronized (this.regionPlans) {
      existingPlan = this.regionPlans.get(encodedName);
      if (forceNewPlan || existingPlan == null
          || existingPlan.getDestination() == null
          || existingPlan.getDestination().equals(serverToExclude)) {
        newPlan = true;
        this.regionPlans.put(encodedName, randomPlan);
      }
    }
    if (newPlan) {
      LOG.debug("No previous transition plan was found (or we are ignoring "
          + "an existing plan) for "
          + state.getRegion().getRegionNameAsString()
          + " so generated a random one; " + randomPlan + "; "
          +
          // serverManager.countOfRegionServers() +
          " (online=" + serverManager.getOnlineServers().size() + ", exclude="
          + serverToExclude + ") available servers");
      return randomPlan;
    }
    LOG.debug("Using pre-existing plan for region "
        + state.getRegion().getRegionNameAsString() + "; plan=" + existingPlan);
    return existingPlan;
  }

  private String getGroupString(HTableDescriptor des) {

    byte[] groupsb = des.getValue(GROUP_KEY);
    if (groupsb != null) {
      return Bytes.toString(des.getValue(GROUP_KEY));
    } else {
      return "0";
    }

  }

  // private String[] getGroupString(HRegionInfo info) {
  // String groups = getGroupString(info.getTableDesc());
  // return groups.split(GROUP_SPLITER);
  //
  // }

  private String getGroupStringRandom(HRegionInfo info) {
    String groups = getGroupString(info.getTableDesc());
    String[] ret = groups.split(GROUP_SPLITER);
    if (ret.length == 1) {
      return ret[0];
    } else {
      return ret[RANDOM.nextInt(ret.length)];
    }

  }

  // private HashSet<HServerInfo> getGroupServer(HRegionInfo info) {
  //
  // String[] groups = getGroupString(info);
  // String group = groups[RANDOM.nextInt(groups.length)];
  // if (groupServers.get(group) == null) {
  // return groupServers.get("0");
  // }
  // return groupServers.get(group);
  //
  // }

  // private List<HServerInfo> getGroupServerList(HRegionInfo info) {
  //
  // String[] groups = getGroupString(info);
  // String group = groups[RANDOM.nextInt(groups.length)];
  // ArrayList<HServerInfo> ret = new ArrayList<HServerInfo>();
  //
  // if (groupServers.get(group) == null) {
  // ret.addAll(groupServers.get("0"));
  // return ret;
  // }
  // ret.addAll(this.groupServers.get(group));
  // return ret;
  //
  // }

  private Map<String, List<HServerInfo>> getServerGroups(
      List<HServerInfo> servers) {
    {
      Map<String, List<HServerInfo>> ret = new TreeMap<String, List<HServerInfo>>();
      for (HServerInfo server : servers) {
        String group = "0";
        for (Entry<String, HashSet<HServerInfo>> entry : groupServers
            .entrySet()) {
          if (entry.getValue().contains(server)) {
            group = entry.getKey();
            break;
          }
        }
        if (ret.get(group) == null) {
          ret.put(group, new ArrayList<HServerInfo>());
        }
        ret.get(group).add(server);
      }
      return ret;
    }
  }

  /**
   * assign regions to servers according to group information,and try to use the
   * assignment remained in meta
   * 
   * @param regions
   *          regions and assignments
   * @param servers
   *          available servers
   * @return the new assignment
   */
  private Map<HServerInfo, List<HRegionInfo>> retainGroupAssignRegions(
      Map<HRegionInfo, HServerAddress> regions, List<HServerInfo> servers) {

    Map<HServerInfo, List<HRegionInfo>> assignments = new TreeMap<HServerInfo, List<HRegionInfo>>();

    Map<HServerAddress, HServerInfo> serverMap = new TreeMap<HServerAddress, HServerInfo>();

    Map<String, List<HServerInfo>> groupMap = getServerGroups(servers);

    for (HServerInfo server : servers) {
      serverMap.put(server.getServerAddress(), server);
      assignments.put(server, new ArrayList<HRegionInfo>());
    }
    for (Map.Entry<HRegionInfo, HServerAddress> region : regions.entrySet()) {
      HServerAddress hsa = region.getValue();
      HServerInfo server = hsa == null ? null : serverMap.get(hsa);
      if (server != null
          && this.regionAndServerIsSameGroup(region.getKey(), server)) {
        assignments.get(server).add(region.getKey());
      } else {
        List<HServerInfo> avaServers = getAvailableServer(region.getKey()
            .getTableDesc().getNameAsString());
        assignments.get(avaServers.get(RANDOM.nextInt(avaServers.size()))).add(
            region.getKey());
      }
    }

    return assignments;
  }

  /**
   * Assigns list of user regions in round-robin fashion, if any.
   * 
   * @param sync
   *          True if we are to wait on all assigns.
   * @param startup
   *          True if this is server startup time.
   * @throws InterruptedException
   * @throws IOException
   */
  void bulkAssignUserRegions(final HRegionInfo[] regions,
      final List<HServerInfo> servers, final boolean sync) throws IOException {

    List<Pair<List<HRegionInfo>, List<HServerInfo>>> ret = this
        .groupAssignRegions(java.util.Arrays.asList(regions), servers);
    for (Pair<List<HRegionInfo>, List<HServerInfo>> p : ret) {
      Map<HServerInfo, List<HRegionInfo>> bulkPlan = LoadBalancer
          .roundRobinAssignment(p.getFirst(), p.getSecond());
      LOG.info("Bulk assigning " + regions.length + " region(s) "
          + "round-robin across " + servers.size() + " server(s)");
      // Use fixed count thread pool assigning.
      BulkAssigner ba = new GeneralBulkAssigner(this.master, bulkPlan, this);
      try {
        ba.bulkAssign(sync);
      } catch (InterruptedException e) {
        throw new IOException("InterruptedException bulk assigning", e);
      }

    }
    LOG.info("Bulk assigning done");
  }

  private boolean groupExist(String groupString) {

    String[] groups = groupString.split(GROUP_SPLITER);
    if (groups.length != 0) {
      for (String s : groups) {
        if (groupServers.get(s) != null) {
          return true;
        }
      }
    }
    return false;

  }

  /**
   * assign regions according to the group information,
   * 
   * @param regions
   *          regions to assigne
   * @param servers
   *          available servers
   * @return the assignment
   */
  private List<Pair<List<HRegionInfo>, List<HServerInfo>>> groupAssignRegions(
      List<HRegionInfo> regions, List<HServerInfo> servers) {
    List<Pair<List<HRegionInfo>, List<HServerInfo>>> ret = new ArrayList<Pair<List<HRegionInfo>, List<HServerInfo>>>();
    HashMap<String, List<HRegionInfo>> groupToRegion = new HashMap<String, List<HRegionInfo>>();
    servers = filterRootServer(servers);
    for (HRegionInfo info : regions) {
      String groups = getGroupString(info.getTableDesc());
      if (!groupExist(groups)) {
        groups = DEFAULT_GROUP;
      }
      if (groupToRegion.get(groups) == null) {
        groupToRegion.put(groups, new ArrayList<HRegionInfo>());
      }
      groupToRegion.get(groups).add(info);
    }

    for (List<HRegionInfo> list : groupToRegion.values()) {
      Pair<List<HRegionInfo>, List<HServerInfo>> p = new Pair<List<HRegionInfo>, List<HServerInfo>>();
      p.setFirst(list);
      p.setSecond(new ArrayList<HServerInfo>());
      for (HServerInfo server : servers) {
        if (this.regionAndServerIsSameGroup(list.get(0), server)) {
          p.getSecond().add(server);
        }
      }
      ret.add(p);
    }
    return ret;
  }

  private void assignUserRegions(List<HRegionInfo> regions,
      List<HServerInfo> servers) throws IOException, InterruptedException {
    if (regions == null)
      return;
    Map<HServerInfo, List<HRegionInfo>> bulkPlan = null;
    // Generate a round-robin bulk assignment plan
    bulkPlan = LoadBalancer.roundRobinAssignment(regions, servers);
    LOG.info("Bulk assigning " + regions.size()
        + " region(s) round-robin across " + servers.size() + " server(s)");
    // Use fixed count thread pool assigning.
    BulkAssigner ba = new StartupBulkAssigner(master, bulkPlan, this);
    ba.bulkAssign();
    LOG.info("Bulk assigning done");
  }

  /**
   * Assigns all user regions, if any exist. Used during cluster startup.
   * <p>
   * This is a synchronous call and will return once every region has been
   * assigned. If anything fails, an exception is thrown and the cluster should
   * be shutdown.
   * 
   * @throws InterruptedException
   * @throws IOException
   */
  @Override
  public void assignAllUserRegions() throws IOException, InterruptedException {
    // Get all available servers
    initValue();
    List<HServerInfo> servers = serverManager.getOnlineServersList();

    // Scan META for all user regions, skipping any disabled tables
    Map<HRegionInfo, HServerAddress> allRegions = MetaReader.fullScan(
        catalogTracker, this.zkTableIn.getDisabledTables(), true);
    if (allRegions == null || allRegions.isEmpty())
      return;

    // Determine what type of assignment to do on startup
    boolean retainAssignment = master.getConfiguration().getBoolean(
        "hbase.master.startup.retainassign", true);

    Map<HServerInfo, List<HRegionInfo>> bulkPlan = null;

    if (retainAssignment) {
      // Reuse existing assignment info
      bulkPlan = this.retainGroupAssignRegions(allRegions, servers);
      // LoadBalancer.retainAssignment(allRegions, servers);
    } else {
      List<Pair<List<HRegionInfo>, List<HServerInfo>>> ret = this
          .groupAssignRegions(new ArrayList<HRegionInfo>(allRegions.keySet()),
              servers);
      // assign regions in round-robin fashion
      for (Pair<List<HRegionInfo>, List<HServerInfo>> p : ret)
        assignUserRegions(p.getFirst(), p.getSecond());
      return;
    }
    LOG.info("Bulk assigning " + allRegions.size() + " region(s) across "
        + servers.size() + " server(s), retainAssignment=" + retainAssignment);

    // Use fixed count thread pool assigning.
    BulkAssigner ba = new StartupBulkAssigner(master, bulkPlan, this);
    ba.bulkAssign();
    LOG.info("Bulk assigning done");
  }

  /**
   * Handle a ZK unassigned node transition triggered by HBCK repair tool.
   * <p>
   * This is handled in a separate code path because it breaks the normal rules.
   * the following is copied from AssignmentMananger.java
   * 
   * @param data
   */
  private void handleHBCK(RegionTransitionData data) {
    String encodedName = HRegionInfo.encodeRegionName(data.getRegionName());
    LOG.info("Handling HBCK triggered transition=" + data.getEventType()
        + ", server=" + data.getServerName() + ", region="
        + HRegionInfo.prettyPrint(encodedName));
    RegionState regionState = regionsInTransition.get(encodedName);
    switch (data.getEventType()) {
    case M_ZK_REGION_OFFLINE:
      HRegionInfo regionInfo = null;
      if (regionState != null) {
        regionInfo = regionState.getRegion();
      } else {
        try {
          regionInfo = MetaReader.getRegion(catalogTracker,
              data.getRegionName()).getFirst();
        } catch (IOException e) {
          LOG.info("Exception reading META doing HBCK repair operation", e);
          return;
        }
      }
      LOG.info("HBCK repair is triggering assignment of region="
          + regionInfo.getRegionNameAsString());
      // trigger assign, node is already in OFFLINE so don't need to
      // update ZK
      assign(regionInfo, false);
      break;

    default:
      LOG.warn("Received unexpected region state from HBCK ("
          + data.getEventType() + ")");
      break;
    }
  }

  /**
   * Handles various states an unassigned node can be in.
   * <p>
   * Method is called when a state change is suspected for an unassigned node.
   * <p>
   * This deals with skipped transitions (we got a CLOSED but didn't see CLOSING
   * yet). the following is copied from AssignmentMananger.java
   * 
   * @param data
   */
  @SuppressWarnings("unused")
  private void handleRegion(final RegionTransitionData data) {
    synchronized (regionsInTransition) {
      if (data == null || data.getServerName() == null) {
        LOG.warn("Unexpected NULL input " + data);
        return;
      }
      // Check if this is a special HBCK transition
      if (data.getServerName().equals(HConstants.HBCK_CODE_NAME)) {
        handleHBCK(data);
        return;
      }
      // Verify this is a known server
      if (!serverManager.isServerOnline(data.getServerName())
          && !master.getServerName().equals(data.getServerName())) {
        LOG.warn("Attempted to handle region transition for server but "
            + "server is not online: " + data.getRegionName());
        return;
      }
      String encodedName = HRegionInfo.encodeRegionName(data.getRegionName());
      String prettyPrintedRegionName = HRegionInfo.prettyPrint(encodedName);
      LOG.debug("Handling transition=" + data.getEventType() + ", server="
          + data.getServerName() + ", region=" + prettyPrintedRegionName);
      RegionState regionState = regionsInTransition.get(encodedName);
      switch (data.getEventType()) {
      case M_ZK_REGION_OFFLINE:
        // Nothing to do.
        break;

      case RS_ZK_REGION_CLOSING:
        // Should see CLOSING after we have asked it to CLOSE or
        // additional
        // times after already being in state of CLOSING
        if (regionState == null
            || (!regionState.isPendingClose() && !regionState.isClosing())) {
          LOG.warn("Received CLOSING for region " + prettyPrintedRegionName
              + " from server " + data.getServerName() + " but region was in "
              + " the state " + regionState + " and not "
              + "in expected PENDING_CLOSE or CLOSING states");
          return;
        }
        // Transition to CLOSING (or update stamp if already CLOSING)
        regionState.update(RegionState.State.CLOSING, data.getStamp());
        break;

      case RS_ZK_REGION_CLOSED:
        // Should see CLOSED after CLOSING but possible after
        // PENDING_CLOSE
        if (regionState == null
            || (!regionState.isPendingClose() && !regionState.isClosing())) {
          LOG.warn("Received CLOSED for region " + prettyPrintedRegionName
              + " from server " + data.getServerName() + " but region was in "
              + " the state " + regionState + " and not "
              + "in expected PENDING_CLOSE or CLOSING states");
          try {
            HRegionInfo info = this.getAssignment(data.getRegionName())
                .getFirst();
            this.setOffline(info);
            this.clearRegionFromTransition(info);
          } catch (Exception e) {

            e.printStackTrace();
          }
          return;

        }
        regionState.update(RegionState.State.CLOSED, data.getStamp());
        this.executorServiceIn.submit(new ClosedRegionHandler(master, this,
            regionState.getRegion()));
        break;

      case RS_ZK_REGION_OPENING:
        // Should see OPENING after we have asked it to OPEN or
        // additional
        // times after already being in state of OPENING
        if (regionState == null
            || (!regionState.isPendingOpen() && !regionState.isOpening())) {
          LOG.warn("Received OPENING for region " + prettyPrintedRegionName
              + " from server " + data.getServerName() + " but region was in "
              + " the state " + regionState + " and not "
              + "in expected PENDING_OPEN or OPENING states");
          return;
        }
        // Transition to OPENING (or update stamp if already OPENING)
        regionState.update(RegionState.State.OPENING, data.getStamp());
        break;

      case RS_ZK_REGION_OPENED:
        // Should see OPENED after OPENING but possible after
        // PENDING_OPEN
        if (regionState == null
            || (!regionState.isPendingOpen() && !regionState.isOpening())) {
          LOG.warn("Received OPENED for region " + prettyPrintedRegionName
              + " from server " + data.getServerName() + " but region was in "
              + " the state " + regionState + " and not "
              + "in expected PENDING_OPEN or OPENING states");
          return;
        }
        // Handle OPENED by removing from transition and deleted zk node
        regionState.update(RegionState.State.OPEN, data.getStamp());
        this.executorServiceIn.submit(new OpenedRegionHandler(master, this,
            regionState.getRegion(), serverManager.getServerInfo(data
                .getServerName())));
        break;
      }
    }
  }

  static Random RANDOM = new Random();

}
