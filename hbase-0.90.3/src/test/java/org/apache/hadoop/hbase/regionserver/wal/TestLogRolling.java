/**
 * Copyright 2007 The Apache Software Foundation
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
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Test log deletion as logs are rolled.
 */
public class TestLogRolling  {
  private static final Log LOG = LogFactory.getLog(TestLogRolling.class);
  private HRegionServer server;
  private HLog log;
  private String tableName;
  private byte[] value;
  private static FileSystem fs;
  private static MiniDFSCluster dfsCluster;
  private static HBaseAdmin admin;
  private static MiniHBaseCluster cluster;
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

 // verbose logging on classes that are touched in these tests
 {
   ((Log4JLogger)DataNode.LOG).getLogger().setLevel(Level.ALL);
   ((Log4JLogger)LeaseManager.LOG).getLogger().setLevel(Level.ALL);
   ((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
   ((Log4JLogger)DFSClient.LOG).getLogger().setLevel(Level.ALL);
   ((Log4JLogger)HRegionServer.LOG).getLogger().setLevel(Level.ALL);
   ((Log4JLogger)HRegion.LOG).getLogger().setLevel(Level.ALL);
   ((Log4JLogger)HLog.LOG).getLogger().setLevel(Level.ALL);
 }

  /**
   * constructor
   * @throws Exception
   */
  public TestLogRolling()  {
    // start one regionserver and a minidfs.
    super();
      this.server = null;
      this.log = null;
      this.tableName = null;
      this.value = null;

      String className = this.getClass().getName();
      StringBuilder v = new StringBuilder(className);
      while (v.length() < 1000) {
        v.append(className);
      }
      value = Bytes.toBytes(v.toString());
  }

  // Need to override this setup so we can edit the config before it gets sent
 // to the HDFS & HBase cluster startup.
 @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    /**** configuration for testLogRolling ****/
    // Force a region split after every 768KB
    TEST_UTIL.getConfiguration().setLong("hbase.hregion.max.filesize", 768L * 1024L);

    // We roll the log after every 32 writes
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.maxlogentries", 32);

    // For less frequently updated regions flush after every 2 flushes
    TEST_UTIL.getConfiguration().setInt("hbase.hregion.memstore.optionalflushcount", 2);

    // We flush the cache after every 8192 bytes
    TEST_UTIL.getConfiguration().setInt("hbase.hregion.memstore.flush.size", 8192);

    // Increase the amount of time between client retries
    TEST_UTIL.getConfiguration().setLong("hbase.client.pause", 15 * 1000);

    // Reduce thread wake frequency so that other threads can get
    // a chance to run.
    TEST_UTIL.getConfiguration().setInt(HConstants.THREAD_WAKE_FREQUENCY, 2 * 1000);

   /**** configuration for testLogRollOnDatanodeDeath ****/
   // make sure log.hflush() calls syncFs() to open a pipeline
    TEST_UTIL.getConfiguration().setBoolean("dfs.support.append", true);
   // lower the namenode & datanode heartbeat so the namenode
   // quickly detects datanode failures
    TEST_UTIL.getConfiguration().setInt("heartbeat.recheck.interval", 5000);
    TEST_UTIL.getConfiguration().setInt("dfs.heartbeat.interval", 1);
   // the namenode might still try to choose the recently-dead datanode
   // for a pipeline, so try to a new pipeline multiple times
    TEST_UTIL.getConfiguration().setInt("dfs.client.block.write.retries", 30);
    TEST_UTIL.startMiniCluster(2);

    cluster = TEST_UTIL.getHBaseCluster();
    dfsCluster = TEST_UTIL.getDFSCluster();
    fs = TEST_UTIL.getTestFileSystem();
    admin = TEST_UTIL.getHBaseAdmin();
  }

  @AfterClass
  public  static void tearDownAfterClass() throws IOException  {
    TEST_UTIL.cleanupTestDir();
    TEST_UTIL.shutdownMiniCluster();
  }

  private void startAndWriteData() throws IOException {
    // When the META table can be opened, the region servers are running
    new HTable(TEST_UTIL.getConfiguration(), HConstants.META_TABLE_NAME);
    this.server = cluster.getRegionServerThreads().get(0).getRegionServer();
    this.log = server.getWAL();

    // Create the test table and open it
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    admin.createTable(desc);
    HTable table = new HTable(TEST_UTIL.getConfiguration(), tableName);

    server = TEST_UTIL.getRSForFirstRegionInTable(Bytes.toBytes(tableName));
    this.log = server.getWAL();
    for (int i = 1; i <= 256; i++) {    // 256 writes should cause 8 log rolls
      Put put = new Put(Bytes.toBytes("row" + String.format("%1$04d", i)));
      put.add(HConstants.CATALOG_FAMILY, null, value);
      table.put(put);
      if (i % 32 == 0) {
        // After every 32 writes sleep to let the log roller run
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          // continue
        }
      }
    }
  }

  /**
   * Tests that logs are deleted
   * @throws IOException
   * @throws FailedLogCloseException
   */
  @Test
  public void testLogRolling() throws FailedLogCloseException, IOException {
    this.tableName = getName();
      startAndWriteData();
      LOG.info("after writing there are " + log.getNumLogFiles() + " log files");

      // flush all regions

      List<HRegion> regions =
        new ArrayList<HRegion>(server.getOnlineRegionsLocalContext());
      for (HRegion r: regions) {
        r.flushcache();
      }

      // Now roll the log
      log.rollWriter();

      int count = log.getNumLogFiles();
      LOG.info("after flushing all regions and rolling logs there are " +
          log.getNumLogFiles() + " log files");
      assertTrue(("actual count: " + count), count <= 2);
  }

  private static String getName() {
    return "TestLogRolling";
  }

  void writeData(HTable table, int rownum) throws IOException {
    Put put = new Put(Bytes.toBytes("row" + String.format("%1$04d", rownum)));
    put.add(HConstants.CATALOG_FAMILY, null, value);
    table.put(put);

    // sleep to let the log roller run (if it needs to)
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      // continue
    }
  }

  /**
   * Give me the HDFS pipeline for this log file
   */
  DatanodeInfo[] getPipeline(HLog log) throws IllegalArgumentException,
      IllegalAccessException, InvocationTargetException {
    OutputStream stm = log.getOutputStream();
    Method getPipeline = null;
    for (Method m : stm.getClass().getDeclaredMethods()) {
      if (m.getName().endsWith("getPipeline")) {
        getPipeline = m;
        getPipeline.setAccessible(true);
        break;
      }
    }

    assertTrue("Need DFSOutputStream.getPipeline() for this test",
        null != getPipeline);
    Object repl = getPipeline.invoke(stm, new Object[] {} /* NO_ARGS */);
    return (DatanodeInfo[]) repl;
  }

  /**
   * Tests that logs are rolled upon detecting datanode death
   * Requires an HDFS jar with HDFS-826 & syncFs() support (HDFS-200)
   * @throws IOException
   * @throws InterruptedException
   * @throws InvocationTargetException 
   * @throws IllegalAccessException
   * @throws IllegalArgumentException 
    */
  @Test
  public void testLogRollOnDatanodeDeath() throws IOException,
      InterruptedException, IllegalArgumentException, IllegalAccessException,
      InvocationTargetException {
    assertTrue("This test requires HLog file replication.",
      fs.getDefaultReplication() > 1);
    LOG.info("Replication=" + fs.getDefaultReplication());
    // When the META table can be opened, the region servers are running
    new HTable(TEST_UTIL.getConfiguration(), HConstants.META_TABLE_NAME);

    this.server = cluster.getRegionServer(0);
    this.log = server.getWAL();
    
    // Create the test table and open it
    String tableName = getName();
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));

    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
    admin.createTable(desc);
    HTable table = new HTable(TEST_UTIL.getConfiguration(), tableName);

    server = TEST_UTIL.getRSForFirstRegionInTable(Bytes.toBytes(tableName));
    this.log = server.getWAL();

    assertTrue("Need HDFS-826 for this test", log.canGetCurReplicas());
    // don't run this test without append support (HDFS-200 & HDFS-142)
    assertTrue("Need append support for this test", FSUtils
        .isAppendSupported(TEST_UTIL.getConfiguration()));

    // add up the datanode count, to ensure proper replication when we kill 1
    dfsCluster
        .startDataNodes(TEST_UTIL.getConfiguration(), 1, true, null, null);
    dfsCluster.waitActive();
    assertTrue(dfsCluster.getDataNodes().size() >= fs.getDefaultReplication() + 1);

    writeData(table, 2);

    table.setAutoFlush(true);

    long curTime = System.currentTimeMillis();
    long oldFilenum = log.getFilenum();
    assertTrue("Log should have a timestamp older than now",
        curTime > oldFilenum && oldFilenum != -1);

    assertTrue("The log shouldn't have rolled yet", oldFilenum == log.getFilenum());
    DatanodeInfo[] pipeline = getPipeline(log);
    assertTrue(pipeline.length == fs.getDefaultReplication());

    // kill a datanode in the pipeline to force a log roll on the next sync()
    assertTrue(dfsCluster.stopDataNode(pipeline[0].getName()) != null);
    Thread.sleep(10000);
    // this write should succeed, but trigger a log roll
    writeData(table, 2);
    long newFilenum = log.getFilenum();

    assertTrue("Missing datanode should've triggered a log roll",
        newFilenum > oldFilenum && newFilenum > curTime);

    // write some more log data (this should use a new hdfs_out)
    writeData(table, 3);
    assertTrue("The log should not roll again.", log.getFilenum() == newFilenum);
    assertTrue("New log file should have the default replication", log
        .getLogReplication() == fs.getDefaultReplication());
  }
}
