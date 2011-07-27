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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.master.GroupAssignmentManager;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * Class user for auto balance all tables in hbase start with hbase
 * 
 */
public class AutoBalance {
  private static Log LOG = LogFactory.getLog(AutoBalance.class);
  private long period;

  /**
   * Construct method
   * 
   * @param period
   *          , auto balance period , ms
   */
  public AutoBalance(long period) {
    super();
    this.period = period;
  }

  /**
   * Start balance
   */
  public void startBalance() {
    Thread balancethread = new Thread(new Balance(period));
    balancethread.start();
  }

  /**
   * Inner Class for balance all table
   * 
   */
  class Balance implements Runnable, Abortable {
    private long period;
    private ZooKeeperWatcher zkw;
    Configuration conf = HBaseConfiguration.create();

    public Balance(long period) {
      this.period = period;
      try {
        zkw = new ZooKeeperWatcher(conf, "BalanceTracker", (Abortable) this);
        LOG.info("init a new zookeeper watcher !");
      } catch (ZooKeeperConnectionException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    /**
     * Check if cluster is running
     * <p>
     * if cluster is running ,then read all table from zookeeper , then balance
     * them
     */
    public synchronized void run() {
      while (!checkClusterIsRunning()) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      try {
        final long SLEEP_WHENSTART = 600000; // 10 minutes
        Thread.sleep(SLEEP_WHENSTART);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      while (true) {
        // auto balance all table
        try {
          LOG.info("Before Balance table...");
          balanceAllTable();
          LOG.info("After Balance table...");
        } catch (KeeperException e) {
          e.printStackTrace();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        try {
          LOG.info("Before Balance sleep...");
          int numbers = (int) (period / 1000) + 1;
          for (int i = 0; i < numbers; i++) {
            // check if cluster is down
            Thread.sleep(1000);
            boolean status = checkClusterIsRunning();
            if (!status)
              break;
          }
          LOG.info("After  Balance sleep...");
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }

    private void balanceAllTable() throws KeeperException, InterruptedException {
      HBaseAdmin admin = null;
      try {
        admin = new HBaseAdmin(conf);
      } catch (MasterNotRunningException e) {
        e.printStackTrace();
        return;
      } catch (ZooKeeperConnectionException e) {
        e.printStackTrace();
        return;
      }
      HTableDescriptor[] tablelist = null;
      try {
        tablelist = admin.listTables();
      } catch (IOException e) {
        e.printStackTrace();
      }
      LOG.info("Get all table from client ,and table size is "
          + tablelist.length);
      for (HTableDescriptor tabledesc : tablelist) {
        String table = tabledesc.getNameAsString();
        LOG.info("table name is :" + table);
        boolean isprocess = ServerWithGroup.isIsprocess();
        System.out.println("Is  Processing ? " + isprocess);
        if (!isprocess) {
          LOG.info("Ready to balance table " + table);
          synchronized (ServerWithGroup.class) {
            ServerWithGroup.setIsprocess(true);
            ServerWithGroup.setIsbalance(true);
          }
          synchronized (GroupAssignmentManager.class) {
            GroupAssignmentManager.balanceTable(table);
          }
          synchronized (ServerWithGroup.class) {
            ServerWithGroup.setIsprocess(false);
            ServerWithGroup.setIsbalance(false);
          }
          LOG.info("After balance table " + table);
        }
      }
    }

    private boolean checkClusterIsRunning() {
      String clusterStateZNode = zkw.clusterStateZNode;
      byte[] data;
      try {
        data = zkw.getZooKeeper().getData(clusterStateZNode, false, null);
      } catch (KeeperException e) {
        LOG.debug("when get clusterstate form zookeeper " + e.getMessage());
        return false;
      } catch (InterruptedException e) {
        LOG.debug("when get clusterstate form zookeeper " + e.getMessage());
        return false;
      }
      // LOG.debug("data is "+Bytes.toString(data));
      if (data != null) {
        return true;
      } else {
        return false;
      }
    }

    @Override
    public void abort(String arg0, Throwable arg1) {
      LOG.info("Get exceptuion when auto balance , abort.......");
    }
  }

  public static void main(String[] args) {
    AutoBalance autobalance = new AutoBalance(100000);
    autobalance.startBalance();
  }
}
