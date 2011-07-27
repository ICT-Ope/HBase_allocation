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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionException;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.ipc.HMasterInterface;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.StringUtils;

/**
 * Provides an interface to manage HBase database table metadata + general 
 * administrative functions.  Use HBaseAdmin to create, drop, list, enable and 
 * disable tables. Use it also to add and drop table column families. 
 * 
 * <p>See {@link HTable} to add, update, and delete data from an individual table.
 * <p>Currently HBaseAdmin instances are not expected to be long-lived.  For
 * example, an HBaseAdmin instance will not ride over a Master restart.
 */
public class HBaseAdmin implements Abortable {
  private final Log LOG = LogFactory.getLog(this.getClass().getName());
//  private final HConnection connection;
  final HConnection connection;
  private volatile Configuration conf;
  private final long pause;
  private final int numRetries;
  // Some operations can take a long time such as disable of big table.
  // numRetries is for 'normal' stuff... Mutliply by this factor when
  // want to wait a long time.
  private final int retryLongerMultiplier;

  /**
   * Constructor
   *
   * @param conf Configuration object
   * @throws MasterNotRunningException if the master is not running
   * @throws ZooKeeperConnectionException if unable to connect to zookeeper
   */
  public HBaseAdmin(Configuration conf)
  throws MasterNotRunningException, ZooKeeperConnectionException {
    this.connection = HConnectionManager.getConnection(conf);
    this.conf = conf;
    this.pause = conf.getLong("hbase.client.pause", 1000);
    this.numRetries = conf.getInt("hbase.client.retries.number", 10);
    this.retryLongerMultiplier = conf.getInt("hbase.client.retries.longer.multiplier", 10);
    this.connection.getMaster();
  }

  /**
   * @return A new CatalogTracker instance; call {@link #cleanupCatalogTracker(CatalogTracker)}
   * to cleanup the returned catalog tracker.
   * @throws ZooKeeperConnectionException
   * @throws IOException
   * @see #cleanupCatalogTracker(CatalogTracker);
   */
  private synchronized CatalogTracker getCatalogTracker()
  throws ZooKeeperConnectionException, IOException {
    CatalogTracker ct = null;
    try {
      HConnection connection =
        HConnectionManager.getConnection(new Configuration(this.conf));
      ct = new CatalogTracker(connection);
      ct.start();
    } catch (InterruptedException e) {
      // Let it out as an IOE for now until we redo all so tolerate IEs
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted", e);
    }
    return ct;
  }

  private void cleanupCatalogTracker(final CatalogTracker ct) {
    ct.stop();
    HConnectionManager.deleteConnection(ct.getConnection().getConfiguration(), true);
  }

  @Override
  public void abort(String why, Throwable e) {
    // Currently does nothing but throw the passed message and exception
    throw new RuntimeException(why, e);
  }

  /** @return HConnection used by this object. */
  public HConnection getConnection() {
    return connection;
  }

  /**
   * Get a connection to the currently set master.
   * @return proxy connection to master server for this instance
   * @throws MasterNotRunningException if the master is not running
   * @throws ZooKeeperConnectionException if unable to connect to zookeeper
   */
  public HMasterInterface getMaster()
  throws MasterNotRunningException, ZooKeeperConnectionException {
    return this.connection.getMaster();
  }

  /** @return - true if the master server is running
   * @throws ZooKeeperConnectionException
   * @throws MasterNotRunningException */
  public boolean isMasterRunning()
  throws MasterNotRunningException, ZooKeeperConnectionException {
    return this.connection.isMasterRunning();
  }

  /**
   * @param tableName Table to check.
   * @return True if table exists already.
   * @throws IOException 
   */
  public boolean tableExists(final String tableName)
  throws IOException {
    boolean b = false;
    CatalogTracker ct = getCatalogTracker();
    try {
      b = MetaReader.tableExists(ct, tableName);
    } finally {
      cleanupCatalogTracker(ct);
    }
    return b;
  }

  /**
   * @param tableName Table to check.
   * @return True if table exists already.
   * @throws IOException 
   */
  public boolean tableExists(final byte [] tableName)
  throws IOException {
    return tableExists(Bytes.toString(tableName));
  }

  /**
   * List all the userspace tables.  In other words, scan the META table.
   *
   * If we wanted this to be really fast, we could implement a special
   * catalog table that just contains table names and their descriptors.
   * Right now, it only exists as part of the META table's region info.
   *
   * @return - returns an array of HTableDescriptors
   * @throws IOException if a remote or network exception occurs
   */
  public HTableDescriptor[] listTables() throws IOException {
    return this.connection.listTables();
  }


  /**
   * Method for getting the tableDescriptor
   * @param tableName as a byte []
   * @return the tableDescriptor
   * @throws IOException if a remote or network exception occurs
   */
  public HTableDescriptor getTableDescriptor(final byte [] tableName)
  throws IOException {
    return this.connection.getHTableDescriptor(tableName);
  }

  private long getPauseTime(int tries) {
    int triesCount = tries;
    if (triesCount >= HConstants.RETRY_BACKOFF.length) {
      triesCount = HConstants.RETRY_BACKOFF.length - 1;
    }
    return this.pause * HConstants.RETRY_BACKOFF[triesCount];
  }

  /**
   * Creates a new table.
   * Synchronous operation.
   *
   * @param desc table descriptor for table
   *
   * @throws IllegalArgumentException if the table name is reserved
   * @throws MasterNotRunningException if master is not running
   * @throws TableExistsException if table already exists (If concurrent
   * threads, the table may have been created between test-for-existence
   * and attempt-at-creation).
   * @throws IOException if a remote or network exception occurs
   */
  public void createTable(HTableDescriptor desc)
  throws IOException {
    createTable(desc, null);
  }

  /**
   * Creates a new table with the specified number of regions.  The start key
   * specified will become the end key of the first region of the table, and
   * the end key specified will become the start key of the last region of the
   * table (the first region has a null start key and the last region has a
   * null end key).
   *
   * BigInteger math will be used to divide the key range specified into
   * enough segments to make the required number of total regions.
   *
   * Synchronous operation.
   *
   * @param desc table descriptor for table
   * @param startKey beginning of key range
   * @param endKey end of key range
   * @param numRegions the total number of regions to create
   *
   * @throws IllegalArgumentException if the table name is reserved
   * @throws MasterNotRunningException if master is not running
   * @throws TableExistsException if table already exists (If concurrent
   * threads, the table may have been created between test-for-existence
   * and attempt-at-creation).
   * @throws IOException
   */
  public void createTable(HTableDescriptor desc, byte [] startKey,
      byte [] endKey, int numRegions)
  throws IOException {
    HTableDescriptor.isLegalTableName(desc.getName());
    if(numRegions < 3) {
      throw new IllegalArgumentException("Must create at least three regions");
    } else if(Bytes.compareTo(startKey, endKey) >= 0) {
      throw new IllegalArgumentException("Start key must be smaller than end key");
    }
    byte [][] splitKeys = Bytes.split(startKey, endKey, numRegions - 3);
    if(splitKeys == null || splitKeys.length != numRegions - 1) {
      throw new IllegalArgumentException("Unable to split key range into enough regions");
    }
    createTable(desc, splitKeys);
  }

  /**
   * Creates a new table with an initial set of empty regions defined by the
   * specified split keys.  The total number of regions created will be the
   * number of split keys plus one (the first region has a null start key and
   * the last region has a null end key).
   * Synchronous operation.
   *
   * @param desc table descriptor for table
   * @param splitKeys array of split keys for the initial regions of the table
   *
   * @throws IllegalArgumentException if the table name is reserved
   * @throws MasterNotRunningException if master is not running
   * @throws TableExistsException if table already exists (If concurrent
   * threads, the table may have been created between test-for-existence
   * and attempt-at-creation).
   * @throws IOException
   */
  public void createTable(HTableDescriptor desc, byte [][] splitKeys)
  throws IOException {
    HTableDescriptor.isLegalTableName(desc.getName());
    if(splitKeys != null && splitKeys.length > 1) {
      Arrays.sort(splitKeys, Bytes.BYTES_COMPARATOR);
      // Verify there are no duplicate split keys
      byte [] lastKey = null;
      for(byte [] splitKey : splitKeys) {
        if(lastKey != null && Bytes.equals(splitKey, lastKey)) {
          throw new IllegalArgumentException("All split keys must be unique, " +
            "found duplicate: " + Bytes.toStringBinary(splitKey) +
            ", " + Bytes.toStringBinary(lastKey));
        }
        lastKey = splitKey;
      }
    }
    createTableAsync(desc, splitKeys);
    for (int tries = 0; tries < numRetries; tries++) {
      try {
        // Wait for new table to come on-line
        connection.locateRegion(desc.getName(), HConstants.EMPTY_START_ROW);
        break;

      } catch (RegionException e) {
        if (tries == numRetries - 1) {
          // Ran out of tries
          throw e;
        }
      }
      try {
        Thread.sleep(getPauseTime(tries));
      } catch (InterruptedException e) {
        // Just continue; ignore the interruption.
      }
    }
  }

  /**
   * Creates a new table but does not block and wait for it to come online.
   * Asynchronous operation.
   *
   * @param desc table descriptor for table
   *
   * @throws IllegalArgumentException Bad table name.
   * @throws MasterNotRunningException if master is not running
   * @throws TableExistsException if table already exists (If concurrent
   * threads, the table may have been created between test-for-existence
   * and attempt-at-creation).
   * @throws IOException
   */
  public void createTableAsync(HTableDescriptor desc, byte [][] splitKeys)
  throws IOException {
    HTableDescriptor.isLegalTableName(desc.getName());
    try {
      getMaster().createTable(desc, splitKeys);
    } catch (RemoteException e) {
      throw e.unwrapRemoteException();
    }
  }

  /**
   * Deletes a table.
   * Synchronous operation.
   *
   * @param tableName name of table to delete
   * @throws IOException if a remote or network exception occurs
   */
  public void deleteTable(final String tableName) throws IOException {
    deleteTable(Bytes.toBytes(tableName));
  }

  /**
   * Deletes a table.
   * Synchronous operation.
   *
   * @param tableName name of table to delete
   * @throws IOException if a remote or network exception occurs
   */
  public void deleteTable(final byte [] tableName) throws IOException {
    isMasterRunning();
    HTableDescriptor.isLegalTableName(tableName);
    HRegionLocation firstMetaServer = getFirstMetaServerForTable(tableName);
    try {
      getMaster().deleteTable(tableName);
    } catch (RemoteException e) {
      throw RemoteExceptionHandler.decodeRemoteException(e);
    }
    // Wait until all regions deleted
    HRegionInterface server =
      connection.getHRegionConnection(firstMetaServer.getServerAddress());
    for (int tries = 0; tries < (this.numRetries * this.retryLongerMultiplier); tries++) {
      long scannerId = -1L;
      try {

        Scan scan = MetaReader.getScanForTableName(tableName);
        scan.addColumn(HConstants.CATALOG_FAMILY,
            HConstants.REGIONINFO_QUALIFIER);
        scannerId = server.openScanner(
          firstMetaServer.getRegionInfo().getRegionName(), scan);
        // Get a batch at a time.
        Result values = server.next(scannerId);
        if (values == null) {
          break;
        }
      } catch (IOException ex) {
        if(tries == numRetries - 1) {           // no more tries left
          if (ex instanceof RemoteException) {
            ex = RemoteExceptionHandler.decodeRemoteException((RemoteException) ex);
          }
          throw ex;
        }
      } finally {
        if (scannerId != -1L) {
          try {
            server.close(scannerId);
          } catch (Exception ex) {
            LOG.warn(ex);
          }
        }
      }
      try {
        Thread.sleep(getPauseTime(tries));
      } catch (InterruptedException e) {
        // continue
      }
    }
    // Delete cached information to prevent clients from using old locations
    this.connection.clearRegionCache(tableName);
    LOG.info("Deleted " + Bytes.toString(tableName));
  }

  public void enableTable(final String tableName)
  throws IOException {
    enableTable(Bytes.toBytes(tableName));
  }

  /**
   * Enable a table.  May timeout.  Use {@link #enableTableAsync(byte[])}
   * and {@link #isTableEnabled(byte[])} instead.
   * @param tableName name of the table
   * @throws IOException if a remote or network exception occurs
   * @see #isTableEnabled(byte[])
   * @see #disableTable(byte[])
   * @see #enableTableAsync(byte[])
   */
  public void enableTable(final byte [] tableName)
  throws IOException {
    enableTableAsync(tableName);
 
    // Wait until all regions are enabled
    boolean enabled = false;
    for (int tries = 0; tries < (this.numRetries * this.retryLongerMultiplier); tries++) {
      enabled = isTableEnabled(tableName);
      if (enabled) {
        break;
      }
      long sleep = getPauseTime(tries);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Sleeping= " + sleep + "ms, waiting for all regions to be " +
          "enabled in " + Bytes.toString(tableName));
      }
      try {
        Thread.sleep(sleep);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        // Do this conversion rather than let it out because do not want to
        // change the method signature.
        throw new IOException("Interrupted", e);
      }
    }
    if (!enabled) {
      throw new IOException("Unable to enable table " +
        Bytes.toString(tableName));
    }
    LOG.info("Enabled table " + Bytes.toString(tableName));
  }

  public void enableTableAsync(final String tableName)
  throws IOException {
    enableTableAsync(Bytes.toBytes(tableName));
  }

  /**
   * Brings a table on-line (enables it).  Method returns immediately though
   * enable of table may take some time to complete, especially if the table
   * is large (All regions are opened as part of enabling process).  Check
   * {@link #isTableEnabled(byte[])} to learn when table is fully online.  If
   * table is taking too long to online, check server logs.
   * @param tableName
   * @throws IOException
   * @since 0.90.0
   */
  public void enableTableAsync(final byte [] tableName)
  throws IOException {
    isMasterRunning();
    try {
      getMaster().enableTable(tableName);
    } catch (RemoteException e) {
      throw e.unwrapRemoteException();
    }
    LOG.info("Started enable of " + Bytes.toString(tableName));
  }

  public void disableTableAsync(final String tableName) throws IOException {
    disableTableAsync(Bytes.toBytes(tableName));
  }

  /**
   * Starts the disable of a table.  If it is being served, the master
   * will tell the servers to stop serving it.  This method returns immediately.
   * The disable of a table can take some time if the table is large (all
   * regions are closed as part of table disable operation).
   * Call {@link #isTableDisabled(byte[])} to check for when disable completes.
   * If table is taking too long to online, check server logs.
   * @param tableName name of table
   * @throws IOException if a remote or network exception occurs
   * @see #isTableDisabled(byte[])
   * @see #isTableEnabled(byte[])
   * @since 0.90.0
   */
  public void disableTableAsync(final byte [] tableName) throws IOException {
    isMasterRunning();
    try {
      getMaster().disableTable(tableName);
    } catch (RemoteException e) {
      throw e.unwrapRemoteException();
    }
    LOG.info("Started disable of " + Bytes.toString(tableName));
  }

  public void disableTable(final String tableName)
  throws IOException {
    disableTable(Bytes.toBytes(tableName));
  }

  /**
   * Disable table and wait on completion.  May timeout eventually.  Use
   * {@link #disableTableAsync(byte[])} and {@link #isTableDisabled(String)}
   * instead.
   * @param tableName
   * @throws IOException
   */
  public void disableTable(final byte [] tableName)
  throws IOException {
    disableTableAsync(tableName);
    // Wait until table is disabled
    boolean disabled = false;
    for (int tries = 0; tries < (this.numRetries * this.retryLongerMultiplier); tries++) {
      disabled = isTableDisabled(tableName);
      if (disabled) {
        break;
      }
      long sleep = getPauseTime(tries);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Sleeping= " + sleep + "ms, waiting for all regions to be " +
          "disabled in " + Bytes.toString(tableName));
      }
      try {
        Thread.sleep(sleep);
      } catch (InterruptedException e) {
        // Do this conversion rather than let it out because do not want to
        // change the method signature.
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted", e);
      }
    }
    if (!disabled) {
      throw new RegionException("Retries exhausted, it took too long to wait"+
        " for the table " + Bytes.toString(tableName) + " to be disabled.");
    }
    LOG.info("Disabled " + Bytes.toString(tableName));
  }

  /**
   * @param tableName name of table to check
   * @return true if table is on-line
   * @throws IOException if a remote or network exception occurs
   */
  public boolean isTableEnabled(String tableName) throws IOException {
    return isTableEnabled(Bytes.toBytes(tableName));
  }
  /**
   * @param tableName name of table to check
   * @return true if table is on-line
   * @throws IOException if a remote or network exception occurs
   */
  public boolean isTableEnabled(byte[] tableName) throws IOException {
    return connection.isTableEnabled(tableName);
  }

  /**
   * @param tableName name of table to check
   * @return true if table is off-line
   * @throws IOException if a remote or network exception occurs
   */
  public boolean isTableDisabled(final String tableName) throws IOException {
    return isTableDisabled(Bytes.toBytes(tableName));
  }

  /**
   * @param tableName name of table to check
   * @return true if table is off-line
   * @throws IOException if a remote or network exception occurs
   */
  public boolean isTableDisabled(byte[] tableName) throws IOException {
    return connection.isTableDisabled(tableName);
  }

  /**
   * @param tableName name of table to check
   * @return true if all regions of the table are available
   * @throws IOException if a remote or network exception occurs
   */
  public boolean isTableAvailable(byte[] tableName) throws IOException {
    return connection.isTableAvailable(tableName);
  }

  /**
   * @param tableName name of table to check
   * @return true if all regions of the table are available
   * @throws IOException if a remote or network exception occurs
   */
  public boolean isTableAvailable(String tableName) throws IOException {
    return connection.isTableAvailable(Bytes.toBytes(tableName));
  }

  /**
   * Add a column to an existing table.
   * Asynchronous operation.
   *
   * @param tableName name of the table to add column to
   * @param column column descriptor of column to be added
   * @throws IOException if a remote or network exception occurs
   */
  public void addColumn(final String tableName, HColumnDescriptor column)
  throws IOException {
    addColumn(Bytes.toBytes(tableName), column);
  }

  /**
   * Add a column to an existing table.
   * Asynchronous operation.
   *
   * @param tableName name of the table to add column to
   * @param column column descriptor of column to be added
   * @throws IOException if a remote or network exception occurs
   */
  public void addColumn(final byte [] tableName, HColumnDescriptor column)
  throws IOException {
    HTableDescriptor.isLegalTableName(tableName);
    try {
      getMaster().addColumn(tableName, column);
    } catch (RemoteException e) {
      throw RemoteExceptionHandler.decodeRemoteException(e);
    }
  }

  /**
   * Delete a column from a table.
   * Asynchronous operation.
   *
   * @param tableName name of table
   * @param columnName name of column to be deleted
   * @throws IOException if a remote or network exception occurs
   */
  public void deleteColumn(final String tableName, final String columnName)
  throws IOException {
    deleteColumn(Bytes.toBytes(tableName), Bytes.toBytes(columnName));
  }

  /**
   * Delete a column from a table.
   * Asynchronous operation.
   *
   * @param tableName name of table
   * @param columnName name of column to be deleted
   * @throws IOException if a remote or network exception occurs
   */
  public void deleteColumn(final byte [] tableName, final byte [] columnName)
  throws IOException {
    try {
      getMaster().deleteColumn(tableName, columnName);
    } catch (RemoteException e) {
      throw RemoteExceptionHandler.decodeRemoteException(e);
    }
  }

  /**
   * Modify an existing column family on a table.
   * Asynchronous operation.
   *
   * @param tableName name of table
   * @param columnName name of column to be modified
   * @param descriptor new column descriptor to use
   * @throws IOException if a remote or network exception occurs
   * @deprecated The <code>columnName</code> is redundant. Use {@link #addColumn(String, HColumnDescriptor)}
   */
  public void modifyColumn(final String tableName, final String columnName,
      HColumnDescriptor descriptor)
  throws IOException {
    modifyColumn(tableName,  descriptor);
  }

  /**
   * Modify an existing column family on a table.
   * Asynchronous operation.
   *
   * @param tableName name of table
   * @param descriptor new column descriptor to use
   * @throws IOException if a remote or network exception occurs
   */
  public void modifyColumn(final String tableName, HColumnDescriptor descriptor)
  throws IOException {
    modifyColumn(Bytes.toBytes(tableName), descriptor);
  }

  /**
   * Modify an existing column family on a table.
   * Asynchronous operation.
   *
   * @param tableName name of table
   * @param columnName name of column to be modified
   * @param descriptor new column descriptor to use
   * @throws IOException if a remote or network exception occurs
   * @deprecated The <code>columnName</code> is redundant. Use {@link #modifyColumn(byte[], HColumnDescriptor)}
   */
  public void modifyColumn(final byte [] tableName, final byte [] columnName,
    HColumnDescriptor descriptor)
  throws IOException {
    modifyColumn(tableName, descriptor);
  }

  /**
   * Modify an existing column family on a table.
   * Asynchronous operation.
   *
   * @param tableName name of table
   * @param descriptor new column descriptor to use
   * @throws IOException if a remote or network exception occurs
   */
  public void modifyColumn(final byte [] tableName, HColumnDescriptor descriptor)
  throws IOException {
    try {
      getMaster().modifyColumn(tableName, descriptor);
    } catch (RemoteException re) {
      // Convert RE exceptions in here; client shouldn't have to deal with them,
      // at least w/ the type of exceptions that come out of this method:
      // TableNotFoundException, etc.
      throw RemoteExceptionHandler.decodeRemoteException(re);
    }
  }

  /**
   * Close a region. For expert-admins.  Runs close on the regionserver.  The
   * master will not be informed of the close.
   * @param regionname region name to close
   * @param hostAndPort If supplied, we'll use this location rather than
   * the one currently in <code>.META.</code>
   * @throws IOException if a remote or network exception occurs
   */
  public void closeRegion(final String regionname, final String hostAndPort)
  throws IOException {
    closeRegion(Bytes.toBytes(regionname), hostAndPort);
  }

  /**
   * Close a region.  For expert-admins  Runs close on the regionserver.  The
   * master will not be informed of the close.
   * @param regionname region name to close
   * @param hostAndPort If supplied, we'll use this location rather than
   * the one currently in <code>.META.</code>
   * @throws IOException if a remote or network exception occurs
   */
  public void closeRegion(final byte [] regionname, final String hostAndPort)
  throws IOException {
    CatalogTracker ct = getCatalogTracker();
    try {
      if (hostAndPort != null) {
        HServerAddress hsa = new HServerAddress(hostAndPort);
        Pair<HRegionInfo, HServerAddress> pair =
          MetaReader.getRegion(ct, regionname);
        if (pair == null || pair.getSecond() == null) {
          LOG.info("No server in .META. for " +
            Bytes.toString(regionname) + "; pair=" + pair);
        } else {
          closeRegion(hsa, pair.getFirst());
        }
      } else {
        Pair<HRegionInfo, HServerAddress> pair =
          MetaReader.getRegion(ct, regionname);
        if (pair == null || pair.getSecond() == null) {
          LOG.info("No server in .META. for " +
            Bytes.toString(regionname) + "; pair=" + pair);
        } else {
          closeRegion(pair.getSecond(), pair.getFirst());
        }
      }
    } finally {
      cleanupCatalogTracker(ct);
    }
  }

  private void closeRegion(final HServerAddress hsa, final HRegionInfo hri)
  throws IOException {
    HRegionInterface rs = this.connection.getHRegionConnection(hsa);
    // Close the region without updating zk state.
    rs.closeRegion(hri, false);
  }

  /**
   * Flush a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to flush
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException 
   */
  public void flush(final String tableNameOrRegionName)
  throws IOException, InterruptedException {
    flush(Bytes.toBytes(tableNameOrRegionName));
  }

  /**
   * Flush a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to flush
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException 
   */
  public void flush(final byte [] tableNameOrRegionName)
  throws IOException, InterruptedException {
    boolean isRegionName = isRegionName(tableNameOrRegionName);
    CatalogTracker ct = getCatalogTracker();
    try {
      if (isRegionName) {
        Pair<HRegionInfo, HServerAddress> pair =
          MetaReader.getRegion(getCatalogTracker(), tableNameOrRegionName);
        if (pair == null || pair.getSecond() == null) {
          LOG.info("No server in .META. for " +
            Bytes.toString(tableNameOrRegionName) + "; pair=" + pair);
        } else {
          flush(pair.getSecond(), pair.getFirst());
        }
      } else {
        List<Pair<HRegionInfo, HServerAddress>> pairs =
          MetaReader.getTableRegionsAndLocations(getCatalogTracker(),
              Bytes.toString(tableNameOrRegionName));
        for (Pair<HRegionInfo, HServerAddress> pair: pairs) {
          if (pair.getFirst().isOffline()) continue;
          if (pair.getSecond() == null) continue;
          try {
            flush(pair.getSecond(), pair.getFirst());
          } catch (NotServingRegionException e) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Trying to flush " + pair.getFirst() + ": " +
                StringUtils.stringifyException(e));
            }
          }
        }
      }
    } finally {
      cleanupCatalogTracker(ct);
    }
  }

  private void flush(final HServerAddress hsa, final HRegionInfo hri)
  throws IOException {
    HRegionInterface rs = this.connection.getHRegionConnection(hsa);
    rs.flushRegion(hri);
  }

  /**
   * Compact a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to compact
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException 
   */
  public void compact(final String tableNameOrRegionName)
  throws IOException, InterruptedException {
    compact(Bytes.toBytes(tableNameOrRegionName));
  }

  /**
   * Compact a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to compact
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException 
   */
  public void compact(final byte [] tableNameOrRegionName)
  throws IOException, InterruptedException {
    compact(tableNameOrRegionName, false);
  }

  /**
   * Major compact a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to major compact
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException 
   */
  public void majorCompact(final String tableNameOrRegionName)
  throws IOException, InterruptedException {
    majorCompact(Bytes.toBytes(tableNameOrRegionName));
  }

  /**
   * Major compact a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to major compact
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException 
   */
  public void majorCompact(final byte [] tableNameOrRegionName)
  throws IOException, InterruptedException {
    compact(tableNameOrRegionName, true);
  }

  /**
   * Compact a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to compact
   * @param major True if we are to do a major compaction.
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException 
   */
  private void compact(final byte [] tableNameOrRegionName, final boolean major)
  throws IOException, InterruptedException {
    CatalogTracker ct = getCatalogTracker();
    try {
      if (isRegionName(tableNameOrRegionName)) {
        Pair<HRegionInfo, HServerAddress> pair =
          MetaReader.getRegion(ct, tableNameOrRegionName);
        if (pair == null || pair.getSecond() == null) {
          LOG.info("No server in .META. for " +
            Bytes.toString(tableNameOrRegionName) + "; pair=" + pair);
        } else {
          compact(pair.getSecond(), pair.getFirst(), major);
        }
      } else {
        List<Pair<HRegionInfo, HServerAddress>> pairs =
          MetaReader.getTableRegionsAndLocations(ct,
              Bytes.toString(tableNameOrRegionName));
        for (Pair<HRegionInfo, HServerAddress> pair: pairs) {
          if (pair.getFirst().isOffline()) continue;
          if (pair.getSecond() == null) continue;
          try {
            compact(pair.getSecond(), pair.getFirst(), major);
          } catch (NotServingRegionException e) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Trying to" + (major ? " major" : "") + " compact " +
                pair.getFirst() + ": " +
                StringUtils.stringifyException(e));
            }
          }
        }
      }
    } finally {
      cleanupCatalogTracker(ct);
    }
  }

  private void compact(final HServerAddress hsa, final HRegionInfo hri,
      final boolean major)
  throws IOException {
    HRegionInterface rs = this.connection.getHRegionConnection(hsa);
    rs.compactRegion(hri, major);
  }

  /**
   * Move the region <code>r</code> to <code>dest</code>.
   * @param encodedRegionName The encoded region name; i.e. the hash that makes
   * up the region name suffix: e.g. if regionname is
   * <code>TestTable,0094429456,1289497600452.527db22f95c8a9e0116f0cc13c680396.</code>,
   * then the encoded region name is: <code>527db22f95c8a9e0116f0cc13c680396</code>.
   * @param destServerName The servername of the destination regionserver.  If
   * passed the empty byte array we'll assign to a random server.  A server name
   * is made of host, port and startcode.  Here is an example:
   * <code> host187.example.com,60020,1289493121758</code>.
   * @throws UnknownRegionException Thrown if we can't find a region named
   * <code>encodedRegionName</code>
   * @throws ZooKeeperConnectionException 
   * @throws MasterNotRunningException 
   */
  public void move(final byte [] encodedRegionName, final byte [] destServerName)
  throws UnknownRegionException, MasterNotRunningException, ZooKeeperConnectionException {
    getMaster().move(encodedRegionName, destServerName);
  }

  /**
   * @param regionName Region name to assign.
   * @param force True to force assign.
   * @throws MasterNotRunningException
   * @throws ZooKeeperConnectionException
   * @throws IOException
   */
  public void assign(final byte [] regionName, final boolean force)
  throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
    getMaster().assign(regionName, force);
  }

  /**
   * Unassign a region from current hosting regionserver.  Region will then be
   * assigned to a regionserver chosen at random.  Region could be reassigned
   * back to the same server.  Use {@link #move(byte[], byte[])} if you want
   * to control the region movement.
   * @param regionName Region to unassign. Will clear any existing RegionPlan
   * if one found.
   * @param force If true, force unassign (Will remove region from
   * regions-in-transition too if present).
   * @throws MasterNotRunningException
   * @throws ZooKeeperConnectionException
   * @throws IOException
   */
  public void unassign(final byte [] regionName, final boolean force)
  throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
    getMaster().unassign(regionName, force);
  }

  /**
   * Turn the load balancer on or off.
   * @param b If true, enable balancer. If false, disable balancer.
   * @return Previous balancer value
   */
  public boolean balanceSwitch(final boolean b)
  throws MasterNotRunningException, ZooKeeperConnectionException {
    return getMaster().balanceSwitch(b);
  }

  /**
   * Invoke the balancer.  Will run the balancer and if regions to move, it will
   * go ahead and do the reassignments.  Can NOT run for various reasons.  Check
   * logs.
   * @return True if balancer ran, false otherwise.
   */
  public boolean balancer()
  throws MasterNotRunningException, ZooKeeperConnectionException {
    return getMaster().balance();
  }

  /**
   * Split a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to split
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException 
   */
  public void split(final String tableNameOrRegionName)
  throws IOException, InterruptedException {
    split(Bytes.toBytes(tableNameOrRegionName));
  }

  /**
   * Split a table or an individual region.  Implicitly finds an optimal split
   * point.  Asynchronous operation.
   *
   * @param tableNameOrRegionName table to region to split
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException 
   */
  public void split(final byte [] tableNameOrRegionName)
  throws IOException, InterruptedException {
    split(tableNameOrRegionName, null);
  }

  /**
   * Split a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table to region to split
   * @param splitPoint the explicit position to split on
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException interrupt exception occurred
   */
  public void split(final byte [] tableNameOrRegionName,
      final byte [] splitPoint) throws IOException, InterruptedException {
    CatalogTracker ct = getCatalogTracker();
    try {
      if (isRegionName(tableNameOrRegionName)) {
        // Its a possible region name.
        Pair<HRegionInfo, HServerAddress> pair =
          MetaReader.getRegion(getCatalogTracker(), tableNameOrRegionName);
        if (pair == null || pair.getSecond() == null) {
          LOG.info("No server in .META. for " +
            Bytes.toString(tableNameOrRegionName) + "; pair=" + pair);
        } else {
          split(pair.getSecond(), pair.getFirst(), splitPoint);
        }
      } else {
        List<Pair<HRegionInfo, HServerAddress>> pairs =
          MetaReader.getTableRegionsAndLocations(getCatalogTracker(),
              Bytes.toString(tableNameOrRegionName));
        for (Pair<HRegionInfo, HServerAddress> pair: pairs) {
          // May not be a server for a particular row
          if (pair.getSecond() == null) continue;
          HRegionInfo r = pair.getFirst();
          // check for parents
          if (r.isSplitParent()) continue;
          if (splitPoint != null) {
            // if a split point given, only split that particular region
            if (!r.containsRow(splitPoint)) continue;
          }
          split(pair.getSecond(), pair.getFirst(), splitPoint);
        }
      }
    } finally {
      cleanupCatalogTracker(ct);
    }
  }

  private void split(final HServerAddress hsa, final HRegionInfo hri,
      byte[] splitPoint) throws IOException {
    HRegionInterface rs = this.connection.getHRegionConnection(hsa);
    rs.splitRegion(hri, splitPoint);
  }

  /**
   * Modify an existing table, more IRB friendly version.
   * Asynchronous operation.  This means that it may be a while before your
   * schema change is updated across all of the table.
   *
   * @param tableName name of table.
   * @param htd modified description of the table
   * @throws IOException if a remote or network exception occurs
   */
  public void modifyTable(final byte [] tableName, HTableDescriptor htd)
  throws IOException {
    try {
      getMaster().modifyTable(tableName, htd);
    } catch (RemoteException re) {
      // Convert RE exceptions in here; client shouldn't have to deal with them,
      // at least w/ the type of exceptions that come out of this method:
      // TableNotFoundException, etc.
      throw RemoteExceptionHandler.decodeRemoteException(re);
    }
  }

  /**
   * @param tableNameOrRegionName Name of a table or name of a region.
   * @return True if <code>tableNameOrRegionName</code> is *possibly* a region
   * name else false if a verified tablename (we call {@link #tableExists(byte[])};
   * else we throw an exception.
   * @throws IOException 
   */
  private boolean isRegionName(final byte [] tableNameOrRegionName)
  throws IOException {
    if (tableNameOrRegionName == null) {
      throw new IllegalArgumentException("Pass a table name or region name");
    }
    return !tableExists(tableNameOrRegionName);
  }

  /**
   * Shuts down the HBase cluster
   * @throws IOException if a remote or network exception occurs
   */
  public synchronized void shutdown() throws IOException {
    isMasterRunning();
    try {
      getMaster().shutdown();
    } catch (RemoteException e) {
      throw RemoteExceptionHandler.decodeRemoteException(e);
    }
  }

  /**
   * Shuts down the current HBase master only.
   * Does not shutdown the cluster.
   * @see #shutdown()
   * @throws IOException if a remote or network exception occurs
   */
  public synchronized void stopMaster() throws IOException {
    isMasterRunning();
    try {
      getMaster().stopMaster();
    } catch (RemoteException e) {
      throw RemoteExceptionHandler.decodeRemoteException(e);
    }
  }

  /**
   * Stop the designated regionserver.
   * @throws IOException if a remote or network exception occurs
   */
  public synchronized void stopRegionServer(final HServerAddress hsa)
  throws IOException {
    HRegionInterface rs = this.connection.getHRegionConnection(hsa);
    rs.stop("Called by admin client " + this.connection.toString());
  }

  /**
   * @return cluster status
   * @throws IOException if a remote or network exception occurs
   */
  public ClusterStatus getClusterStatus() throws IOException {
    return getMaster().getClusterStatus();
  }

  private HRegionLocation getFirstMetaServerForTable(final byte [] tableName)
  throws IOException {
    return connection.locateRegion(HConstants.META_TABLE_NAME,
      HRegionInfo.createRegionName(tableName, null, HConstants.NINES, false));
  }

  /**
   * @return Configuration used by the instance.
   */
  public Configuration getConfiguration() {
    return this.conf;
  }

  /**
   * Check to see if HBase is running. Throw an exception if not.
   *
   * @param conf system configuration
   * @throws MasterNotRunningException if the master is not running
   * @throws ZooKeeperConnectionException if unable to connect to zookeeper
   */
  public static void checkHBaseAvailable(Configuration conf)
  throws MasterNotRunningException, ZooKeeperConnectionException {
    Configuration copyOfConf = HBaseConfiguration.create(conf);
    copyOfConf.setInt("hbase.client.retries.number", 1);
    new HBaseAdmin(copyOfConf);
  }
}
