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
package org.apache.hadoop.hbase.allocation;

import java.io.IOException;
import java.util.HashMap;
import java.util.TreeSet;
import java.util.WeakHashMap;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;

/**
 * Get region info in meta table
 * 
 * 
 */
public class CheckMeta {
  private static final Log LOG = LogFactory.getLog(CheckMeta.class);
  static HTable t = null;

  /**
   * get table descriptors.
   * 
   * @return table descriptors.
   */
  public static HTableDescriptor[] getTables() {
    HTableDescriptor[] ret = null;
    try {
      Scan s = new Scan();
      if (t == null)
        t = new HTable(".META.");
      t.setScannerCaching(1000);
      s.setCaching(1000);
      ResultScanner sn = t.getScanner(s);
      Result r = null;
      final TreeSet<HTableDescriptor> uniqueTables = new TreeSet<HTableDescriptor>();
      while ((r = sn.next()) != null) {
        byte[] value = r.getValue(HConstants.CATALOG_FAMILY,
            HConstants.REGIONINFO_QUALIFIER);
        HRegionInfo info = null;
        if (value != null) {
          info = Writables.getHRegionInfo(value);
        }
        // Only examine the rows where the startKey is zero length
        if (info != null && info.getStartKey().length == 0) {
          uniqueTables.add(info.getTableDesc());
        }
      }
      try {
        sn.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
      ret = uniqueTables.toArray(new HTableDescriptor[uniqueTables.size()]);
      return ret;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * test if the server and address are equal.
   * 
   * @param server
   * @param address
   * @return
   */
  public static boolean isThisAddress(HServerInfo server, HServerAddress address) {
    return server.getServerAddress().getHostname()
        .equals(address.getHostname())
        && (server.getServerAddress().getPort() == address.getPort());
  }

  /**
   * Get region and server infos according to the server informations use sent
   * in
   * 
   * @param infos
   *          method will return the regions which assigned to these servers.
   * @param filterOffline
   *          need to check if region online.
   * @return the region info and the server info which region assigned to
   */
  public static HashMap<HRegionInfo, HServerInfo> getServerRegions(
      HServerInfo[] infos, boolean filterOffline) {
    try {
      HashMap<HRegionInfo, HServerInfo> ret = new HashMap<HRegionInfo, HServerInfo>();
      HashMap<HRegionInfo, HServerAddress> regionInfo = getAllRegionInfo();
      for (HRegionInfo rinfo : regionInfo.keySet()) {

        for (int i = 0; i < infos.length; i++) {
          if (isThisAddress(infos[i], regionInfo.get(rinfo))) {
            if (filterOffline) {
              if (rinfo != null && !rinfo.isOffline())
                ret.put(rinfo, infos[i]);
            } else {
              ret.put(rinfo, infos[i]);
            }

          }
        }
      }
      return ret;
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    return null;

  }

  // /**
  // *
  // * @param filter
  // * @return
  // * @throws IOException
  // */
  // public static HashMap<HRegionInfo,HServerAddress> getAllRegionInfo(boolean
  // filter) throws IOException
  // {
  // Scan s=new Scan();
  // if(t==null)
  // t=new HTable(".META.");
  // ResultScanner sn=t.getScanner(s);
  // Result r=null;
  // HashMap<HRegionInfo, HServerAddress> m=new HashMap<HRegionInfo,
  // HServerAddress>();
  // while((r=sn.next()) != null)
  // {
  //
  // HRegionInfo info=null;
  // try {
  //
  // info = Writables.getHRegionInfo(
  // r.getValue(HConstants.CATALOG_FAMILY,
  // HConstants.REGIONINFO_QUALIFIER));
  // //System.out.println(info);
  // } catch (Exception e) {
  // // TODO Auto-generated catch block
  // //System.out.println(e.getMessage());
  // //System.out.println(Bytes.toString(r.getRow()));
  // //t.delete(new Delete(r.getRow()));
  // //e.printStackTrace();
  // }
  // if(info==null)
  // continue;
  // HServerAddress server = new HServerAddress();
  // byte [] value = r.getValue(HConstants.CATALOG_FAMILY,
  // HConstants.SERVER_QUALIFIER);
  // if (value != null && value.length > 0) {
  // String address = Bytes.toString(value);
  // server = new HServerAddress(address);
  // }
  //
  // if (!(info.isOffline() || !info.isSplit())) {
  // m.put(info, server);
  // }
  // }
  // try {
  // sn.close();
  // } catch (Exception e) {
  // e.printStackTrace();
  // }
  // return m;
  // }

  /**
   * Get all region and server informations from meta table
   * 
   * @return the region infos
   */
  public static HashMap<HRegionInfo, HServerAddress> getAllRegionInfo()
      throws IOException, InterruptedException {
    Scan s = new Scan();
    if (t == null)
      t = new HTable(".META.");
    ResultScanner sn = t.getScanner(s);
    Result r = null;
    HashMap<HRegionInfo, HServerAddress> m = new HashMap<HRegionInfo, HServerAddress>();
    while ((r = sn.next()) != null) {

      HRegionInfo info = null;
      try {

        info = Writables.getHRegionInfo(r.getValue(HConstants.CATALOG_FAMILY,
            HConstants.REGIONINFO_QUALIFIER));
      } catch (Exception e) {
        LOG.error("There are error rows in meta table:" + r);
        // TODO: we should fix meta table here.
      }
      if (info == null)
        continue;
      HServerAddress server = new HServerAddress();
      byte[] value = r.getValue(HConstants.CATALOG_FAMILY,
          HConstants.SERVER_QUALIFIER);
      if (value != null && value.length > 0) {
        String address = Bytes.toString(value);
        server = new HServerAddress(address);
      }

      if ((!(info.isOffline()) && (!info.isSplit()))) {
        m.put(info, server);
      }
    }
    try {
      sn.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return m;
  }

  /**
   * get HRegionInfo and HServerAddress pairs according to the table name
   * 
   * @param table
   *          table the returned Region belong to.
   * @return HRegionInfo and HServerAddress pairs
   */
  public static HashMap<HRegionInfo, HServerAddress> getRegionAddress(
      String table) {
    try {
      return getRegionAddress(table, getAllRegionInfo());
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return null;

  }

  /**
   * main test
   * 
   * @param args
   */
  public static void main(String args[]) {
    System.out.println(getRegionAddress("testPri10"));
  }

  /**
   * filter region and address pairs according to the table name you send in.
   * 
   * @param table
   *          the returned regions belong to this table
   * @param maps
   *          the region and address pairs you want to filter
   * @return
   */
  public static HashMap<HRegionInfo, HServerAddress> getRegionAddress(
      String table, HashMap<HRegionInfo, HServerAddress> maps) {
    HashMap<HRegionInfo, HServerAddress> m = new HashMap<HRegionInfo, HServerAddress>();
    for (Entry<HRegionInfo, HServerAddress> e : maps.entrySet()) {
      if (e.getKey().getTableDesc().getNameAsString().equals(table))
        m.put(e.getKey(), e.getValue());
    }
    return m;
  }

}