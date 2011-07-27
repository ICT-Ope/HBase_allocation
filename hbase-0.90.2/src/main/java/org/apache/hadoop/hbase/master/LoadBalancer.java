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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;

/**
 * Makes decisions about the placement and movement of Regions across
 * RegionServers.
 *
 * <p>Cluster-wide load balancing will occur only when there are no regions in
 * transition and according to a fixed period of a time using {@link #balanceCluster(Map)}.
 *
 * <p>Inline region placement with {@link #immediateAssignment} can be used when
 * the Master needs to handle closed regions that it currently does not have
 * a destination set for.  This can happen during master failover.
 *
 * <p>On cluster startup, bulk assignment can be used to determine
 * locations for all Regions in a cluster.
 *
 * <p>This classes produces plans for the {@link AssignmentManager} to execute.
 */
public class LoadBalancer {
  private static final Log LOG = LogFactory.getLog(LoadBalancer.class);
  private static final Random RANDOM = new Random(System.currentTimeMillis());
  // slop for regions
  private float slop;

  LoadBalancer(Configuration conf) {
    this.slop = conf.getFloat("hbase.regions.slop", (float) 0.0);
    if (slop < 0) slop = 0;
    else if (slop > 1) slop = 1;
  }
  
  static class RegionPlanComparator implements Comparator<RegionPlan> {
    @Override
    public int compare(RegionPlan l, RegionPlan r) {
      long diff = r.getRegionInfo().getRegionId() - l.getRegionInfo().getRegionId();
      if (diff < 0) return -1;
      if (diff > 0) return 1;
      return 0;
    }
  }
  static RegionPlanComparator rpComparator = new RegionPlanComparator();

  /**
   * Generate a global load balancing plan according to the specified map of
   * server information to the most loaded regions of each server.
   *
   * The load balancing invariant is that all servers are within 1 region of the
   * average number of regions per server.  If the average is an integer number,
   * all servers will be balanced to the average.  Otherwise, all servers will
   * have either floor(average) or ceiling(average) regions.
   *
   * The algorithm is currently implemented as such:
   *
   * <ol>
   * <li>Determine the two valid numbers of regions each server should have,
   *     <b>MIN</b>=floor(average) and <b>MAX</b>=ceiling(average).
   *
   * <li>Iterate down the most loaded servers, shedding regions from each so
   *     each server hosts exactly <b>MAX</b> regions.  Stop once you reach a
   *     server that already has &lt;= <b>MAX</b> regions.
   *     <p>
   *     Order the regions to move from most recent to least.
   *
   * <li>Iterate down the least loaded servers, assigning regions so each server
   *     has exactly </b>MIN</b> regions.  Stop once you reach a server that
   *     already has &gt;= <b>MIN</b> regions.
   *
   *     Regions being assigned to underloaded servers are those that were shed
   *     in the previous step.  It is possible that there were not enough
   *     regions shed to fill each underloaded server to <b>MIN</b>.  If so we
   *     end up with a number of regions required to do so, <b>neededRegions</b>.
   *
   *     It is also possible that we were able fill each underloaded but ended
   *     up with regions that were unassigned from overloaded servers but that
   *     still do not have assignment.
   *
   *     If neither of these conditions hold (no regions needed to fill the
   *     underloaded servers, no regions leftover from overloaded servers),
   *     we are done and return.  Otherwise we handle these cases below.
   *
   * <li>If <b>neededRegions</b> is non-zero (still have underloaded servers),
   *     we iterate the most loaded servers again, shedding a single server from
   *     each (this brings them from having <b>MAX</b> regions to having
   *     <b>MIN</b> regions).
   *
   * <li>We now definitely have more regions that need assignment, either from
   *     the previous step or from the original shedding from overloaded servers.
   *
   *     Iterate the least loaded servers filling each to <b>MIN</b>.
   *
   * <li>If we still have more regions that need assignment, again iterate the
   *     least loaded servers, this time giving each one (filling them to
   *     </b>MAX</b>) until we run out.
   *
   * <li>All servers will now either host <b>MIN</b> or <b>MAX</b> regions.
   *
   *     In addition, any server hosting &gt;= <b>MAX</b> regions is guaranteed
   *     to end up with <b>MAX</b> regions at the end of the balancing.  This
   *     ensures the minimal number of regions possible are moved.
   * </ol>
   *
   * TODO: We can at-most reassign the number of regions away from a particular
   *       server to be how many they report as most loaded.
   *       Should we just keep all assignment in memory?  Any objections?
   *       Does this mean we need HeapSize on HMaster?  Or just careful monitor?
   *       (current thinking is we will hold all assignments in memory)
   *
   * @param clusterState Map of regionservers and their load/region information to
   *                   a list of their most loaded regions
   * @return a list of regions to be moved, including source and destination,
   *         or null if cluster is already balanced
   */
  public List<RegionPlan> balanceCluster(
      Map<HServerInfo,List<HRegionInfo>> clusterState) {
    long startTime = System.currentTimeMillis();

    // Make a map sorted by load and count regions
    TreeMap<HServerInfo,List<HRegionInfo>> serversByLoad =
      new TreeMap<HServerInfo,List<HRegionInfo>>(
          new HServerInfo.LoadComparator());
    int numServers = clusterState.size();
    if (numServers == 0) {
      LOG.debug("numServers=0 so skipping load balancing");
      return null;
    }
    int numRegions = 0;
    // Iterate so we can count regions as we build the map
    for(Map.Entry<HServerInfo, List<HRegionInfo>> server:
        clusterState.entrySet()) {
      server.getKey().getLoad().setNumberOfRegions(server.getValue().size());
      numRegions += server.getKey().getLoad().getNumberOfRegions();
      serversByLoad.put(server.getKey(), server.getValue());
    }

    // Check if we even need to do any load balancing
    float average = (float)numRegions / numServers; // for logging
    // HBASE-3681 check sloppiness first
    int floor = (int) Math.floor(average * (1 - slop));
    int ceiling = (int) Math.ceil(average * (1 + slop));
    if(serversByLoad.lastKey().getLoad().getNumberOfRegions() <= ceiling &&
       serversByLoad.firstKey().getLoad().getNumberOfRegions() >= floor) {
      // Skipped because no server outside (min,max) range
      LOG.info("Skipping load balancing.  servers=" + numServers + " " +
          "regions=" + numRegions + " average=" + average + " " +
          "mostloaded=" + serversByLoad.lastKey().getLoad().getNumberOfRegions() +
          " leastloaded=" + serversByLoad.lastKey().getLoad().getNumberOfRegions());
      return null;
    }
    int min = numRegions / numServers;
    int max = numRegions % numServers == 0 ? min : min + 1;

    // Balance the cluster
    // TODO: Look at data block locality or a more complex load to do this
    List<RegionPlan> regionsToMove = new ArrayList<RegionPlan>();
    int regionidx = 0; // track the index in above list for setting destination

    // Walk down most loaded, pruning each to the max
    int serversOverloaded = 0;
    Map<HServerInfo,BalanceInfo> serverBalanceInfo =
      new TreeMap<HServerInfo,BalanceInfo>();
    for(Map.Entry<HServerInfo, List<HRegionInfo>> server :
      serversByLoad.descendingMap().entrySet()) {
      HServerInfo serverInfo = server.getKey();
      int regionCount = serverInfo.getLoad().getNumberOfRegions();
      if(regionCount <= max) {
        serverBalanceInfo.put(serverInfo, new BalanceInfo(0, 0));
        break;
      }
      serversOverloaded++;
      List<HRegionInfo> regions = randomize(server.getValue());
      int numToOffload = Math.min(regionCount - max, regions.size());
      int numTaken = 0;
      for (int i = regions.size() - 1; i >= 0; i--) {
        HRegionInfo hri = regions.get(i);
        // Don't rebalance meta regions.
        if (hri.isMetaRegion()) continue;
        regionsToMove.add(new RegionPlan(hri, serverInfo, null));
        numTaken++;
        if (numTaken >= numToOffload) break;
      }
      serverBalanceInfo.put(serverInfo,
          new BalanceInfo(numToOffload, (-1)*numTaken));
    }

    // Walk down least loaded, filling each to the min
    int serversUnderloaded = 0; // number of servers that get new regions
    int neededRegions = 0; // number of regions needed to bring all up to min
    for(Map.Entry<HServerInfo, List<HRegionInfo>> server :
      serversByLoad.entrySet()) {
      int regionCount = server.getKey().getLoad().getNumberOfRegions();
      if(regionCount >= min) {
        break;
      }
      serversUnderloaded++;
      int numToTake = min - regionCount;
      int numTaken = 0;
      while(numTaken < numToTake && regionidx < regionsToMove.size()) {
        regionsToMove.get(regionidx).setDestination(server.getKey());
        numTaken++;
        regionidx++;
      }
      serverBalanceInfo.put(server.getKey(), new BalanceInfo(0, numTaken));
      // If we still want to take some, increment needed
      if(numTaken < numToTake) {
        neededRegions += (numToTake - numTaken);
      }
    }

    // If none needed to fill all to min and none left to drain all to max,
    // we are done
    if(neededRegions == 0 && regionidx == regionsToMove.size()) {
      long endTime = System.currentTimeMillis();
      LOG.info("Calculated a load balance in " + (endTime-startTime) + "ms. " +
          "Moving " + regionsToMove.size() + " regions off of " +
          serversOverloaded + " overloaded servers onto " +
          serversUnderloaded + " less loaded servers");
      return regionsToMove;
    }

    // Need to do a second pass.
    // Either more regions to assign out or servers that are still underloaded

    // If we need more to fill min, grab one from each most loaded until enough
    if (neededRegions != 0) {
      // Walk down most loaded, grabbing one from each until we get enough
      for(Map.Entry<HServerInfo, List<HRegionInfo>> server :
        serversByLoad.descendingMap().entrySet()) {
        BalanceInfo balanceInfo = serverBalanceInfo.get(server.getKey());
        int idx =
          balanceInfo == null ? 0 : balanceInfo.getNextRegionForUnload();
        if (idx >= server.getValue().size()) break;
        HRegionInfo region = server.getValue().get(idx);
        if (region.isMetaRegion()) continue; // Don't move meta regions.
        regionsToMove.add(new RegionPlan(region, server.getKey(), null));
        if(--neededRegions == 0) {
          // No more regions needed, done shedding
          break;
        }
      }
    }

    // Now we have a set of regions that must be all assigned out
    // Assign each underloaded up to the min, then if leftovers, assign to max

    // Walk down least loaded, assigning to each to fill up to min
    for(Map.Entry<HServerInfo, List<HRegionInfo>> server :
      serversByLoad.entrySet()) {
      int regionCount = server.getKey().getLoad().getNumberOfRegions();
      if (regionCount >= min) break;
      BalanceInfo balanceInfo = serverBalanceInfo.get(server.getKey());
      if(balanceInfo != null) {
        regionCount += balanceInfo.getNumRegionsAdded();
      }
      if(regionCount >= min) {
        continue;
      }
      int numToTake = min - regionCount;
      int numTaken = 0;
      while(numTaken < numToTake && regionidx < regionsToMove.size()) {
        regionsToMove.get(regionidx).setDestination(server.getKey());
        numTaken++;
        regionidx++;
      }
    }

    // If we still have regions to dish out, assign underloaded to max
    if(regionidx != regionsToMove.size()) {
      for(Map.Entry<HServerInfo, List<HRegionInfo>> server :
        serversByLoad.entrySet()) {
        int regionCount = server.getKey().getLoad().getNumberOfRegions();
        if(regionCount >= max) {
          break;
        }
        regionsToMove.get(regionidx).setDestination(server.getKey());
        regionidx++;
        if(regionidx == regionsToMove.size()) {
          break;
        }
      }
    }

    long endTime = System.currentTimeMillis();

    if (regionidx != regionsToMove.size() || neededRegions != 0) {
      // Emit data so can diagnose how balancer went astray.
      LOG.warn("regionidx=" + regionidx + ", regionsToMove=" + regionsToMove.size() +
      ", numServers=" + numServers + ", serversOverloaded=" + serversOverloaded +
      ", serversUnderloaded=" + serversUnderloaded);
      StringBuilder sb = new StringBuilder();
      for (Map.Entry<HServerInfo, List<HRegionInfo>> e: clusterState.entrySet()) {
        if (sb.length() > 0) sb.append(", ");
        sb.append(e.getKey().getServerName());
        sb.append(" ");
        sb.append(e.getValue().size());
      }
      LOG.warn("Input " + sb.toString());
    }

    // All done!
    LOG.info("Calculated a load balance in " + (endTime-startTime) + "ms. " +
        "Moving " + regionsToMove.size() + " regions off of " +
        serversOverloaded + " overloaded servers onto " +
        serversUnderloaded + " less loaded servers");

    return regionsToMove;
  }

  /**
   * @param regions
   * @return Randomization of passed <code>regions</code>
   */
  static List<HRegionInfo> randomize(final List<HRegionInfo> regions) {
    Collections.shuffle(regions, RANDOM);
    return regions;
  }

  /**
   * Stores additional per-server information about the regions added/removed
   * during the run of the balancing algorithm.
   *
   * For servers that receive additional regions, we are not updating the number
   * of regions in HServerInfo once we decide to reassign regions to a server,
   * but we need this information later in the algorithm.  This is stored in
   * <b>numRegionsAdded</b>.
   *
   * For servers that shed regions, we need to track which regions we have
   * already shed.  <b>nextRegionForUnload</b> contains the index in the list
   * of regions on the server that is the next to be shed.
   */
  private static class BalanceInfo {

    private final int nextRegionForUnload;
    private final int numRegionsAdded;

    public BalanceInfo(int nextRegionForUnload, int numRegionsAdded) {
      this.nextRegionForUnload = nextRegionForUnload;
      this.numRegionsAdded = numRegionsAdded;
    }

    public int getNextRegionForUnload() {
      return nextRegionForUnload;
    }

    public int getNumRegionsAdded() {
      return numRegionsAdded;
    }
  }

  /**
   * Generates a bulk assignment plan to be used on cluster startup using a
   * simple round-robin assignment.
   * <p>
   * Takes a list of all the regions and all the servers in the cluster and
   * returns a map of each server to the regions that it should be assigned.
   * <p>
   * Currently implemented as a round-robin assignment.  Same invariant as
   * load balancing, all servers holding floor(avg) or ceiling(avg).
   *
   * TODO: Use block locations from HDFS to place regions with their blocks
   *
   * @param regions all regions
   * @param servers all servers
   * @return map of server to the regions it should take, or null if no
   *         assignment is possible (ie. no regions or no servers)
   */
  public static Map<HServerInfo,List<HRegionInfo>> roundRobinAssignment(
      List<HRegionInfo> regions, List<HServerInfo> servers) {
    if(regions.size() == 0 || servers.size() == 0) {
      return null;
    }
    Map<HServerInfo,List<HRegionInfo>> assignments =
      new TreeMap<HServerInfo,List<HRegionInfo>>();
    int numRegions = regions.size();
    int numServers = servers.size();
    int max = (int)Math.ceil((float)numRegions/numServers);
    int serverIdx = 0;
    if (numServers > 1) {
      serverIdx = RANDOM.nextInt(numServers);
    }
    int regionIdx = 0;
    for (int j = 0; j < numServers; j++) {
      HServerInfo server = servers.get((j+serverIdx) % numServers);
      List<HRegionInfo> serverRegions = new ArrayList<HRegionInfo>(max);
      for (int i=regionIdx; i<numRegions; i += numServers) {
        serverRegions.add(regions.get(i % numRegions));
      }
      assignments.put(server, serverRegions);
      regionIdx++;
    }
    return assignments;
  }

  /**
   * Generates a bulk assignment startup plan, attempting to reuse the existing
   * assignment information from META, but adjusting for the specified list of
   * available/online servers available for assignment.
   * <p>
   * Takes a map of all regions to their existing assignment from META.  Also
   * takes a list of online servers for regions to be assigned to.  Attempts to
   * retain all assignment, so in some instances initial assignment will not be
   * completely balanced.
   * <p>
   * Any leftover regions without an existing server to be assigned to will be
   * assigned randomly to available servers.
   * @param regions regions and existing assignment from meta
   * @param servers available servers
   * @return map of servers and regions to be assigned to them
   */
  public static Map<HServerInfo, List<HRegionInfo>> retainAssignment(
      Map<HRegionInfo, HServerAddress> regions, List<HServerInfo> servers) {
    Map<HServerInfo, List<HRegionInfo>> assignments =
      new TreeMap<HServerInfo, List<HRegionInfo>>();
    // Build a map of server addresses to server info so we can match things up
    Map<HServerAddress, HServerInfo> serverMap =
      new TreeMap<HServerAddress, HServerInfo>();
    for (HServerInfo server : servers) {
      serverMap.put(server.getServerAddress(), server);
      assignments.put(server, new ArrayList<HRegionInfo>());
    }
    for (Map.Entry<HRegionInfo, HServerAddress> region : regions.entrySet()) {
      HServerAddress hsa = region.getValue();
      HServerInfo server = hsa == null? null: serverMap.get(hsa);
      if (server != null) {
        assignments.get(server).add(region.getKey());
      } else {
        assignments.get(servers.get(RANDOM.nextInt(assignments.size()))).add(
            region.getKey());
      }
    }
    return assignments;
  }

  /**
   * Find the block locations for all of the files for the specified region.
   *
   * Returns an ordered list of hosts that are hosting the blocks for this
   * region.  The weight of each host is the sum of the block lengths of all
   * files on that host, so the first host in the list is the server which
   * holds the most bytes of the given region's HFiles.
   *
   * TODO: Make this work.  Need to figure out how to match hadoop's hostnames
   *       given for block locations with our HServerAddress.
   * TODO: Use the right directory for the region
   * TODO: Use getFileBlockLocations on the files not the directory
   *
   * @param fs the filesystem
   * @param region region
   * @return ordered list of hosts holding blocks of the specified region
   * @throws IOException if any filesystem errors
   */
  @SuppressWarnings("unused")
  private List<String> getTopBlockLocations(FileSystem fs, HRegionInfo region)
  throws IOException {
    String encodedName = region.getEncodedName();
    Path path = new Path("/hbase/table/" + encodedName);
    FileStatus status = fs.getFileStatus(path);
    BlockLocation [] blockLocations =
      fs.getFileBlockLocations(status, 0, status.getLen());
    Map<HostAndWeight,HostAndWeight> hostWeights =
      new TreeMap<HostAndWeight,HostAndWeight>(new HostAndWeight.HostComparator());
    for(BlockLocation bl : blockLocations) {
      String [] hosts = bl.getHosts();
      long len = bl.getLength();
      for(String host : hosts) {
        HostAndWeight haw = hostWeights.get(host);
        if(haw == null) {
          haw = new HostAndWeight(host, len);
          hostWeights.put(haw, haw);
        } else {
          haw.addWeight(len);
        }
      }
    }
    NavigableSet<HostAndWeight> orderedHosts = new TreeSet<HostAndWeight>(
        new HostAndWeight.WeightComparator());
    orderedHosts.addAll(hostWeights.values());
    List<String> topHosts = new ArrayList<String>(orderedHosts.size());
    for(HostAndWeight haw : orderedHosts.descendingSet()) {
      topHosts.add(haw.getHost());
    }
    return topHosts;
  }

  /**
   * Stores the hostname and weight for that hostname.
   *
   * This is used when determining the physical locations of the blocks making
   * up a region.
   *
   * To make a prioritized list of the hosts holding the most data of a region,
   * this class is used to count the total weight for each host.  The weight is
   * currently just the size of the file.
   */
  private static class HostAndWeight {

    private final String host;
    private long weight;

    public HostAndWeight(String host, long weight) {
      this.host = host;
      this.weight = weight;
    }

    public void addWeight(long weight) {
      this.weight += weight;
    }

    public String getHost() {
      return host;
    }

    public long getWeight() {
      return weight;
    }

    private static class HostComparator implements Comparator<HostAndWeight> {
      @Override
      public int compare(HostAndWeight l, HostAndWeight r) {
        return l.getHost().compareTo(r.getHost());
      }
    }

    private static class WeightComparator implements Comparator<HostAndWeight> {
      @Override
      public int compare(HostAndWeight l, HostAndWeight r) {
        if(l.getWeight() == r.getWeight()) {
          return l.getHost().compareTo(r.getHost());
        }
        return l.getWeight() < r.getWeight() ? -1 : 1;
      }
    }
  }

  /**
   * Generates an immediate assignment plan to be used by a new master for
   * regions in transition that do not have an already known destination.
   *
   * Takes a list of regions that need immediate assignment and a list of
   * all available servers.  Returns a map of regions to the server they
   * should be assigned to.
   *
   * This method will return quickly and does not do any intelligent
   * balancing.  The goal is to make a fast decision not the best decision
   * possible.
   *
   * Currently this is random.
   *
   * @param regions
   * @param servers
   * @return map of regions to the server it should be assigned to
   */
  public static Map<HRegionInfo,HServerInfo> immediateAssignment(
      List<HRegionInfo> regions, List<HServerInfo> servers) {
    Map<HRegionInfo,HServerInfo> assignments =
      new TreeMap<HRegionInfo,HServerInfo>();
    for(HRegionInfo region : regions) {
      assignments.put(region, servers.get(RANDOM.nextInt(servers.size())));
    }
    return assignments;
  }

  public static HServerInfo randomAssignment(List<HServerInfo> servers) {
    if (servers == null || servers.isEmpty()) {
      LOG.warn("Wanted to do random assignment but no servers to assign to");
      return null;
    }
    return servers.get(RANDOM.nextInt(servers.size()));
  }

  /**
   * Stores the plan for the move of an individual region.
   *
   * Contains info for the region being moved, info for the server the region
   * should be moved from, and info for the server the region should be moved
   * to.
   *
   * The comparable implementation of this class compares only the region
   * information and not the source/dest server info.
   */
  public static class RegionPlan implements Comparable<RegionPlan> {
    private final HRegionInfo hri;
    private final HServerInfo source;
    private HServerInfo dest;

    /**
     * Instantiate a plan for a region move, moving the specified region from
     * the specified source server to the specified destination server.
     *
     * Destination server can be instantiated as null and later set
     * with {@link #setDestination(HServerInfo)}.
     *
     * @param hri region to be moved
     * @param source regionserver region should be moved from
     * @param dest regionserver region should be moved to
     */
    public RegionPlan(final HRegionInfo hri, HServerInfo source, HServerInfo dest) {
      this.hri = hri;
      this.source = source;
      this.dest = dest;
    }

    /**
     * Set the destination server for the plan for this region.
     */
    public void setDestination(HServerInfo dest) {
      this.dest = dest;
    }

    /**
     * Get the source server for the plan for this region.
     * @return server info for source
     */
    public HServerInfo getSource() {
      return source;
    }

    /**
     * Get the destination server for the plan for this region.
     * @return server info for destination
     */
    public HServerInfo getDestination() {
      return dest;
    }

    /**
     * Get the encoded region name for the region this plan is for.
     * @return Encoded region name
     */
    public String getRegionName() {
      return this.hri.getEncodedName();
    }

    public HRegionInfo getRegionInfo() {
      return this.hri;
    }

    /**
     * Compare the region info.
     * @param o region plan you are comparing against
     */
    @Override
    public int compareTo(RegionPlan o) {
      return getRegionName().compareTo(o.getRegionName());
    }

    @Override
    public String toString() {
      return "hri=" + this.hri.getRegionNameAsString() + ", src=" +
        (this.source == null? "": this.source.getServerName()) +
        ", dest=" + (this.dest == null? "": this.dest.getServerName());
    }
  }
}
