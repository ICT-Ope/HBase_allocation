package org.apache.hadoop.hbase.ipc;

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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.allocation.CheckMeta;
import org.apache.hadoop.hbase.allocation.ScheduleQueue;
import org.apache.hadoop.hbase.client.Action;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.MultiAction;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.io.WritableWithSize;
import org.apache.hadoop.hbase.ipc.ByteBufferOutputStream;
import org.apache.hadoop.hbase.ipc.HBaseRPCErrorHandler;
import org.apache.hadoop.hbase.ipc.HBaseRpcMetrics;
import org.apache.hadoop.hbase.ipc.HBaseServer;
import org.apache.hadoop.hbase.ipc.ServerNotRunningException;
import org.apache.hadoop.hbase.ipc.HBaseRPC.Invocation;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * An abstract IPC service. IPC calls take a single {@link Writable} as a
 * parameter, and return a {@link Writable} as their value. A service runs on a
 * port and is defined by a parameter class and a value class.
 * 
 * Extends HBaseServer,and add schedule function to make table priority take
 * effect.
 */
public class ScheduleHBaseServer extends HBaseServer {

  public final static String pri_string = "priority";

  final static int PING_CALL_ID = -1;

  /**
   * priority refresh interval
   */
  private final static int initInter = 120000;

  /**
   * the priority map,cache the region table and scanner's priority in memory.
   */
  private static Map<String, InternalScanner> scannersMap = new ConcurrentHashMap<String, InternalScanner>();
  private final static ConcurrentHashMap<String, Integer> regionPriMap = new ConcurrentHashMap<String, Integer>();
  private final static ConcurrentHashMap<String, Integer> tablePriMap = new ConcurrentHashMap<String, Integer>();
  private final static ConcurrentHashMap<Long, String> scannerPriMap = new ConcurrentHashMap<Long, String>();
  private final static ConcurrentHashMap<Long, Integer> scannerPriMapInteger = new ConcurrentHashMap<Long, Integer>();

  // 1 : Introduce ping and server does not throw away RPCs
  // 3 : RPC was refactored in 0.19

  /**
   * How many calls/handler are allowed in the queue.
   */
  private static final int MAX_QUEUE_SIZE_PER_HANDLER = 100;

  private static final String WARN_RESPONSE_SIZE = "hbase.ipc.warn.response.size";

  /** Default value for above param */
  private static final int DEFAULT_WARN_RESPONSE_SIZE = 100 * 1024 * 1024;

  private final int warnResponseSize;

  public static final Log LOG = LogFactory
      .getLog("org.apache.hadoop.ipc.ScheduleHBaseServer");

  protected static final ThreadLocal<ScheduleHBaseServer> SERVER = new ThreadLocal<ScheduleHBaseServer>();
  private volatile boolean started = false;

  /**
   * Returns the server instance called under or null. May be called under
   * {@link #call(Writable, long)} implementations, and under {@link Writable}
   * methods of paramters and return values. Permits applications to access the
   * server context. copied from HBaseServer
   * 
   * @return HBaseServer
   */
  public static ScheduleHBaseServer get() {
    return SERVER.get();
  }

  /**
   * This is set to Call object before Handler invokes an RPC and reset after
   * the call returns.
   */
  protected static final ThreadLocal<Call> CurCall = new ThreadLocal<Call>();

  /**
   * Returns the remote side ip address when invoked inside an RPC Returns null
   * incase of an error.
   * 
   * @return InetAddress
   */
  public static InetAddress getRemoteIp() {
    Call call = CurCall.get();
    if (call != null) {
      return call.connection.socket.getInetAddress();
    }
    return null;
  }

  /**
   * Returns remote address as a string when invoked inside an RPC. Returns null
   * in case of an error. copied from HBaseServer
   * 
   * @return String
   */
  public static String getRemoteAddress() {
    InetAddress addr = getRemoteIp();
    return (addr == null) ? null : addr.getHostAddress();
  }

  protected String bindAddress;
  protected int port; // port we listen on
  private int handlerCount; // number of handler threads
  private int priorityHandlerCount;
  private int readThreads; // number of read threads
  protected Class<? extends Writable> paramClass; // class of call parameters
  protected int maxIdleTime; // the maximum idle time after
  // which a client may be
  // disconnected
  protected int thresholdIdleConnections; // the number of idle
  // connections after which we
  // will start cleaning up idle
  // connections
  int maxConnectionsToNuke; // the max number of
  // connections to nuke
  // during a cleanup

  protected HBaseRpcMetrics rpcMetrics;

  protected Configuration conf;

  private int maxQueueSize;
  protected int socketSendBufferSize;
  protected final boolean tcpNoDelay; // if T then disable Nagle's Algorithm
  protected final boolean tcpKeepAlive; // if T then use keepalives

  volatile protected boolean running = true; // true while server runs
  // protected BlockingQueue<Call> callQueue; // queued calls
  protected BlockingQueue<Call> priorityCallQueue;

  private int highPriorityLevel; // what level a high priority call is at

  protected final List<Connection> connectionList = Collections
      .synchronizedList(new LinkedList<Connection>());
  // maintain a list
  // of client connections
  private Listener listener = null;
  protected Responder responder = null;
  protected int numConnections = 0;
  private Handler[] handlers = null;
  private Handler[] priorityHandlers = null;
  protected HBaseRPCErrorHandler errorHandler = null;
  private static int queueCapacity = 300;
  private static final ScheduleQueue<Call> queue = new ScheduleQueue<Call>(
      queueCapacity, 10);

  private Object instance;
  private HRegionServer regionserver;
  private Class<?> implementation;
  private Class<?> ifaces[];
  private boolean verbose;

  private static String classNameBase(String className) {
    String[] names = className.split("\\.", -1);
    if (names == null || names.length == 0) {
      return className;
    }
    return names[names.length - 1];
  }

  /**
   * Construct an RPC server.
   * 
   * @param instance
   *          the instance whose methods will be called
   * @param conf
   *          the configuration to use
   * @param bindAddress
   *          the address to bind on to listen for connection
   * @param port
   *          the port to listen for connections on
   * @param numHandlers
   *          the number of method handler threads to run
   * @param verbose
   *          whether each call should be logged
   * @throws IOException
   *           e
   */
  @SuppressWarnings({ "unchecked" })
  public ScheduleHBaseServer(Object instance, final Class<?>[] ifaces,
      Configuration conf, String bindAddress, int port, int numHandlers,
      int metaHandlerCount, boolean verbose, int highPriorityLevel)
      throws IOException {
   
    this(bindAddress, port, Invocation.class, numHandlers, metaHandlerCount,
        conf, classNameBase(instance.getClass().getName()), highPriorityLevel);
    this.instance = instance;
    this.regionserver = (HRegionServer) instance;
    this.implementation = instance.getClass();
    this.verbose = verbose;
    this.ifaces = ifaces;
    Field f;
    try {
      f = HRegionServer.class.getDeclaredField("scanners");
      f.setAccessible(true);
      Map<String, InternalScanner> scanMap = (Map<String, InternalScanner>) f
          .get(instance);
      scannersMap = scanMap;
    } catch (Exception e) {

    }
  }

  /**
   * Initiate the region priority
   * 
   * @param regions
   *          the region want to get priority
   * @param force
   *          force refresh priority,if true will get priority from table
   *          descriptor.
   * @return the region priority
   */

  @SuppressWarnings("unused")
  private int initRegionPri(byte[] regions, boolean force) {
    String region = Bytes.toString(regions);
    return this.initRegionPri(region, force);

  }

  /**
   * Initiate the region priority
   * 
   * @param regions
   *          the region want to get priority
   * @param force
   *          force refresh priority,if true will get priority from table
   *          descriptor.
   * @return the region priority
   */
  private int initRegionPri(String region, boolean force) {
    if (!force) {
      Integer ret = regionPriMap.get(region);
      if (ret != null)
        return ret;
    }
    Integer prii;
    int pri = defaultPri;
    HRegion hr = ((HRegionServer) this.instance).getOnlineRegion(Bytes
        .toBytes(region));

    if (hr != null) {
      if (hr.getRegionInfo().isMetaRegion()
          || hr.getRegionInfo().isRootRegion()) {
        pri = highestPri;
        System.out.println("int init region" + region + ",pri:" + pri);
        regionPriMap.put(region, pri);
        return pri;
      }
      String tableName = hr.getTableDesc().getNameAsString();

      prii = tablePriMap.get(tableName);
      if (prii == null) {
        if (hr.getTableDesc().getValue(Bytes.toBytes(pri_string)) != null) {
          try {
            pri = Integer.parseInt(Bytes.toString(hr.getTableDesc().getValue(
                Bytes.toBytes(pri_string))));
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
        tablePriMap.put(tableName, pri);
      } else {
        pri = prii;
      }

    }
    regionPriMap.put(region, pri);
    return pri;
  }

  /**
   * Initiate the scanner's priority,invoked by openscanner
   * 
   * @param call
   * @param value
   *          scanner id
   */
  public void initScannerPri(Invocation call, Object value) {
    Long id = (Long) value;
    byte[] region = (byte[]) call.getParameters()[0];
    String regionN = Bytes.toString(region);
    Integer prii = regionPriMap.get(regionN);
    if (prii == null) {
      this.initRegionPri(regionN, false);
    }
    scannerPriMap.put(id, regionN);
  }

  @Override
  public Writable call(Writable param, long receivedTime) throws IOException {
    try {
      Invocation call = (Invocation) param;
      if (call.getMethodName() == null) {
        throw new IOException("Could not find requested method, the usual "
            + "cause is a version mismatch between client and server.");
      }

      Method method = implementation.getMethod(call.getMethodName(),
          call.getParameterClasses());

      long startTime = System.currentTimeMillis();

      Object value = method.invoke(instance, call.getParameters());
      /**
       * do with openScanner option, added by ScheduleHBaseServer
       */
      if (call.getMethodName().endsWith("openScanner")) {
        this.initScannerPri(call, value);
      }

      int processingTime = (int) (System.currentTimeMillis() - startTime);
      int qTime = (int) (startTime - receivedTime);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Served: " + call.getMethodName() + " queueTime= " + qTime
            + " procesingTime= " + processingTime);
      }
      rpcMetrics.rpcQueueTime.inc(qTime);
      rpcMetrics.rpcProcessingTime.inc(processingTime);
      rpcMetrics.inc(call.getMethodName(), processingTime);

      return new HbaseObjectWritable(method.getReturnType(), value);

    } catch (InvocationTargetException e) {
      Throwable target = e.getTargetException();
      if (target instanceof IOException) {
        throw (IOException) target;
      }
      IOException ioe = new IOException(target.toString());
      ioe.setStackTrace(target.getStackTrace());
      throw ioe;
    } catch (Throwable e) {
      IOException ioe = new IOException(e.toString());
      ioe.setStackTrace(e.getStackTrace());
      throw ioe;
    }
  }

  public static final int lowestPri = 10;
  public static final int defaultPri = 5;
  public static final int highestPri = -10;
  public static final int highPri = 0;

  public static int handleFreshInter = 6;
  public static int move = Integer.SIZE - handleFreshInter;
  private Thread priorityIniter = new Thread() {
    public void run() {
      while (running) {
        initPriority();
        try {

          sleep(initInter);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

    }
  };

  @Override
  public void start() {
    this.started = true;
    startThreads();
    openServer();
    this.priorityIniter.start();
  }

  /**
   * Initiate the table priorities.
   */
  private void initPriority() {
    try {
      handleFreshInter = conf.getInt("hbase.schedule.refreshinter", 7);
      move = Integer.SIZE - handleFreshInter;
      HTableDescriptor[] tableDs = CheckMeta.getTables();
      int pri = defaultPri;
      for (HTableDescriptor des : tableDs) {
        byte[] prib = des.getValue(Bytes.toBytes(pri_string));
        if (prib != null) {
          try {
            pri = Integer.parseInt(Bytes.toString((prib)));
          } catch (Exception e) {
            LOG.error("table priority error :" + Bytes.toString(prib)
                + " table name:" + des.getNameAsString());
          }
        }
        tablePriMap.put(des.getNameAsString(), pri);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    for (Long id : scannerPriMap.keySet()) {
      if (scannersMap.get(String.valueOf(id)) == null) {
        scannerPriMap.remove(id);
      }
    }
    for (Long id : scannerPriMapInteger.keySet()) {
      if (scannersMap.get(String.valueOf(id)) == null) {
        scannerPriMapInteger.remove(id);
      }
    }

    for (String regionName : regionPriMap.keySet()) {
      this.initRegionPri(regionName, true);
    }

    // List<HRegionInfo> infos = ((HRegionServer) this.instance)
    // .getOnlineRegions();
    // for (HRegionInfo info : infos) {
    // //System.out.println(Bytes.toString(info.getEncodedName() ));
    // Integer pri = this.tablePriMap.get(info.getTableDesc().getName());
    // if (pri != null) {
    // this.regionPriMap.put(info.getEncodedNameAsBytes(), pri);
    // } else if (info.isMetaRegion()) {
    // this.regionPriMap.put(info.getEncodedNameAsBytes(),
    // this.highestPri);
    // } else if (info.isRootRegion()) {
    // this.regionPriMap.put(info.getEncodedNameAsBytes(),
    // this.highestPri);
    // } else {
    // this.regionPriMap.put(info.getEncodedNameAsBytes(),
    // this.defaultPri);
    // }
    // }
  }

  /**
   * A convenience method to bind to a given address and report better
   * exceptions if the address is not a valid host.
   * 
   * @param socket
   *          the socket to bind
   * @param address
   *          the address to bind to
   * @param backlog
   *          the number of connections allowed in the queue
   * @throws BindException
   *           if the address can't be bound
   * @throws UnknownHostException
   *           if the address isn't a valid host name
   * @throws IOException
   *           other random errors from bind
   */
  public static void bind(ServerSocket socket, InetSocketAddress address,
      int backlog) throws IOException {
    try {
      socket.bind(address, backlog);
    } catch (BindException e) {
      BindException bindException = new BindException("Problem binding to "
          + address + " : " + e.getMessage());
      bindException.initCause(e);
      throw bindException;
    } catch (SocketException e) {
      // If they try to bind to a different host's address, give a better
      // error message.
      if ("Unresolved address".equals(e.getMessage())) {
        throw new UnknownHostException("Invalid hostname for server: "
            + address.getHostName());
      }
      throw e;
    }
  }

  /** A call queued for handling.copied from HBaseServer */
  static class Call {
    protected int id; // the client's call id
    protected Writable param; // the parameter passed
    protected Connection connection; // connection to client
    protected long timestamp; // the time received when response is null
    // the time served when response is not null
    protected ByteBuffer response; // the response for this call

    public Call(int id, Writable param, Connection connection) {
      this.id = id;
      this.param = param;
      this.connection = connection;
      this.timestamp = System.currentTimeMillis();
      this.response = null;
    }

    public String toString() {
      return param.toString() + " from " + connection.toString();
    }

    public void setResponse(ByteBuffer response) {
      this.response = response;
    }
  }

  /**
   * Listens on the socket. Creates jobs for the handler threads copied from
   * HBaseServer
   */
  private class Listener extends Thread {

    private ServerSocketChannel acceptChannel = null; // the accept channel
    private Selector selector = null; // the selector that we use for the
    // server
    private Reader[] readers = null;
    private int currentReader = 0;
    private InetSocketAddress address; // the address we bind at
    private Random rand = new Random();
    private long lastCleanupRunTime = 0; // the last time when a cleanup
    // connec-
    // -tion (for idle connections) ran
    private long cleanupInterval = 10000; // the minimum interval between
    // two cleanup runs
    private int backlogLength = conf
        .getInt("ipc.server.listen.queue.size", 128);

    private ExecutorService readPool;

    public Listener() throws IOException {
      address = new InetSocketAddress(bindAddress, port);
      // Create a new server socket and set to non blocking mode
      acceptChannel = ServerSocketChannel.open();
      acceptChannel.configureBlocking(false);

      // Bind the server socket to the local host and port
      bind(acceptChannel.socket(), address, backlogLength);
      port = acceptChannel.socket().getLocalPort(); // Could be an
      // ephemeral port
      // create a selector;
      selector = Selector.open();

      readers = new Reader[readThreads];
      readPool = Executors.newFixedThreadPool(
          readThreads,
          new ThreadFactoryBuilder().setNameFormat(
              "IPC Reader %d on port " + port).build());
      for (int i = 0; i < readThreads; ++i) {
        Selector readSelector = Selector.open();
        Reader reader = new Reader(readSelector);
        readers[i] = reader;
        readPool.execute(reader);
      }

      // Register accepts on the server socket with the selector.
      acceptChannel.register(selector, SelectionKey.OP_ACCEPT);
      this.setName("IPC Server listener on " + port);
      this.setDaemon(true);
    }

    private class Reader implements Runnable {
      private volatile boolean adding = false;
      private Selector readSelector = null;

      Reader(Selector readSelector) {
        this.readSelector = readSelector;
      }

      public void run() {
        synchronized (this) {
          while (running) {
            SelectionKey key = null;
            try {
              readSelector.select();
              while (adding) {
                this.wait(1000);
              }

              Iterator<SelectionKey> iter = readSelector.selectedKeys()
                  .iterator();
              while (iter.hasNext()) {
                key = iter.next();
                iter.remove();
                if (key.isValid()) {
                  if (key.isReadable()) {
                    doRead(key);
                  }
                }
                key = null;
              }
            } catch (InterruptedException e) {
              if (running) { // unexpected -- log it
                LOG.info(getName() + "caught: "
                    + StringUtils.stringifyException(e));
              }
            } catch (IOException ex) {
              LOG.error("Error in Reader", ex);
            }
          }
        }
      }

      /**
       * This gets reader into the state that waits for the new channel to be
       * registered with readSelector. If it was waiting in select() the thread
       * will be woken up, otherwise whenever select() is called it will return
       * even if there is nothing to read and wait in while(adding) for
       * finishAdd call
       */
      public void startAdd() {
        adding = true;
        readSelector.wakeup();
      }

      public synchronized SelectionKey registerChannel(SocketChannel channel)
          throws IOException {
        return channel.register(readSelector, SelectionKey.OP_READ);
      }

      public synchronized void finishAdd() {
        adding = false;
        this.notify();
      }
    }

    /**
     * cleanup connections from connectionList. Choose a random range to scan
     * and also have a limit on the number of the connections that will be
     * cleanedup per run. The criteria for cleanup is the time for which the
     * connection was idle. If 'force' is true then all connections will be
     * looked at for the cleanup.
     * 
     * @param force
     *          all connections will be looked at for cleanup
     */
    private void cleanupConnections(boolean force) {
      if (force || numConnections > thresholdIdleConnections) {
        long currentTime = System.currentTimeMillis();
        if (!force && (currentTime - lastCleanupRunTime) < cleanupInterval) {
          return;
        }
        int start = 0;
        int end = numConnections - 1;
        if (!force) {
          start = rand.nextInt() % numConnections;
          end = rand.nextInt() % numConnections;
          int temp;
          if (end < start) {
            temp = start;
            start = end;
            end = temp;
          }
        }
        int i = start;
        int numNuked = 0;
        while (i <= end) {
          Connection c;
          synchronized (connectionList) {
            try {
              c = connectionList.get(i);
            } catch (Exception e) {
              return;
            }
          }
          if (c.timedOut(currentTime)) {
            if (LOG.isDebugEnabled())
              LOG.debug(getName() + ": disconnecting client "
                  + c.getHostAddress());
            closeConnection(c);
            numNuked++;
            end--;
            // noinspection UnusedAssignment
            c = null;
            if (!force && numNuked == maxConnectionsToNuke)
              break;
          } else
            i++;
        }
        lastCleanupRunTime = System.currentTimeMillis();
      }
    }

    public void run() {
      LOG.info(getName() + ": starting");
      SERVER.set(ScheduleHBaseServer.this);

      while (running) {
        SelectionKey key = null;
        try {
          selector.select(); // FindBugs IS2_INCONSISTENT_SYNC
          Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
          while (iter.hasNext()) {
            key = iter.next();
            iter.remove();
            try {
              if (key.isValid()) {
                if (key.isAcceptable())
                  doAccept(key);
              }
            } catch (IOException ignored) {
            }
            key = null;
          }
        } catch (OutOfMemoryError e) {
          if (errorHandler != null) {
            if (errorHandler.checkOOME(e)) {
              LOG.info(getName() + ": exiting on OOME");
              closeCurrentConnection(key);
              cleanupConnections(true);
              return;
            }
          } else {
            // we can run out of memory if we have too many threads
            // log the event and sleep for a minute and give
            // some thread(s) a chance to finish
            LOG.warn("Out of Memory in server select", e);
            closeCurrentConnection(key);
            cleanupConnections(true);
            try {
              Thread.sleep(60000);
            } catch (Exception ignored) {
            }
          }
        } catch (Exception e) {
          closeCurrentConnection(key);
        }
        cleanupConnections(false);
      }
      LOG.info("Stopping " + this.getName());

      synchronized (this) {
        try {
          acceptChannel.close();
          selector.close();
        } catch (IOException ignored) {
        }

        selector = null;
        acceptChannel = null;

        // clean up all connections
        while (!connectionList.isEmpty()) {
          closeConnection(connectionList.remove(0));
        }
      }
    }

    private void closeCurrentConnection(SelectionKey key) {
      if (key != null) {
        Connection c = (Connection) key.attachment();
        if (c != null) {
          if (LOG.isDebugEnabled())
            LOG.debug(getName() + ": disconnecting client "
                + c.getHostAddress());
          closeConnection(c);
        }
      }
    }

    InetSocketAddress getAddress() {
      return (InetSocketAddress) acceptChannel.socket().getLocalSocketAddress();
    }

    void doAccept(SelectionKey key) throws IOException, OutOfMemoryError {
      Connection c;
      ServerSocketChannel server = (ServerSocketChannel) key.channel();

      SocketChannel channel;
      while ((channel = server.accept()) != null) {
        channel.configureBlocking(false);
        channel.socket().setTcpNoDelay(tcpNoDelay);
        channel.socket().setKeepAlive(tcpKeepAlive);

        Reader reader = getReader();
        try {
          reader.startAdd();
          SelectionKey readKey = reader.registerChannel(channel);
          c = new Connection(channel, System.currentTimeMillis());
          readKey.attach(c);
          synchronized (connectionList) {
            connectionList.add(numConnections, c);
            numConnections++;
          }
          // if (LOG.isDebugEnabled())
          // LOG.debug("Server connection from " + c.toString()
          // + "; # active connections: " + numConnections
          // + "; # queued calls: " + callQueue.size());
        } finally {
          reader.finishAdd();
        }
      }
    }

    void doRead(SelectionKey key) throws InterruptedException {
      int count = 0;
      Connection c = (Connection) key.attachment();
      if (c == null) {
        return;
      }
      c.setLastContact(System.currentTimeMillis());

      try {
        count = c.readAndProcess();
      } catch (InterruptedException ieo) {
        throw ieo;
      } catch (Exception e) {
        LOG.warn(getName() + ": readAndProcess threw exception " + e
            + ". Count of bytes read: " + count, e);
        count = -1; // so that the (count < 0) block is executed
      }
      if (count < 0) {
        if (LOG.isDebugEnabled())
          LOG.debug(getName() + ": disconnecting client " + c.getHostAddress()
              + ". Number of active connections: " + numConnections);
        closeConnection(c);
        // c = null;
      } else {
        c.setLastContact(System.currentTimeMillis());
      }
    }

    synchronized void doStop() {
      if (selector != null) {
        selector.wakeup();
        Thread.yield();
      }
      if (acceptChannel != null) {
        try {
          acceptChannel.socket().close();
        } catch (IOException e) {
          LOG.info(getName() + ":Exception in closing listener socket. " + e);
        }
      }
      readPool.shutdownNow();
    }

    // The method that will return the next reader to work with
    // Simplistic implementation of round robin for now
    Reader getReader() {
      currentReader = (currentReader + 1) % readers.length;
      return readers[currentReader];
    }
  }

  // Sends responses of RPC back to clients. copied from HBaseServer
  private class Responder extends Thread {
    private Selector writeSelector;
    private int pending; // connections waiting to register

    final static int PURGE_INTERVAL = 900000; // 15mins

    Responder() throws IOException {
      this.setName("IPC Server Responder");
      this.setDaemon(true);
      writeSelector = Selector.open(); // create a selector
      pending = 0;
    }

    public void run() {
      LOG.info(getName() + ": starting");
      SERVER.set(ScheduleHBaseServer.this);
      long lastPurgeTime = 0; // last check for old calls.

      while (running) {
        try {
          waitPending(); // If a channel is being registered, wait.
          writeSelector.select(PURGE_INTERVAL);
          Iterator<SelectionKey> iter = writeSelector.selectedKeys().iterator();
          while (iter.hasNext()) {
            SelectionKey key = iter.next();
            iter.remove();
            try {
              if (key.isValid() && key.isWritable()) {
                doAsyncWrite(key);
              }
            } catch (IOException e) {
              LOG.info(getName() + ": doAsyncWrite threw exception " + e);
            }
          }
          long now = System.currentTimeMillis();
          if (now < lastPurgeTime + PURGE_INTERVAL) {
            continue;
          }
          lastPurgeTime = now;
          //
          // If there were some calls that have not been sent out for
          // a
          // long time, discard them.
          //
          LOG.debug("Checking for old call responses.");
          ArrayList<Call> calls;

          // get the list of channels from list of keys.
          synchronized (writeSelector.keys()) {
            calls = new ArrayList<Call>(writeSelector.keys().size());
            iter = writeSelector.keys().iterator();
            while (iter.hasNext()) {
              SelectionKey key = iter.next();
              Call call = (Call) key.attachment();
              if (call != null && key.channel() == call.connection.channel) {
                calls.add(call);
              }
            }
          }

          for (Call call : calls) {
            doPurge(call, now);
          }
        } catch (OutOfMemoryError e) {
          if (errorHandler != null) {
            if (errorHandler.checkOOME(e)) {
              LOG.info(getName() + ": exiting on OOME");
              return;
            }
          } else {
            //
            // we can run out of memory if we have too many threads
            // log the event and sleep for a minute and give
            // some thread(s) a chance to finish
            //
            LOG.warn("Out of Memory in server select", e);
            try {
              Thread.sleep(60000);
            } catch (Exception ignored) {
            }
          }
        } catch (Exception e) {
          LOG.warn("Exception in Responder "
              + StringUtils.stringifyException(e));
        }
      }
      LOG.info("Stopping " + this.getName());
    }

    private void doAsyncWrite(SelectionKey key) throws IOException {
      Call call = (Call) key.attachment();
      if (call == null) {
        return;
      }
      if (key.channel() != call.connection.channel) {
        throw new IOException("doAsyncWrite: bad channel");
      }

      synchronized (call.connection.responseQueue) {
        if (processResponse(call.connection.responseQueue, false)) {
          try {
            key.interestOps(0);
          } catch (CancelledKeyException e) {
            /*
             * The Listener/reader might have closed the socket. We don't
             * explicitly cancel the key, so not sure if this will ever fire.
             * This warning could be removed.
             */
            LOG.warn("Exception while changing ops : " + e);
          }
        }
      }
    }

    //
    // Remove calls that have been pending in the responseQueue
    // for a long time.
    //
    private void doPurge(Call call, long now) {
      synchronized (call.connection.responseQueue) {
        Iterator<Call> iter = call.connection.responseQueue.listIterator(0);
        while (iter.hasNext()) {
          Call nextCall = iter.next();
          if (now > nextCall.timestamp + PURGE_INTERVAL) {
            closeConnection(nextCall.connection);
            break;
          }
        }
      }
    }

    // Processes one response. Returns true if there are no more pending
    // data for this channel.
    //
    @SuppressWarnings({ "ConstantConditions" })
    private boolean processResponse(final LinkedList<Call> responseQueue,
        boolean inHandler) throws IOException {
      boolean error = true;
      boolean done = false; // there is more data for this channel.
      int numElements;
      Call call = null;
      try {
        // noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized (responseQueue) {
          //
          // If there are no items for this channel, then we are done
          //
          numElements = responseQueue.size();
          if (numElements == 0) {
            error = false;
            return true; // no more data for this channel.
          }
          //
          // Extract the first call
          //
          call = responseQueue.removeFirst();
          SocketChannel channel = call.connection.channel;
          if (LOG.isDebugEnabled()) {
            LOG.debug(getName() + ": responding to #" + call.id + " from "
                + call.connection);
          }
          //
          // Send as much data as we can in the non-blocking fashion
          //
          int numBytes = channelWrite(channel, call.response);
          if (numBytes < 0) {
            return true;
          }
          if (!call.response.hasRemaining()) {
            call.connection.decRpcCount();
            // noinspection RedundantIfStatement
            if (numElements == 1) { // last call fully processes.
              done = true; // no more data for this channel.
            } else {
              done = false; // more calls pending to be sent.
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug(getName() + ": responding to #" + call.id + " from "
                  + call.connection + " Wrote " + numBytes + " bytes.");
            }
          } else {
            //
            // If we were unable to write the entire response out,
            // then
            // insert in Selector queue.
            //
            call.connection.responseQueue.addFirst(call);

            if (inHandler) {
              // set the serve time when the response has to be
              // sent later
              call.timestamp = System.currentTimeMillis();

              incPending();
              try {
                // Wakeup the thread blocked on select, only
                // then can the call
                // to channel.register() complete.
                writeSelector.wakeup();
                channel.register(writeSelector, SelectionKey.OP_WRITE, call);
              } catch (ClosedChannelException e) {
                // Its ok. channel might be closed else where.
                done = true;
              } finally {
                decPending();
              }
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug(getName() + ": responding to #" + call.id + " from "
                  + call.connection + " Wrote partial " + numBytes + " bytes.");
            }
          }
          error = false; // everything went off well
        }
      } finally {
        if (error && call != null) {
          LOG.warn(getName() + ", call " + call + ": output error");
          done = true; // error. no more data for this channel.
          closeConnection(call.connection);
        }
      }
      return done;
    }

    //
    // Enqueue a response from the application.
    //
    void doRespond(Call call) throws IOException {
      synchronized (call.connection.responseQueue) {
        call.connection.responseQueue.addLast(call);
        if (call.connection.responseQueue.size() == 1) {
          processResponse(call.connection.responseQueue, true);
        }
      }
    }

    private synchronized void incPending() { // call waiting to be enqueued.
      pending++;
    }

    private synchronized void decPending() { // call done enqueueing.
      pending--;
      notify();
    }

    private synchronized void waitPending() throws InterruptedException {
      while (pending > 0) {
        wait();
      }
    }
  }

  /**
   * Reads calls from a connection and queues them for handling. copied from
   * HBaseServer but changed in getCallPri() and processData() methods.
   * */
  private class Connection {
    private boolean versionRead = false; // if initial signature and
    // version are read
    private boolean headerRead = false; // if the connection header that
    // follows version is read.
    protected SocketChannel channel;
    private ByteBuffer data;
    private ByteBuffer dataLengthBuffer;
    protected final LinkedList<Call> responseQueue;
    private volatile int rpcCount = 0; // number of outstanding rpcs
    private long lastContact;
    private int dataLength;
    protected Socket socket;
    // Cache the remote host & port info so that even if the socket is
    // disconnected, we can say where it used to connect to.
    private String hostAddress;
    private int remotePort;
    protected UserGroupInformation ticket = null;

    public Connection(SocketChannel channel, long lastContact) {
      this.channel = channel;
      this.lastContact = lastContact;
      this.data = null;
      this.dataLengthBuffer = ByteBuffer.allocate(4);
      this.socket = channel.socket();
      InetAddress addr = socket.getInetAddress();
      if (addr == null) {
        this.hostAddress = "*Unknown*";
      } else {
        this.hostAddress = addr.getHostAddress();
      }
      this.remotePort = socket.getPort();
      this.responseQueue = new LinkedList<Call>();
      if (socketSendBufferSize != 0) {
        try {
          socket.setSendBufferSize(socketSendBufferSize);
        } catch (IOException e) {
          LOG.warn("Connection: unable to set socket send buffer size to "
              + socketSendBufferSize);
        }
      }
    }

    public String toString() {
      return getHostAddress() + ":" + remotePort;
    }

    public String getHostAddress() {
      return hostAddress;
    }

    public void setLastContact(long lastContact) {
      this.lastContact = lastContact;
    }

    public long getLastContact() {
      return lastContact;
    }

    /* Return true if the connection has no outstanding rpc */
    private boolean isIdle() {
      return rpcCount == 0;
    }

    /* Decrement the outstanding RPC count */
    protected void decRpcCount() {
      rpcCount--;
    }

    /* Increment the outstanding RPC count */
    private void incRpcCount() {
      rpcCount++;
    }

    protected boolean timedOut(long currentTime) {
      return isIdle() && currentTime - lastContact > maxIdleTime;
    }

    public int readAndProcess() throws IOException, InterruptedException {
      while (true) {
        /*
         * Read at most one RPC. If the header is not read completely yet then
         * iterate until we read first RPC or until there is no data left.
         */
        int count;
        if (dataLengthBuffer.remaining() > 0) {
          count = channelRead(channel, dataLengthBuffer);
          if (count < 0 || dataLengthBuffer.remaining() > 0)
            return count;
        }

        if (!versionRead) {
          // Every connection is expected to send the header.
          ByteBuffer versionBuffer = ByteBuffer.allocate(1);
          count = channelRead(channel, versionBuffer);
          if (count <= 0) {
            return count;
          }
          int version = versionBuffer.get(0);

          dataLengthBuffer.flip();
          if (!HEADER.equals(dataLengthBuffer) || version != CURRENT_VERSION) {
            // Warning is ok since this is not supposed to happen.
            LOG.warn("Incorrect header or version mismatch from " + hostAddress
                + ":" + remotePort + " got version " + version
                + " expected version " + CURRENT_VERSION);
            return -1;
          }
          dataLengthBuffer.clear();
          versionRead = true;
          continue;
        }

        if (data == null) {
          dataLengthBuffer.flip();
          dataLength = dataLengthBuffer.getInt();

          if (dataLength == ScheduleHBaseServer.PING_CALL_ID) {
            dataLengthBuffer.clear();
            return 0; // ping message
          }
          data = ByteBuffer.allocate(dataLength);
          incRpcCount(); // Increment the rpc count
        }

        count = channelRead(channel, data);

        if (data.remaining() == 0) {
          dataLengthBuffer.clear();
          data.flip();
          if (headerRead) {
            processData();
            data = null;
            return count;
          }
          processHeader();
          headerRead = true;
          data = null;
          continue;
        }
        return count;
      }
    }

    // / Reads the header following version
    private void processHeader() throws IOException {
      /*
       * In the current version, it is just a ticket. Later we could introduce a
       * "ConnectionHeader" class.
       */
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(
          data.array()));
      ticket = (UserGroupInformation) ObjectWritable.readObject(in, conf);
    }

    int report = 0;

    private void processData() throws IOException, InterruptedException {
      DataInputStream dis = new DataInputStream(new ByteArrayInputStream(
          data.array()));
      int id = dis.readInt(); // try to read an id

      if (LOG.isDebugEnabled())
        LOG.debug(" got #" + id);

      Writable param = ReflectionUtils.newInstance(paramClass, conf); // read
      // param
      param.readFields(dis);

      Call call = new Call(id, param, this);
      report++;
      if (((report << 54) >>> 54) == 0) {
        // System.out.println("client :" + call);
      }
      if (priorityCallQueue != null && getQosLevel(param) > highPriorityLevel) {
        priorityCallQueue.put(call);
      } else {

        // add call and find the call's priority.
        queue.add(call, getCallPri(call));
        // callQueue.put(call); // queue the call; maybe blocked here
      }
    }

    protected int getCallPri(Call call) {
      Invocation invo = (Invocation) call.param;
      if (invo.getMethodName().endsWith("next")) {
        Long scanN = (Long) invo.getParameters()[0];
        Integer pri = scannerPriMapInteger.get(scanN);
        if (pri == null) {
          String region = scannerPriMap.get(scanN);
          if (region != null) {
            pri = regionPriMap.get(region);
            if (pri != null) {
              scannerPriMapInteger.put(scanN, pri);
              return pri;
            } else {
              if (scannerPriMap.get(scanN) != null) {
                return initRegionPri(region, false);
              }
            }
          }
        }
        if (pri == null)
          return defaultPri;
        return pri;

      }

      else if (invo.getMethodName().endsWith("multi")) {
        MultiAction multi = (MultiAction) invo.getParameters()[0];
        for (Map.Entry<byte[], List<Action>> e : multi.actions.entrySet()) {
          String regionName = Bytes.toString(e.getKey());
          // byte[] regionName = e.getKey();
          Integer pri = regionPriMap.get(regionName);
          if (pri == null) {
            pri = initRegionPri(regionName, false);
          }
          if (pri == null)
            return defaultPri;
          return pri;

        }
        return defaultPri;

      } else if (invo.getMethodName().endsWith("get")) {
        byte[] region = (byte[]) invo.getParameters()[0];
        String regionN = Bytes.toString(region);

        Integer pri = regionPriMap.get(regionN);
        if (pri == null) {
          pri = initRegionPri(regionN, false);
        }
        if (pri == null)
          return defaultPri;
        return pri;
      } else if (invo.getMethodName().endsWith("put")) {
        String region = Bytes.toString((byte[]) invo.getParameters()[0]);

        Integer pri = regionPriMap.get(region);
        if (pri == null) {
          pri = initRegionPri(region, false);
        }
        if (pri == null)
          return defaultPri;
        return pri;

      } else if (invo.getMethodName().endsWith("delete")) {
        // byte[] region = (byte[]) invo.getParameters()[0];
        String region = Bytes.toString((byte[]) invo.getParameters()[0]);
        Integer pri = regionPriMap.get(region);
        if (pri == null) {
          pri = initRegionPri(region, false);
        }
        if (pri == null)
          return defaultPri;
        return pri;

      } else {
        return highPri;
      }

    }

    protected synchronized void close() {
      data = null;
      dataLengthBuffer = null;
      if (!channel.isOpen())
        return;
      try {
        socket.shutdownOutput();
      } catch (Exception ignored) {
      } // FindBugs DE_MIGHT_IGNORE
      if (channel.isOpen()) {
        try {
          channel.close();
        } catch (Exception ignored) {
        }
      }
      try {
        socket.close();
      } catch (Exception ignored) {
      }
    }
  }

  /**
   * Handles queued calls . copied from HBase server but get data from the
   * prioriryQueue,and handle has priority
   */
  private class Handler extends Thread {
    private ScheduleQueue<Call> myCallQueue = null;
    static final int BUFFER_INITIAL_SIZE = 1024;
    private BlockingQueue<Call> myCallQueueBlock = null;
    private int qPriority = 5;

    public int getqPriority() {
      return qPriority;
    }

    public void setqPriority(int qPriority) {
      this.qPriority = qPriority;
    }

    public Handler(final ScheduleQueue<Call> cq, int instanceNumber) {
      this.myCallQueue = cq;
      this.setDaemon(true);

      String threadName = "IPC Server handler " + instanceNumber + " on "
          + port;
      if (cq == priorityCallQueue) {
        // this is just an amazing hack, but it works.
        threadName = "PRI " + threadName;
      }
      this.setName(threadName);
    }

    public Handler(final BlockingQueue<Call> cq, int instanceNumber) {
      this.myCallQueueBlock = cq;
      this.setDaemon(true);

      String threadName = "IPC Server handler " + instanceNumber + " on "
          + port;
      if (cq == priorityCallQueue) {
        // this is just an amazing hack, but it works.
        threadName = "PRI " + threadName;
      }
      this.setName(threadName);
    }

    public void run() {
      LOG.info(getName() + ": starting");
      SERVER.set(ScheduleHBaseServer.this);
      int flush = 3;
      while (running) {
        try {
          Call call = null;
          if (myCallQueue != null) {
            call = myCallQueue.get(this.getqPriority());
            if (((flush << move) >>> move) == 0) {
              myCallQueue.refresh();
            }
            flush++;
          } else {
            call = this.myCallQueueBlock.take();
          }

          if (LOG.isDebugEnabled())
            LOG.debug(getName() + ": has #" + call.id + " from "
                + call.connection);

          String errorClass = null;
          String error = null;
          Writable value = null;

          CurCall.set(call);
          try {
            if (!started)
              throw new ServerNotRunningException("Server is not running yet");
            value = call(call.param, call.timestamp); // make the
            // call
          } catch (Throwable e) {
            LOG.debug(getName() + ", call " + call + ": error: " + e, e);
            errorClass = e.getClass().getName();
            error = StringUtils.stringifyException(e);
          }
          CurCall.set(null);

          int size = BUFFER_INITIAL_SIZE;
          if (value instanceof WritableWithSize) {
            // get the size hint.
            WritableWithSize ohint = (WritableWithSize) value;
            long hint = ohint.getWritableSize() + Bytes.SIZEOF_BYTE
                + Bytes.SIZEOF_INT;
            if (hint > 0) {
              if ((hint) > Integer.MAX_VALUE) {
                // oops, new problem.
                IOException ioe = new IOException(
                    "Result buffer size too large: " + hint);
                errorClass = ioe.getClass().getName();
                error = StringUtils.stringifyException(ioe);
              } else {
                size = (int) hint;
              }
            }
          }
          ByteBufferOutputStream buf = new ByteBufferOutputStream(size);
          DataOutputStream out = new DataOutputStream(buf);
          out.writeInt(call.id); // write call id
          out.writeBoolean(error != null); // write error flag

          if (error == null) {
            value.write(out);
          } else {
            WritableUtils.writeString(out, errorClass);
            WritableUtils.writeString(out, error);
          }

          if (buf.size() > warnResponseSize) {
            LOG.warn(getName() + ", responseTooLarge for: " + call + ": Size: "
                + StringUtils.humanReadableInt(buf.size()));
          }

          call.setResponse(buf.getByteBuffer());
          responder.doRespond(call);
        } catch (InterruptedException e) {
          if (running) { // unexpected -- log it
            LOG.info(getName() + " caught: "
                + StringUtils.stringifyException(e));
          }
        } catch (OutOfMemoryError e) {
          if (errorHandler != null) {
            if (errorHandler.checkOOME(e)) {
              LOG.info(getName() + ": exiting on OOME");
              return;
            }
          } else {
            // rethrow if no handler
            throw e;
          }
        } catch (Exception e) {
          LOG.warn(getName() + " caught: " + StringUtils.stringifyException(e));
        }
      }
      LOG.info(getName() + ": exiting");
    }

  }

  public synchronized InetSocketAddress getListenerAddress() {
    return listener.getAddress();
  }

  /*
   * Constructs a server listening on the named port and address. Parameters
   * passed must be of the named class. The <code>handlerCount</handlerCount>
   * determines the number of handler threads that will be used to process
   * calls.
   */
  protected ScheduleHBaseServer(String bindAddress, int port,
      Class<? extends Writable> paramClass, int handlerCount,
      int priorityHandlerCount, Configuration conf, String serverName,
      int highPriorityLevel) throws IOException {
    super(bindAddress, port + 1, paramClass, handlerCount,
        priorityHandlerCount, conf, serverName, highPriorityLevel);

    this.bindAddress = bindAddress;
    this.conf = conf;
    handleFreshInter = conf.getInt("hbase.schedule.refreshinter", 7);
    this.move = Integer.SIZE - handleFreshInter;
    this.port = port;
    this.paramClass = paramClass;
    this.handlerCount = handlerCount >= 10 ? handlerCount : 10;
    this.handlerCount = (int) (this.handlerCount * 1.5);
    this.priorityHandlerCount = priorityHandlerCount;
    this.socketSendBufferSize = 0;
    this.readThreads = conf.getInt("ipc.server.read.threadpool.size", 10);
    this.tcpNoDelay = conf.getBoolean("ipc.server.tcpnodelay", false);
    this.tcpKeepAlive = conf.getBoolean("ipc.server.tcpkeepalive", true);
    this.maxQueueSize = handlerCount * MAX_QUEUE_SIZE_PER_HANDLER;
    if (priorityHandlerCount > 0) {
      this.priorityCallQueue = new LinkedBlockingQueue<Call>(maxQueueSize);
    } else {
      this.priorityCallQueue = null;
    }
    this.highPriorityLevel = highPriorityLevel;
    this.maxIdleTime = 2 * conf.getInt("ipc.client.connection.maxidletime",
        1000);
    this.maxConnectionsToNuke = conf.getInt("ipc.client.kill.max", 10);
    this.thresholdIdleConnections = conf.getInt("ipc.client.idlethreshold",
        4000);

    // Start the listener here and let it bind to the port

    super.stop();
    listener = new Listener();
    responder = new Responder();
    this.port = listener.getAddress().getPort();
    this.rpcMetrics = new HBaseRpcMetrics(serverName,
        Integer.toString(this.port));

    this.warnResponseSize = conf.getInt(WARN_RESPONSE_SIZE,
        DEFAULT_WARN_RESPONSE_SIZE);
    // Create the responder here

  }

  protected void closeConnection(Connection connection) {
    synchronized (connectionList) {
      if (connectionList.remove(connection))
        numConnections--;
    }
    connection.close();
  }

  /**
   * translate thread priority to system priority
   * 
   * @param tpri
   * @return
   */
  private int priTrans(int tpri) {
    switch (tpri) {
    case 10:
      return 1;
    case 9:
      return 2;
    case 8:
      return 3;
    case 7:
      return 4;
    case 6:
      return 5;
    case 5:
      return 6;
    case 4:
      return 7;
    case 3:
      return 8;
    case 2:
      return 9;
    case 1:
      return 10;
    default:
      return 5;
    }
  }

  /**
   * start Threads and set priority of handlers
   */
  @Override
  public synchronized void startThreads() {
    responder.start();
    listener.start();
    handlers = new Handler[handlerCount];
    int pri = 10;
    int minPri = 10;
    for (int i = 0; i < handlerCount; i++) {
      if (pri <= 0) {
        pri = 10;
      }
      if (pri < minPri) {
        minPri = pri;
      }
      handlers[i] = new Handler(queue, i);
      handlers[i].setPriority(pri);
      handlers[i].setqPriority(priTrans(pri));
      pri--;
    }
    for (int i = 0; i < handlerCount; i++) {
      handlers[i].start();
    }

    if (priorityHandlerCount > 0) {
      priorityHandlers = new Handler[priorityHandlerCount];
      for (int i = 0; i < priorityHandlerCount; i++) {
        priorityHandlers[i] = new Handler(priorityCallQueue, i);
        priorityHandlers[i].start();
      }
    }
  }

  /** Stops the service. No new calls will be handled after this is called. */
  @Override
  public synchronized void stop() {
    LOG.info("Stopping server on " + port);
    running = false;
    if (handlers != null) {
      for (Handler handler : handlers) {
        if (handler != null) {
          handler.interrupt();
        }
      }
    }
    if (priorityHandlers != null) {
      for (Handler handler : priorityHandlers) {
        if (handler != null) {
          handler.interrupt();
        }
      }
    }
    listener.interrupt();
    listener.doStop();
    responder.interrupt();
    notifyAll();
    if (this.rpcMetrics != null) {
      this.rpcMetrics.shutdown();
    }
    if (this.priorityIniter != null) {
      this.priorityIniter.interrupt();
    }
    /**
     * added here to stop the priority refresher.
     */
    this.queue.stop();
  }

  /**
   * The number of rpc calls in the queue. this method return the
   * {@link ScheduleQueue} size.
   * 
   * @return The number of rpc calls in the queue.
   */
  public int getCallQueueLen() {
    return this.queue.size();
  }

}
