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

import java.util.Calendar;
import java.util.Random;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.ipc.HBaseRPC.Invocation;

/**
 * 
 * this queue is used by {@link ScheduleHBaseServer}
 * 
 * @param <T>
 *          the class contained by the queue
 * 
 */
public class ScheduleQueue<T> {
  private static final Log LOG = LogFactory.getLog(ScheduleQueue.class);
  private final PriorityBlockingQueue<Job<T>> queue = new PriorityBlockingQueue<Job<T>>();
  private int size = 0;
  private int capacity = 100;
  private int maxWait = 1000;
  private boolean running = true;
  ReentrantLock readLock = new ReentrantLock();
  Condition[] lockList = new Condition[10];
  ReentrantLock addLock = new ReentrantLock();
  Condition queueFull = addLock.newCondition();
  int lowestThreadPir = 10;
  boolean verbose = false;

  private void setSize(int size) {
    this.addLock.lock();
    this.size = size;
    if (this.size < this.capacity) {
      this.queueFull.signalAll();
    }
    this.addLock.unlock();
  }

  private void addSize() {
    this.addLock.lock();
    this.size++;
    this.addLock.unlock();
  }

  private void decreaseSize() {
    this.addLock.lock();
    this.size--;
    if (this.size < this.capacity) {
      this.queueFull.signalAll();
    }
    this.addLock.unlock();
  }

  private void testAdd(Job<T> j) {
    int wait = 0;
    while (this.size >= this.capacity) {
      try {
        addLock.lock();
        this.queueFull.await(10, TimeUnit.MILLISECONDS);
        // this.queueFull.await();
        addLock.unlock();
        wait++;
        if (wait > this.maxWait)
          break;
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    // readLock.lock();
    this.queue.add(j);
    // readLock.unlock();
    this.addSize();

  }

  private int getSize() {
    return this.size;
  }

  private Thread refresher = new Thread() {
    public void run() {
      while (running) {
        refreshIner();
        try {
          sleep(1000);
        } catch (Exception e) {

          // e.printStackTrace();
        }
      }
    }
  };

  public void stop() {
    this.running = false;
  }

  /**
   * Init a queue
   * 
   * @param size
   *          the capacity of the queue,not a precision value,if queue size
   *          exceed this value, workers which add jobs should wait
   * @param lowestPrid
   *          the lowest priority which worker thread hold,the default priority
   *          is range from 1 to 10,reverse from java thread priority
   */
  public ScheduleQueue(int size, int lowestPrid) {

    this.capacity = size;
    this.refresher.start();
    for (int i = 0; i < 10; i++) {
      lockList[i] = readLock.newCondition();
    }
    this.lowestThreadPir = lowestPrid;

  }

  /**
   * add a job to this queue
   * 
   * @param call
   *          the job instance
   * @param pri
   *          the job's priority
   */
  public void add(T call, int pri) {
    this.testAdd(new Job<T>(pri, call));
    testHead();

  }

  /**
   * get the size of the queue,maintain a integer to indicate the size for
   * performance.
   * 
   * @return the size of the queue
   */
  public int size() {
    return this.size;
  }

  /**
   * get the size of the queue
   * 
   * @return the size of the queue
   */
  public int queueSize() {
    return queue.size();
  }

  private int getCondition(int pri) {
    if (pri <= 10 && pri >= 1) {
      return pri - 1;
    } else if (pri > 10) {
      return 9;
    } else {
      return 0;
    }
  }

  private void testHead() {
    readLock.lock();
    Job<T> jobt = queue.peek();
    if (jobt != null) {
      if (verbose)
        System.out.println(Thread.currentThread().getName() + " "
            + "begain testHead " + getCondition(jobt.orgPri));
      if (verbose) {
        System.out.println(this.queue);
      }
      this.lockList[getCondition(jobt.orgPri)].signal();
      if (verbose)
        System.out.println(Thread.currentThread().getName() + " "
            + "over testHead " + getCondition(jobt.orgPri));
    }
    readLock.unlock();

  }

  /**
   * if handler's priority lower than job's priority, then this handler can't
   * get this job.
   * 
   * @param job
   *          the job which worker want to get
   * @param pri
   *          the worker thread's priority
   * @return should the worker get this job
   */
  public boolean shouldWork(Job<T> job, int pri) {
    if (job == null)
      return false;
    return (pri >= job.orgPri) || (job.orgPri < 1 && pri == 1)
        || (job.orgPri > this.lowestThreadPir && pri == this.lowestThreadPir);
  }

  /**
   * get a job from the queue ,will test whether the thread can get this job
   * 
   * @param pri
   *          the worker thread's priority
   * @return the job
   * @throws InterruptedException
   */
  public T get(int pri) throws InterruptedException {
    Job<T> job = null;

    job = queue.peek();
    while (!shouldWork(job, pri)) {
      if (verbose)
        System.out.println(Thread.currentThread().getName() + " "
            + "begain wait " + getCondition(pri));
      readLock.lock();
      this.lockList[getCondition(pri)].await(100, TimeUnit.MILLISECONDS);
      readLock.unlock();
      if (verbose)
        System.out.println(Thread.currentThread().getName() + " "
            + "over wait " + getCondition(pri));
      job = queue.peek();

    }
    Job<T> ret = queue.take();
    if (ret.orgPri > pri && pri != this.lowestThreadPir) {

      System.err.println("error");
    }
    this.testHead();
    if (ret == null) {
      this.setSize(0);
      return null;
    } else {
      this.decreaseSize();
      return ret.getCall();
    }
  }

  /**
   * refresh the priorities of the jobs in queue,simply -1
   */
  public void refresh() {
    this.refresher.interrupt();
  }

  static int outputIndicator = 0;

  private void refreshIner() {
    try {
      if ((outputIndicator << 58) >>> 58 == 0)
        LOG.debug(Calendar.getInstance().getTime() + ":" + this.queue);
      outputIndicator++;
      for (Job<T> job : queue) {
        if (job != null) {
          job.add();
        }
      }
      testHead();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void refreshIner(int n) {
    try {
      for (Job<T> job : queue) {
        if (job != null) {
          job.add(n);
        }
      }
      testHead();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * 
   * The Job hold by queue
   * 
   * @param <T>
   */
  public static class Job<T> implements Comparable<Job<T>> {
    int orgPri = 0;
    int priority = 0;
    long initTime = 0;
    T call;

    /**
     * increase job's priority
     */
    public void add() {
      this.priority--;
    }

    /**
     * increase job's priority by n
     * 
     * @param n
     */
    public void add(int n) {

      this.priority = this.priority - n;

    }

    /**
     * get the instance hold by the job
     * 
     * @return the call instance
     */
    public T getCall() {
      return call;
    }

    /**
     * set the instance hold byt the job
     * 
     * @param call
     *          the call instance
     */
    public void setCall(T call) {
      this.call = call;
    }

    /**
     * Initiate a job
     * 
     * @param pri
     *          the job priority
     * @param call
     *          the instance hold by the job
     */
    public Job(int pri, T call) {
      this.orgPri = pri;
      this.priority = pri;
      this.initTime = System.currentTimeMillis();
      this.call = call;
    }

    /**
     * print the job
     */
    public String toString() {

      return "orgPri:" + this.orgPri + ", lastPri:" + this.priority
          + ", wait time:" + ((System.currentTimeMillis() - this.initTime))
          + ",ino:";
    }

    @Override
    public int compareTo(Job<T> arg0) {
      // TODO Auto-generated method stub
      return this.priority - arg0.priority;
    }

  }

  /**
   * test the queue's function
   * 
   * @param args
   */
  public static void main(String args[]) {
    final Random r = new Random();
    final ScheduleQueue<String> queue = new ScheduleQueue<String>(100, 10);
    Thread tt[] = new Thread[50];
    int pri = 1;
    for (int i = 0; i < tt.length; i++, pri++) {
      tt[i] = new Thread(i + "") {
        public void run() {
          while (true) {
            String j = null;
            try {
              j = queue.get(Math.abs(this.getPriority() - 10) + 1);
            } catch (InterruptedException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }
            System.out.println("thread pri: "
                + (Math.abs(this.getPriority() - 10) + 1) + "  job:" + j
                + " ,thread real pri is:" + this.getPriority());
          }
        }
      };
      if (pri >= 11)
        pri = 1;
      tt[i].setPriority(pri);
      tt[i].start();
    }

    Thread tt2[] = new Thread[10];
    for (int i = 0; i < tt2.length; i++) {
      tt2[i] = new Thread(i + "") {
        @SuppressWarnings("static-access")
        public void run() {
          for (int i = 0; i < 10000000; i++) {
            System.out.println("add ten jobs...............");
            for (int j = 0; j < 100000; j++) {
              int jobpri = (r.nextInt(19) - 4);
              queue.add("" + jobpri, jobpri);

            }
            System.out.println(queue.size());
            try {
              Thread.currentThread().sleep(r.nextInt(10000));
            } catch (InterruptedException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }
          }
        }
      };
      tt2[i].start();
    }

  }

}
