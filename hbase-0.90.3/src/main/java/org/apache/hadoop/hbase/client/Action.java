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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

/*
 * A Get, Put or Delete associated with it's region.  Used internally by  
 * {@link HTable::batch} to associate the action with it's region and maintain 
 * the index from the original request. 
 */
public class Action implements Writable, Comparable {

  private byte[] regionName;
  private Row action;
  private int originalIndex;
  private Result result;

  public Action() {
    super();
  }

  public Action(byte[] regionName, Row action, int originalIndex) {
    super();
    this.regionName = regionName;
    this.action = action;
    this.originalIndex = originalIndex;
  }

  public byte[] getRegionName() {
    return regionName;
  }

  public void setRegionName(byte[] regionName) {
    this.regionName = regionName;
  }

  public Result getResult() {
    return result;
  }

  public void setResult(Result result) {
    this.result = result;
  }

  public Row getAction() {
    return action;
  }

  public int getOriginalIndex() {
    return originalIndex;
  }

  @Override
  public int compareTo(Object o) {
    return action.compareTo(((Action) o).getAction());
  }

  // ///////////////////////////////////////////////////////////////////////////
  // Writable
  // ///////////////////////////////////////////////////////////////////////////

  public void write(final DataOutput out) throws IOException {
    Bytes.writeByteArray(out, regionName);
    HbaseObjectWritable.writeObject(out, action, Row.class, null);
    out.writeInt(originalIndex);
    HbaseObjectWritable.writeObject(out, result, Result.class, null);
  }

  public void readFields(final DataInput in) throws IOException {
    this.regionName = Bytes.readByteArray(in);
    this.action = (Row) HbaseObjectWritable.readObject(in, null);
    this.originalIndex = in.readInt();
    this.result = (Result) HbaseObjectWritable.readObject(in, null);
  }

}
