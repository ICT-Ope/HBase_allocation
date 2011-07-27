/*
 * Copyright 2009 The Apache Software Foundation
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
package org.apache.hadoop.hbase.regionserver;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This class is used for the tracking and enforcement of columns and numbers
 * of versions during the course of a Get or Scan operation, when explicit
 * column qualifiers have been asked for in the query.
 *
 * With a little magic (see {@link ScanQueryMatcher}), we can use this matcher
 * for both scans and gets.  The main difference is 'next' and 'done' collapse
 * for the scan case (since we see all columns in order), and we only reset
 * between rows.
 *
 * <p>
 * This class is utilized by {@link ScanQueryMatcher} through two methods:
 * <ul><li>{@link #checkColumn} is called when a Put satisfies all other
 * conditions of the query.  This method returns a {@link org.apache.hadoop.hbase.regionserver.ScanQueryMatcher.MatchCode} to define
 * what action should be taken.
 * <li>{@link #update} is called at the end of every StoreFile or memstore.
 * <p>
 * This class is NOT thread-safe as queries are never multi-threaded
 */
public class ExplicitColumnTracker implements ColumnTracker {

  private final int maxVersions;
  private final List<ColumnCount> columns;
  private final List<ColumnCount> columnsToReuse;
  private int index;
  private ColumnCount column;
  /** Keeps track of the latest timestamp included for current column.
   * Used to eliminate duplicates. */
  private long latestTSOfCurrentColumn;

  /**
   * Default constructor.
   * @param columns columns specified user in query
   * @param maxVersions maximum versions to return per column
   */
  public ExplicitColumnTracker(NavigableSet<byte[]> columns, int maxVersions) {
    this.maxVersions = maxVersions;
    this.columns = new ArrayList<ColumnCount>(columns.size());
    this.columnsToReuse = new ArrayList<ColumnCount>(columns.size());
    for(byte [] column : columns) {
      this.columnsToReuse.add(new ColumnCount(column,maxVersions));
    }
    reset();
  }

  /**
   * Done when there are no more columns to match against.
   */
  public boolean done() {
    return this.columns.size() == 0;
  }

  public ColumnCount getColumnHint() {
    return this.column;
  }

  /**
   * Checks against the parameters of the query and the columns which have
   * already been processed by this query.
   * @param bytes KeyValue buffer
   * @param offset offset to the start of the qualifier
   * @param length length of the qualifier
   * @param timestamp timestamp of the key being checked
   * @return MatchCode telling ScanQueryMatcher what action to take
   */
  public ScanQueryMatcher.MatchCode checkColumn(byte [] bytes, int offset,
      int length, long timestamp) {
    do {
      // No more columns left, we are done with this query
      if(this.columns.size() == 0) {
        return ScanQueryMatcher.MatchCode.SEEK_NEXT_ROW; // done_row
      }

      // No more columns to match against, done with storefile
      if(this.column == null) {
        return ScanQueryMatcher.MatchCode.SEEK_NEXT_ROW; // done_row
      }

      // Compare specific column to current column
      int ret = Bytes.compareTo(column.getBuffer(), column.getOffset(),
          column.getLength(), bytes, offset, length);

      // Column Matches. If it is not a duplicate key, decrement versions left
      // and include.
      if(ret == 0) {
        //If column matches, check if it is a duplicate timestamp
        if (sameAsPreviousTS(timestamp)) {
          //If duplicate, skip this Key
          return ScanQueryMatcher.MatchCode.SKIP;
        }
        if(this.column.decrement() == 0) {
          // Done with versions for this column
          this.columns.remove(this.index);
          resetTS();
          if(this.columns.size() == this.index) {
            // Will not hit any more columns in this storefile
            this.column = null;
          } else {
            this.column = this.columns.get(this.index);
          }
        } else {
          setTS(timestamp);
        }
        return ScanQueryMatcher.MatchCode.INCLUDE;
      }

      resetTS();

      if (ret > 0) {
        // Specified column is smaller than the current, skip to next column.
        return ScanQueryMatcher.MatchCode.SEEK_NEXT_COL;
      }

      // Specified column is bigger than current column
      // Move down current column and check again
      if(ret <= -1) {
        if(++this.index >= this.columns.size()) {
          // No more to match, do not include, done with storefile
          return ScanQueryMatcher.MatchCode.SEEK_NEXT_ROW; // done_row
        }
        // This is the recursive case.
        this.column = this.columns.get(this.index);
      }
    } while(true);
  }

  /**
   * Called at the end of every StoreFile or memstore.
   */
  public void update() {
    if(this.columns.size() != 0) {
      this.index = 0;
      this.column = this.columns.get(this.index);
    } else {
      this.index = -1;
      this.column = null;
    }
  }

  // Called between every row.
  public void reset() {
    buildColumnList();
    this.index = 0;
    this.column = this.columns.get(this.index);
    resetTS();
  }

  private void resetTS() {
    latestTSOfCurrentColumn = HConstants.LATEST_TIMESTAMP;
  }

  private void setTS(long timestamp) {
    latestTSOfCurrentColumn = timestamp;
  }

  private boolean sameAsPreviousTS(long timestamp) {
    return timestamp == latestTSOfCurrentColumn;
  }

  private void buildColumnList() {
    this.columns.clear();
    this.columns.addAll(this.columnsToReuse);
    for(ColumnCount col : this.columns) {
      col.setCount(this.maxVersions);
    }
  }

  /**
   * This method is used to inform the column tracker that we are done with
   * this column. We may get this information from external filters or
   * timestamp range and we then need to indicate this information to
   * tracker. It is required only in case of ExplicitColumnTracker.
   * @param bytes
   * @param offset
   * @param length
   */
  public void doneWithColumn(byte [] bytes, int offset, int length) {
    while (this.column != null) {
      int compare = Bytes.compareTo(column.getBuffer(), column.getOffset(),
          column.getLength(), bytes, offset, length);
      resetTS();
      if (compare == 0) {
        this.columns.remove(this.index);
        if (this.columns.size() == this.index) {
          // Will not hit any more columns in this storefile
          this.column = null;
        } else {
          this.column = this.columns.get(this.index);
        }
        return;
      } else if ( compare <= -1) {
        if(++this.index != this.columns.size()) {
          this.column = this.columns.get(this.index);
        } else {
          this.column = null;
        }
      } else {
        return;
      }
    }
  }

}
