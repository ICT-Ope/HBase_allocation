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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;

/**
 * Scanner that returns the next KeyValue.
 */
public interface KeyValueScanner {
  /**
   * Look at the next KeyValue in this scanner, but do not iterate scanner.
   * @return the next KeyValue
   */
  public KeyValue peek();

  /**
   * Return the next KeyValue in this scanner, iterating the scanner
   * @return the next KeyValue
   */
  public KeyValue next() throws IOException;

  /**
   * Seek the scanner at or after the specified KeyValue.
   * @param key seek value
   * @return true if scanner has values left, false if end of scanner
   */
  public boolean seek(KeyValue key) throws IOException;

  /**
   * Reseek the scanner at or after the specified KeyValue.
   * This method is guaranteed to seek to or before the required key only if the
   * key comes after the current position of the scanner. Should not be used
   * to seek to a key which may come before the current position.
   * @param key seek value (should be non-null)
   * @return true if scanner has values left, false if end of scanner
   */
  public boolean reseek(KeyValue key) throws IOException;

  /**
   * Get the sequence id associated with this KeyValueScanner. This is required
   * for comparing multiple files to find out which one has the latest data.
   * The default implementation for this would be to return 0. A file having
   * lower sequence id will be considered to be the older one.
   */
  public long getSequenceID();

  /**
   * Close the KeyValue scanner.
   */
  public void close();
}
