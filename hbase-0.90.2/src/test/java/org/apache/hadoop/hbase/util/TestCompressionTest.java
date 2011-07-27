/*
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

package org.apache.hadoop.hbase.util;

import org.apache.hadoop.hbase.io.hfile.Compression;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class TestCompressionTest {

  @Test
  public void testTestCompression() {

    // This test will fail if you run the tests with LZO compression available.
    try {
      CompressionTest.testCompression(Compression.Algorithm.LZO);
      fail(); // always throws
    } catch (IOException e) {
      // there should be a 'cause'.
      assertNotNull(e.getCause());
    }

    // this is testing the caching of the test results.
    try {
      CompressionTest.testCompression(Compression.Algorithm.LZO);
      fail(); // always throws
    } catch (IOException e) {
      // there should be NO cause because it's a direct exception not wrapped
      assertNull(e.getCause());
    }


    assertFalse(CompressionTest.testCompression("LZO"));
    assertTrue(CompressionTest.testCompression("NONE"));
    assertTrue(CompressionTest.testCompression("GZ"));
  }
}
