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

/**
 * Used for change regionserver groups
 * <p>
 * Include move regions between regionservers and update configuration fold and
 * library fold
 * 
 */
public class MoveGroupPlan {
  private String servername;
  private String originalgroup;
  private String targetgroup;
  private boolean status;

  /**
   * Default Construct Method
   */
  public MoveGroupPlan() {
  }

  /**
   * Construct Method
   * 
   * @param servername
   *          regionserver name ,for example "dw83.kgb.sqa.cm4,60020"
   * @param originalgroup
   *          origninal groupname ,for example "0"
   * @param targetgroup
   *          target group name ,for example ''1"
   */
  public MoveGroupPlan(String servername, String originalgroup,
      String targetgroup) {
    this.servername = servername;
    this.originalgroup = originalgroup;
    this.targetgroup = targetgroup;
    this.status = false;
  }

  /**
   * Get servername
   * 
   * @return regionserver name
   */
  public String getServername() {
    return servername;
  }

  /**
   * get origina group name
   * 
   * @return original group name
   */
  public String getOriginalgroup() {
    return originalgroup;
  }

  /**
   * get target group name
   * 
   * @return target groupname
   */
  public String getTargetgroup() {
    return targetgroup;
  }

  /**
   * Get change group status
   * 
   * @return status if regionserver is changed group
   */
  public boolean getStatus() {
    return status;
  }

  /**
   * Set change group status
   * 
   * @param status
   *          if regionserver is changed group
   */
  public void setStatus(boolean status) {
    this.status = status;
  }
}
