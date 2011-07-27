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

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class is used for update regionserver configuration and library
 * <p>
 * It use two shell script in ${hbase_home}/hbase-webapps/master/ ,
 * moveremoteconf.sh and restartserver.sh , make sure you have permission
 */
public class MoveConfImpl {
	static final Log LOG = LogFactory.getLog(MoveConfImpl.class);
	final String CONFPATHNAME = "HBASE_CONF_DIR";
	final String HBASEPATHNAME = "HBASE_HOME";
	public String ConfDIR;
	public String HbaseDIR;
	public String currentdir;

	/**
	 * Construct method
	 * 
	 * @param path
	 *            Path name of ${master_home}/hbase-webapp/master
	 */
	public MoveConfImpl(String path) {
		ConfDIR = System.getenv(CONFPATHNAME);
		HbaseDIR = System.getenv(HBASEPATHNAME);
		currentdir = path;
		LOG.info("current  path  is   " + currentdir);
		if (HbaseDIR == null || HbaseDIR.length() <= 0) {
			String line = currentdir.substring(0, currentdir.lastIndexOf("/"));
			HbaseDIR = line.substring(0, line.lastIndexOf("/"));
		}
		if (ConfDIR == null || ConfDIR.length() <= 0) {
			ConfDIR = HbaseDIR + "/conf";
		}
		LOG.info("hbase   path  is   " + HbaseDIR);
		LOG.info("configuration   path  is   " + ConfDIR);
	}

	/**
	 * Scp configuration between regionservers
	 * 
	 * @param server
	 *            regionserver name ,example "dw83.kgb.sqa.cm4,60020"
	 * @param command
	 *            if command is "get" ,it will get remote regionserver
	 *            configuration and library to local temp file ; if command is
	 *            "put", it will put temp configuration and library to remote
	 *            regionserver.
	 * @return true if success ,else false
	 */
	public boolean ScpConf(String server, String command) {
		Process process = null;
		if (!command.equals("get") && !command.equals("put")) {
			LOG.info("This shell script only support get and put command.");
			return false;
		}
		String[] getcommand = new String[5];
		getcommand[0] = currentdir + "/moveremoteconf.sh";
		getcommand[1] = server;
		getcommand[2] = ConfDIR;
		getcommand[3] = HbaseDIR;
		getcommand[4] = command;
		try {
			process = Runtime.getRuntime().exec(getcommand, null,
					new File(currentdir));
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		try {
			process.waitFor();
		} catch (InterruptedException e) {
			e.printStackTrace();
			return false;
		}
		process.destroy();
		return true;
	}

	/**
	 * Start or stop regionserver from master use shell script
	 * 
	 * @param server
	 *            regionserver name ,example "dw83.kgb.sqa.cm4,60020"
	 * @param command
	 *            "start" or "stop" ,means start or stop regionserver
	 * @return true if success ,else false
	 */
	public boolean ImplRegionServer(String server, String command) {
		Process process = null;
		if (!command.equals("start") && !command.equals("stop")) {
			LOG.info("This shell script only support start and stop command.");
			return false;
		}
		String[] cmd = new String[4];
		cmd[0] = currentdir + "/restartserver.sh";
		cmd[1] = server;
		cmd[2] = HbaseDIR;
		cmd[3] = command;
		try {
			process = Runtime.getRuntime()
					.exec(cmd, null, new File(currentdir));
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		try {
			process.waitFor();
		} catch (InterruptedException e) {
			e.printStackTrace();
			return false;
		}
		process.destroy();
		return true;
	}
	
	/**
	 * DistributeConf configuration to regionserver
	 * 
	 * @param server
	 *            regionserver name ,example "dw83.kgb.sqa.cm4,60020"
	 * @param command
	 *            if command is "distribute " ,it will distribute hbase-site,xml to remote regionservwer
	 * @return true if success ,else false
	 */
	public boolean DistributeConf(String server, String command) {
		Process process = null;
		if (!command.equals("distribute")) {
			LOG.info("This shell script only support distribute  command.");
			return false;
		}
		String[] distributecommand = new String[5];
		distributecommand[0] = currentdir + "/moveremoteconf.sh";
		distributecommand[1] = server;
		distributecommand[2] = ConfDIR;
		distributecommand[3] = HbaseDIR;
		distributecommand[4] = command;
		try {
			process = Runtime.getRuntime().exec(distributecommand, null,
					new File(currentdir));
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		try {
			process.waitFor();
		} catch (InterruptedException e) {
			e.printStackTrace();
			return false;
		}
		process.destroy();
		return true;
	}
}
