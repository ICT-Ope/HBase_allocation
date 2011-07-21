<%@ page language="java" import="java.util.*"
	import="org.apache.hadoop.hbase.master.GroupAssignmentManager"
	import="org.apache.hadoop.hbase.allocation.group.ServerWithGroup"
	import="org.apache.hadoop.hbase.master.HMaster"
	import="org.apache.hadoop.hbase.HRegionInfo"
	import="org.apache.hadoop.hbase.HServerInfo"
	import="org.apache.hadoop.hbase.util.Bytes"
	import="org.apache.hadoop.hbase.UnknownRegionException"
	import="org.apache.hadoop.hbase.MasterNotRunningException"
	import="org.apache.hadoop.hbase.ZooKeeperConnectionException"
	import="org.apache.hadoop.hbase.HTableDescriptor"
	contentType="text/html; charset=GB18030" pageEncoding="GB18030"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=GB18030">
<link rel="stylesheet" type="text/css" href="/static/hbase.css" />
<title>Table balance</title>
</head>
<body>
	<a id="logo" href="http://wiki.apache.org/lucene-hadoop/Hbase"><img src="/static/hbase_logo_med.gif" alt="HBase Logo" title="HBase Logo" /></a>
	<h1 id="page_title">Table Balance</h1>
	<p id="links_menu"><a href="/master.jsp">Master</a>,<a href="/logs/">Local logs</a>, <a href="/stacks">Thread Dump</a>, <a href="/logLevel">Log Level</a>,<a href="showgroup.jsp">Group Information</a></p>
	
	<hr id="head_rule" />
	<%
		/**
	 	 * Class used for balance table regions
	     */
		class Balancetable implements Runnable {
			private String table;

			public Balancetable(String table) {
				this.table = table;
			}

			public void run() {
				try {
					ServerWithGroup.setIserror(false);
					synchronized (GroupAssignmentManager.class) {
						GroupAssignmentManager.balanceTable(table);
					}
				} catch (Exception ex) {
					ServerWithGroup.setIserror(true);
					ServerWithGroup.setErrormsg("Table " + table
							+ " Balance error ,please check log...");
					ex.printStackTrace();
				} finally {
					synchronized (ServerWithGroup.class) {
						ServerWithGroup.setIsprocess(false);
						ServerWithGroup.setIsbalance(false);
					}
				}
			}
		}
	%>
	<%
	
		String table = "";
		if (request.getParameter("tablename") != null) {
			table = request.getParameter("tablename");
		} else {
			out.println("<script type=\"text/javascript\">");
			out.println("alert(\"Error, Set tablename first ............\")");
			out.println("window.location.href='showgroup.jsp'");
			out.println("</script>");
		}
		HMaster master = (HMaster) getServletContext().getAttribute(
				HMaster.MASTER);
		List<HServerInfo> availalbeserver = GroupAssignmentManager
				.getAvailableServer(table);
		Map<String ,Integer> balancestate = new HashMap<String,Integer>();
		int totalregion = 0;
		int totalserver = 0;
		boolean finishedbalance = true;
		for(HServerInfo info : availalbeserver)	{
			String servername = info.getHostname()+","+info.getServerAddress().getPort();
			int size=ServerWithGroup.getRegionOfTableOnServer(master,table,info);
			balancestate.put(servername,size);
			totalregion+= size;
			totalserver+=1;
		}
		if(totalregion == 0 || totalserver== 0 ){
			out.println("<br>No region available in this table<br>");
		}else{
			int min = totalregion/totalserver;
			int max = min+GroupAssignmentManager.div;
			for (Map.Entry<String,Integer> entry : balancestate.entrySet()){
				int size = entry.getValue();
				if (size < min || size > max){
					finishedbalance = false;
					break;
				}
			}
		}
		boolean isprocess = ServerWithGroup.isIsprocess();
		if (!isprocess) {
			//start a thread
			if (finishedbalance == false){
				synchronized (ServerWithGroup.class) {
					ServerWithGroup.setIsprocess(true);
					ServerWithGroup.setIsbalance(true);
				}
				Thread balancethread = new Thread(new Balancetable(table));
				balancethread.start();
			}else{
				out.println("<script type=\"text/javascript\">");
				out.println("alert(\"This table is balanced already.\")");
				out.println("</script>");
			}
		}
		if (ServerWithGroup.isIsbalance()){
			if (finishedbalance== false){
				out.println("<br>Warning, System is busy<br>");
				out.println("<form name='flushform' action='#' method=post>");
				out.println("<input type=submit name=submit value=Refresh style=\"width:200px;height:100px\" >");
				out.println("</form><br>");
			}
		}
		boolean haserror = ServerWithGroup.isIserror();
		if (haserror){
			out.println("<br>"+ServerWithGroup.getErrormsg()+"<br>");
		}
		out.println("<h4>Table name :"+table+"</h4>");
		if (finishedbalance){
			out.println("<br>Table is balanced already ,<a href=\"showgroup.jsp\">Back to Home</a> <br><br>");
		}
		out.println("<TABLE BORDER=1>");
		out.println("<TR><TD>Servername</TD>");
		out.println("<TD>region num</TD></TR>");

		for(HServerInfo info : availalbeserver)	{
			String servername = info.getHostname()+","+info.getServerAddress().getPort();
			int size=ServerWithGroup.getRegionOfTableOnServer(master,table,info);
			out.println("<TR><TD>"+servername+"</TD>");
			out.println("<TD>("+size+")</TD></TR>");
		}
		out.println("</TABLE>");
	%>
</body>
</html>