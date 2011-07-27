<%@ page language="java" import="java.util.*" import="java.io.*"
	import="org.apache.hadoop.hbase.master.HMaster"
	import="org.apache.hadoop.hbase.HTableDescriptor"
	import="org.apache.hadoop.hbase.HRegionInfo"
	import="org.apache.hadoop.hbase.HServerAddress"
	import="org.apache.hadoop.hbase.HServerInfo"
	import="org.apache.hadoop.hbase.util.Bytes"
	import="org.apache.hadoop.hbase.allocation.group.ServerWithGroup"
	import="org.apache.hadoop.hbase.ipc.ScheduleHBaseServer"
	import="org.apache.hadoop.hbase.master.GroupAssignmentManager"
	contentType="text/html; charset=GB18030" pageEncoding="GB18030"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=GB18030">
<title>Group Information Detail</title>
<link rel="stylesheet" type="text/css" href="/static/hbase.css" />
</head>
<body>
	<a id="logo" href="http://wiki.apache.org/lucene-hadoop/Hbase"><img
		src="/static/hbase_logo_med.gif" alt="HBase Logo" title="HBase Logo" />
	</a>
	<h1 id="page_title">Detail Group Info</h1>
	<p id="links_menu">
		<a href="/master.jsp">Master</a>,<a href="/logs/">Local logs</a>, <a
			href="/stacks">Thread Dump</a>, <a href="/logLevel">Log Level</a>,<a
			href="showgroup.jsp">Group Information</a>
	</p>
	<hr id="head_rule" />
	<%
	  final String defaultgroup = "0";
	  final String groupinfofilename = "groupinformation.conf";
	  boolean isprocess = ServerWithGroup.isIsprocess();
	  if (isprocess) {
	    if (ServerWithGroup.isIschangetable()) {
	      out.println("<br>System is change table...........");
	      out.println("<form name='flushform' action='#' method=post>");
	      out.println("<input type=submit name=submit value=Refresh style=\"width:200px;height:80px\" >");
	      out.println("</form>");
	    }
	  }
	  if (request.getParameter("groupname") != null) {
	    String group = request.getParameter("groupname");
	    out.println("<br><h4>Group Detail Information</h4>");
	    HMaster master = (HMaster) getServletContext().getAttribute(
	        HMaster.MASTER);
	    Map<String, List<String>> groupmap = new HashMap<String, List<String>>();
	    String line = "";
	    try {
	      line = ServerWithGroup.readGroupInfo(master, groupinfofilename);
	    } catch (IOException ex) {
	    }
	    groupmap = ServerWithGroup.initGroupMap(master, line);
	    if (groupmap.size() <= 0) {
	      out.println("No group Info ,error.......");
	      return;
	    }

	    Collection<HServerInfo> servercollect = master.getClusterStatus()
	        .getServerInfo();

	    List<String> serverlist = groupmap.get(group);
	    out.println("<h4>  group: " + group + " </h4>");
	    out.println("Need balance ? <a href=\"dogroupbalacne.jsp?groupname="
	        + group + "\">balance</a>");
	    out.println("<br>Server num :" + serverlist.size() + "<br><br>");
	    String spliter = GroupAssignmentManager.GROUP_SPLITER;
	    out.println("<TABLE BORDER=1>");
	    out.println("<TR><TD>Server name</TD>");
	    out.println("<TD>Server detail</TD>");
	    out.println("<TD>Table num</TD>");
	    out.println("<TD>Region num</TD></TR>");
	    List<HTableDescriptor> tablelist = new ArrayList<HTableDescriptor>();
	    List<HRegionInfo> regionlist = new ArrayList<HRegionInfo>();
	    for (String server : serverlist) {
	      out.println("<TR><TD>Server" + server + "</TD>");
	      List<HTableDescriptor> tables = ServerWithGroup
	          .listTableOnRegionServer(master, server);
	      for (HTableDescriptor table : tables) {
	        if (!tablelist.contains(table)) {
	          tablelist.add(table);
	        }
	      }
	      List<HRegionInfo> regions = ServerWithGroup.listRegionOnRegionServer(
	          master, server);
	      for (HRegionInfo region : regions) {
	        if (!regionlist.contains(region)) {
	          regionlist.add(region);
	        }
	      }
	      HServerInfo regionserver = null;
	      for (HServerInfo info : servercollect) {
	        String name = info.getHostname() + ","
	            + info.getServerAddress().getPort();
	        if (name.equals(server)) {
	          regionserver = info;
	        }
	      }
	      out.println("<TD><a href=\"http://" + regionserver.getHostname()
	          + ":" + regionserver.getInfoPort() + "/regionserver.jsp\">"
	          + server + "</a></TD>");
	      out.println("<TD>table num:" + tables.size() + "</TD>");
	      out.println("<TD>region num:" + regions.size() + "</TD></TR>");
	    }
	    out.println("</TABLE>");
	    out.println("<br><br>Table detail info in this group</br>");
	    out.println("<br>Total  Table num:" + tablelist.size());
	    out.println("<br>Total Region num:" + regionlist.size());
	    out.println("<br>If you want change table group ,new groups must be int ,and must be split with \""
	        + spliter + "\"<br>");
	    out.println("<br>The priority of table you want to change must between 1 and 10 ,and small num means high priority.<br>");
	    if (regionlist.size() <= 0)
	      return;
	    out.println("<TABLE BORDER=1>");
	    out.println("<TR><TD>Table name</TD>");
	    out.println("<TD>Region num</TD>");
	    out.println("<TD>Table Priority</TD>");
	    out.println("<TD>Belong Groups</TD>");
	    out.println("<TD>Change Groups</TD>");
	    out.println("<TD>Change Priority</TD>");
	    out.println("<TD>Submit</TD>");
	    out.println("<TD>Table Balance</TD></TR>");
	    out.println("<FORM name=\"tableform\" action=\"changetablegroup.jsp\" method=post >");
	    for (HTableDescriptor table : tablelist) {
	      String tableaddress = "http://"
	          + master.getMasterAddress().getHostname() + ":"
	          + master.getInfoServer().getPort() + "/table.jsp?name="
	          + table.getNameAsString();
	      out.println("<TR><TD><a href=" + tableaddress + ">"
	          + table.getNameAsString() + "</a></TD>");
	      int regionnum = 0;
	      for (HRegionInfo region : regionlist) {
	        HTableDescriptor regiontable = region.getTableDesc();
	        if (regiontable.equals(table)) {
	          regionnum++;
	        }
	      }
	      out.println("<TD>" + regionnum + "</TD>");
	      byte[] pvalue = table.getValue(Bytes.toBytes("priority"));
	      if (pvalue == null || pvalue.length <= 0) {
	        String tablename = table.getNameAsString();
	        if (tablename.equals("-ROOT-") || tablename.equals(".META.")) {
	          out.println("<TD>" + ScheduleHBaseServer.highestPri + "</TD>");
	        } else {
	          out.println("<TD>" + ScheduleHBaseServer.defaultPri + "</TD>");
	        }
	      } else {
	        out.println("<TD>" + Bytes.toString(pvalue) + "</TD>");
	      }
	      byte[] gvalue = table.getValue(GroupAssignmentManager.GROUP_KEY);

	      if (gvalue != null) {
	        out.println("<TD>" + Bytes.toString(gvalue) + "</TD>");
	      } else {
	        out.println("<TD>null</TD>");
	      }
	      String tablename = table.getNameAsString();
	      if (!tablename.equals("-ROOT-") && !tablename.equals(".META.")) {
	        out.println("<TD><input type=text name=" + table.getNameAsString()
	            + ".grp size=5 style=\"width:80%\"></TD>");
	        out.println("<TD><input type=text name=" + table.getNameAsString()
	            + ".pri size=5 style=\"width:80%\"></TD>");
	        out.println("<TD><input type=submit name='changetable' value='ChangeTable'></TD>");
	        out.println("<TD><a href=dotablebalance.jsp?tablename="
	            + table.getNameAsString() + ">balacne</a></TD>");
	      }
	      out.println("</TR>");
	    }
	    out.println("</FORM>");
	    out.println("</TABLE>");
	  }
	%>
</body>
</html>