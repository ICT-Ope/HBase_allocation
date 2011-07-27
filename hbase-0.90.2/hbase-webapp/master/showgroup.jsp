<%@ page language="java" import="java.util.*" import="java.io.*"
	import="org.apache.hadoop.hbase.master.HMaster"
	import="org.apache.hadoop.hbase.HTableDescriptor"
	import="org.apache.hadoop.hbase.HRegionInfo"
	import="org.apache.hadoop.hbase.HServerAddress"
	import="org.apache.hadoop.hbase.HServerInfo"
	import="org.apache.hadoop.hbase.allocation.group.ServerWithGroup"
	import="org.apache.hadoop.hbase.allocation.group.MoveGroupPlan"
	contentType="text/html; charset=GB18030" pageEncoding="GB18030"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=GB18030">
<title>Show Regionserver With Group</title>
<link rel="stylesheet" type="text/css" href="/static/hbase.css" />
<script type="text/javascript">
	function validate_moveform() {
		var result = confirm("Do you want to change regionserver's group information??");
		if (result == true) {
			return true;
		} else {
			return false;
		}
	}
</script>
</head>
<body>
	<a id="logo" href="http://wiki.apache.org/lucene-hadoop/Hbase"><img
		src="/static/hbase_logo_med.gif" alt="HBase Logo" title="HBase Logo" />
	</a>
	<h1 id="page_title">Show Regionserver Group Information</h1>
	<p id="links_menu">
		<a href="/master.jsp">Master</a>,<a href="/logs/">Local logs</a>, <a
			href="/stacks">Thread Dump</a>, <a href="/logLevel">Log Level</a>,<a
			href="showgroup.jsp">Group Information</a>
	</p>
	<hr id="head_rule" />
	<%
	  final String DEFAULT_GROUP = "0";
	  final String groupinfofilename = "groupinformation.conf";
	  HMaster master = (HMaster) getServletContext().getAttribute(
	      HMaster.MASTER);
	  Collection<HServerInfo> servercollect = master.getClusterStatus()
	      .getServerInfo();
	  Map<String, List<String>> groupmap = new HashMap<String, List<String>>();
	  Map<String, Boolean> grouppropertymap = new HashMap<String, Boolean>();

	  if (ServerWithGroup.isIserror()) {
	    out.println("<script type=\"text/javascript\">");
	    out.println("alert(\"Failure  warning , "
	        + ServerWithGroup.getErrormsg() + "!!!\")");
	    out.println("window.location.href='showgroup.jsp'");
	    out.println("</script>");
	    ServerWithGroup.setIserror(false);
	    return;
	  }
	  boolean isprocess = ServerWithGroup.isIsprocess();
	  if (isprocess) {
	    out.println("<TABLE BORDER=1 ALIGN=\"center\" WIDTH=\"60%\">");
	    out.println("<TR><TD>System is busy , you can't do any other operations.</TD></TR>");
	    if (ServerWithGroup.isIsmoveregion()) {
	      Set<MoveGroupPlan> movegroupPlanset = ServerWithGroup
	          .getMovegroupPlanset();
	      String currentserver = ServerWithGroup.getCurrentserver();
	      boolean doMoveconf = ServerWithGroup.isDoMoveconf();
	      out.println("<TR><TD>Change Regionserver Plan</TD></TR>");

	      for (MoveGroupPlan plan : movegroupPlanset) {
	        String servername = plan.getServername();
	        String originalgp = plan.getOriginalgroup();
	        String targetgp = plan.getTargetgroup();
	        ServerWithGroup.setCurrentserver(servername);
	        out.println("<TR><TD>Move " + servername + " form " + originalgp
	            + " to " + targetgp + "</TD>");

	        int remainsize = 0;
	        if (!plan.getStatus()) {
	          List<HRegionInfo> regionlist = ServerWithGroup
	              .listRegionOnRegionServer(master, servername);
	          remainsize = regionlist.size();
	        }
	        out.println("<TD>Remaining Region num :" + remainsize
	            + "</TD></TR>");
	        if (servername.equals(currentserver) && doMoveconf) {
	          out.println("<TR><TD>Update configuration</TD></TR>");
	        }
	      }
	    } else if (ServerWithGroup.isIsbalance()) {
	      out.println("<TR><TD>Waiting ,system is balance group or table</TD></TR>");
	    } else if (ServerWithGroup.isIschangetable()) {
	      out.println("<TR><TD>Waiting ,system is change table group or table priority</TD></TR>");
	    }
	    out.println("<TR><TD><form name='flushform' action='#' method=post>");
	    out.println("<input type=submit name=submit value=Refresh style=\"width:60%;height:30px\" >");
	    out.println("</form></TD></TR></TABLE>");
	  }
	  String line = "";
	  try {
	    line = ServerWithGroup.readGroupInfo(master, groupinfofilename);
	  } catch (IOException ex) {
	  }
	  groupmap = ServerWithGroup.initGroupMap(master, line);
	  grouppropertymap = ServerWithGroup.initGroupPropertyMap(master, line);
	  if (!grouppropertymap.keySet().contains(DEFAULT_GROUP)) {
	    grouppropertymap.put(DEFAULT_GROUP, false);
	  }
	  if (groupmap.size() <= 0) {
	    out.println("No group Info ,error.......");
	    out.println("alert(\"No group Info ,error.......!!!\")");
	    return;
	  }
	  //show groups
	  //show group list
	  out.println("<form name=groupform action=\"processgroup.jsp\" method=post>");
	  out.println("<TABLE BORDER=1 ALIGN=\"center\" WIDTH=\"60%\">");
	  out.println("<CAPTION><h2>List Group</h2></CAPTION>");
	  out.println("<TR>");
	  out.println("<TD>Group name</TD>");
	  out.println("<TD>Special Configuration</TD>");
	  out.println("<TD>Server num</TD>");
	  out.println("<TD>Region num</TD>");
	  out.println("<TD>Action</TD>");
	  out.println("<TD>Change Group Configuration Or Restart Group</TD>");
	  out.println("</TR>");
	  out.println("<TR>");
	  Map<String, Integer> servermap = new HashMap<String, Integer>();
	  for (String gp : groupmap.keySet()) {
	    int servernum = groupmap.get(gp).size();
	    if (!servermap.keySet().contains(gp)) {
	      servermap.put(gp, servernum);
	    } else {
	      Integer orinum = servermap.get(gp);
	      servermap.put(gp, servernum + orinum);
	    }
	  }
	  //out.println("Group size :"+groupmap.keySet().size());
	  for (String gp : groupmap.keySet()) {
	    boolean isspecial = grouppropertymap.get(gp);
	    out.println("<TR>");
	    out.println("<TD><a href=\"groupinfo.jsp?groupname=" + gp
	        + "\">group:   " + gp + "</a></TD>");
	    out.println("<TD>" + isspecial + "</TD>");
	    int servernum = servermap.get(gp);
	    out.println("<TD ><font color=blue>(" + servernum + ")</font></TD> ");
	    int regionsize = ServerWithGroup.getRegionNumOnGroup(master, gp,
	        groupmap);
	    out.println("<TD ><font color=black>(" + regionsize + ")</font></TD> ");
	    if (!gp.equals(DEFAULT_GROUP)) {
	      out.println("<TD><input type=submit name=" + gp
	          + ".del value=Delete ></TD>");
	      out.println("<TD><a href='changegroupconf.jsp'>Change</a></TD>");
	    } else {
	      out.println("<TD>default group</TD>");
	      out.println("<TD></TD>");
	    }

	    out.println("</TR>");
	  }
	  out.println("<TR>");
	  out.println("<TD>Add Group:</TD>");
	  out.println("<TD><input type=text name=addnewgroup></TD>");
	  out.println("<TD>Special ?<input type=checkbox name=setgroupproperty ></TD>");
	  out.println("<TD></TD>");
	  out.println("<TD><input type=submit name=groupchange value=AddNew></TD>");
	  out.println("</TR>");
	  out.println("</form>");
	  out.println("<br><br>");
	  //show regionserver groups
	  out.println("<form name=serverform action=\"processgroup.jsp\" method=post onSubmit=\"return validate_moveform()\">");

	  out.println("<TABLE BORDER=1 ALIGN=\"center\" WIDTH=\"60%\">");
	  out.println("<CAPTION><h2>RegionServer Group</h2></CAPTION>");
	  out.println("<TR>");
	  out.println("<TD>Region Server name</TD>");
	  out.println("<TD>Request num</TD>");
	  out.println("<TD>Region num</TD>");
	  out.println("<TD>Original Group</TD>");
	  out.println("<TD>Update to New Group</TD>");
	  out.println("</TR>");
	  for (Map.Entry<String, List<String>> entry : groupmap.entrySet()) {
	    String group = entry.getKey();
	    List<String> gplist = entry.getValue();
	    if (gplist.size() <= 0)
	      continue;
	    String[] gparray = gplist.toArray(new String[gplist.size()]);
	    Arrays.sort(gparray, new Comparator<String>() {
	      public int compare(String left, String right) {
	        return left.compareTo(right);
	      }
	    });
	    for (String server : gplist) {
	      out.println("<TR>");
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
	      out.println("<TD>" + regionserver.getLoad().getNumberOfRequests()
	          + "</TD>");
	      out.println("<TD>" + regionserver.getLoad().getNumberOfRegions()
	          + "</TD>");
	      out.println("<TD>" + group + "</TD>");
	      out.println("<TD><select name=" + server
	          + ".select size=1 style=\"width:80%\">");
	      out.println("<option value=null> </option>");
	      for (String gp : groupmap.keySet()) {
	        if (!gp.equals(group)) {
	          out.println("<option value=" + gp + " >" + gp + "</option>");
	        }
	      }
	      out.println("</select></TD>");
	      out.println("</TR>");
	    }
	  }
	  out.println("<TR>");
	  out.println("<TD></TD><TD></TD><TD></TD><TD></TD>");
	  out.println("<TD><input type=submit name=regionserverupdate value=UpdateRegionserverGroup style=\"width:80%;height:30px\"></TD>");
	  out.println("</TR>");
	  out.println("</TABLE>");
	  out.println("</form>");
	%>
</body>
</html>