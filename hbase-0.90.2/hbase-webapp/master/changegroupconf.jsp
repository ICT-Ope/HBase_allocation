<%@ page language="java" import="java.io.*" import="java.util.*"
	import="org.apache.hadoop.hbase.master.HMaster"
	import="org.apache.hadoop.hbase.HTableDescriptor"
	import="org.apache.hadoop.hbase.HRegionInfo"
	import="org.apache.hadoop.hbase.HServerAddress"
	import="org.apache.hadoop.hbase.HServerInfo"
	import="org.apache.hadoop.hbase.ClusterStatus"
	import="org.apache.hadoop.hbase.master.GroupAssignmentManager"
	import="org.apache.hadoop.hbase.allocation.group.ServerWithGroup"
	import="org.apache.hadoop.hbase.allocation.group.MoveConfImpl"
	contentType="text/html; charset=GB18030" pageEncoding="GB18030"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=GB18030">
<link rel="stylesheet" type="text/css" href="/static/hbase.css" />
<title>Distribute Group hbase-site.xml</title>
<script type="text/javascript">
	function validate_distform() {

		var result = confirm("Do you want to distribute group hbase-site.xml or restart group regionservers??");
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
	<h1 id="page_title">Distribute and Restart Group</h1>
	<p id="links_menu">
		<a href="/master.jsp">Master</a>,<a href="/logs/">Local logs</a>, <a
			href="/stacks">Thread Dump</a>, <a href="/logLevel">Log Level</a>,<a
			href="showgroup.jsp">Group Information</a>
	</p>
	<hr id="head_rule" />
	<%
	  class Distribute implements Runnable {
	    private HMaster master;
	    private List<String> groupserverlist;
	    private String currentdir;
	    private String command;

	    public Distribute(HMaster master, List<String> groupserverlist,
	        String currentdir, String command) {
	      this.master = master;
	      this.groupserverlist = groupserverlist;
	      this.currentdir = currentdir;
	      this.command = command;
	    }

	    public void run() {
	      if (command.equals("distconf")) {
	        MoveConfImpl moveimpl = new MoveConfImpl(currentdir);
	        for (String serveraddr : groupserverlist) {
	          try {
	            String addr = serveraddr.substring(0, serveraddr.indexOf(","));
	            System.out.println("Before Distribute " + addr
	                + " configuration");
	            //distribute new configuration
	            moveimpl.DistributeConf(addr, "distribute");
	            System.out.println("After Distribute configuration");
	          } catch (Exception e) {
	            e.printStackTrace();
	          }
	        }
	      } else if (command.equals("restart")) {
	        synchronized (GroupAssignmentManager.class) {
	          MoveConfImpl moveimpl = new MoveConfImpl(currentdir);
	          for (String serveraddr : groupserverlist) {
	            try {
	              String addr = serveraddr
	                  .substring(0, serveraddr.indexOf(","));
	              System.out.println("Before Restart " + addr
	                  + " configuration");
	              moveimpl.ImplRegionServer(addr, "stop");
	              moveimpl.ImplRegionServer(addr, "start");
	              System.out.println("After Restart configuration");
	            } catch (Exception e) {
	              e.printStackTrace();
	            }
	          }

	          while (true) {
	            int onlinenum = 0;
	            ClusterStatus status = master.getClusterStatus();
	            Collection<HServerInfo> servers = status.getServerInfo();
	            for (HServerInfo info : servers) {
	              String onlineserver = info.getHostname() + ","
	                  + info.getServerAddress().getPort();
	              if (groupserverlist.contains(onlineserver)) {
	                onlinenum++;
	              }
	            }
	            if (groupserverlist.size() != onlinenum) {
	              try {
	                Thread.sleep(1000);
	              } catch (InterruptedException ex) {
	                ex.printStackTrace();
	              }
	              continue;
	            } else {
	              break;
	            }
	          }
	        }
	      }
	      synchronized (ServerWithGroup.class) {
	        ServerWithGroup.setIsprocess(false);
	        ServerWithGroup.setDoMoveconf(false);
	      }
	    }
	  }
	%>
	<%
	  final String DEFAULT_GROUP = "0";
	  final String groupinfofilename = "groupinformation.conf";
	  HMaster master = (HMaster) getServletContext().getAttribute(
	      HMaster.MASTER);
	  String CURRENTDIR = request.getRealPath("/");

	  Map<String, List<String>> groupmap = new HashMap<String, List<String>>();
	  List<String> groupserverlist = new ArrayList<String>();
	  String submit = request.getParameter("cfsubmit");
	  boolean isprocess = ServerWithGroup.isIsprocess();

	  if (!isprocess) {
	    String line = "";
	    try {
	      line = ServerWithGroup.readGroupInfo(master, groupinfofilename);
	    } catch (IOException ex) {
	    }
	    groupmap = ServerWithGroup.initGroupMap(master, line);
	    String configuration = null;
	    if (request.getParameter("configurename") != null) {
	      configuration = request.getParameter("configurename");
	      if (configuration.length() > 0) {
	        configuration = configuration.replaceAll("[&]lt;", "<");
	        configuration = configuration.replaceAll("[&]gt;", ">");
	        configuration = configuration.replaceAll("[&]quot;", "\"");
	        File xmlfile = new File(CURRENTDIR + "/hbase-site.xml");
	        BufferedWriter bw = new BufferedWriter(new FileWriter(xmlfile));
	        bw.write(configuration);
	        bw.close();
	      }
	    }
	    if (request.getParameter("groupname") != null) {
	      String group = request.getParameter("groupname");
	      if (group.equals("null")) {
	        out.println("<script type=\"text/javascript\">");
	        out.println("alert(\"Warning , please choose right group...\")");
	        out.println("history.back(-1)");
	        out.println("</script>");
	        return;
	      }

	      groupserverlist = groupmap.get(group);
	      if (groupserverlist == null || groupserverlist.size() <= 0) {
	        out.println("<script type=\"text/javascript\">");
	        out.println("alert(\"Warning , there is no regionserver in this group.\")");
	        out.println("history.back(-1)");
	        out.println("</script>");
	      }
	      if (submit != null && submit.length() > 0) {
	        if (submit.equals("Distribute")) {
	          if (configuration != null && configuration.length() > 0) {
	            out.println("Distribute group " + group);
	            synchronized (ServerWithGroup.class) {
	              ServerWithGroup.setIsprocess(true);
	              ServerWithGroup.setDoMoveconf(true);
	            }
	            Thread DistributeThread = new Thread(new Distribute(master,
	                groupserverlist, CURRENTDIR, "distconf"));
	            DistributeThread.setName("Distribute Thread");
	            DistributeThread.start();
	            out.println("<script type=\"text/javascript\">");
	            out.println("alert(\"Ready to distribute hbase-site.xml ,please wait...\")");
	            out.println("window.location.href='changegroupconf.jsp'");
	            out.println("</script>");
	            return;
	          } else {
	            out.println("<script type=\"text/javascript\">");
	            out.println("alert(\"Hbase-site.xml is null ,please write it.\")");
	            out.println("history.back(-1)");
	            out.println("</script>");
	            return;
	          }
	        } else if (submit.equals("Restart")) {
	          out.println("Restart group " + group);
	          synchronized (ServerWithGroup.class) {
	            ServerWithGroup.setIsprocess(true);
	            ServerWithGroup.setDoMoveconf(true);
	          }
	          Thread RestartThread = new Thread(new Distribute(master,
	              groupserverlist, CURRENTDIR, "restart"));
	          RestartThread.setName("Restart Thread");
	          RestartThread.start();
	          out.println("<script type=\"text/javascript\">");
	          out.println("alert(\"Ready to restart group regionservers ,please wait...\")");
	          out.println("window.location.href='changegroupconf.jsp'");
	          out.println("</script>");
	          return;
	        }
	      }

	    }
	  } else {
	    boolean ismoving = ServerWithGroup.isDoMoveconf();
	    if (ismoving) {
	      out.println("<center>");
	      out.println("<form name='flushform' action='#' method=post>");
	      out.println("<input type=submit name=submit value=Refresh style=\"width:60%;height:30px\" >");
	      out.println("</form>");
	      out.println("</center>");
	    } else {
	      out.println("<script type=\"text/javascript\">");
	      out.println("alert(\"System is busy ,do it later , back to home \")");
	      out.println("window.location.href='showgroup.jsp'");
	      out.println("</script>");

	    }
	  }
	  out.println("<center>");
	  out.println("<form name=distform action='#' method='post' onSubmit=\"return validate_distform()\">");
	  out.println("<br><font size=16 >Choose Group:</font>");
	  out.println("<select name=groupname size=1 style=\"width:160px\">");
	  out.println("<option value=null > </option>");
	  for (String gp : groupmap.keySet()) {
	    if (!gp.equals(DEFAULT_GROUP)) {
	      out.println("<option value=" + gp + " >" + gp + "</option>");
	    }
	  }
	  out.println("</select><br>");

	  out.println("<br>New hbase-site.xml:<br>");
	  out.println("<textarea cols=60 rows=25 name=configurename></textarea>");
	  out.println("<br><br>Distribute group configure:<input type=submit name=cfsubmit value=\"Distribute\" style=\"width:20%;height:30px\">");
	  out.println("<br><br>Restart group regionserver:<input type=submit name=cfsubmit value=\"Restart\" style=\"width:20%;height:30px\">");
	  out.println("</form>");
	  out.println("</center>");
	%>
</body>
</html>