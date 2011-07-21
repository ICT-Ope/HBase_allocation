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
<title>Change Table Group Info</title>
</head>
<body>
	<a id="logo" href="http://wiki.apache.org/lucene-hadoop/Hbase"><img src="/static/hbase_logo_med.gif" alt="HBase Logo" title="HBase Logo" /></a>
	<h1 id="page_title">Change Table</h1>
	<p id="links_menu"><a href="/master.jsp">Master</a>,<a href="/logs/">Local logs</a>, <a href="/stacks">Thread Dump</a>, <a href="/logLevel">Log Level</a>,<a href="showgroup.jsp">Group Information</a></p>

	<hr id="head_rule" />
	<%
		/**
		 * Class used for change table priority or groups
		 */
		class ChangeTableGP implements Runnable {
			private String[] groups;
			private String priporty;
			private String tablename;

			public ChangeTableGP(String[] groups, String priporty,
					String tablename) {
				this.groups = groups;
				this.priporty = priporty;
				this.tablename = tablename;
			}

			public void run() {
				synchronized (GroupAssignmentManager.class) {
					ServerWithGroup.setIserror(false);
					try {
						if (groups != null && groups.length > 0) {
							System.out.println("in a thread , before change table group");
							GroupAssignmentManager.setGroup(groups,
									tablename);
							System.out.println("in a thread , after change table group");
						}
						if (priporty != null && priporty.length()>0 ) {
							System.out.println("in a thread , before change table priporty");
							GroupAssignmentManager.setPriority(priporty,
									tablename);
							System.out.println("in a thread , after change table priporty");
						}
					} catch (Exception ex) {
						ServerWithGroup.setIserror(true);
						ServerWithGroup.setErrormsg("Change table property error ,please check log...");
						ex.printStackTrace();
					}finally{
						synchronized (ServerWithGroup.class) {
							ServerWithGroup.setIsprocess(false);
							ServerWithGroup.setIschangetable(false);
						}
					}
				}
			}
		}
		
		final String groupinfofilename = "groupinformation.conf";
		HMaster master = (HMaster) getServletContext().getAttribute(
				HMaster.MASTER);
		Map<String, List<String>> groupmap = new HashMap<String, List<String>>();
		String line = "";
		boolean isprocess = ServerWithGroup.isIsprocess();
		if (isprocess) {
			out.println("<script type=\"text/javascript\">");
			out.println("alert(\"System busy ,you can't change table group or pripority!!!\")");
			out.println("history.back(-1)");
			out.println("</script>");
			return;
		}
		try {
			line = ServerWithGroup.readGroupInfo(master, groupinfofilename);
		} catch (IOException ex) {
		}
		groupmap = ServerWithGroup.initGroupMap(master, line);
		if (groupmap.size() <= 0) {
			out.println("No group Info ,error.......");
			return;
		}
		List<HTableDescriptor> tablelist = ServerWithGroup
				.listAllTables(master);
		HTableDescriptor targettable1 = null;
		HTableDescriptor targettable2 = null;
		String newgroups = null;
		String priority = null;
		boolean needchangegrp = false;
		boolean needchangepri = false;
		for (HTableDescriptor table : tablelist) {
			System.out.println("table name is " + table.getNameAsString());
			if (request.getParameter(table.getNameAsString() + ".grp") != null) {
				newgroups = request.getParameter(table.getNameAsString()
						+ ".grp");
				if (newgroups.length() <= 0) continue;
				System.out.println("newgroups is " + newgroups);
				targettable1 = table;
				needchangegrp = true;
				break;
			}
		}
		for (HTableDescriptor table : tablelist) {
			if (request.getParameter(table.getNameAsString() + ".pri") != null) {
				priority = request.getParameter(table.getNameAsString()
						+ ".pri");
				if (priority.length() <= 0) continue;
				System.out.println("priority is " + priority);
				targettable2 = table;
				needchangepri = true;
				break;
			}
		}
		if (targettable1 == null && targettable2 == null) {
			out.println("<script type=\"text/javascript\">");
			out.println("alert(\"Table don't exist !!!\")");
			out.println("history.back(-1)");
			out.println("</script>");
		}
		if (needchangegrp && needchangepri) {
			if (!targettable1.equals(targettable2)) {
				out.println("<script type=\"text/javascript\">");
				out.println("alert(\"Error Table  confuse to change  !!!\")");
				out.println("history.back(-1)");
				out.println("</script>");
				return;
			}
		}
		if (!needchangegrp && !needchangepri) {
			out.println("<script type=\"text/javascript\">");
			out.println("alert(\"No table to change  !!!\")");
			out.println("history.back(-1)");
			out.println("</script>");
			return;
		}
		String[] groups = null;
		String tablename = null;
		if (needchangegrp) {
			System.out.println("need change group!!");
			tablename = targettable1.getNameAsString();
			if (tablename.equals("-ROOT-") || tablename.equals(".META."))
				return;
			System.out.println("newgroup " + newgroups);
			groups = newgroups.split(GroupAssignmentManager.GROUP_SPLITER);
			for (String gp : groups) {
				System.out.println("group " + gp);
				try {
					int i = Integer.valueOf(gp);
				} catch (Exception ex) {
					out.println("<script type=\"text/javascript\">");
					out.println("alert(\"Error , Wrong group format, group must be int!!!\")");
					out.println("history.back(-1)");
					out.println("</script>");
					return;
				}
				if (!groupmap.keySet().contains(gp)) {
					out.println("<script type=\"text/javascript\">");
					out.println("alert(\"Error ,This group don't exist!!!\")");
					out.println("history.back(-1)");
					out.println("</script>");
					return;
				}
				List<String> serverlist = groupmap.get(gp);
				if (serverlist.size() <= 0) {
					out.println("<script type=\"text/javascript\">");
					out.println("alert(\"Error ,This group don't contain regionserver!!!\")");
					out.println("history.back(-1)");
					out.println("</script>");
					return;
				}
			}
		}
		if (needchangepri) {
			System.out.println("need change priority!!, and priority is "+priority);
			tablename = targettable2.getNameAsString();
			try {
				int i = Integer.valueOf(priority);
				if (i<1 || i> 10){
					out.println("<script type=\"text/javascript\">");
					out.println("alert(\"Error , priority must between 1 and 10 !!!\")");
					out.println("history.back(-1)");
					out.println("</script>");
					return;
				}
			} catch (Exception ex) {
				out.println("<script type=\"text/javascript\">");
				out.println("alert(\"Error , Wrong priority format, priority must be int!!!\")");
				out.println("history.back(-1)");
				out.println("</script>");
				return;
			}
		}
		synchronized (ServerWithGroup.class) {
			ServerWithGroup.setIsprocess(true);
			ServerWithGroup.setIschangetable(true);
		}
		System.out.println("start change group or priority....,group is "+groups+" and pripority is "+priority+" and table is "+tablename);
		Thread chtable = new Thread(new ChangeTableGP(groups, priority,
				tablename));
		chtable.start();
		out.println("<script type=\"text/javascript\">");
		out.println("alert(\"Process Change table  ,may cost lots of  time ,please wait...\")");
		out.println("history.back(-1)");
		out.println("</script>");
	%>
</body>
</html>