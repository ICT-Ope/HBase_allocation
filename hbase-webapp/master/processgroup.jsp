<%@ page language="java" import="java.util.*" import="java.io.*"
	import="org.apache.hadoop.hbase.master.HMaster"
	import="org.apache.hadoop.hbase.HRegionInfo"
	import="org.apache.hadoop.hbase.HServerInfo"
	import="org.apache.hadoop.hbase.util.Bytes"
	import="org.apache.hadoop.hbase.UnknownRegionException"
	import="org.apache.hadoop.hbase.MasterNotRunningException"
	import="org.apache.hadoop.hbase.ZooKeeperConnectionException"
	import="org.apache.hadoop.hbase.HTableDescriptor"
	import="org.apache.hadoop.hbase.master.GroupAssignmentManager"
	import="org.apache.hadoop.hbase.allocation.group.ServerWithGroup"
	import="org.apache.hadoop.hbase.allocation.group.MoveGroupPlan"
	import="org.apache.hadoop.hbase.allocation.group.MoveConfImpl"
	import="org.apache.hadoop.hbase.allocation.group.ProcessMove"
	contentType="text/html; charset=GB18030" pageEncoding="GB18030"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=GB18030">
<title>Process Group</title>
</head>
<body>
	<%
		final String DEFAULT_GROUP = "0";
			final String groupinfofilename = "groupinformation.conf";
			HMaster master = (HMaster) getServletContext().getAttribute(
			HMaster.MASTER);
			Map<String, List<String>> groupmap = new HashMap<String, List<String>>();
			Map<String, Boolean> grouppropertymap = new HashMap<String, Boolean>();
			boolean isprocess = ServerWithGroup.isIsprocess();
			//out.println("Is moving regionservers ??? "+isprocess);
			if (!isprocess) {
		//get groupmap from hdfs
		String line = "";
		try {
			line = ServerWithGroup.readGroupInfo(master,
					groupinfofilename);
		} catch (IOException ex) {
		}
		groupmap = ServerWithGroup.initGroupMap(master, line);
		grouppropertymap=ServerWithGroup.initGroupPropertyMap(master,line);
		if(!grouppropertymap.keySet().contains(DEFAULT_GROUP)){
			grouppropertymap.put(DEFAULT_GROUP,false);
		}
		if (groupmap.size() <= 0) {
			out.println("No group Info ,error.......");
			return;
		}
		 boolean modifygroup = false;
		//add group
		if (request.getParameter("addnewgroup") != null) {
			String newgroup = request.getParameter("addnewgroup");
			Set<String> grouplist = groupmap.keySet();
			if (!grouplist.contains(newgroup)) {
				if (newgroup.length() > 0) {
					try {
						int id = Integer.valueOf(newgroup);
					} catch (NumberFormatException ex) {
						out.println("<script type=\"text/javascript\">");
						out.println("alert(\"Error , Groupname must be int!\")");
						out.println("window.location.href='showgroup.jsp'");
						out.println("</script>");
						return;
					}
					modifygroup = true;
					groupmap.put(newgroup, new ArrayList<String>());
					if (request.getParameter("setgroupproperty")!=null){
						grouppropertymap.put(newgroup,true);
					}else{
						grouppropertymap.put(newgroup,false);
					}
					out.println("<br>Add group " + newgroup
							+ " information successfully !!");
				}
			}else{
				out.println("<script type=\"text/javascript\">");
				out.println("alert(\"Error , Groupname must be unique , use another groupname!\")");
				out.println("window.location.href='showgroup.jsp'");
				out.println("</script>");
				return;
			}
		}
		//del group
		Set<String> grouplist = new HashSet<String>();
		for (String group : groupmap.keySet()) {
			grouplist.add(group);
		}
		for (String group : grouplist) {
			if (request.getParameter(group + ".del") != null) {
				List<String> rgplist = groupmap.get(group);
				if (rgplist != null && rgplist.size() > 0) {
					out.println("<script type=\"text/javascript\">");
					out.println("alert(\"Error , This group is not null ,move regionservers to other group first!!\")");
					out.println("window.location.href='showgroup.jsp'");
					out.println("</script>");
					return;
				} else {
					modifygroup = true;
					groupmap.remove(group);
					out.println("<br>Remove  group " + group
							+ " information successfully !!");
				}
			}
		}
		if(modifygroup){
			//write back
			ServerWithGroup.writeGroupInfo(master, groupinfofilename,
				groupmap,grouppropertymap);
			// tell someone else
			Thread initthread = new Thread(new Runnable(){
				public void run(){
					GroupAssignmentManager.initValue(false);
				}
			});
			initthread.start();
			out.println("<script type=\"text/javascript\">");
			out.println("window.location.href='showgroup.jsp'");
			out.println("</script>");
			return;
		}
		// move regionserver group
		Set<MoveGroupPlan> movegroupPlanset = new HashSet<MoveGroupPlan>();
		for (Map.Entry<String, List<String>> gp : groupmap.entrySet()) {
			String originalgroup = gp.getKey();
			for (String servername : gp.getValue()) {
				if (request.getParameter(servername + ".select") != null) {
					String newgroup = request.getParameter(servername
							+ ".select");
					if (!newgroup.equals("null")) {
						if (originalgroup.equals(DEFAULT_GROUP)) {
							if (groupmap.get(originalgroup).size() <= 1) {
								out.println("<script type=\"text/javascript\">");
								out.println("alert(\"Error , Default group 0 must have at least one regionserver, can't change !!\")");
								out.println("window.location.href='showgroup.jsp'");
								out.println("</script>");
								return;
							}
						}
						MoveGroupPlan newplan = new MoveGroupPlan(
								servername, originalgroup, newgroup);
						movegroupPlanset.add(newplan);
					}
				}
			}
		}
		
		//do move
		if (movegroupPlanset.size() <= 0) {
			return;
		} else {
			if (movegroupPlanset.size() >= 5) {
				out.println("<script type=\"text/javascript\">");
				out.println("alert(\"Error , At most change 5 regionserver once,please check carefully!!!\")");
				out.println("window.location.href='showgroup.jsp'");
				out.println("</script>");
				return;
			}
			boolean haveunmoveableregion = false;
			for (MoveGroupPlan plan : movegroupPlanset){
				String servername = plan.getServername();
				List<HRegionInfo> regionlist = new ArrayList<HRegionInfo>();
				try {
					regionlist = ServerWithGroup.listRegionOnRegionServer(master,servername);
					for (HRegionInfo info : regionlist ){
						if (info.isMetaRegion() || info.isRootRegion()){
							haveunmoveableregion = true;
							break;
						}
					}
				} catch (MasterNotRunningException e1) {
					e1.printStackTrace();
				} catch (ZooKeeperConnectionException e1) {
					e1.printStackTrace();
				}
				if (haveunmoveableregion)
					break;
			}
			if (haveunmoveableregion){
				out.println("<script type=\"text/javascript\">");
				out.println("alert(\"Failure  warning , because theas server has -ROOT- or .META. region, they can not be moved.!!!\")");
				out.println("window.location.href='showgroup.jsp'");
				out.println("</script>");
				return;
			}
			List<String> willmoveservers = new ArrayList<String>();			
			for (MoveGroupPlan plan : movegroupPlanset){
				String servername = plan.getServername();
				willmoveservers.add(servername);
			}
			boolean noserverfotable = false;
			HTableDescriptor lonetable = null;
			for (MoveGroupPlan plan : movegroupPlanset){
				String servername = plan.getServername();
				String originalgp = plan.getOriginalgroup();
				String targetgp = plan.getTargetgroup();
				if (originalgp.equals(targetgp)){
					willmoveservers.remove(servername);
					continue;
				}	
				List<HRegionInfo> regionlist = new ArrayList<HRegionInfo>();
				try {
					regionlist = ServerWithGroup.listRegionOnRegionServer(master,servername);
				} catch (MasterNotRunningException e1) {
					e1.printStackTrace();
				} catch (ZooKeeperConnectionException e1) {
					e1.printStackTrace();
				}
				if (regionlist== null) continue;
				//find absolate table 
				for (HRegionInfo hri : regionlist) {
					String tablename = hri.getTableDesc().getNameAsString();
					List<HServerInfo> availalbeserver = GroupAssignmentManager.getAvailableServer(tablename);
					List<String> availservernames = new ArrayList<String>();
					for (HServerInfo hsr : availalbeserver){
							String name = hsr.getHostname()+","+hsr.getServerAddress().getPort();
							//cancel will delete server
							if (!willmoveservers.contains(name)){
								availservernames.add(name);
							}
						}
					if (availservernames.size() <= 0){
						noserverfotable = true;	
						lonetable = hri.getTableDesc();
						break;
					}
				}
				if (noserverfotable ){
						break;
				}
			}
			if (noserverfotable ){
				out.println("<script type=\"text/javascript\">");
				out.println("alert(\"Failure  warning , because the table "+lonetable.getNameAsString()+" has no available regionserver, you can change its group property first ,and then change this regionserver's group!!!\")");
				out.println("window.location.href='showgroup.jsp'");
				out.println("</script>");
				return;
			}		
			//start a thread
			synchronized (ServerWithGroup.class) {
				ServerWithGroup.setIsprocess(true);
				ServerWithGroup.setIsmoveregion(true);
			}
			String currentdir = request.getRealPath("/");
			ProcessMove pgromove = new ProcessMove(
					movegroupPlanset, groupmap,grouppropertymap, master,
					currentdir);
			Thread movethread = new Thread(pgromove);
			movethread.start();
			out.println("<script type=\"text/javascript\">");
			out.println("alert(\"System is processing move group,please wait...\")");
			out.println("window.location.href='showgroup.jsp'");
			out.println("</script>");
		}
			}
			out.println("<script type=\"text/javascript\">");
			out.println("alert(\"System busy ,wait move group process over !!!\")");
			out.println("history.back(-1)");
			out.println("</script>");
	%>
</body>
</html>