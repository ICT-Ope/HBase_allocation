<%@ page language="java" 
	import="org.apache.hadoop.hbase.master.GroupAssignmentManager"
	import="org.apache.hadoop.hbase.allocation.group.ServerWithGroup"
	contentType="text/html; charset=GB18030"
    pageEncoding="GB18030"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=GB18030">
<title>Balance group</title>
</head>
<body>
<%
	/**
	 * Class used for balance group regions
	 */
	class Balancegroup implements Runnable{
		private String group;
		public Balancegroup(String group){
			this.group = group;
		}
		public void run(){
			try {
				ServerWithGroup.setIserror(false);
				synchronized(GroupAssignmentManager.class){
					GroupAssignmentManager.balanceGroup(group);
				}
			}catch(Exception ex){
				ServerWithGroup.setIserror(true);
				ServerWithGroup.setErrormsg("Group Balance error ,please check log...");
				ex.printStackTrace();
			}finally{
				synchronized (ServerWithGroup.class) {
					ServerWithGroup.setIsprocess(false);
					ServerWithGroup.setIsbalance(false);
				}
			}
		}
	}
%>
<%
	boolean isprocess = ServerWithGroup.isIsprocess();
	if (!isprocess) {
		if (request.getParameter("groupname") != null) {
			String group = request.getParameter("groupname");
			
			//start a thread
			synchronized (ServerWithGroup.class) {
				ServerWithGroup.setIsprocess(true);
				ServerWithGroup.setIsbalance(true);
			}
			Thread balancethread = new Thread(new Balancegroup(group));
			balancethread.start();
			out.println("<script type=\"text/javascript\">");
			out.println("alert(\"Balance group "+ group +" ............\")");
			out.println("window.location.href='groupinfo.jsp?groupname="+group+"'");
			out.println("</script>");
		}else{
			out.println("<script type=\"text/javascript\">");
			out.println("alert(\"Error, Set groupname first ............\")");
			out.println("history.back(-1)");
			out.println("</script>");
		}
	}else{
		out.println("<script type=\"text/javascript\">");
		out.println("alert(\"Wait, System busy............\")");
		out.println("history.back(-1)");
		out.println("</script>");
	}
%>
</body>
</html>