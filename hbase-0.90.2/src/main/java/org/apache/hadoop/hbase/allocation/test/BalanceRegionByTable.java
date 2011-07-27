package org.apache.hadoop.hbase.allocation.test;



import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.allocation.CheckMeta;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;


public class BalanceRegionByTable {
	static final Configuration conf = HBaseConfiguration.create();
	public static String table = "testPri";
 


	public static void main(String args[]) {
		try {
			while(true)
			{
					balanceRegion(true);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void balanceRegion(boolean auto) throws IOException {
		String regionPre="23";
		HBaseAdmin admin = new HBaseAdmin(conf);
		Collection<HServerInfo> serverInfo = admin.getClusterStatus()
				.getServerInfo();
		HashSet<String> regionPreList=new HashSet();
		HashMap<HServerAddress, String> serverCode = new HashMap<HServerAddress, String>();

		for (HServerInfo hinfo : serverInfo) {
			if (!serverCode.containsKey(hinfo)) {
				serverCode.put(
						hinfo.getServerAddress(),
						hinfo.getHostnamePort().replace(":", ",") + ","
								+ hinfo.getStartCode());
				// Map<HServerAddress,String>
			}
		}
		HTable ret = new HTable(conf, table);
		
		Map<HRegionInfo, HServerAddress> info = null;
		HashMap<HServerAddress, List<HRegionInfo>> serverR ;
		int div = 99999;
		
		int maxLoadN = 0;
		int minLoadN = 9999999;
		int flag=1;
		while (true) {
			try {
				//ret.close();
				//ret = new HTable(conf, table);
				
				div = 99999;
				
				maxLoadN = 0;
				minLoadN = 9999999;
				serverInfo = admin.getClusterStatus()
				.getServerInfo();
				
				serverCode = new HashMap<HServerAddress, String>();
				
				for (HServerInfo hinfo : serverInfo) {
					if (!serverCode.containsKey(hinfo)) {
						serverCode.put(
								hinfo.getServerAddress(),
								hinfo.getHostnamePort().replace(":", ",") + ","
										+ hinfo.getStartCode());
						// Map<HServerAddress,String>
					}
				}
				
				try {
					info = CheckMeta.getRegionAddress(ret.getTableDescriptor().getNameAsString());
					//info = ret.getRegionsInfo();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					try {
						Thread.currentThread().sleep(1000);
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					continue;
					//e.printStackTrace();
				}
				
				
				
				serverR = new HashMap<HServerAddress, List<HRegionInfo>>();
				for (HServerInfo hinfo : serverInfo) {
					serverR.put(hinfo.getServerAddress(), new ArrayList<HRegionInfo>());
				
				}

				for (HRegionInfo rinfo : info.keySet()) {
					if(Bytes.toString(rinfo.getStartKey()).length()<3)
						continue;
					if(!regionPreList.contains(Bytes.toString(rinfo.getStartKey()).subSequence(0, 3)))
					{
							regionPreList.add((String) Bytes.toString(rinfo.getStartKey()).subSequence(0, 3));
					}
				}
				flag++;
				if(flag>=regionPreList.size())
				{
					flag=0;
				}
				String []pres=new String[regionPreList.size()];
				if(pres.length==0)
				{
					Thread.currentThread().sleep(20000);
					continue;
				}
				regionPreList.toArray(pres);
				
				regionPre=pres[flag];
		
				for (HRegionInfo rinfo : info.keySet()) {
					if (serverR.get(info.get(rinfo)) == null) {
							serverR.put(info.get(rinfo), new ArrayList<HRegionInfo>());

					}
					
					serverR.get(info.get(rinfo)).add(rinfo);
						//serverR.put(info.get(rinfo), new ArrayList<HRegionInfo>());
				
					//serverR.get(info.get(rinfo)).add(rinfo);
				}
				HServerAddress maxLoad = null;
				HServerAddress minLoad = null;

				for (HServerAddress add : serverR.keySet()) {
					if (serverR.get(add).size() > maxLoadN) {
						maxLoadN = serverR.get(add).size();
						maxLoad = add;
					}
					if (serverR.get(add).size() < minLoadN) {
						minLoadN = serverR.get(add).size();
						minLoad = add;
					}
				}
				div = maxLoadN - minLoadN;
				System.out.println("max load:" + maxLoad.getHostname() + " maxN:"
						+ maxLoadN);
				System.out.println("min load:" + minLoad.getHostname() + " minN:"
						+ minLoadN);
				if(div<2)
				{
					try {
						Thread.currentThread().sleep(1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					continue;
				}
				

				HRegionInfo rtom = serverR.get(maxLoad).get(serverR.get(maxLoad).size()-1);
				serverR.get(maxLoad).remove(0);
				serverR.get(minLoad).add(rtom);
				byte[] rn = rtom.getEncodedNameAsBytes();
				// byte[] sern=maxLoad.;
				// admin.move(null, null);
				if (!auto) {
					System.out.println("move  region ? : [Yes|No]");
					java.util.Scanner scanner = new java.util.Scanner(System.in);
					String sret = scanner.nextLine();
					if (!sret.equals("Yes")) {
						System.out.println("Your anwser is not Yes,now exit.");
						System.exit(0);
					}
				}
				try {
					System.out.println("move " + rtom);
					System.out.println("move to " + serverCode.get(minLoad));
					Thread.currentThread().sleep(1000);
					admin.move(rtom.getEncodedNameAsBytes(),
							Bytes.toBytes(serverCode.get(minLoad)));
				
				} catch (Exception e) {
					// TODO Auto-generated catch block
					//e.printStackTrace();
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}
}
