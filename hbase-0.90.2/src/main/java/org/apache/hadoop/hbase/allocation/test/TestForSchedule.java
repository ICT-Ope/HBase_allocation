package org.apache.hadoop.hbase.allocation.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
/**
 * Test for priority server
 * @author liujia_l.pt
 *
 */
public class TestForSchedule {
	static final Configuration conf=HBaseConfiguration.create();
	static final Random r=new Random();
	public static void main(String args[])
	{
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(conf);
		} catch (MasterNotRunningException e2) {
			e2.printStackTrace();
		} catch (ZooKeeperConnectionException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		HTableDescriptor des;
	try{
			//HBaseAdmin admin=new HBaseAdmin(conf);
			des=new HTableDescriptor ("testPri8");
			des.addFamily(new HColumnDescriptor ("ff"));
			des.setValue(Bytes.toBytes("priority"), Bytes.toBytes(1+""));
			admin.createTable(des);
	} catch (MasterNotRunningException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (ZooKeeperConnectionException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	try{
			des=new HTableDescriptor ("testPri7");
			des.addFamily(new HColumnDescriptor ("ff"));
			des.setValue(Bytes.toBytes("priority"), Bytes.toBytes(5+""));
			admin.createTable(des);
	} catch (MasterNotRunningException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (ZooKeeperConnectionException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	try
	{
			des=new HTableDescriptor ("testPri6");
			des.addFamily(new HColumnDescriptor ("ff"));
			des.setValue(Bytes.toBytes("priority"), Bytes.toBytes(10+""));
			admin.createTable(des);
			//HTable t=new HTable("");
	
	} catch (MasterNotRunningException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (ZooKeeperConnectionException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}try{
			des=new HTableDescriptor ("testPri5");
			des.addFamily(new HColumnDescriptor ("ff"));
			des.setValue(Bytes.toBytes("priority"), Bytes.toBytes(20+""));
			admin.createTable(des);
	} catch (MasterNotRunningException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (ZooKeeperConnectionException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
		
		for(int i=0;i<10;i++)
		{
		
			new Thread()
			{
				public void run()
				{
					HTable t = null;
					try {
						t = new HTable(conf,"testPri8");
						t.setAutoFlush(false);
						t.setWriteBufferSize(1024*1024);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					try {
						TestForSchedule.testT(t);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}.start();
		
			new Thread()
			{
				public void run()
				{
					HTable t = null;
					try {
						t = new HTable(conf,"testPri7");
						t.setAutoFlush(false);
						t.setWriteBufferSize(1024*1024);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	
					try {
						TestForSchedule.testT(t);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}
			}.start();
			
			new Thread()
			{
				public void run()
				{
					HTable t = null;
					try {
						t = new HTable(conf,"testPri6");
						t.setAutoFlush(false);
						t.setWriteBufferSize(1024*1024);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					try {
						TestForSchedule.testT(t);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				}
			}.start();
			new Thread()
			{
				public void run()
				{
					HTable t = null;
					try {
						t = new HTable(conf,"testPri5");		t.setAutoFlush(false);
						t.setWriteBufferSize(1024*1024);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					try {
						TestForSchedule.testT(t);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}.start();
		}
			
	
	}
	public static void putTest(HTable t,int i)
	{
		Put p=new Put(Bytes.toBytes(""+r.nextInt(1000000000)+(i)+r.nextInt(1000000000)));
		
		p.add(Bytes.toBytes("ff"), Bytes.toBytes("ff"), Bytes.toBytes("ff"+i+r.nextInt(1000000000)+r.nextInt(1000000000)+"fffffffffffffffffffffffffffffffffffffffffffffffffffff"+i));
		try {
			t.put(p);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	public static Result scanTest(ResultScanner sn)
	
	{
		try {
			return sn.next();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;

	}
	static int inter=0;
	public static void  testT(HTable t) throws IOException
	{
		int i=0;
		long start=System.currentTimeMillis();
		long end =System.currentTimeMillis();
		int pri=
			Integer.parseInt(Bytes.toString(t.getTableDescriptor().getValue(Bytes.toBytes("priority"))));
		Scan s=new Scan();
		s.setStartRow(Bytes.toBytes(r.nextInt(10)));
		t.setScannerCaching(1000);
		t.setAutoFlush(false);
		t.setWriteBufferSize(1000*6000);
		ResultScanner sn=t.getScanner(s);
		while(true)
		{
			if(i%10000==0)
			{
				end=System.currentTimeMillis();
				System.out.println("table"+t.getTableDescriptor().getNameAsString()+" pri :"+pri+" time:"+ (end-start));
				start=end;
			}
			//scanTest(sn);
			putTest(t,i);
			i++;

		}
	}

}
