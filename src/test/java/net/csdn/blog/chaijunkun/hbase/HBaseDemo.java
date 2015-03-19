package net.csdn.blog.chaijunkun.hbase;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HBase操作Demo
 *
 */
public class HBaseDemo {

	private static final Logger log = LoggerFactory.getLogger(HBaseDemo.class);

	private static final String zkSetKey = "hbase.zookeeper.quorum";

	private static final String zkConn = "192.168.108.143:2181,192.168.108.149:2181,192.168.109.206:2181";

	private static final String tableName = "cctv_logs";

	@Test
	public void doTest() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		Configuration config = HBaseConfiguration.create();
		config.set(zkSetKey, zkConn);
		HBaseAdmin hBaseAdmin = null;
		try{
			hBaseAdmin = new HBaseAdmin(config);
			ClusterStatus clusterStatus = hBaseAdmin.getClusterStatus();
			ServerName master = clusterStatus.getMaster();
			log.info("Master主机:{},端口号:{}", master.getHostname(), master.getPort());
			Collection<ServerName> servers = clusterStatus.getServers();
			for (ServerName serverName : servers) {
				log.info("Region主机{},端口号:{}", serverName.getHostname(), serverName.getPort());
			}
			HTableDescriptor targetTableDesc = null;
			try{
				targetTableDesc = hBaseAdmin.getTableDescriptor(TableName.valueOf(tableName));
				log.info("表已经存在,显示信息");
				log.info("属性:{}", targetTableDesc.toString());
			}catch(TableNotFoundException notFound){
				log.info("表不存在,创建");
				HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
				HColumnDescriptor hcd_info = new HColumnDescriptor("info");
				hcd_info.setMaxVersions(3);
				//一般建议打开压缩,除非对CPU使用率有严格限制,例如云主机.SNAPPY压缩算法由谷歌提出,BSD协议
				hcd_info.setCompressionType(Algorithm.SNAPPY);
				HColumnDescriptor hcd_data = new HColumnDescriptor("data");
				hcd_data.setCompressionType(Algorithm.SNAPPY);
				htd.addFamily(hcd_info);
				htd.addFamily(hcd_data);
				hBaseAdmin.createTable(htd);
				log.info("创建成功");
				targetTableDesc = hBaseAdmin.getTableDescriptor(TableName.valueOf(tableName));
				log.info("属性:{}", targetTableDesc.toString());
			}
			TableName[] listTableNames = hBaseAdmin.listTableNames();
			if (listTableNames == null){
				log.info("无表");
			}else{
				for (TableName tableName : listTableNames) {
					log.info("表名:{}", tableName.getNameAsString());
				}
			}
		}finally{
			IOUtils.closeQuietly(hBaseAdmin);
			log.info("结束");
		}
	}
	
	/**
	 * 高性能插入
	 * @throws IOException
	 */
//	@Test
	public void insertTest() throws IOException{
		Configuration config = HBaseConfiguration.create();
		config.set(zkSetKey, zkConn);
		HConnection conn = null;
		try{
			conn = HConnectionManager.createConnection(config);
			for (int i = 1; i < 1000000; i++) {
				HTableInterface table = null;
				try{
					table = conn.getTable(tableName);
					String appKey = XXTEAUtil.encrypt(String.valueOf(i));
					Put put = new Put(Bytes.toBytes(appKey));
					put.add(Bytes.toBytes("info"), Bytes.toBytes("begin"), Bytes.toBytes(String.valueOf(System.currentTimeMillis())));
					put.add(Bytes.toBytes("info"), Bytes.toBytes("end"), Bytes.toBytes(String.valueOf(System.currentTimeMillis())));
					put.add(Bytes.toBytes("data"), Bytes.toBytes("version"), Bytes.toBytes(String.valueOf(28)));
					log.info("准备插入:{}, {}",i, appKey);
					table.put(put);
					log.info("插入完成:{}, {}",i, appKey);
				}finally{
					IOUtils.closeQuietly(table);	
				}
			}
		}catch (Exception e){
			log.error("fail to put data", e);
		}finally{
			IOUtils.closeQuietly(conn);
		}
	}
	
	@Test
	public void getTest(){
		Configuration config = HBaseConfiguration.create();
		config.set(zkSetKey, zkConn);
		HConnection conn = null;
		try{
			conn = HConnectionManager.createConnection(config);
			for (int i = 1; i < 1000000; i++) {
				HTableInterface table = null;
				try{
					table = conn.getTable(tableName);
					String appKey = XXTEAUtil.encrypt(String.valueOf(i));
					Get get = new Get(Bytes.toBytes(appKey));
					Result result = table.get(get);
					if (result == null || result.isEmpty()){
						log.info("找不到记录:{}, {}", i, appKey);
					}else{
						log.info("/**********{}, {}***********/", i, appKey);
						List<Cell> columnInfoBegin = result.getColumnCells(Bytes.toBytes("info"), Bytes.toBytes("begin"));
						for (Cell cell : columnInfoBegin) {
							log.info("info:begin=>{}, timestamp:{}", Bytes.toLong(cell.getValueArray()), cell.getTimestamp());
						}
						List<Cell> columnInfoEnd = result.getColumnCells(Bytes.toBytes("info"), Bytes.toBytes("end"));
						for (Cell cell : columnInfoEnd) {
							log.info("info:end=>{}, timestamp:{}", Bytes.toLong(cell.getValueArray()), cell.getTimestamp());
						}
						List<Cell> columnDataVersion = result.getColumnCells(Bytes.toBytes("data"), Bytes.toBytes("version"));
						for (Cell cell : columnDataVersion) {
							log.info("data:version=>{}, timestamp:{}", Bytes.toLong(cell.getValueArray()), cell.getTimestamp());
						}
					}
				}finally{
					IOUtils.closeQuietly(table);	
				}
			}
		}catch (Exception e){
			log.error("fail to put data", e);
		}finally{
			IOUtils.closeQuietly(conn);
		}
	}
}
