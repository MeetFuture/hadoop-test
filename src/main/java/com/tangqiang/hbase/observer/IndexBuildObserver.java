package com.tangqiang.hbase.observer;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;

/**
 * 使用协处理器建立索引
 *
 * @author tqiang
 * @email tqiang@grgbanking.com
 */
public class IndexBuildObserver implements RegionObserver {
	private Log LOG = LogFactory.getLog(IndexBuildObserver.class);

	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
		try {
			LOG.info("IndexBuildObserver begin build index !");
			Table table = e.getEnvironment().getConnection().getTable(TableName.valueOf("USER_INFO_INDEX"));

			List<Cell> kv = put.get(Bytes.toBytes("INFO"), Bytes.toBytes("BIRTHDAY"));
			Iterator<Cell> kvItor = kv.iterator();

			while (kvItor.hasNext()) {
				Cell tmp = kvItor.next();
				Put indexPut = new Put(Bytes.toBytes((Bytes.toString(tmp.getValueArray()) + System.currentTimeMillis())));
				indexPut.addColumn(Bytes.toBytes("INFO"), Bytes.toBytes("IDX"), put.getRow());
				table.put(indexPut);
			}
		} catch (Exception e1) {
			LOG.error("IndexBuildObserver Error !", e1);
		}
	}
}