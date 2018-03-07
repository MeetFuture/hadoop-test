package com.tangqiang;

import com.tangqiang.hbase.count.HBaseCountByRegion;
import com.tangqiang.hbase.insert.HBaseInsert;
import com.tangqiang.mapreduce.NumberCount;
import com.tangqiang.mapreduce.WordCount;

/**
 * Test运行的主函数
 *
 * @author tqiang
 * @email tqiang@grgbanking.com
 */
public class TestMain {

	public static void main(String[] args) {
		try {
			ProgramDriver pd = new ProgramDriver();
			pd.addClass(WordCount.name, WordCount.class, WordCount.desc);
			pd.addClass(NumberCount.name, NumberCount.class, NumberCount.desc);

			pd.addClass(HBaseInsert.name, HBaseInsert.class, HBaseInsert.desc);

			pd.addClass(HBaseCountByRegion.name, HBaseCountByRegion.class, HBaseCountByRegion.desc);
			// 运行
			System.exit(pd.run(args));
		} catch (Throwable e) {
			e.printStackTrace();
		}

		System.exit(-1);
	}

}
