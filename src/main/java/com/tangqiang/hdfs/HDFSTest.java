package com.tangqiang.hdfs;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 测试HDFS
 *
 * @author tqiang
 * @email tqiang@grgbanking.com
 */
public class HDFSTest extends Thread {
	protected Logger logger = LoggerFactory.getLogger(getClass());
	private static String hdfsURI = "hdfs://master:8020";
	private static String hdfsPath = "/test/tom/";
	private static String localPath = "./";

	private static int count = 0;

	private static FileSystem fs;
	static {
		try {
			fs = FileSystem.get(new URI(hdfsURI), new Configuration());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		for (int i = 0; i < 1; i++) {
			HDFSTest ht = new HDFSTest();
			ht.setName("" + i);
			ht.start();
		}

	}

	public void run() {
		try {
			hdfsConfig();

			while (true) {
				String remotePath = hdfsPath + new SimpleDateFormat("yyyyMMdd/HH/mm/").format(new Date());
				putFileToHDFS(createLocalFile(localPath), remotePath);
				logger.info("Already write " + (++count) + " Files to HDFS !");
			}
		} catch (Exception e) {
			logger.error("Error ", e);
		}

		// for (int i = 0; i < 1000; i++) {
		// ht.putFileToHDFS(ht.createLocalFile());
		// }
		// ht.getFileToLoacl("/test/tom/ip.txt",localPath+"ip.txt");
	}

	private void hdfsConfig() throws Exception {
		logger.info("HomeDirectory:" + fs.getHomeDirectory());
		logger.info("WorkingDirectory:" + fs.getWorkingDirectory());
	}

	/**
	 * 将文件从HDFS拿取到本地
	 * 
	 * @param hdfsPath
	 * @throws Exception
	 */
	private void getFileToLoacl(String hdfsPath, String localPath) throws Exception {
		logger.info("---Begin get file to local ,HDFS Path : " + hdfsPath + " LocalPath : " + localPath);
		fs.copyToLocalFile(false, new Path(hdfsPath), new Path(localPath));
		logger.info("---End get file ");
	}

	/**
	 * 将文件存储至HDFS
	 * 
	 * @param localFile
	 * @throws Exception
	 */
	private void putFileToHDFS(File localFile, String hdfsPath) throws Exception {
		long begin = System.currentTimeMillis();
		long fileSize = localFile.length();
		logger.info("---Begin put file[" + localFile.getAbsolutePath() + "] to hdfs :" + hdfsPath);
		// fs.delete(new Path(hdfsPath+localFile.getName()), true);
		if (!fs.exists(new Path(hdfsPath))) {
			fs.mkdirs(new Path(hdfsPath));
		}
		fs.copyFromLocalFile(true, new Path(localFile.getAbsolutePath()), new Path(hdfsPath + localFile.getName()));
		long timeUsed = System.currentTimeMillis() - begin;
		logger.info("---End put file ,UsedTime : " + timeUsed + "/ms  Speed : " + fileSize / timeUsed / 1.024 / 1024 + " MB/s");
	}

	/**
	 * 创建文件
	 * 
	 * @return
	 * @throws Exception
	 */
	private File createLocalFile(String localPath) throws Exception {
		long begin = System.currentTimeMillis();
		File file = new File(localPath + "Text" + getName() + begin + ".txt");
		BufferedWriter br = new BufferedWriter(new FileWriter(file));
		for (int i = 0; i < 10000; i++) {
			br.write(getLine() + "\n");
		}
		br.flush();
		br.close();
		logger.info("--Build File UsedTime : " + (System.currentTimeMillis() - begin) + "/ms");
		return file;
	}

	/**
	 * 获取随机文件内容
	 * 
	 * @return
	 */
	private String getLine() {
		StringBuilder sb = new StringBuilder(1000);
		for (int i = 0; i < 1000; i++) {
			sb.append((int) (Math.random() * 10));
		}
		return sb.toString();
	}
}
