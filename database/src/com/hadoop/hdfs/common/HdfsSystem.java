package com.hadoop.hdfs.common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.log4j.Logger;

/**
 * Hadoop�ļ�ϵͳ��ͨ��
 * 
 * @author xujw
 * 
 */
public class HdfsSystem {

	private static Logger log = Logger.getLogger(HdfsSystem.class);

	private static Configuration conf = new Configuration();

	private static FileSystem fs;

	private static DistributedFileSystem hdfs;

	static {
		try {
			conf.set("fs.defaultFS", HdfsConst.ADDRESS);
			fs = FileSystem.get(conf);
			hdfs = (DistributedFileSystem) fs;
		} catch (Exception e) {
			log.error("��ʼ���ļ�ϵͳ����", e);
		}
	}

	/**
	 * ��ȡ�ֲ�ʽ�ļ�ϵͳ����
	 * 
	 * @return hdfs����
	 */
	public static DistributedFileSystem getHdfs() {
		return hdfs;
	}

	public static void main(String[] args) {
		Path dstFile = new Path("/test2/" + Math.random());
		System.out.println(FileSystem.DEFAULT_FS);
		log.info("uploading...");
		long startTime = System.currentTimeMillis() / 1000;
		try {
			Path localFile = new Path("E:/NetFx20SP2_x86.zip");
			hdfs.copyFromLocalFile(localFile, dstFile);
		} catch (IOException e) {
			e.printStackTrace();
			log.error("error:", e);
		}
		long endTime = System.currentTimeMillis() / 1000;
		log.info("upload successful!!!");
		log.info("�����ʱ��" + (endTime - startTime));
	}
}
