package com.hadoop.hdfs.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.log4j.Logger;

import com.hadoop.hdfs.common.HdfsSystem;

/**
 * hdfs�ļ�����������
 * 
 * @author xujw
 * 
 */
public class HdfsUtil {

	private static Logger log = Logger.getLogger(HdfsUtil.class);

	/** hdfs�ļ�ϵͳ */
	private static DistributedFileSystem hdfs = HdfsSystem.getHdfs();

	/**
	 * ��ȡ����DataNode��������Ϣ
	 */
	public static DatanodeInfo[] listDataNodeInfo() {
		try {
			DatanodeInfo[] dataNodes = hdfs.getDataNodeStats();
			return dataNodes;
		} catch (Exception e) {
			e.printStackTrace();
			log.error("��ȡ����DataNode��������Ϣʧ��:", e);
		}
		return null;
	}

	/**
	 * �������ļ��ϴ���hadoop�ļ�ϵͳ��֧�ֽṹ���ǽṹ���ļ�
	 * 
	 * @param srcLocalPath
	 *            ����Ŀ¼���ļ�
	 * @param dstPath
	 *            hadoopĿ���ļ���Ŀ¼
	 */
	public static boolean uploadToHdfs(String srcLocalPath, String dstPath) {
		Path srcFile = new Path(srcLocalPath);
		Path dstFile = new Path(dstPath);
		try {
			hdfs.copyFromLocalFile(srcFile, dstFile);
			return true;
		} catch (IOException e) {
			log.error("�ļ��ϴ�ʧ��:", e);
		}
		return false;
	}

	/**
	 * ��hdfs�ļ�ϵͳ��ָ���ļ���Ŀ¼���ص�����
	 * 
	 * @param srcHdfsPath
	 *            hdfs�ļ�ϵͳ�ļ���Ŀ¼
	 * @param dstLocalPath
	 *            �����ļ���Ŀ¼
	 */
	public static boolean downloadFromHdfs(String srcHdfsPath,
			String dstLocalPath) {
		if (StringUtils.isEmpty(srcHdfsPath)
				|| StringUtils.isEmpty(dstLocalPath)) {
			log.error("�ļ���Ϊ�գ�����!");
			return false;
		}
		if (srcHdfsPath.startsWith("hdfs:")) {
			log.error("��ʹ��������hdfsϵͳ�ļ�·�������磺hdfs://master:9000/test.txt");
			return false;
		}
		Path srcFile = new Path(srcHdfsPath);
		Path dstFile = new Path(dstLocalPath);
		try {
			hdfs.copyToLocalFile(srcFile, dstFile);
			return true;
		} catch (IOException e) {
			log.error("�ļ�����ʧ��:", e);
		}
		return false;
	}

	/**
	 * �鿴�ļ��Ƿ����
	 */
	public void checkFileExist() {
		try {
			Path a = hdfs.getHomeDirectory();
			System.out.println("main path:" + a.toString());

			Path f = new Path("/user/xxx/input01/");
			boolean exist = hdfs.exists(f);
			System.out.println("Whether exist of this file:" + exist);

			// ɾ���ļ�
			// if (exist) {
			// boolean isDeleted = hdfs.delete(f, false);
			// if(isDeleted) {
			// System.out.println("Delete success");
			// }
			// }
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 *�����ļ���HDFSϵͳ��
	 */
	public void createFile() {
		try {
			Path f = new Path("/user/xxx/input02/file01");
			log.info("Create and Write :" + f.getName() + " to hdfs");
			FSDataOutputStream os = hdfs.create(f, true);
			Writer out = new OutputStreamWriter(os, "utf-8");// ��UTF-8��ʽд���ļ���������
			out.write("��� good job");
			out.close();
			os.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * ��ȡ�����ļ���HDFSϵͳ<br>
	 * �뱣֤�ļ���ʽһֱ��UTF-8���ӱ���->HDFS
	 */
	public void writeFileToHDFS() {
		try {
			Path f = new Path("/user/xxx/input02/file01");
			File file = new File("E:\\hadoopTest\\temporary.txt");

			FileInputStream is = new FileInputStream(file);
			InputStreamReader isr = new InputStreamReader(is, "utf-8");
			BufferedReader br = new BufferedReader(isr);

			FSDataOutputStream os = hdfs.create(f, true);
			Writer out = new OutputStreamWriter(os, "utf-8");

			String str = "";
			while ((str = br.readLine()) != null) {
				out.write(str + "\n");
			}
			br.close();
			isr.close();
			is.close();
			out.close();
			os.close();
			System.out.println("Write content of file " + file.getName()
					+ " to hdfs file " + f.getName() + " success");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * ȡ���ļ������ڵ�λ��..
	 */
	public void getLocation() {
		try {
			Path f = new Path("/user/xxx/input02/file01");
			FileStatus fileStatus = hdfs.getFileStatus(f);

			BlockLocation[] blkLocations = hdfs.getFileBlockLocations(
					fileStatus, 0, fileStatus.getLen());
			for (BlockLocation currentLocation : blkLocations) {
				String[] hosts = currentLocation.getHosts();
				for (String host : hosts) {
					System.out.println(host);
				}
			}

			// ȡ������޸�ʱ��
			long modifyTime = fileStatus.getModificationTime();
			Date d = new Date(modifyTime);
			System.out.println(d);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * ��ȡhdfs�е��ļ�����
	 */
	public void readFileFromHdfs() {
		try {
			Path f = new Path("/user/xxx/input02/file01");

			FSDataInputStream dis = hdfs.open(f);
			InputStreamReader isr = new InputStreamReader(dis, "utf-8");
			BufferedReader br = new BufferedReader(isr);
			String str = "";
			while ((str = br.readLine()) != null) {
				System.out.println(str);
			}
			br.close();
			isr.close();
			dis.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * ��ȡ����ָ��Ŀ¼�����ļ����ϴ���hdfs
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// Path p = new Path("hdfs://master:9000/d-pan");
		// System.out.println(hdfs);
		HdfsUtil ht = new HdfsUtil();
		ht.downloadFromHdfs("hdfs://master:9000/test", "d:/testxujw");
	}
}
