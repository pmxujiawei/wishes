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
 * hdfs文件操作工具类
 * 
 * @author xujw
 * 
 */
public class HdfsUtil {

	private static Logger log = Logger.getLogger(HdfsUtil.class);

	/** hdfs文件系统 */
	private static DistributedFileSystem hdfs = HdfsSystem.getHdfs();

	/**
	 * 获取所有DataNode的名字信息
	 */
	public static DatanodeInfo[] listDataNodeInfo() {
		try {
			DatanodeInfo[] dataNodes = hdfs.getDataNodeStats();
			return dataNodes;
		} catch (Exception e) {
			e.printStackTrace();
			log.error("获取所有DataNode的名字信息失败:", e);
		}
		return null;
	}

	/**
	 * 将本地文件上传至hadoop文件系统，支持结构化非结构化文件
	 * 
	 * @param srcLocalPath
	 *            本地目录或文件
	 * @param dstPath
	 *            hadoop目标文件或目录
	 */
	public static boolean uploadToHdfs(String srcLocalPath, String dstPath) {
		Path srcFile = new Path(srcLocalPath);
		Path dstFile = new Path(dstPath);
		try {
			hdfs.copyFromLocalFile(srcFile, dstFile);
			return true;
		} catch (IOException e) {
			log.error("文件上传失败:", e);
		}
		return false;
	}

	/**
	 * 将hdfs文件系统中指定文件或目录下载到本地
	 * 
	 * @param srcHdfsPath
	 *            hdfs文件系统文件或目录
	 * @param dstLocalPath
	 *            本地文件或目录
	 */
	public static boolean downloadFromHdfs(String srcHdfsPath,
			String dstLocalPath) {
		if (StringUtils.isEmpty(srcHdfsPath)
				|| StringUtils.isEmpty(dstLocalPath)) {
			log.error("文件名为空，请检查!");
			return false;
		}
		if (srcHdfsPath.startsWith("hdfs:")) {
			log.error("请使用完整的hdfs系统文件路径，例如：hdfs://master:9000/test.txt");
			return false;
		}
		Path srcFile = new Path(srcHdfsPath);
		Path dstFile = new Path(dstLocalPath);
		try {
			hdfs.copyToLocalFile(srcFile, dstFile);
			return true;
		} catch (IOException e) {
			log.error("文件下载失败:", e);
		}
		return false;
	}

	/**
	 * 查看文件是否存在
	 */
	public void checkFileExist() {
		try {
			Path a = hdfs.getHomeDirectory();
			System.out.println("main path:" + a.toString());

			Path f = new Path("/user/xxx/input01/");
			boolean exist = hdfs.exists(f);
			System.out.println("Whether exist of this file:" + exist);

			// 删除文件
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
	 *创建文件到HDFS系统上
	 */
	public void createFile() {
		try {
			Path f = new Path("/user/xxx/input02/file01");
			log.info("Create and Write :" + f.getName() + " to hdfs");
			FSDataOutputStream os = hdfs.create(f, true);
			Writer out = new OutputStreamWriter(os, "utf-8");// 以UTF-8格式写入文件，不乱码
			out.write("你好 good job");
			out.close();
			os.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 读取本地文件到HDFS系统<br>
	 * 请保证文件格式一直是UTF-8，从本地->HDFS
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
	 * 取得文件块所在的位置..
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

			// 取得最后修改时间
			long modifyTime = fileStatus.getModificationTime();
			Date d = new Date(modifyTime);
			System.out.println(d);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 读取hdfs中的文件内容
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
	 * 读取本地指定目录数据文件并上传至hdfs
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
