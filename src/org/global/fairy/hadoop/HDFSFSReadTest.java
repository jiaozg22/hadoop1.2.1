package org.global.fairy.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;
import org.global.fairy.hadoop.utils.HDFSFSUtil;
import org.junit.Test;

public class HDFSFSReadTest {
	/**
	 * 读取文件内容
	 * 
	 * @throws Exception
	 */
	@Test
	public void testRead() throws Exception {
		// 获取文件系统fs对象
		FileSystem hdfs = HDFSFSUtil.getFileSystem();

		// 文件名称
		Path path = new Path("/opt/data/test3s/dd/touch.data");

		// 打开文件输入流
		FSDataInputStream inputStream = hdfs.open(path);

		// 读取文件到控制台显示
		IOUtils.copyBytes(inputStream, System.out, 4096, false);

		// 关闭文件流
		IOUtils.closeStream(inputStream);

	}

	/**
	 * 查看路径
	 * 
	 * @throws IOException
	 */
	@Test
	public void testList() throws IOException {
		// 获取文件系统fs对象
		FileSystem hdfs = HDFSFSUtil.getFileSystem();
		// 目录
		Path path = new Path("/opt/data/");
		// 获取文件属性
		FileStatus[] fileStatuses = hdfs.listStatus(path);
		System.out.println("ddd");

		for (FileStatus fileStatus : fileStatuses) {
			Path p = fileStatus.getPath();
			// 信息
			String info = fileStatus.isDir() ? "目录" : "文件";

			System.out.println(info + " :" + p);
		}

	}

	/**
	 * 文件上传
	 */
	@Test
	public void testPut() {

		// 获取文件系统fs对象
		FileSystem hdfs = HDFSFSUtil.getFileSystem();
		// 本地文件
		Path srcPath = new Path("E:\\百度云盘\\JEECG-repository.zip");

		Path dstPath = new Path("/opt/data/test2/");

		try {
			hdfs.copyFromLocalFile(srcPath, dstPath);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * 创建目录
	 * 
	 * @throws Exception
	 */
	@Test
	public void testDirectory() throws Exception {
		// 获取文件系统fs对象
		FileSystem hdfs = HDFSFSUtil.getFileSystem();
		// 目录
		Path path = new Path("/opt/data/test3s/dd");
		boolean isSuccess = hdfs.mkdirs(path);
		String info = isSuccess ? "成功" : "失败";
		System.out.println("创建目录[" + path + "]" + info);
	}

	/**
	 * 创建HDFS文件并写入内容
	 * 
	 */
	@Test
	public void testCreate() throws Exception {
		// 获取文件系统fs对象
		FileSystem hdfs = HDFSFSUtil.getFileSystem();
		// 目录
		Path path = new Path("/opt/data/test3s/dd/touch.data");
		// 创建文件，并获取输出流
		FSDataOutputStream fSDataOutputStream = hdfs.create(path);
		fSDataOutputStream.write("你好".getBytes());
		// 通过输出流， 写入数据
		fSDataOutputStream.writeUTF("hadoop!");
		// 关闭输出流
		IOUtils.closeStream(fSDataOutputStream);
	}

	// 对HDFS上文件进行重命名
	@Test
	public void testRename() throws Exception {
		// 获取文件系统fs对象
		FileSystem hdfs = HDFSFSUtil.getFileSystem();
		// 目录
		Path path = new Path("/opt/data/test3s/dd/touch.data");
		Path dstPath = new Path("/opt/data/test3s/dd/touch_new.data");
		boolean flag = hdfs.rename(path, dstPath);
		System.out.println(flag);
	}

	// 删除文件
	@Test
	public void testDelete() throws Exception {
		// 获取文件系统fs对象
		FileSystem hdfs = HDFSFSUtil.getFileSystem();
		// 目录
		Path path = new Path("/opt/data/test3s/dd/touch.data");

		// boolean flag = hdfs.deleteOnExit(path);
		boolean flag = hdfs.delete(path, true);
		System.out.println(flag);
	}

	/**
	 * 查看文件在集群上的存放位置
	 * 
	 * @throws Exception
	 */
	@Test
	public void testLocation() throws Exception {
		// 获取文件系统fs对象
		FileSystem hdfs = HDFSFSUtil.getFileSystem();
		// 目录
		Path path = new Path("/opt/data/test2/JEECG-repository.zip");

		FileStatus fileStatus = hdfs.getFileStatus(path);

		BlockLocation[] blockLocations = hdfs.getFileBlockLocations(fileStatus,
				0, fileStatus.getLen());

		for (BlockLocation blockLocation : blockLocations) {
			String[] hosts = blockLocation.getHosts();
			for (String host : hosts) {
				System.out.println(host + " ");
			}
			System.out.println(" ");
		}
	}
	
	
	/**
	 * 查看集群的节点信息
	 * 
	 * @throws Exception
	 */
	@Test
	public void testCluster() throws Exception {
		// 获取文件系统fs对象
		FileSystem hdfs = HDFSFSUtil.getFileSystem();
		//分布式文件系统
		DistributedFileSystem distributedFileSystem = (DistributedFileSystem) hdfs;
		
		DatanodeInfo[] datanodeInfos = distributedFileSystem.getDataNodeStats();
		
		for(DatanodeInfo datanodeInfo : datanodeInfos){
			String hostName = datanodeInfo.getHostName();
			System.out.println(hostName + " ");
		}
	}

}
