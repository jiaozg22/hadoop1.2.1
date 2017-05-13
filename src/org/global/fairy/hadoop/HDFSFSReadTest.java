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
	 * ��ȡ�ļ�����
	 * 
	 * @throws Exception
	 */
	@Test
	public void testRead() throws Exception {
		// ��ȡ�ļ�ϵͳfs����
		FileSystem hdfs = HDFSFSUtil.getFileSystem();

		// �ļ�����
		Path path = new Path("/opt/data/test3s/dd/touch.data");

		// ���ļ�������
		FSDataInputStream inputStream = hdfs.open(path);

		// ��ȡ�ļ�������̨��ʾ
		IOUtils.copyBytes(inputStream, System.out, 4096, false);

		// �ر��ļ���
		IOUtils.closeStream(inputStream);

	}

	/**
	 * �鿴·��
	 * 
	 * @throws IOException
	 */
	@Test
	public void testList() throws IOException {
		// ��ȡ�ļ�ϵͳfs����
		FileSystem hdfs = HDFSFSUtil.getFileSystem();
		// Ŀ¼
		Path path = new Path("/opt/data/");
		// ��ȡ�ļ�����
		FileStatus[] fileStatuses = hdfs.listStatus(path);
		System.out.println("ddd");

		for (FileStatus fileStatus : fileStatuses) {
			Path p = fileStatus.getPath();
			// ��Ϣ
			String info = fileStatus.isDir() ? "Ŀ¼" : "�ļ�";

			System.out.println(info + " :" + p);
		}

	}

	/**
	 * �ļ��ϴ�
	 */
	@Test
	public void testPut() {

		// ��ȡ�ļ�ϵͳfs����
		FileSystem hdfs = HDFSFSUtil.getFileSystem();
		// �����ļ�
		Path srcPath = new Path("E:\\�ٶ�����\\JEECG-repository.zip");

		Path dstPath = new Path("/opt/data/test2/");

		try {
			hdfs.copyFromLocalFile(srcPath, dstPath);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * ����Ŀ¼
	 * 
	 * @throws Exception
	 */
	@Test
	public void testDirectory() throws Exception {
		// ��ȡ�ļ�ϵͳfs����
		FileSystem hdfs = HDFSFSUtil.getFileSystem();
		// Ŀ¼
		Path path = new Path("/opt/data/test3s/dd");
		boolean isSuccess = hdfs.mkdirs(path);
		String info = isSuccess ? "�ɹ�" : "ʧ��";
		System.out.println("����Ŀ¼[" + path + "]" + info);
	}

	/**
	 * ����HDFS�ļ���д������
	 * 
	 */
	@Test
	public void testCreate() throws Exception {
		// ��ȡ�ļ�ϵͳfs����
		FileSystem hdfs = HDFSFSUtil.getFileSystem();
		// Ŀ¼
		Path path = new Path("/opt/data/test3s/dd/touch.data");
		// �����ļ�������ȡ�����
		FSDataOutputStream fSDataOutputStream = hdfs.create(path);
		fSDataOutputStream.write("���".getBytes());
		// ͨ��������� д������
		fSDataOutputStream.writeUTF("hadoop!");
		// �ر������
		IOUtils.closeStream(fSDataOutputStream);
	}

	// ��HDFS���ļ�����������
	@Test
	public void testRename() throws Exception {
		// ��ȡ�ļ�ϵͳfs����
		FileSystem hdfs = HDFSFSUtil.getFileSystem();
		// Ŀ¼
		Path path = new Path("/opt/data/test3s/dd/touch.data");
		Path dstPath = new Path("/opt/data/test3s/dd/touch_new.data");
		boolean flag = hdfs.rename(path, dstPath);
		System.out.println(flag);
	}

	// ɾ���ļ�
	@Test
	public void testDelete() throws Exception {
		// ��ȡ�ļ�ϵͳfs����
		FileSystem hdfs = HDFSFSUtil.getFileSystem();
		// Ŀ¼
		Path path = new Path("/opt/data/test3s/dd/touch.data");

		// boolean flag = hdfs.deleteOnExit(path);
		boolean flag = hdfs.delete(path, true);
		System.out.println(flag);
	}

	/**
	 * �鿴�ļ��ڼ�Ⱥ�ϵĴ��λ��
	 * 
	 * @throws Exception
	 */
	@Test
	public void testLocation() throws Exception {
		// ��ȡ�ļ�ϵͳfs����
		FileSystem hdfs = HDFSFSUtil.getFileSystem();
		// Ŀ¼
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
	 * �鿴��Ⱥ�Ľڵ���Ϣ
	 * 
	 * @throws Exception
	 */
	@Test
	public void testCluster() throws Exception {
		// ��ȡ�ļ�ϵͳfs����
		FileSystem hdfs = HDFSFSUtil.getFileSystem();
		//�ֲ�ʽ�ļ�ϵͳ
		DistributedFileSystem distributedFileSystem = (DistributedFileSystem) hdfs;
		
		DatanodeInfo[] datanodeInfos = distributedFileSystem.getDataNodeStats();
		
		for(DatanodeInfo datanodeInfo : datanodeInfos){
			String hostName = datanodeInfo.getHostName();
			System.out.println(hostName + " ");
		}
	}

}
