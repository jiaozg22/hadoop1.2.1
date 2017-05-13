package org.global.fairy.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

/**
 * ͨ��FileSystem API ����HDFS
 * 
 * @author jiao
 * 
 */
public class HDFSFsTest {
	/**
	 * 
	 * @throws Exception
	 */
	@Test
	public void testRead() throws Exception {
		// ���������ļ�����ȡ�����ļ���Ϣ
		Configuration conf = new Configuration();

		/**
		 * ��ȡ�ļ�ϵͳ
		 */
		FileSystem hdfs = FileSystem.get(conf);

		// �ļ�����
		Path path = new Path("/opt/data/test3s/dd/merge.data");

		// ���ļ�������
		FSDataInputStream inStream = hdfs.open(path);

		// ��ȡ�ļ����ݵ�����̨��ʾ
		IOUtils.copyBytes(inStream, System.out, 4096, false);
		// �ر���
		IOUtils.closeStream(inStream);

	}

}
