package org.global.fairy.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

/**
 * 通过FileSystem API 操作HDFS
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
		// 根据配置文件，获取配置文件信息
		Configuration conf = new Configuration();

		/**
		 * 获取文件系统
		 */
		FileSystem hdfs = FileSystem.get(conf);

		// 文件名称
		Path path = new Path("/opt/data/test3s/dd/merge.data");

		// 打开文件输入流
		FSDataInputStream inStream = hdfs.open(path);

		// 读取文件内容到控制台显示
		IOUtils.copyBytes(inStream, System.out, 4096, false);
		// 关闭流
		IOUtils.closeStream(inStream);

	}

}
