package org.global.fairy.hadoop;

import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 功能:在向HDFS上传复制文件的过程中，进行合并文件
 * 
 * @author jiao
 * 
 */
public class PutMerge {
	/**
	 * 复制上传文件，并将文件合并
	 * 
	 * @param localDir
	 *            本地要上传的文件目录
	 * @param hdfsFile
	 *            HDFS文件系统上的全路径文件名称
	 */
	public static void put(String localDir, String hdfsFile) {

		//1)获取配置信息
		Configuration configuration = new Configuration();
		
		// 2)
		//本地路径
		Path localfs = new Path(localDir);
		//hdfs 路径
		Path hdfsPath = new Path(hdfsFile);
		
		try{
			//获取本地文件系统
			FileSystem localFs = FileSystem.getLocal(configuration);
			//获取HDFS文件系统
			FileSystem hdfs = FileSystem.get(configuration);
			//本地文件系统中指定目录中的所有文件
			FileStatus[] status = localFs.listStatus(localfs);
			
			FSDataOutputStream outStream = hdfs.create(hdfsPath);
			
			
			//循环遍历本地文件
			for(FileStatus fileStatus : status){
				//获取文件
				Path path = fileStatus.getPath();
				System.out.println("文件为："+path.getName());
				
				//打开文件输入流
				FSDataInputStream inputStream = localFs.open(path);
				//进行流的读写操作
				byte[] buffer = new byte[1024];
				int length = 0;
				while((length = inputStream.read(buffer))>0){
					outStream.write(buffer);
				}
				inputStream.close();
			}
			outStream.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args){
		String localDir = "E:/temp/zookeeper/version-2";
		String hdfsFile = "hdfs://hadoop-master.dragon.org:9000/opt/data/test3s/dd/merge.data";
		
		put(localDir,hdfsFile);
	}

}
