package org.global.fairy.hadoop;

import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * ����:����HDFS�ϴ������ļ��Ĺ����У����кϲ��ļ�
 * 
 * @author jiao
 * 
 */
public class PutMerge {
	/**
	 * �����ϴ��ļ��������ļ��ϲ�
	 * 
	 * @param localDir
	 *            ����Ҫ�ϴ����ļ�Ŀ¼
	 * @param hdfsFile
	 *            HDFS�ļ�ϵͳ�ϵ�ȫ·���ļ�����
	 */
	public static void put(String localDir, String hdfsFile) {

		//1)��ȡ������Ϣ
		Configuration configuration = new Configuration();
		
		// 2)
		//����·��
		Path localfs = new Path(localDir);
		//hdfs ·��
		Path hdfsPath = new Path(hdfsFile);
		
		try{
			//��ȡ�����ļ�ϵͳ
			FileSystem localFs = FileSystem.getLocal(configuration);
			//��ȡHDFS�ļ�ϵͳ
			FileSystem hdfs = FileSystem.get(configuration);
			//�����ļ�ϵͳ��ָ��Ŀ¼�е������ļ�
			FileStatus[] status = localFs.listStatus(localfs);
			
			FSDataOutputStream outStream = hdfs.create(hdfsPath);
			
			
			//ѭ�����������ļ�
			for(FileStatus fileStatus : status){
				//��ȡ�ļ�
				Path path = fileStatus.getPath();
				System.out.println("�ļ�Ϊ��"+path.getName());
				
				//���ļ�������
				FSDataInputStream inputStream = localFs.open(path);
				//�������Ķ�д����
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
