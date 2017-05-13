package org.global.fairy.hadoop;

import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

/**
 * HDFS URL api
 * @author jiao
 *
 */
public class HDFSUrlTest {

	//��java ����ʶ��HDFS��URL
	static {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	
	//�鿴�ļ�����
    @Test
	public void testRead() throws Exception{
    	InputStream in = null;
    	
    	//�ļ�·��
    	String fileUrl = "hdfs://hadoop-master.dragon.org:9000/opt/data/test2/01.data";
    	
    	try{
    		in = new URL(fileUrl).openStream();
    	}catch(Exception e){
    		e.printStackTrace();
    	}finally{
    		IOUtils.closeStream(in);
    	}
    } 
	
	
}
