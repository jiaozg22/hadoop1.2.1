package org.global.fairy.hadoop.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/**
 * HDFSπ§æﬂ¿‡
 * 
 * @author jiao
 * 
 */
public class HDFSFSUtil {

	public static FileSystem getFileSystem() {
		FileSystem fileSystem = null;
		try {
			Configuration config = new Configuration();

			fileSystem = FileSystem.get(config);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return fileSystem;
	}
}
