package ethan.v16.ch03;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

/**
 * @since 2014-9-25
 * @author ethan
 */

public class CommonUtil {
	/**
	 * @param output
	 * @param conf
	 * @throws IOException
	 */
	public static void deleteOutput(String output,Configuration conf) throws IOException {
		Path dstDir = new Path(output);  
		FileSystem hdfs = dstDir.getFileSystem(conf);  
		hdfs.delete(new Path(output), true);
	}
}

