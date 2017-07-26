package com.qcloud.hdfs_to_cos;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CommonHdfsUtils {

    /**
     * 判断指定路径的文件是否有效, 即文件存在，且可读
     * 
     * @param hdsfFilePath
     * @return 有效返回true, 否则返回false
     */
    public static boolean isLegalHdfsFile(FileSystem hdfsFS, String hdsfFilePath) {
        try {
            return hdfsFS.isFile(new Path(hdsfFilePath));
        } catch (IllegalArgumentException iae) {
            return false;
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * 获取文件长度，单位为字节
     * 
     * @param filePath 文件的本地路径
     * @return 文件长度,单位为字节
     * @throws IOException
     */
    public static long getFileLength(FileSystem hdfsFS, String filePath) throws Exception {
        Path path = new Path(filePath);
        return hdfsFS.getFileStatus(path).getLen();
    }

    public static FSDataInputStream getFileContentBytes(FileSystem hdfsFS, String hdfsPath,
            long offset) throws Exception {
        Path filePath = new Path(hdfsPath);
        FSDataInputStream fstream = hdfsFS.open(filePath);
        fstream.skip(offset);
        return fstream;
    }

}
