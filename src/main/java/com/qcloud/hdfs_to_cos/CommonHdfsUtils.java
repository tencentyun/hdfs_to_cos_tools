package com.qcloud.hdfs_to_cos;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.*;

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

    public static boolean isHarFile(FileStatus fileStatus) {
        if (fileStatus.isDirectory() && fileStatus.getPath().toString().endsWith(".har")) {
            return true;
        }

        return false;
    }

    /**
     * @param hdfsFilePath
     * @return
     * @throws URISyntaxException
     */
    public static URI buildHarFsUri(Path hdfsFilePath) throws URISyntaxException {
        String scheme = hdfsFilePath.toUri().getScheme();
        String host = hdfsFilePath.toUri().getHost();
        int port = hdfsFilePath.toUri().getPort();
        String path = hdfsFilePath.toUri().getPath();

        if (scheme.length() == 0 || host.length() == 0) {
            return new URI("har://" + path);
        }else{
            return new URI("har://" + scheme + "-" + host + ":" + String.valueOf(port) + path);
        }
    }

    /**
     * 获取文件长度，单位为字节
     *
     * @param filePath 文件的本地路径
     * @return 文件长度, 单位为字节
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

    public static FSDataInputStream getHarFileContentBytes(FileSystem hdfsFS, String harPath, long offset) throws IOException {
        Path filePath = new Path(harPath);
        HarFileSystem harFileSystem = new HarFileSystem(hdfsFS);
        try {
            harFileSystem.initialize(buildHarFsUri(new Path(harPath)), hdfsFS.getConf());
        } catch (URISyntaxException e) {
            throw new IOException(e.getMessage());
        }
        FSDataInputStream fstream = harFileSystem.open(filePath);
        fstream.skip(offset);
        return fstream;
    }
}
