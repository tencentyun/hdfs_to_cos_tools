package com.qcloud.hdfs_to_cos;

import java.io.IOException;

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

    public static Path convertToCosPath(ConfigReader configReader, Path hdfsFilePath) throws IOException {
        if (null == hdfsFilePath) {
            throw new NullPointerException("hdfs file path is null");
        }
        String srcPath = new Path(configReader.getSrcHdfsPath()).toUri().getPath();
        String hdfsFolderPath = srcPath;
        if (configReader.getHdfsFS().getFileStatus(new Path(srcPath)).isFile()) {
            hdfsFolderPath = srcPath.substring(0, srcPath.lastIndexOf("/")) + "/";
        }
        String filePath = hdfsFilePath.toUri().getPath();           // 文件的实际路径
        String destPath = configReader.getDestCosPath();            // COS上的目的路径
        if (!destPath.endsWith("/")) {
            destPath = destPath + "/";
        }
        if (configReader.getHdfsFS().getFileStatus(new Path(filePath)).isFile()) {
            return new Path(filePath.replaceFirst(hdfsFolderPath, destPath));
        } else {
            return new Path(filePath.replaceFirst(hdfsFolderPath, destPath) + "/");
        }
    }
}
