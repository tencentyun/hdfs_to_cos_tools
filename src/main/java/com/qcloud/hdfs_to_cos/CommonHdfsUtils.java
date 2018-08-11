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

    public static Path convertToCosPath(ConfigReader configReader, Path hdfsFilePath) throws IOException {
        if (null == hdfsFilePath) {
            throw new NullPointerException("hdfs file path is null");
        }
        String srcPath = new Path(configReader.getSrcHdfsPath()).toUri().getPath();
        String hdfsFolderPath = srcPath;
        if (configReader.getHdfsFS().getFileStatus(new Path(srcPath)).isFile()) {
            hdfsFolderPath = srcPath.substring(0, srcPath.lastIndexOf("/"));
        }
        String filePath = hdfsFilePath.toUri().getPath();
        String destPath = configReader.getDestCosPath();
        if (destPath.endsWith("/")) {
            destPath = destPath.substring(0, destPath.length() - 1);
        }
        if (configReader.getHdfsFS().getFileStatus(new Path(filePath)).isFile()) {
            // 是个文件
            if (hdfsFolderPath.compareToIgnoreCase("/") == 0) {
                return new Path(destPath + filePath);
            } else {
                return new Path(filePath.replaceFirst(hdfsFolderPath, destPath));
            }
        } else {
            if (hdfsFilePath.compareTo(new Path("/")) == 0) {
                return new Path(destPath + filePath);
            } else {
                return new Path(filePath.replaceFirst(hdfsFolderPath, destPath) + "/");
            }
        }
    }
}
