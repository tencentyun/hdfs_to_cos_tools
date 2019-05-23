package com.qcloud.hdfs_to_cos;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class CommonHarUtils {
    private final static String HarFileSystemScheme = "har";

    /**
     * 检查一个路径是否为har文件。根据har文件的特征来判断：
     * 1. 以.har结尾的目录
     * 2. 这个目录下存在_index和_masterindex文件
     *
     * @param fileStatus 待检查的文件
     * @param fs         与之匹配文件系统
     * @return 如果是har文件则返回true，否则返回false
     */
    public static boolean isHarFile(FileStatus fileStatus, FileSystem fs) {
        if (!fileStatus.isDirectory()
                || !fileStatus.getPath().toString().endsWith(".har")) {
            return false;
        }

        // 判断该目录下是否有_index文件和_masterindex的文件
        Path dirPath = fileStatus.getPath();
        Path indexFilePath = new Path(dirPath, "_index");
        Path masterIndexFilePath = new Path(dirPath, "_masterindex");

        try {
            FileStatus indexFileStatus = fs.getFileStatus(indexFilePath);
            FileStatus masterIndexFileStatus =
                    fs.getFileStatus(masterIndexFilePath);
            if (null != indexFileStatus && null != masterIndexFileStatus) {
                return true;
            }
        } catch (FileNotFoundException e) {
            return false;
        } catch (IOException ignored) {
        }

        return false;
    }

    public static Path convertToCosPath(ConfigReader configReader,
            Path harFilePath) throws IOException,
            URISyntaxException {
        if (null == harFilePath) {
            throw new NullPointerException("har file path is null");
        }

        HarFileSystem harFileSystem =
                new HarFileSystem(configReader.getHdfsFS());
        harFileSystem.initialize(buildFsUri(harFilePath),
                configReader.getHdfsFS().getConf());

        String harFileFolderPath =
                new Path(configReader.getSrcHdfsPath()).toUri().getPath();
        String destPath = configReader.getDestCosPath();
        if (destPath.endsWith("/")) {
            destPath = destPath.substring(0, destPath.length() - 1);
        }
        String filePath = harFilePath.toUri().getPath();
        String cosPath;
        if (harFileSystem.getFileStatus(new Path(filePath)).isFile()) {
            cosPath = filePath.replaceFirst(harFileFolderPath, destPath);
        } else {
            cosPath = filePath.replaceFirst(harFileFolderPath,
                    destPath) + "/";
        }

        return new Path(Utils.trimDoubleSlash(cosPath));
    }

    public static URI buildFsUri(Path harFilePath) throws URISyntaxException {
        if (null == harFilePath) {
            throw new NullPointerException("har file path is null");
        }

        String scheme = harFilePath.toUri().getScheme();
        String host = harFilePath.toUri().getHost();
        int port = harFilePath.toUri().getPort();
        String filePath = harFilePath.toUri().getPath();

        if (null == scheme || null == host || -1 == port) {
            return new URI(HarFileSystemScheme + "://" + filePath);
        } else {
            if (scheme.compareToIgnoreCase(HarFileSystemScheme) == 0) {
                return harFilePath.toUri();
            }
            return new URI(HarFileSystemScheme + "://"
                    + scheme + "-" + host + ":" + String.valueOf(port)
                    + filePath);
        }
    }
}
