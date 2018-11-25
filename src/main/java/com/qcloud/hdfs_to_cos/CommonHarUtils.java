package com.qcloud.hdfs_to_cos;

import com.sun.jndi.toolkit.url.Uri;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class CommonHarUtils {
    private final static String HarFileSystemScheme = "har";

    public static boolean isHarFile(FileStatus fileStatus) {
        if (fileStatus.isDirectory() && fileStatus.getPath().toString().endsWith(".har")) {
            return true;
        }
        return false;
    }

    public static Path convertToCosPath(ConfigReader configReader, Path harFilePath) throws IOException, URISyntaxException {
        if (null == harFilePath) {
            throw new NullPointerException("har file path is null");
        }

        HarFileSystem harFileSystem = new HarFileSystem(configReader.getHdfsFS());
        harFileSystem.initialize(buildFsUri(harFilePath), configReader.getHdfsFS().getConf());

        String harFileFolderPath = harFileSystem.getHomeDirectory().toUri().getPath();
        String destPath = configReader.getDestCosPath();
        if (destPath.endsWith("/")) {
            destPath = destPath.substring(0, destPath.length() - 1);
        }

        String filePath = harFilePath.toUri().getPath();
        if (harFileSystem.getFileStatus(new Path(filePath)).isFile()) {
            return new Path(filePath.replaceFirst(harFileFolderPath, destPath));
        } else {
            return new Path(filePath.replaceFirst(harFileFolderPath, destPath) + "/");
        }
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
            return new URI(HarFileSystemScheme + "://" + scheme + "-" + host + ":" + String.valueOf(port) + filePath);
        }
    }
}
