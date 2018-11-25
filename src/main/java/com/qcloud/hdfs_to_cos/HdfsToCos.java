package com.qcloud.hdfs_to_cos;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.*;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.model.GetObjectMetadataRequest;


public class HdfsToCos {

    private static final Logger log = LoggerFactory.getLogger(HdfsToCos.class);

    private ConfigReader configReader = null;
    private BlockingQueue<FileToCosTask> taskBlockingQueue = null;
    private COSClient cosClient = null;

    private String configCheckMsg;

    public HdfsToCos(ConfigReader configReader, BlockingQueue<FileToCosTask> taskBlockingQueue, COSClient cosClient) {
        this.configReader = configReader;
        this.taskBlockingQueue = taskBlockingQueue;
        this.cosClient = cosClient;
    }

    private void submitTask(FileToCosTask task) throws Exception {
        if (null == task) {
            throw new IllegalArgumentException("can not submit a null task.");
        }

        if (null == this.taskBlockingQueue) {
            throw new NullPointerException("can not submit a task to null blocking queue.");
        }
        this.taskBlockingQueue.put(task);
    }

    private void scanHarMember(Path filePath, HarFileSystem harFs) throws Exception {
        FileStatus targetPathStatus = harFs.getFileStatus(filePath);
        if (targetPathStatus.isFile()) {
            FileToCosTask task = this.buildHarFileToCosTask(targetPathStatus);
            this.submitTask(task);
            return;
        }

        FileStatus[] pathStatus = harFs.listStatus(filePath);
        for (FileStatus fileStatus : pathStatus) {
            if (CommonHarUtils.isHarFile(fileStatus)) {
                harFs.initialize(CommonHarUtils.buildFsUri(fileStatus.getPath()), harFs.getConf());
                scanHarMember(fileStatus.getPath(), harFs);
            }

            if (fileStatus.isFile()) {
                this.submitTask(this.buildHarFileToCosTask(fileStatus));
            }

            if (fileStatus.isDirectory()) {
                scanHarMember(fileStatus.getPath(), harFs);
            }
        }
    }

    private void scanHdfsMember(Path hdfsPath, FileSystem hdfsFS) throws Exception {
        FileStatus targetPathStatus = hdfsFS.getFileStatus(hdfsPath);
        if (targetPathStatus.isFile()) {
            this.submitTask(this.buildHdfsFileToCosTask(targetPathStatus));
            return;
        }
        FileStatus[] memberArray = hdfsFS.listStatus(hdfsPath);
        for (FileStatus member : memberArray) {
            if (CommonHarUtils.isHarFile(member)) {
                HarFileSystem harFS = new HarFileSystem(configReader.getHdfsFS());
                harFS.initialize(CommonHarUtils.buildFsUri(member.getPath()), hdfsFS.getConf());
                scanHarMember(member.getPath(), harFS);
            } else {
                this.submitTask(this.buildHdfsFileToCosTask(member));
                if (member.isDirectory()) {
                    scanHdfsMember(member.getPath(), hdfsFS);
                }
            }
        }
    }

    private FileToCosTask buildHdfsFileToCosTask(FileStatus fileStatus) {
        if (null == fileStatus) {
            log.error("parameter file status is null.");
            return null;
        }
        FileToCosTask task = null;
        try {
            task = new FileToCosTask(
                    this.configReader,
                    this.cosClient, fileStatus,
                    this.configReader.getHdfsFS(),
                    CommonHdfsUtils.convertToCosPath(configReader, fileStatus.getPath()).toString());
        } catch (IOException e) {
            log.error("build a hdfsFileToCosTask for " + fileStatus.toString() + " failed. exception: " + e.getMessage());
            return null;
        }
        return task;
    }

    private FileToCosTask buildHarFileToCosTask(FileStatus fileStatus) {
        if (null == fileStatus) {
            log.error("parameter file status is null.");
            return null;
        }

        FileToCosTask task = null;
        try {
            HarFileSystem harFileSystem = new HarFileSystem(configReader.getHdfsFS());
            harFileSystem.initialize(CommonHarUtils.buildFsUri(fileStatus.getPath()), configReader.getHdfsFS().getConf());
            task = new FileToCosTask(
                    this.configReader,
                    this.cosClient,
                    fileStatus,
                    harFileSystem,
                    CommonHarUtils.convertToCosPath(configReader, fileStatus.getPath()).toString());
        } catch (IOException e) {
            log.error("build harFileToCosTask for " + fileStatus.toString() + " failed. exception: " + e.getMessage());
            return null;
        } catch (URISyntaxException e) {
            log.error("build harFileToCosTask for " + fileStatus.toString() + " failed. exception: " + e.getMessage());
            return null;
        }

        return task;
    }

    private boolean checkCosClientLegal() {
        GetObjectMetadataRequest statRequest =
                new GetObjectMetadataRequest(this.configReader.getBucket(), "/");

        // 首先检查Bucket是否存在
        for (int i = 0; i < 2; i++) {
            try {
                boolean isBucketExist = this.cosClient.doesBucketExist(this.configReader.getBucket());
                if (!isBucketExist) {
                    this.configCheckMsg = new String("The specified bucket in the configuration does not exist.\n"
                            + "Please check if the appid, bucket or endpoint configuration in the configuration is correct.");
                    return false;
                }
                return true;
            } catch (CosServiceException e) {
                if (e.getStatusCode() == 403) {
                    // 鉴权不过，检查secret id和secret key
                    this.configCheckMsg = new String("The provided SecretID or Secret key are invalid.\n"
                            + "Please check if ak and sk are configured correctly.");
                    break;
                }
                continue;
            }
        }

        return false;
    }


    public void run() {
        if (!checkCosClientLegal()) {
            StringBuilder errMsgBuilder =
                    new StringBuilder("Configuration information verification error:\n");
            errMsgBuilder.append(this.configCheckMsg);
            System.err.println(errMsgBuilder.toString());
            cosClient.shutdown();
            return;
        }

        try {
            if (configReader.getSrcHdfsPath().startsWith("har://")) {
                HarFileSystem harFs = new HarFileSystem(configReader.getHdfsFS());
                harFs.initialize(CommonHarUtils.buildFsUri(new Path(configReader.getSrcHdfsPath())), configReader.getHdfsFS().getConf());
                String srcPath = configReader.getSrcHdfsPath();
                this.scanHarMember(new Path(srcPath), harFs);
            } else {
                FileSystem hdfsFS = configReader.getHdfsFS();
                String srcPath = configReader.getSrcHdfsPath();
                this.scanHdfsMember(new Path(srcPath), hdfsFS);
            }
        } catch (Exception e) {             // 这里直接捕获一个基类的异常，就不判断了
            log.error("Scanning hdfs/har files occurs an exception.", e);
        }

        cosClient.shutdown();
    }
}
