package com.qcloud.hdfs_to_cos;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.model.*;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class FileToCosTask implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(FileToCosTask.class);
    private static final long MAX_PART_SIZE = 2 * 1024 * 1024 * 1024L;            // 2G
    private static final long MAX_PART_NUM = 10000L;
    private static final long MAX_FILE_SIZE = MAX_PART_SIZE * MAX_PART_NUM;
    private static final long MULTIPART_UPLOAD_THRESHOLD = 64 * 1024 * 1024L;

    private final int kMaxRetryNum = 3;
    private final int kRetryInterval = 3000;                                    // 重试间隔时间

    protected ConfigReader configReader = null;
    protected COSClient cosClient = null;
    protected FileStatus fileStatus = null;
    protected FileSystem fileSystem = null;
    protected String cosPath = null;

    public FileToCosTask(ConfigReader configReader, COSClient cosClient, FileStatus fileStatus, FileSystem fileSystem, String cosPath) {
        this.configReader = configReader;
        this.cosClient = cosClient;
        this.fileStatus = fileStatus;
        this.fileSystem = fileSystem;
        this.cosPath = cosPath;
    }

    private void checkInternalMember() throws NullPointerException, IllegalArgumentException {
        if (null == this.configReader) {
            throw new NullPointerException("config reader is null.");
        }

        if (null == this.cosClient) {
            throw new NullPointerException("cos client is null.");
        }

        if (null == this.cosPath) {
            throw new NullPointerException("cos path is null.");
        }
        if (this.cosPath.length() == 0) {
            throw new IllegalArgumentException("cos path is empty.");
        }

        if (null == this.fileStatus) {
            throw new NullPointerException("file status is null.");
        }

        if (null == this.fileSystem) {
            throw new NullPointerException("file system is null.");
        }
    }

    protected boolean ifSkipUploadFile() {
        this.checkInternalMember();

        if (!configReader.isSkipIfLengthMatch()) {
            return false;
        }

        //对比文件是否一致
        try {
            GetObjectMetadataRequest statRequest = new GetObjectMetadataRequest(configReader.getBucket(), cosPath);
            ObjectMetadata metadata = cosClient.getObjectMetadata(statRequest);
            long cosFileSize = metadata.getContentLength();
            long localFileSize = this.fileStatus.getLen();
            if (cosFileSize == localFileSize) {
                log.info("local file len equal to cos. skip upload. file path: " + fileStatus.getPath().toString());
                Statistics.instance.addSkipFile();
                return true;
            }
        } catch (Exception e) {
            log.error("file is not exist. bucket: " + configReader.getBucket() + "," + "cos path: " + cosPath + "msg: " + e.getMessage());
        }
        return false;
    }

    protected void UploadFile() throws Exception {
        this.checkInternalMember();

        //判断是否需要跳过文件
        if (this.ifSkipUploadFile()) {
            return;
        }

        long fileSize = this.fileSystem.getFileStatus(this.fileStatus.getPath()).getLen();
        if (fileSize > FileToCosTask.MAX_FILE_SIZE) {
            throw new IOException("exceed max support file size, current file size:" + fileSize + " max file size: " + FileToCosTask.MAX_FILE_SIZE);
        }
        if (fileSize <= FileToCosTask.MULTIPART_UPLOAD_THRESHOLD) {
            this.uploadSingleFileWithRetry();
        } else {
            this.uploadMultipartWithRetry();
        }

        String taskInfo = String.format("[upload file success] [file path: %s] [cos path: %s]", this.fileStatus.getPath().toString(), this.cosPath.toString());
        log.info(taskInfo);
        Statistics.instance.addUploadFileOk();
        String printlnStr =
                String.format("[ok] [file path: %s]", this.fileStatus.getPath().toString());
        System.out.println(printlnStr);
    }

    private void uploadSingleFileWithRetry() throws Exception {
        this.checkInternalMember();
        for (int i = 0; i < this.kMaxRetryNum; i++) {
            InputStream fStream = null;
            try {
                fStream = this.fileSystem.open(this.fileStatus.getPath());
                fStream.skip(0);
                ObjectMetadata metadata = new ObjectMetadata();
                long fileSize = this.fileSystem.getFileStatus(this.fileStatus.getPath()).getLen();
                metadata.setContentLength(fileSize);
                PutObjectRequest putObjectRequest = new PutObjectRequest(configReader.getBucket(), this.cosPath, fStream, metadata);
                this.cosClient.putObject(putObjectRequest);
                return;
            } catch (CosServiceException e) {
                log.error("upload single file failure. retry_num:" + String.valueOf(i) + " msg:" + e.getMessage() + " retcode: " + e.getErrorCode() + " xml: " + e.getErrorResponseXml());
                try {
                    Thread.sleep(this.kRetryInterval);
                } catch (InterruptedException e1) {
                    // 等待被中断
                    break;
                }
                continue;
            } finally {
                if (null != fStream) {
                    try {
                        fStream.close();
                    } catch (IOException e2) {
                        log.warn("close file input stream failed. exception: " + e2.getMessage());
                    }
                }
            }
        }
        throw new Exception("upload single file failure. cos path: " + this.cosPath);
    }

    protected void uploadMultipartWithRetry() throws Exception {
        String uploadId = this.buildUploadId();

        Map<Integer, PartSummary> existedParts = this.identifyExistingPartsForResume(uploadId);

        // 先规整partSize
        long fileSize = this.fileSystem.getFileStatus(this.fileStatus.getPath()).getLen();
        long partSize = this.configReader.getPartSize();
        while (partSize * MAX_PART_NUM < fileSize) {
            partSize *= 2;
            if (partSize > MAX_PART_SIZE) {
                partSize = MAX_PART_SIZE;
            }
        }
        // 然后开始上传
        log.info("cospath: " + cosPath + ", fileSize: " + fileSize + ", partSize:" + partSize);
        List<Future<PartETag>> allUploadPartTasks = new ArrayList<Future<PartETag>>();
        int threadNum = this.configReader.getMaxUploadPartTaskNum();
        ExecutorService service = Executors.newFixedThreadPool(threadNum);
        Semaphore tmpSemaphore = new Semaphore(threadNum);
        for (int partNum = 1, pos = 0; pos < fileSize; partNum++) {
            partSize = Math.min(partSize, fileSize - pos);
            if (existedParts.containsKey(partNum)) {
                log.info("part has already been uploaded, cos path: " + cosPath + " part num: " + partNum + " pos: " + pos + " part size: " + partSize);
                pos += partSize;
                continue;
            }

            while (true) {
                try {
                    tmpSemaphore.acquire();
                    break;
                } catch (InterruptedException e) {
                    log.error("upload multipart with retry acquire occurs an exception: " + e.getMessage());
                    continue;
                }
            }

            UploadPartTask uploadPartTask = new UploadPartTask(this.fileSystem, this.fileStatus.getPath(), this.cosPath, uploadId, partNum, pos, partSize, this.cosClient, tmpSemaphore, this.configReader);
            allUploadPartTasks.add(service.submit(uploadPartTask));
            pos += partSize;
        }

        try {
            service.shutdown();
            service.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
            service.shutdownNow();
        } catch (Exception e) {
            log.error("shutdown and wait part end occur a excpetion: " + e.toString());
            return;
        }
        log.info("Upload all part success, cosPath: " + cosPath);

        // complete multipart upload
        List<PartETag> partETags = new ArrayList<PartETag>();
        for (Map.Entry<Integer, PartSummary> entry : existedParts.entrySet()) {
            partETags.add(new PartETag(entry.getKey(), entry.getValue().getETag()));
        }
        for (int i = 0; i < allUploadPartTasks.size(); i++) {
            partETags.add(allUploadPartTasks.get(i).get());
        }
        CompleteMultipartUploadRequest completeMultipartUploadRequest = new CompleteMultipartUploadRequest(configReader.getBucket(), this.cosPath, uploadId, partETags);
        for (int i = 0; i < this.kMaxRetryNum; i++) {
            try {
                CompleteMultipartUploadResult result = this.cosClient.completeMultipartUpload(completeMultipartUploadRequest);
                this.delScpFile(this.cosPath, uploadId);
                log.info("complete multipart file success, cos path: " + cosPath + " file size: " + String.valueOf(fileSize) + " file path: " + this.fileStatus.getPath().toString() + " etag: " + result.getETag());
                return;
            } catch (CosServiceException e) {
                log.error("complete multi-part upload failure, retry num: " + String.valueOf(i) + " msg: " + e.getErrorMessage() + " ret_code: " + e.getErrorCode() + " xml: " + e.getErrorResponseXml());
                try {
                    Thread.sleep(this.kRetryInterval);
                } catch (InterruptedException e1) {
                    break;
                }
                continue;
            }
        }
        throw new Exception("upload file failure, cos path: " + this.cosPath + " file path: " + this.fileStatus.getPath().toString());
    }

    protected void createFolderWithRetry() {
        this.checkInternalMember();

        for (int i = 0; i < this.kMaxRetryNum; i++) {
            InputStream inputStream = null;
            try {
                inputStream = new ByteArrayInputStream(new byte[0]);
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentLength(0);
                PutObjectRequest putObjectRequest = new PutObjectRequest(configReader.getBucket(), this.cosPath, inputStream, metadata);
                this.cosClient.putObject(putObjectRequest);
                return;
            } catch (CosServiceException e) {
                log.error("create folder failure, retry num: " + String.valueOf(i) + " msg: " + e.getErrorMessage() + " ret code: " + e.getErrorCode() + " xml: " + e.getErrorResponseXml());
                try {
                    Thread.sleep(this.kRetryInterval);
                } catch (InterruptedException e1) {
                    break;
                }
                continue;
            } finally {
                if (null != inputStream) {
                    try {
                        inputStream.close();
                    } catch (IOException e2) {
                        log.warn("close byte input stream failed. exception: " + e2.getMessage());
                    }
                }
            }
        }
        log.error("create cos folder failed. cos path: " + this.cosPath);
    }

    protected void CreateFolder() {
        this.checkInternalMember();

        try {
            if (!this.isCosFolderExist()) {
                this.createFolderWithRetry();
            } else {
                log.info("folder already exist. cos path: " + this.cosPath);
            }
            String taskInfo = String.format("[create folder] [file_path: %s] [cos_path: %s] [ret: %s]", this.fileStatus.getPath().toString(), this.cosPath, "success");
            log.info(taskInfo);
            Statistics.instance.addCreateFolderOk();
            String printlnStr = String.format("[ok] [file_path: %s]", this.fileStatus.getPath().toString());
            System.out.println(printlnStr);
        } catch (Exception e) {
            String taskInfo = String.format("[create folder] [file_path: %s] [cos_path: %s] [ret: %s]", this.fileStatus.getPath().toString(), this.cosPath, e.getMessage());
            log.error(taskInfo);
            Statistics.instance.addCreateFolderFail();
            String printlnStr = String.format("[create folder] [file_path: %s] [cos_path: %s]", this.fileStatus.getPath().toString(), this.cosPath);
            System.out.println(printlnStr);
        }
    }

    private boolean isCosFolderExist() {
        try {
            this.cosClient.getObjectMetadata(this.configReader.getBucket(), this.cosPath);
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    private String getScpFilePath() {
        String scpFileName =
                DigestUtils.md5Hex(cosPath + "_" + this.fileStatus.getPath().toString()) + ".scp";
        String scpFilePath = "./scp/" + scpFileName;
        return scpFilePath;
    }

    private String getUploadIdFromScp(File scpFile) {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(scpFile));
            return br.readLine();
        } catch (IOException e) {
            log.error("opening scp file occurs a exception.");
            return "";
        } finally {
            if (null != br) {
                try {
                    br.close();
                } catch (IOException e) {
                    log.error("close the buffered reader failed. exception: " + e.getMessage());
                }
            }
        }
    }

    private void saveUploadId(String uploadId) {
        String scpFilePath = this.getScpFilePath();
        BufferedWriter bw = null;

        try {
            bw = new BufferedWriter(new FileWriter(scpFilePath));
            bw.write(uploadId);
        } catch (IOException e) {
            log.error("save point file occur an exception: " + e.getMessage());
        } finally {
            if (null == bw) {
                try {
                    bw.close();
                } catch (IOException e) {
                    log.error("close buffered writer failed. exception: " + e.getMessage());
                }
            }
        }
    }

    private void delScpFile(String cosPath, String uploadId) {
        String scpFilePath = this.getScpFilePath();
        File scpFile = new File(scpFilePath);
        scpFile.delete();
    }

    private String buildUploadId() throws Exception {
        this.checkInternalMember();

        String uploadId = null;
        File scpFile = new File(this.getScpFilePath());
        if (scpFile.exists()) {
            uploadId = this.getUploadIdFromScp(scpFile);
            ListPartsRequest listPartsRequest = new ListPartsRequest(configReader.getBucket(), this.cosPath, uploadId);
            try {
                this.cosClient.listParts(listPartsRequest);
                return uploadId;
            } catch (CosServiceException e) {
                log.info("upload id is invalid. ready to init again. upload id: " + uploadId + " cos path: " + this.cosPath);
            }
        }

        InitiateMultipartUploadRequest initiateMultipartUploadRequest = new InitiateMultipartUploadRequest(this.configReader.getBucket(), this.cosPath);
        InitiateMultipartUploadResult initiateMultipartUploadResult = null;

        for (int i = 0; i < kMaxRetryNum; i++) {
            try {
                initiateMultipartUploadResult = this.cosClient.initiateMultipartUpload(initiateMultipartUploadRequest);
                this.saveUploadId(uploadId);
                break;
            } catch (CosServiceException e) {
                log.error("init multipart upload failure. cos path: " + this.cosPath + " try num: " + String.valueOf(i) + " msg: " + e.getErrorMessage() + " ret code: " + e.getErrorCode() + " xml: " + e.getErrorResponseXml());
                try {
                    Thread.sleep(this.kRetryInterval);
                } catch (InterruptedException e1) {
                    break;
                }
                continue;
            }
        }

        if (null == initiateMultipartUploadResult) {
            throw new Exception("init upload multipart failure. cos path: " + this.cosPath);
        }
        log.info("Init multi-part upload success, cos path: " + this.cosPath + " upload id: " + initiateMultipartUploadResult.getUploadId());
        return initiateMultipartUploadResult.getUploadId();
    }

    private Map<Integer, PartSummary> identifyExistingPartsForResume(String uploadId) {
        Map<Integer, PartSummary> partNumbers = new HashMap<Integer, PartSummary>();
        if (null == uploadId || uploadId.length() == 0) {
            return partNumbers;         // 空的partnumber
        }
        int partNum = 0;
        while (true) {
            PartListing parts = this.cosClient.listParts(new ListPartsRequest(this.configReader.getBucket(), this.cosPath, uploadId).withPartNumberMarker(partNum));
            for (PartSummary partSummary : parts.getParts()) {
                partNumbers.put(partSummary.getPartNumber(), partSummary);
            }
            if (!parts.isTruncated()) {
                return partNumbers;
            }
            partNum = parts.getNextPartNumberMarker();
        }
    }

    public void run() {
        this.checkInternalMember();
        try {
            if (this.fileStatus.isFile()) {
                this.UploadFile();
            }
            if (this.fileStatus.isDirectory()) {
                this.CreateFolder();
            }
        } catch (Exception e) {
        } finally {
        }
    }
}