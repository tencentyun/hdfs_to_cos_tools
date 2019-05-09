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
    private static final Logger log =
            LoggerFactory.getLogger(FileToCosTask.class);
    private static final long MAX_PART_SIZE =
            2 * 1024 * 1024 * 1024L;            // 2G
    private static final long MAX_PART_NUM =
            10000L;                              // 最多10000块
    private static final long MAX_FILE_SIZE =
            MAX_PART_SIZE * MAX_PART_NUM;       // 能够支持的最大文件大小
    private static final long MULTIPART_UPLOAD_THRESHOLD =
            128 * 1024 * 1024L;    // 超过128MB以后采用分块上传

    private int kMaxRetryNum = 3;
    private long kRetryInterval = 3000;   // 重试间隔时间，3秒

    protected ConfigReader configReader = null;
    protected COSClient cosClient = null;
    protected FileStatus fileStatus = null;
    protected FileSystem fileSystem = null;
    protected String md5sum = null;         // 文件的md5sum
    protected String cosPath = null;

    public FileToCosTask(
            ConfigReader configReader,
            COSClient cosClient,
            FileStatus fileStatus,
            FileSystem fileSystem,
            String cosPath) {
        this(configReader, cosClient, fileStatus, fileSystem, null, cosPath);
    }

    public FileToCosTask(
            ConfigReader configReader,
            COSClient cosClient,
            FileStatus fileStatus,
            FileSystem fileSystem,
            String md5sum,
            String cosPath) {
        this.configReader = configReader;
        this.cosClient = cosClient;
        this.fileStatus = fileStatus;
        this.fileSystem = fileSystem;
        this.md5sum = md5sum;
        this.cosPath = cosPath;
        this.kMaxRetryNum = configReader.getMaxRetryNum();
        this.kRetryInterval = configReader.getRetryInterval();
    }

    private void checkInternalMember() throws NullPointerException,
            IllegalArgumentException {
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

    /**
     * 根据文件长度判断COS文件是否存在
     *
     * @return 如果COS文件存在，则返回true，否则返回false
     */
    protected boolean checkFileExistsWithLength() {
        this.checkInternalMember();
        log.debug("check if file:{} exists with length.",
                this.fileStatus.getPath().toString());
        try {
            GetObjectMetadataRequest metadataRequest =
                    new GetObjectMetadataRequest(configReader.getBucket(),
                            cosPath);
            ObjectMetadata metadata =
                    this.cosClient.getObjectMetadata(metadataRequest);
            long cosFileSize = metadata.getContentLength();
            long localFileSize = this.fileStatus.getLen();

            if (cosFileSize == localFileSize) {
                return true;
            }
        } catch (Exception e) {
            log.debug("file is not exist. bucket: " + configReader.getBucket()
                    + "," + "cos path: " + cosPath + "msg: " + e.getMessage());
        }

        return false;
    }

    /**
     * 根据文件的长度和MD5值来判断COS文件是否存在
     *
     * @return 如果COS文件存在，则返回true，否则返回false
     */
    protected boolean checkFileExistsWithMD5Sum() {
        this.checkInternalMember();
        log.debug("check if file:{} exists with MD5 checksum.",
                this.fileStatus.getPath().toString());
        try {
            GetObjectMetadataRequest metadataRequest =
                    new GetObjectMetadataRequest(configReader.getBucket(),
                            cosPath);
            ObjectMetadata metadata =
                    this.cosClient.getObjectMetadata(metadataRequest);
            long cosFileSize = metadata.getContentLength();
            long localFileSize = this.fileStatus.getLen();
            // 首先要满足长度是一致的
            if (cosFileSize == localFileSize) {
                if (this.md5sum != null) {
                    if (this.md5sum.compareToIgnoreCase(metadata.getETag()) == 0) {
                        // 这里单文件是可以校验MD5值的
                        return true;
                    }
                } else {
                    // 没有给文件的原始MD5sum，这里如果开启了强制校验MD5值的话，就应该认为不存在
                    if (configReader.isForceCheckMD5Sum()) {
                        return false;
                    }
                }
            }
        } catch (Exception e) {
            log.debug("file is not exist. bucket: " + configReader.getBucket()
                    + "," + "cos path: " + cosPath + "msg: " + e.getMessage());
        }

        return false;
    }

    /**
     * 根据文件长度判断是否需要跳过该文件的上传
     *
     * @return
     */
    protected boolean ifSkipUploadFile() {
        this.checkInternalMember();

        if (!configReader.isSkipIfLengthMatch()) {
            return false;
        }

        // 根据文件长度判断是否需要跳过文件
        return this.checkFileExistsWithLength();
    }

    protected void UploadFile() throws Exception {
        this.checkInternalMember();

        //判断是否需要跳过文件
        if (this.ifSkipUploadFile()) {
            log.info("file:{} already exists on COS. Skip to upload it.",
                    this.fileStatus.getPath().toString());
            Statistics.instance.addSkipFile();
            return;
        }

        long fileSize =
                this.fileSystem.getFileStatus(this.fileStatus.getPath()).getLen();
        if (fileSize > FileToCosTask.MAX_FILE_SIZE) {
            throw new IOException("exceed max support file size, current file"
                    + " size:" + fileSize + " max file size: " + FileToCosTask.MAX_FILE_SIZE);
        }

        // 文件完整性校验
        boolean isUploadSuccess = false;
        if (fileSize <= FileToCosTask.MULTIPART_UPLOAD_THRESHOLD) {
            log.debug("upload file:{} by using single file mode.",
                    this.fileStatus.getPath().toString());
            isUploadSuccess = this.uploadSingleFileWithRetry();
            // 单文件上传需要校验文件的MD5值
            if (configReader.isForceCheckMD5Sum()) {
                isUploadSuccess &= this.checkFileExistsWithMD5Sum();
            } else {
                // 没开启MD5校验，则至少也要校验文件的长度信息
                isUploadSuccess &= this.checkFileExistsWithLength();
            }
        } else {
            log.debug("upload file:{} by using multipart upload mode.",
                    this.fileStatus.getPath().toString());
            isUploadSuccess = this.uploadMultipartWithRetry();
            // 分块上传文件需要校验文件的长度
            isUploadSuccess &= this.checkFileExistsWithLength();
        }

        if (isUploadSuccess) {
            // 检查文件确实已经上传成功了
            String taskInfo = String.format("[upload file successfully] [file"
                            + " path: %s] [cos path: %s]",
                    this.fileStatus.getPath().toString(),
                    this.cosPath.toString());
            log.info(taskInfo);
            Statistics.instance.addUploadFileOk();
            String printlnStr =
                    String.format("[success] [file path: %s]",
                            this.fileStatus.getPath().toString());
            System.out.println(printlnStr);
        } else {
            String taskInfo = String.format("[upload file failed] [file path:"
                            + " %s] [cos path: %s]",
                    this.fileStatus.getPath().toString(), this.cosPath);
            log.error(taskInfo);
            Statistics.instance.addUploadFileFail();
            String printlnStr = String.format("[failure] [file path: %s]",
                    this.fileStatus.getPath(), this.cosPath);
            System.err.println(printlnStr);
        }
    }

    private boolean uploadSingleFileWithRetry() throws Exception {
        this.checkInternalMember();
        boolean isUploadSuccess = false;
        for (int i = 0; i < this.kMaxRetryNum; i++) {
            InputStream fStream = null;
            try {
                // 如果开启了强制校验MD5，那么首先要检查文件的MD5值

                if (configReader.isForceCheckMD5Sum() && null == this.md5sum) {
                    fStream = this.fileSystem.open(this.fileStatus.getPath());
                    fStream.skip(0);
                    try {
                        this.md5sum = Utils.calInputStreamCheckSum(fStream,
                                "MD5");
                        log.debug("The file: {} 's MD5 checksum is {}",
                                this.fileStatus.getPath().toString(),
                                this.md5sum);
                    } catch (Exception e) {
                        log.error("Calculate the checksum of the original "
                                        + "file: {} occurs an exception: {}.",
                                this.fileStatus.getPath().toString(), e);
                        this.md5sum = null;         // MD5校验和无效
                        isUploadSuccess = false;
                        break;
                    } finally {
                        if (null != fStream) {
                            try {
                                fStream.close();
                                fStream = null;
                            } catch (IOException e2) {
                                log.warn("close file input stream failed. "
                                        + "exception: " + e2.getMessage());
                            }
                        }
                    }
                }

                fStream = this.fileSystem.open(this.fileStatus.getPath());      // 重新打开文件，正式开始上传
                fStream.skip(0);
                ObjectMetadata metadata = new ObjectMetadata();
                long fileSize =
                        this.fileSystem.getFileStatus(this.fileStatus.getPath()).getLen();
                metadata.setContentLength(fileSize);
                PutObjectRequest putObjectRequest =
                        new PutObjectRequest(configReader.getBucket(),
                                this.cosPath, fStream, metadata);
                PutObjectResult result =
                        this.cosClient.putObject(putObjectRequest);
                isUploadSuccess = true;
                // 如果开启了强制校验MD5值，则会强制校验一遍MD5
                if (configReader.isForceCheckMD5Sum()) {
                    if (null != this.md5sum && this.md5sum.compareToIgnoreCase(result.getETag()) == 0) {
                        isUploadSuccess = true;
                        break;
                    } else {
                        // 由于开启了强制校验MD5Sum，因此，如果没有提供文件的原始MD5Sum，则也会认为是失败的
                        isUploadSuccess = false;
                        break;
                    }
                }
                if (isUploadSuccess) {
                    log.info("upload single file: {} successfully. "
                                    + "cos_path:{} request_id: {}",
                            this.fileStatus.getPath(), this.cosPath,
                            result.getRequestId());
                    break;
                } else {
                    log.error("upload single file:{} failed. retry count: {},"
                                    + " total retry num: {}, request_id: {}",
                            this.fileStatus.getPath(), String.valueOf(i),
                            kMaxRetryNum, result.getRequestId());
                    continue;
                }
            } catch (CosServiceException e) {
                log.error("upload single file occurs an exception. "
                        + "retry count:" + String.valueOf(i)
                        + " msg:" + e.getMessage()
                        + " ret code: "
                        + e.getErrorCode()
                        + " xml: " + e.getErrorResponseXml());
                try {
                    Thread.sleep(this.kRetryInterval);
                } catch (InterruptedException e1) {
                    break;
                }
                continue;           // 继续重试
            } finally {
                if (null != fStream) {
                    try {
                        fStream.close();
                    } catch (IOException e2) {
                        log.warn("close file input stream failed. exception: "
                                + e2.getMessage());
                    }
                }
            }
        }

        return isUploadSuccess;
    }

    protected boolean uploadMultipartWithRetry() throws Exception {
        this.checkInternalMember();

        boolean isUploadSuccess = false;
        String uploadId = this.buildUploadId();
        Map<Integer, PartSummary> existedParts =
                this.identifyExistingPartsForResume(uploadId);

        // 先规整partSize
        long fileSize =
                this.fileSystem.getFileStatus(this.fileStatus.getPath()).getLen();
        long partSize = this.configReader.getPartSize();
        while (partSize * MAX_PART_NUM < fileSize) {
            partSize *= 2;
            if (partSize > MAX_PART_SIZE) {
                partSize = MAX_PART_SIZE;
            }
        }
        // 然后开始上传
        List<Future<PartETag>> allUploadPartTasks =
                new ArrayList<Future<PartETag>>();
        int threadNum = this.configReader.getMaxUploadPartTaskNum();
        ExecutorService service = Executors.newFixedThreadPool(threadNum);
        Semaphore tmpSemaphore = new Semaphore(threadNum);
        long pos = 0;
        for (int partNum = 1; pos < fileSize; partNum++) {
            partSize = Math.min(partSize, fileSize - pos);
            if (existedParts.containsKey(partNum)) {
                log.info("part has already been uploaded, "
                        + "cos path: " + cosPath
                        + " part num: " + partNum
                        + " pos: " + pos
                        + " part size: " + partSize);
                pos += partSize;
                continue;
            }

            while (true) {
                try {
                    tmpSemaphore.acquire();
                    break;
                } catch (InterruptedException e) {
                    log.error("upload multipart with retry acquire occurs an "
                            + "exception: " + e.getMessage());
                    continue;
                }
            }

            UploadPartTask uploadPartTask = new UploadPartTask(
                    this.fileSystem,
                    this.fileStatus.getPath(),
                    this.cosPath,
                    uploadId, partNum, pos, partSize, this.cosClient,
                    tmpSemaphore, this.configReader);
            allUploadPartTasks.add(service.submit(uploadPartTask));
            pos += partSize;
            log.debug("pos : " + pos);
        }

        try {
            service.shutdown();
            service.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
            service.shutdownNow();
        } catch (Exception e) {
            log.error("shutdown and wait part end occur an exception: " + e.toString());
        }

        log.info("Upload all part successfully, localPath:{} cosPath: {}",
                this.fileStatus.getPath(), this.cosPath);

        // complete multipart upload
        List<PartETag> partETags = new ArrayList<PartETag>();
        for (Map.Entry<Integer, PartSummary> entry : existedParts.entrySet()) {
            partETags.add(new PartETag(entry.getKey(),
                    entry.getValue().getETag()));
        }
        for (int i = 0; i < allUploadPartTasks.size(); i++) {
            partETags.add(allUploadPartTasks.get(i).get());
        }
        CompleteMultipartUploadRequest completeMultipartUploadRequest
                = new CompleteMultipartUploadRequest(configReader.getBucket()
                , this.cosPath, uploadId, partETags);
        for (int i = 0; i < this.kMaxRetryNum; i++) {
            try {
                CompleteMultipartUploadResult result =
                        this.cosClient.completeMultipartUpload(completeMultipartUploadRequest);
                this.delScpFile(this.cosPath, uploadId);
                isUploadSuccess = true;
                log.info("complete multipart file successfully, "
                        + "cos path: " + cosPath
                        + " file size: " + String.valueOf(fileSize)
                        + " file path: " + this.fileStatus.getPath().toString()
                        + " etag: " + result.getETag()
                        + " request id: " + result.getRequestId());
                break;
            } catch (CosServiceException e) {
                isUploadSuccess = false;
                log.error("complete multi-part upload failed, "
                        + "retry num: " + String.valueOf(i)
                        + " msg: " + e.getErrorMessage()
                        + " ret_code: " + e.getErrorCode()
                        + " xml: " + e.getErrorResponseXml());
                try {
                    Thread.sleep(this.kRetryInterval);
                } catch (InterruptedException e1) {
                    break;
                }
                continue;
            }
        }

        return isUploadSuccess;
    }

    protected boolean createFolderWithRetry() throws Exception {
        this.checkInternalMember();
        boolean isCreateSuccess = false;
        for (int i = 0; i < this.kMaxRetryNum; i++) {
            InputStream inputStream = null;
            try {
                inputStream = new ByteArrayInputStream(new byte[0]);
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentLength(0);
                if (!this.cosPath.endsWith("/")) {
                    this.cosPath += "/";
                }
                PutObjectRequest putObjectRequest = new PutObjectRequest(
                        configReader.getBucket(), this.cosPath, inputStream,
                        metadata);
                PutObjectResult result =
                        this.cosClient.putObject(putObjectRequest);
                isCreateSuccess = true;
                log.info("create folder: {} successfully, cos path: {}, "
                                + "request id: {}",
                        this.fileStatus.getPath().toString(), this.cosPath,
                        result.getRequestId());
                break;
            } catch (CosServiceException e) {
                isCreateSuccess = false;
                log.error("create folder failed, "
                        + "retry num: " + String.valueOf(i)
                        + " msg: " + e.getErrorMessage()
                        + " ret code: " + e.getErrorCode()
                        + " xml: " + e.getErrorResponseXml());
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
                        log.warn("close byte input stream failed. exception: "
                                + e2.getMessage());
                    }
                }
            }
        }

        return isCreateSuccess;
    }

    private boolean checkFolderExist() {
        try {
            this.cosClient.getObjectMetadata(this.configReader.getBucket(),
                    this.cosPath);
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    protected void CreateFolder() {
        this.checkInternalMember();

        boolean isCreateSuccess = false;

        try {
            if (!this.checkFolderExist()) {
                isCreateSuccess = this.createFolderWithRetry();
            } else {
                log.info("folder already exist. cos path: " + this.cosPath);
                isCreateSuccess = true;
            }
        } catch (Exception e) {
            isCreateSuccess = false;
        }


        if (isCreateSuccess) {
            Statistics.instance.addCreateFolderOk();
            String taskInfo = String.format("[create folder successfully] "
                            + "[folder_path: %s] [cos_path: %s]",
                    this.fileStatus.getPath().toString(), this.cosPath);
            log.info(taskInfo);
            String printlnStr = String.format("[success] [folder path: %s]",
                    this.fileStatus.getPath().toString());
            System.out.println(printlnStr);
        } else {
            Statistics.instance.addCreateFolderFail();
            String taskInfo = String.format("[create folder failed] "
                            + "[folder_path: %s] [cos_path: %s]",
                    this.fileStatus.getPath().toString(), this.cosPath);
            log.error(taskInfo);
            String printlnStr = String.format("[failure] [folder_path: %s] "
                            + "[cos_path: %s]",
                    this.fileStatus.getPath().toString(), this.cosPath);
            System.out.println(printlnStr);
        }
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
            if (null != bw) {
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
            ListPartsRequest listPartsRequest =
                    new ListPartsRequest(configReader.getBucket(),
                            this.cosPath, uploadId);
            try {
                this.cosClient.listParts(listPartsRequest);
                return uploadId;
            } catch (CosServiceException e) {
                log.info("upload id is invalid. ready to init again. upload "
                        + "id: " + uploadId + " cos path: " + this.cosPath);
            }
        }

        InitiateMultipartUploadRequest initiateMultipartUploadRequest =
                new InitiateMultipartUploadRequest(this.configReader.getBucket(), this.cosPath);
        InitiateMultipartUploadResult initiateMultipartUploadResult = null;

        for (int i = 0; i < kMaxRetryNum; i++) {
            try {
                initiateMultipartUploadResult =
                        this.cosClient.initiateMultipartUpload(initiateMultipartUploadRequest);
                this.saveUploadId(initiateMultipartUploadResult.getUploadId());
                break;
            } catch (CosServiceException e) {
                log.error("init multipart upload failed. "
                        + "cos path: " + this.cosPath
                        + " try num: " + String.valueOf(i)
                        + " msg: " + e.getErrorMessage()
                        + " ret code: " + e.getErrorCode()
                        + " xml: " + e.getErrorResponseXml());
                try {
                    Thread.sleep(this.kRetryInterval);
                } catch (InterruptedException e1) {
                    break;
                }
                continue;
            }
        }

        if (null == initiateMultipartUploadResult) {
            throw new Exception("init upload multipart failed. cos path: " + this.cosPath);
        }
        log.info("Init multi-part upload success, cos path: " + this.cosPath + " upload id: " + initiateMultipartUploadResult.getUploadId());
        return initiateMultipartUploadResult.getUploadId();
    }

    private Map<Integer, PartSummary> identifyExistingPartsForResume(String uploadId) {
        Map<Integer, PartSummary> partNumbers = new HashMap<Integer,
                PartSummary>();
        if (null == uploadId || uploadId.length() == 0) {
            return partNumbers;         // 空的partnumber
        }
        int partNum = 0;
        while (true) {
            PartListing parts =
                    this.cosClient.listParts(new ListPartsRequest(this.configReader.getBucket(), this.cosPath, uploadId).withPartNumberMarker(partNum));
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
            log.error("upload file or create directory occurs an exception: "
                    , e);
            String printlnStr = String.format("[failed] [file_path: %s] "
                            + "[cos_path: %s]",
                    this.fileStatus.getPath().toString(),
                    this.cosPath);
            System.out.println(printlnStr);
            Statistics.instance.addUploadFileFail();
        } finally {
        }
    }
}