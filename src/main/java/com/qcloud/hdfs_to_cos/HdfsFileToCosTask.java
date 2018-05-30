package com.qcloud.hdfs_to_cos;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.model.CompleteMultipartUploadRequest;
import com.qcloud.cos.model.CompleteMultipartUploadResult;
import com.qcloud.cos.model.GetObjectMetadataRequest;
import com.qcloud.cos.model.InitiateMultipartUploadRequest;
import com.qcloud.cos.model.InitiateMultipartUploadResult;
import com.qcloud.cos.model.ListPartsRequest;
import com.qcloud.cos.model.ObjectMetadata;
import com.qcloud.cos.model.PutObjectRequest;
import com.qcloud.cos.model.PartETag;
import com.qcloud.cos.model.PartListing;
import com.qcloud.cos.model.PartSummary;

public class HdfsFileToCosTask implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(HdfsFileToCosTask.class);

    private ConfigReader configReader = null;
    private COSClient cosClient = null;
    private FileStatus hdfsFileStatus = null;
    private String cosPath = null;
    private static final long MAX_PART_SIZE = 2 * 1024 * 1024 * 1024L;
    private static final long MAX_PART_NUM = 10000L;
    private static final long MAX_FILE_SIZE = MAX_PART_NUM * MAX_PART_SIZE;
    private static final long MULTIPART_UPLOAD_THROLD = 64 * 1024 * 1024L;

    private final int kMaxRetryNum = 3;


    public HdfsFileToCosTask(ConfigReader configReader, COSClient cosClient,
            FileStatus hdfsFileStatus) {
        super();
        this.configReader = configReader;
        this.cosClient = cosClient;
        this.hdfsFileStatus = hdfsFileStatus;
    }

    private String ConvertHdfsPathToCosPath() throws IllegalArgumentException, IOException {
        String hdfsFolderPath = configReader.getSrcHdfsPath();
        if (configReader.getHdfsFS().getFileStatus(new Path(hdfsFolderPath)).isFile()) {
            hdfsFolderPath = hdfsFolderPath.substring(0, hdfsFolderPath.lastIndexOf("/"));
        }
        String hdfsFilePath = hdfsFileStatus.getPath().toString();
        String tmpPath = "";
        if (!hdfsFolderPath.endsWith("/")) {
            hdfsFolderPath += "/";
        }

        // hdfs folder path example: hdfs://222:111:333:444:8020/tmp/chengwu/
        if (hdfsFolderPath.startsWith("hdfs://")) {
            hdfsFolderPath =
                    hdfsFolderPath.substring(hdfsFolderPath.indexOf("/", "hdfs://".length()));
            // /tmp/chengwu/
        }

        // hdfs file path example: hdfs://222:111:333:444:8020/tmp/chengwu/xxx/yyy/len5M.txt

        if (hdfsFilePath.startsWith("hdfs://")) {
            hdfsFilePath = hdfsFilePath.substring(hdfsFilePath.indexOf("/", "hdfs://".length()));
            // /tmp/chengwu/xxx/yyy/len5M.txt
        }

        tmpPath = hdfsFilePath.substring(hdfsFolderPath.length());
        String cosFolder = configReader.getDestCosPath();
        if (!cosFolder.endsWith("/")) {
            cosFolder += "/";
        }
        tmpPath = cosFolder + tmpPath;
        if (!tmpPath.endsWith("/") && hdfsFileStatus.isDirectory()) {
            tmpPath += "/";
        }
        return tmpPath;
    }

    private boolean ifSkipUploadFile() {
        if (!configReader.isSkipIfLengthMatch()) {
            return false;
        }
        // 对比文件是否一致
        try {
            GetObjectMetadataRequest statRequest =
                    new GetObjectMetadataRequest(configReader.getBucket(), cosPath);
            ObjectMetadata statObjectMeta = cosClient.getObjectMetadata(statRequest);
            long fileSize = statObjectMeta.getContentLength();
            long hdfsFileLen = hdfsFileStatus.getLen();
            if (fileSize == hdfsFileLen) { // 如果hdfs文件长度和cos的一致, 则认为文件是一样的, 这里只做简单比较
                log.info("hdfs file len equal cos, skip upload. hdfs_path:"
                        + hdfsFileStatus.getPath().toString());
                Statistics.instance.addSkipFile();
                return true;
            }
        } catch (Exception e) {
            log.info("Hdfs File Not Exist, bucket:" + configReader.getBucket() + ", cosPath:"
                    + cosPath + ", msg: " + e.getMessage());
        }
        return false;
    }

    public void UploadFile() {
        try {
            cosPath = ConvertHdfsPathToCosPath();
        } catch (Exception e) {
            Statistics.instance.addUploadFileFail();
            log.error("convertHdfsPathToCos occur a exception. hdfsPath: "
                    + configReader.getSrcHdfsPath() + ", hdfsFilePath: "
                    + hdfsFileStatus.getPath().toString() + ", exception: " + e.toString());
            return;
        }

        // 判断是否要跳过文件
        if (ifSkipUploadFile()) {
            return;
        }


        try {
            long fileSize = CommonHdfsUtils.getFileLength(configReader.getHdfsFS(),
                    hdfsFileStatus.getPath().toString());
            if (fileSize > MAX_FILE_SIZE) {
                throw new Exception("exceed max support file size, current_file_size: " + fileSize
                        + ", max_file_size: " + MAX_FILE_SIZE);
            }
            if (fileSize <= MULTIPART_UPLOAD_THROLD) {
                // 简单上传

                uploadSingeFileWithRetry(fileSize);
            } else {
                long partSize = configReader.getPartSize();

                while (partSize * MAX_PART_NUM < fileSize) {
                    partSize *= 2;
                    if (partSize > MAX_PART_SIZE) {
                        partSize = MAX_PART_SIZE;
                    }
                }

                // 分块上传
                uploadMultiPartWithRetry(cosPath, fileSize, partSize);
            }
            String taskInfo = String.format("[upload file success] [hdfs_path: %s] [cos_path: %s]",
                    hdfsFileStatus.getPath().toString(), cosPath);
            log.info(taskInfo);
            Statistics.instance.addUploadFileOk();
            String printlnStr =
                    String.format("[ok] [hdfs_path: %s]", hdfsFileStatus.getPath().toString());
            System.out.println(printlnStr);
        } catch (Exception e) {
            String taskInfo =
                    String.format("[upload file failure] [hdfs_path: %s] [cos_path: %s] [msg: %s]",
                            hdfsFileStatus.getPath().toString(), cosPath, e.getMessage());
            System.out.println();
            log.info(taskInfo);
            Statistics.instance.addUploadFileFail();
            String printlnStr =
                    String.format("[fail] [hdfs_path: %s]", hdfsFileStatus.getPath().toString());
            System.out.println(printlnStr);
        }
    }

    private void uploadSingeFileWithRetry(long fileSize) throws Exception {
        for (int i = 0; i < kMaxRetryNum; ++i) {
            FSDataInputStream fStream = null;
            try {
                fStream = CommonHdfsUtils.getFileContentBytes(configReader.getHdfsFS(),
                        hdfsFileStatus.getPath().toString(), 0);
                ObjectMetadata meta = new ObjectMetadata();
                meta.setContentLength(fileSize);
                PutObjectRequest putObjectRequest =
                        new PutObjectRequest(configReader.getBucket(), cosPath, fStream, meta);
                cosClient.putObject(putObjectRequest);
                return;
            } catch (CosServiceException e) {
                log.error("upload Singe File failure, retry_num:" + i + ", msg: "
                        + e.getErrorMessage() + ", retcode:" + e.getErrorCode() + ", xml:"
                        + e.getErrorResponseXml());
                continue;
            } finally {
                if (fStream != null) {
                    try {
                        fStream.close();
                    } catch (Exception e2) {
                        log.warn("close hdfs inputstream failed. excption: " + e2.toString());
                    }
                }
            }
        }
        throw new Exception("upload Singe File failure, cosPath: " + cosPath);
    }

    private void createFolderWithRetry() throws Exception {
        for (int i = 0; i < kMaxRetryNum; ++i) {
            ByteArrayInputStream inputStream = null;
            try {
                inputStream = new ByteArrayInputStream(new byte[0]);
                ObjectMetadata meta = new ObjectMetadata();
                meta.setContentLength(0);
                PutObjectRequest putObjectRequest =
                        new PutObjectRequest(configReader.getBucket(), cosPath, inputStream, meta);
                cosClient.putObject(putObjectRequest);
                return;
            } catch (CosServiceException e) {
                log.error("create Folder failure, retry_num:" + i + ", msg: " + e.getErrorMessage()
                        + ", retcode:" + e.getErrorCode() + ", xml:" + e.getErrorResponseXml());
                continue;
            } finally {
                if (inputStream != null) {
                    try {
                        inputStream.close();
                    } catch (Exception e2) {
                        log.warn("close byte inputstream failed, exception: " + e2.toString());
                    }
                }
            }
        }
    }

    private String getUploadIdFromScp(File scpFile) {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(scpFile));
            return br.readLine();
        } catch (IOException e) {
            log.error("open scp file occur a exception.");
            return "";
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (Exception e2) {
            }
        }
    }

    private String getScpFilePath(String cosPath) {
        String scpFileName =
                DigestUtils.md5Hex(cosPath + "_" + hdfsFileStatus.getPath().toString()) + ".scp";
        String scpFilePath = "./scp/" + scpFileName;
        return scpFilePath;
    }

    private String buildUploadId(String cosPath) throws Exception {
        File scpFile = new File(getScpFilePath(cosPath));
        String uploadId = "";
        if (scpFile.exists()) {
            uploadId = getUploadIdFromScp(scpFile);
            // 检查下uploadId是否还有效
            ListPartsRequest listPartsRequest =
                    new ListPartsRequest(configReader.getBucket(), cosPath, uploadId);
            try {
                cosClient.listParts(listPartsRequest);
                return uploadId;
            } catch (CosServiceException e) {
                log.info("upload id is invalid. ready to init again. uploadid: " + uploadId
                        + ", cospath:" + cosPath);
            }
        }

        // step 1 init upload
        InitiateMultipartUploadRequest initRequest =
                new InitiateMultipartUploadRequest(configReader.getBucket(), cosPath);
        InitiateMultipartUploadResult initResponse = null;
        for (int i = 0; i < kMaxRetryNum; ++i) {
            try {
                initResponse = cosClient.initiateMultipartUpload(initRequest);
                saveUploadId(cosPath, initResponse.getUploadId());
                break;
            } catch (CosServiceException e) {
                log.error("init multi upload failure, cos_path: " + cosPath + ", retry_num:" + i
                        + ", msg: " + e.getErrorMessage() + ", retcode:" + e.getErrorCode()
                        + ", xml:" + e.getErrorResponseXml());
                continue;
            } catch (Exception e) {
                throw e;
            }
        }
        if (initResponse == null) {
            throw new Exception("init upload multi part failure, cosPath:" + cosPath);
        }
        log.info("Init multi-part upload success, cos_path:" + cosPath + ", uploadId: "
                + initResponse.getUploadId());
        return initResponse.getUploadId();
    }

    private void saveUploadId(String cosPath, String uploadId) {
        String scpFilePath = getScpFilePath(cosPath);
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(scpFilePath));
            bw.write(uploadId);
        } catch (IOException e) {
            log.error("save point file occur a exception. " + e.toString());
        } finally {
            if (bw != null) {
                try {
                    bw.close();
                } catch (IOException e) {
                }
            }
        }
    }

    private void delScpFile(String cosPath, String uploadId) {
        String scpFilePath = getScpFilePath(cosPath);
        File scpFile = new File(scpFilePath);
        scpFile.delete();
    }

    private Map<Integer, PartSummary> identifyExistingPartsForResume(String cosPath,
            String uploadId) {
        Map<Integer, PartSummary> partNumbers = new HashMap<Integer, PartSummary>();
        if (uploadId == null) {
            return partNumbers;
        }
        int partNumber = 0;

        while (true) {
            PartListing parts = cosClient
                    .listParts(new ListPartsRequest(configReader.getBucket(), cosPath, uploadId)
                            .withPartNumberMarker(partNumber));
            for (PartSummary partSummary : parts.getParts()) {
                partNumbers.put(partSummary.getPartNumber(), partSummary);
            }
            if (!parts.isTruncated()) {
                return partNumbers;
            }
            partNumber = parts.getNextPartNumberMarker();
        }
    }

    private void uploadMultiPartWithRetry(String cosPath, long fileSize, long partSize)
            throws Exception {

        // step get uploadid
        String uploadId = buildUploadId(cosPath);

        // List Upload Part
        Map<Integer, PartSummary> existedParts = identifyExistingPartsForResume(cosPath, uploadId);

        // step 2 parallel upload part(retry in task)
        List<Future<PartETag>> allUploadPartTasks = new ArrayList<Future<PartETag>>();
        int threadNum = configReader.getMaxUploadPartTaskNum();
        log.info("cospath: " + cosPath + ", fileSize: " + fileSize + ", partSize:" + partSize);
        ExecutorService service = Executors.newFixedThreadPool(threadNum);
        Semaphore semaphore = new Semaphore(threadNum);
        long pos = 0;
        for (int partNum = 1; pos < fileSize; ++partNum) {
            partSize = (int) Math.min(partSize, fileSize - pos);
            if (existedParts.containsKey(partNum)) {
                log.info("part has already been uploaded, cosPath: " + cosPath + ", partNumber:"
                        + partNum + ", pos: " + pos + ", partSize:" + partSize);
                pos += partSize;
                continue;
            }

            while (true) {
                try {
                    semaphore.acquire();
                    break;
                } catch (InterruptedException e) {
                    log.error("uploadMultiPartWithRetry semaphore acuire occur a exception: "
                            + e.toString());
                    continue;
                }
            }

            UploadPartTask task = new UploadPartTask(hdfsFileStatus.getPath().toString(), cosPath,
                    uploadId, partNum, pos, partSize, cosClient, semaphore, configReader);
            allUploadPartTasks.add(service.submit(task));
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
        // step 3 complete multipart
        List<PartETag> partETags = new ArrayList<PartETag>();
        for (Entry<Integer, PartSummary> entry : existedParts.entrySet()) {
            partETags.add(new PartETag(entry.getKey(), entry.getValue().getETag()));
        }
        for (int i = 0; i < allUploadPartTasks.size(); ++i) {
            partETags.add(allUploadPartTasks.get(i).get());
        }
        CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(
                configReader.getBucket(), cosPath, uploadId, partETags);
        for (int i = 0; i < kMaxRetryNum; ++i) {
            try {
                CompleteMultipartUploadResult result =
                        cosClient.completeMultipartUpload(compRequest);
                delScpFile(cosPath, uploadId);
                log.info("complete multipart file success, cosPath:" + cosPath + ", fileSize:"
                        + fileSize + ", hdfspath:" + hdfsFileStatus.getPath().toString()
                        + ", Etag: " + result.getETag());
                return;
            } catch (CosServiceException e) {
                log.error("complete multi upload failure, retry_num:" + i + ", msg: "
                        + e.getErrorMessage() + ", retcode:" + e.getErrorCode() + ", xml:"
                        + e.getErrorResponseXml());
                continue;
            } catch (Exception e) {
                throw e;
            }
        }

        throw new Exception("upload file failure, cosPath: " + cosPath + ", hdfspath: "
                + hdfsFileStatus.getPath().toString());
    }

    private boolean JudgeFolderExist() {
        try {
            cosClient.getobjectMetadata(configReader.getBucket(), cosPath);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public void CreateFolder() {
        try {
            cosPath = ConvertHdfsPathToCosPath();
        } catch (Exception e) {
            Statistics.instance.addUploadFileFail();
            log.error("convertHdfsPathToCos occur a exception. hdfsPath: "
                    + configReader.getSrcHdfsPath() + ", hdfsFolderPath: "
                    + hdfsFileStatus.getPath().toString() + ", exception: " + e.toString());
        }



        try {

            if (!JudgeFolderExist()) {
                createFolderWithRetry();
            } else {
                log.info("folder already exist!" + cosPath);
            }
            String taskInfo =
                    String.format("[create folder] [hdfs_path: %s] [cos_path: %s] [ret: %s]",
                            hdfsFileStatus.getPath().toString(), cosPath, "success");
            log.info(taskInfo);
            Statistics.instance.addCreateFolderOk();
            String printlnStr =
                    String.format("[ok] [hdfs_path: %s]", hdfsFileStatus.getPath().toString());
            System.out.println(printlnStr);
        } catch (Exception e) {
            String taskInfo =
                    String.format("[create folder] [hdfs_path: %s] [cos_path: %s] [ret: %s]",
                            hdfsFileStatus.getPath().toString(), cosPath, e.getMessage());
            log.error(taskInfo);
            Statistics.instance.addCreateFolderFail();
            String printlnStr =
                    String.format("[fail] [hdfs_path: %s]", hdfsFileStatus.getPath().toString());
            System.out.println(printlnStr);
        }
    }

    public void run() {
        if (hdfsFileStatus.isFile()) {
            UploadFile();
        } else if (hdfsFileStatus.isDirectory()) {
            CreateFolder();
        } else {
        }
    }

}
