package com.qcloud.hdfs_to_cos;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.model.PartETag;
import com.qcloud.cos.model.UploadPartRequest;


public class UploadPartTask implements Callable<PartETag> {
    private static final Logger log =
            LoggerFactory.getLogger(UploadPartTask.class);
    private int kMaxRetryNum = 3;
    private long retryInterval = 500;

    public UploadPartTask(FileSystem fileSystem, Path filePath, String key,
                          String uploadId, int partNumber, long pos,
                          long partSize, COSClient cosClient, Semaphore semaphore,
                          ConfigReader configReader) {
        super();
        this.fileSystem = fileSystem;
        this.filePath = filePath;
        this.key = key;
        this.uploadId = uploadId;
        this.partNumber = partNumber;
        this.pos = pos;
        this.partSize = partSize;
        this.cosClient = cosClient;
        this.semaphore = semaphore;
        this.configReader = configReader;
        this.kMaxRetryNum = configReader.getMaxRetryNum();
        this.retryInterval = configReader.getRetryInterval();
    }

    public PartETag call() throws Exception {
        try {
            return uploadPartWithRetry();
        } finally {
            if (null != this.semaphore) {
                semaphore.release();
            }
        }
    }

    private PartETag uploadPartWithRetry() throws Exception {
        for (int i = 0; i < kMaxRetryNum; ++i) {
            FSDataInputStream fStream = null;
            try {
                fStream = this.fileSystem.open(this.filePath);
                fStream.skip(this.pos);
                UploadPartRequest uploadRequest =
                        new UploadPartRequest().withBucketName(configReader.getBucket())
                                .withUploadId(uploadId).withKey(key).withPartNumber(partNumber)
                                .withInputStream(fStream).withPartSize(partSize);
                if (this.configReader.getTrafficLimit() > 0) {
                    uploadRequest.setTrafficLimit(this.configReader.getTrafficLimit());
                }
                PartETag etag =
                        cosClient.uploadPart(uploadRequest).getPartETag();
                log.info("upload part successfully, etag: " + etag.getETag() + ", part_number: "
                        + etag.getPartNumber() + ", bucket: " + configReader.getBucket() + ", key:"
                        + key);
                return etag;
            } catch (CosServiceException e) {
                log.error("upload part occurs an exception. "
                        + "retry count:" + String.valueOf(i)
                        + " msg:" + e.getMessage()
                        + " ret code: "
                        + e.getErrorCode()
                        + " xml: " + e.getErrorResponseXml());
                try {
                    Utils.sleep(i, this.retryInterval);
                } catch (InterruptedException e1) {
                    break;
                }
            } finally {
                if (fStream != null) {
                    try {
                        fStream.close();
                    } catch (IOException e) {
                        log.warn("close hdfs file input stream failed");
                    }
                }
            }
        }
        throw new Exception("upload part failed, msg: " + this.toString());
    }

    public void setkMaxRetryNum(int kMaxRetryNum) {
        this.kMaxRetryNum = kMaxRetryNum;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("bucketName:").append(configReader.getBucket()).append(", "
                + "keyName:").append(key)
                .append(", uploadId:").append(uploadId).append(", partNumber"
                + ":").append(partNumber);
        return sb.toString();
    }

    private FileSystem fileSystem;
    private Path filePath;
    private String key;
    private String uploadId;
    private int partNumber;
    private long pos;
    private long partSize;
    private COSClient cosClient;
    private Semaphore semaphore;
    private ConfigReader configReader;
}
