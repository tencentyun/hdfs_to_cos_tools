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
    private static final Logger log = LoggerFactory.getLogger(UploadPartTask.class);


    public UploadPartTask(FileSystem fileSystem, Path filePath, String key, String uploadId, int partNumber, long pos,
                          long partSize, COSClient cosClient, Semaphore semaphore, ConfigReader configReader) {
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
        final int kMaxRetryNum = 3;
        for (int i = 0; i < kMaxRetryNum; ++i) {
            FSDataInputStream fStream = null;
            try {
                fStream = this.fileSystem.open(this.filePath);
                fStream.skip(this.pos);
                UploadPartRequest uploadRequest =
                        new UploadPartRequest().withBucketName(configReader.getBucket())
                                .withUploadId(uploadId).withKey(key).withPartNumber(partNumber)
                                .withInputStream(fStream).withPartSize(partSize);
                PartETag etag = cosClient.uploadPart(uploadRequest).getPartETag();
                log.info("upload part success, etag: " + etag.getETag() + ", part_number: "
                        + etag.getPartNumber() + ", bucket: " + configReader.getBucket() + ", key:"
                        + key);
                return etag;
            } catch (CosServiceException e) {
                continue;
            } finally {
                if (fStream != null) {
                    try {
                        fStream.close();
                    } catch (IOException e) {
                        log.warn("close hdfs file inputstream failed");
                    }
                }
            }
        }
        throw new Exception("upload part failure, msg: " + this.toString());
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("bucketName:").append(configReader.getBucket()).append(", keyName:").append(key)
                .append(", uploadId:").append(uploadId).append(", partNumber:").append(partNumber);
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
