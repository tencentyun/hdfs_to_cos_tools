package com.qcloud.hdfs_to_cos;

import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Statistics {
    private static final Logger log = LoggerFactory.getLogger(Statistics.class);

    private AtomicLong createFolderOkNum = new AtomicLong();
    private AtomicLong createFolderFailedNum = new AtomicLong();
    private AtomicLong uploadFileOkNum = new AtomicLong();
    private AtomicLong uploadFileFailedNum = new AtomicLong();
    private AtomicLong skipFileNum = new AtomicLong();
    private Date startTime;

    public static final Statistics instance = new Statistics();

    private Statistics() {
    }

    public void start() {
        this.startTime = new Date();
    }

    public void addCreateFolderOk() {
        this.createFolderOkNum.incrementAndGet();
    }

    public void addCreateFolderFail() {
        this.createFolderFailedNum.incrementAndGet();
    }

    public void addUploadFileOk() {
        this.uploadFileOkNum.incrementAndGet();
    }

    public void addUploadFileFail() {
        this.uploadFileFailedNum.incrementAndGet();
    }

    public void addSkipFile() {
        this.skipFileNum.incrementAndGet();
    }

    public void printStatics() {
        Date endTime = new Date();
        String infoMsg = String.format("[Folder Operation Result: [%d(sum)/ %d(ok) / %d(fail)]",
                this.createFolderOkNum.get() + this.createFolderFailedNum.get(),
                this.createFolderOkNum.get(), this.createFolderFailedNum.get());
        log.info(infoMsg);
        System.out.println(infoMsg);


        infoMsg = String.format("[File Operation Result: [%d(sum)/ %d(ok) / %d(fail) / %d(skip)]",
                this.uploadFileOkNum.get() + this.uploadFileFailedNum.get() + this.skipFileNum.get(),
                this.uploadFileOkNum.get(), this.uploadFileFailedNum.get(), this.skipFileNum.get());
        log.info(infoMsg);
        System.out.println(infoMsg);
        System.out.println(String.format("[Used Time: %d s]", (endTime.getTime() - startTime.getTime()) / 1000));
    }

}
