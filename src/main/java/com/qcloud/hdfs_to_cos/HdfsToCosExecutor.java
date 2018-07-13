package com.qcloud.hdfs_to_cos;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class HdfsToCosExecutor implements Runnable{
    private static final Logger log = LoggerFactory.getLogger(HdfsToCosExecutor.class);

    private AtomicBoolean stop = null;
    private BlockingQueue<FileToCosTask> taskBlockingQueue = null;

    public HdfsToCosExecutor(BlockingQueue<FileToCosTask> taskBlockingQueue, boolean isStart) {
        this.taskBlockingQueue = taskBlockingQueue;
        this.stop = new AtomicBoolean(!isStart);
    }

    public void run() {
        if(null == this.stop || null == this.taskBlockingQueue){
            log.error("running flag or task queue is null, finish.");
            return;
        }

        while(!this.stop.get()){
            try {
                FileToCosTask task = this.taskBlockingQueue.poll(300, TimeUnit.MILLISECONDS);             // 等待取出一个待执行的任务
                if(null != task){
                    task.run();                                                                                     // 开始执行任务
                }
            } catch (InterruptedException e) {
                log.error("taking a task is interrupted. continue to take...");
            }
        }
    }

    public void stop(){
        if(null != this.stop){
            this.stop.set(true);                                                // 停止执行
        }
    }
}
