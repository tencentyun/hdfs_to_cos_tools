package com.qcloud.hdfs_to_cos;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.region.Region;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

public class App {
    static final Logger LOG = LoggerFactory.getLogger(App.class);

    public static COSClient cosClient = null;
    public static BlockingQueue<FileToCosTask> taskBlockingQueue = null;
    public static ExecutorService executorPool = null;

    public static List<HdfsToCosExecutor> executors =
            new LinkedList<HdfsToCosExecutor>();

    public static COSClient buildCosClient(ConfigReader configReader) {
        if (null == configReader) {
            return null;
        }
        ClientConfig clientConfig =
                new ClientConfig(new Region(configReader.getRegion()));
        if (null != configReader.getEndpointSuffix()) {
            clientConfig.setEndPointSuffix(configReader.getEndpointSuffix());
        }
        COSCredentials cred;
        if (null == configReader.getAppid()
                || configReader.getAppid().isEmpty()) {
            cred = new BasicCOSCredentials(configReader.getSecretId(),
                    configReader.getSecretKey());
        } else {
            cred = new BasicCOSCredentials(configReader.getAppid(),
                    configReader.getSecretId(), configReader.getSecretKey());
        }

        return new COSClient(cred, clientConfig);
    }

    public static void main(String[] args) {
        CommandLineParser parser = new DefaultParser();
        CommandLine cli;
        try {
            cli = parser.parse(OptionsArgsName.getAllSupportOption(), args);
            if (cli.hasOption(OptionsArgsName.HELP)) {
                OptionsArgsName.printHelpOption();
                return;
            }
        } catch (ParseException exp) {
            System.err.println("Parsing Argument failed. Reason: " + exp.getMessage());
            OptionsArgsName.printHelpOption();
            return;
        }

        ConfigReader configReader = new ConfigReader(cli);
        if (!configReader.isInitConfigFlag()) {
            System.err.println(configReader.getInitErrMsg());
            return;
        }
        if (null == App.cosClient) {
            App.cosClient = App.buildCosClient(configReader);
        }
        if (null == App.executorPool) {
            App.executorPool =
                    Executors.newFixedThreadPool(configReader.getMaxTaskNum());                         // 任务并发线程池
        }
        if (null == App.taskBlockingQueue) {
            App.taskBlockingQueue =
                    new LinkedBlockingQueue<FileToCosTask>(configReader.getMaxTaskNum() * 2);           // 暂定为任务队列为并发数的2倍
        }

        Statistics.instance.start();
        // 启动消费者
        for (int i = 0; i < configReader.getMaxTaskNum(); i++) {
            HdfsToCosExecutor executor =
                    new HdfsToCosExecutor(App.taskBlockingQueue, true);
            App.executorPool.submit(executor);
            App.executors.add(executor);
        }
        App.executorPool.shutdown();                // 停止提交新的任务
        HdfsToCos hdfsToCos = new HdfsToCos(configReader,
                App.taskBlockingQueue, App.cosClient);
        hdfsToCos.run();
        while (App.taskBlockingQueue.size() != 0) {
            try {
                Thread.sleep(1 * 1000);
            } catch (InterruptedException e) {
                LOG.error("An exception occurred during the polling interval"
                        , e);
            }
        }
        for (HdfsToCosExecutor executor : App.executors) {
            executor.stop();
        }
        try {
            App.executorPool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.error("An exception occurred while waiting for the thread "
                    + "pool to complete", e);
        }
        Statistics.instance.printStatics();
    }
}
