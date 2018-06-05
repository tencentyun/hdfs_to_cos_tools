package com.qcloud.hdfs_to_cos;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.model.GetObjectMetadataRequest;
import com.qcloud.cos.region.Region;


public class HdfsToCos {

    private static final Logger log = LoggerFactory.getLogger(HdfsToCos.class);

    private ConfigReader configReader = null;
    private ArrayList<FileStatus> fileStatusArry = null;
    private ArrayList<FileStatus> harFileStatusArray = null;
    private ExecutorService threadPool = null;
    private COSClient cosClient = null;

    public HdfsToCos(ConfigReader configReader) {
        super();
        this.configReader = configReader;
        this.fileStatusArry = new ArrayList<FileStatus>();
        this.harFileStatusArray = new ArrayList<FileStatus>();
        this.threadPool = Executors.newFixedThreadPool(configReader.getMaxTaskNum());
    }

    private void scanHarMember(Path filePath, HarFileSystem harFs) throws IOException, URISyntaxException {
        FileStatus targetPathStatus = harFs.getFileStatus(filePath);
        if (targetPathStatus.isFile()) {
            this.harFileStatusArray.add(targetPathStatus);
            return;
        }

        FileStatus[] pathStatus = harFs.listStatus(filePath);
        for (FileStatus fileStatus : pathStatus) {
            if (CommonHdfsUtils.isHarFile(fileStatus)) {
                harFs.initialize(CommonHdfsUtils.buildHarFsUri(fileStatus.getPath()), harFs.getConf());
                scanHarMember(fileStatus.getPath(), harFs);
            }

            if (fileStatus.isFile()) {
                this.harFileStatusArray.add(fileStatus);
            }

            if (fileStatus.isDirectory()) {
                scanHarMember(fileStatus.getPath(), harFs);
            }
        }
    }

    private void scanHdfsMember(Path hdfsPath, FileSystem hdfsFS)
            throws FileNotFoundException, IOException, URISyntaxException {
        FileStatus targetPathStatus = hdfsFS.getFileStatus(hdfsPath);
        if (targetPathStatus.isFile()) {
            fileStatusArry.add(targetPathStatus);
            return;
        }
        FileStatus[] memberArry = hdfsFS.listStatus(hdfsPath);
        for (FileStatus member : memberArry) {
            if (CommonHdfsUtils.isHarFile(member)) {
                HarFileSystem harFileSystem = new HarFileSystem(hdfsFS);
                harFileSystem.initialize(CommonHdfsUtils.buildHarFsUri(member.getPath()), hdfsFS.getConf());
                scanHarMember(member.getPath(), harFileSystem);
            } else {
                this.fileStatusArry.add(member);
                if (member.isDirectory()) {
                    scanHdfsMember(member.getPath(), hdfsFS);
                }
            }
        }
    }


    private void buildHdfsFileList() throws IOException, URISyntaxException {
        fileStatusArry.clear();
        this.harFileStatusArray.clear();
        FileSystem hdfsFS = configReader.getHdfsFS();
        scanHdfsMember(new Path(configReader.getSrcHdfsPath()), hdfsFS);
    }

    private void buildCosClient() {
        ClientConfig clientConfig = new ClientConfig(new Region(this.configReader.getRegion()));
        clientConfig.setEndPointSuffix(this.configReader.getEndpointSuffix());
        COSCredentials cred = null;
        if (this.configReader.getAppid() == 0) {
            cred = new BasicCOSCredentials(this.configReader.getSecretId(), this.configReader.getSecretKey());
        } else {
            cred = new BasicCOSCredentials(String.valueOf(this.configReader.getAppid()),
                    this.configReader.getSecretId(), this.configReader.getSecretKey());
        }
        cosClient = new COSClient(cred, clientConfig);
    }

    private boolean checkCosClientLegal() {
        GetObjectMetadataRequest statRequest =
                new GetObjectMetadataRequest(this.configReader.getBucket(), "/");
        for (int i = 0; i < 2; ++i) {
            try {
                cosClient.getObjectMetadata(statRequest);
                log.debug("checkCosClient success!");
                return true;
            } catch (CosServiceException cse) {
                log.error("catch CosServiceException, error msg:" + cse.getMessage());
                continue;
            } catch (CosClientException cse) {
                log.error("catch CosClientException, error msg:" + cse.getMessage());
                continue;
            } catch (Exception e) {
                log.error("catch unkow exception:" + e.toString());
                continue;
            }
        }
        return false;
    }


    public void run() {
        buildCosClient();
        if (!checkCosClientLegal()) {
            StringBuilder errMsgBuilder =
                    new StringBuilder("Get bucket info error! please check your config info\n");
            errMsgBuilder.append("These clues may help you.\n");
            errMsgBuilder.append("1. check appid, ak, sk\n");
            errMsgBuilder.append("2. check bucket and region info\n");
            errMsgBuilder.append("3. check your machine time");
            System.err.println(errMsgBuilder.toString());
            cosClient.shutdown();
            return;
        }

        try {
            buildHdfsFileList();
        } catch (IOException e) {
            String errMsg = String.format("get hdfs member occur a exception: %s", e.getMessage());
            log.error(errMsg);
            System.err.println(errMsg);
            cosClient.shutdown();
            return;
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        for (int index = 0; index < fileStatusArry.size(); ++index) {
            FileStatus fileStatus = fileStatusArry.get(index);
            HdfsFileToCosTask task =
                    new HdfsFileToCosTask(this.configReader, this.cosClient, fileStatus, false);
            threadPool.submit(task);
        }

        for (int index = 0; index < harFileStatusArray.size(); index++) {
            FileStatus fileStatus = this.harFileStatusArray.get(index);
            HdfsFileToCosTask task = new HdfsFileToCosTask(this.configReader, this.cosClient, fileStatus, true);
            threadPool.submit(task);
        }

        threadPool.shutdown();
        try {
            threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            log.error("thread pool await occur a exception: " + e.getMessage());
        }
        cosClient.shutdown();
    }

}
