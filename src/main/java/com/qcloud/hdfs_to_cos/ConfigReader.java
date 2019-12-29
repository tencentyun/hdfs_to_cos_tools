package com.qcloud.hdfs_to_cos;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ConfigReader {

    private boolean initConfigFlag = true;
    private String initErrMsg = "";

    private String appid;
    private String secretId;
    private String secretKey;
    private String bucket;
    private String region;
    private String endpointSuffix;
    private String srcHdfsPath;
    private String destCosPath;
    private String storageClass;
    private boolean skipIfLengthMatch = false;
    // 是否开启强制校验MD5值,如果没有开启则只校验文件长度
    private boolean forceCheckMD5Sum = false;
    private boolean decompressHarFile = false;  // 是否自动解压har文件
    private int maxTaskNum = 4;
    private int maxMultiPartUploadTaskNum = 4;
    private int partSize = 0;
    private static final int DEFAULT_PART_SIZE = 8 * 1024 * 1024;       //
    // 默认的块大小为8MB
    private CommandLine cli;
    private Properties userInfoProp = null;
    private FileSystem hdfsFS = null;

    private int maxRetryNum = DEFAULT_MAX_RETRY_NUM;
    private static final int DEFAULT_MAX_RETRY_NUM = 5;

    private long retryInterval = DEFAULT_MAX_RETRY_INTERVAL;
    private static final long DEFAULT_MAX_RETRY_INTERVAL = 500;

    public ConfigReader(CommandLine cli) {
        this.cli = cli;
        init();
    }

    private void init() {
        if (!buildHdfsFS()) {
            return;
        }

        if (!buildCosInfoPath()) {
            return;
        }

        try {
            this.appid = getRequiredStringParam(OptionsArgsName.APPID, null);
            this.secretId = getRequiredStringParam(OptionsArgsName.SECRET_ID,
                    null);
            this.secretKey =
                    getRequiredStringParam(OptionsArgsName.SECRET_KEY, null);
            this.bucket = getRequiredStringParam(OptionsArgsName.BUCKET, null);
            this.endpointSuffix =
                    getRequiredStringParam(OptionsArgsName.ENDPOINT_SUFFIX,
                            null);
            if (null == this.endpointSuffix) {
                this.region = getRequiredStringParam(OptionsArgsName.REGION,
                        "");
            }
            this.srcHdfsPath =
                    getRequiredStringParam(OptionsArgsName.HDFS_PATH, null);
            this.destCosPath =
                    getRequiredStringParam(OptionsArgsName.COS_PATH, null);
            this.storageClass =
                    getRequiredStringParam(OptionsArgsName.STORAGE_CLASS, "STANDARD");
            this.maxTaskNum = formatLongStr(OptionsArgsName.MAX_TASK_NUM,
                    getRequiredStringParam(OptionsArgsName.MAX_TASK_NUM, "4")).intValue();
            this.maxMultiPartUploadTaskNum =
                    formatLongStr(OptionsArgsName.MAX_MULTIPART_UPLOAD_TASK_NUM,
                            getRequiredStringParam(OptionsArgsName.MAX_MULTIPART_UPLOAD_TASK_NUM, "4")).intValue();
            if (cli.hasOption(OptionsArgsName.SKIP_IF_LENGTH_MATCH)) {
                this.skipIfLengthMatch = true;
            }

            if (cli.hasOption(OptionsArgsName.FORCE_CHECK_MD5SUM)) {
                this.forceCheckMD5Sum = true;
            }

            if (cli.hasOption(OptionsArgsName.DECOMPRESS_HAR)) {
                this.decompressHarFile = true;
            }

            if (cli.hasOption(OptionsArgsName.UPLOAD_PART_SIZE)) {
                this.partSize = formatLongStr(
                        OptionsArgsName.UPLOAD_PART_SIZE,
                        getRequiredStringParam(OptionsArgsName.UPLOAD_PART_SIZE,
                                String.valueOf(ConfigReader.DEFAULT_PART_SIZE))).intValue();
            }

            if (cli.hasOption(OptionsArgsName.MAX_RETRY_NUM)) {
                this.maxRetryNum = formatLongStr(OptionsArgsName.MAX_RETRY_NUM,
                        getRequiredStringParam(OptionsArgsName.MAX_RETRY_NUM,
                                String.valueOf(ConfigReader.DEFAULT_MAX_RETRY_NUM))).intValue();
            }

            if (cli.hasOption(OptionsArgsName.RETRY_INTERVAL)) {
                this.retryInterval =
                        formatLongStr(OptionsArgsName.RETRY_INTERVAL,
                                getRequiredStringParam(OptionsArgsName.RETRY_INTERVAL,
                                        String.valueOf(ConfigReader.DEFAULT_MAX_RETRY_INTERVAL))).intValue();
            }

        } catch (IllegalArgumentException e) {
            this.initConfigFlag = false;
            this.initErrMsg = e.getMessage();
        }
    }

    private boolean buildHdfsFS() {
        String hdfsConfPath = "./conf/core-site.xml";
        if (cli.hasOption(OptionsArgsName.HDFS_CONF_FILE)) {
            hdfsConfPath = cli.getOptionValue(OptionsArgsName.HDFS_CONF_FILE);
        }

        File hdfsConfFile = new File(hdfsConfPath);
        if (!hdfsConfFile.isFile() || !hdfsConfFile.canRead()) {
            this.initConfigFlag = false;
            this.initErrMsg = String.format(
                    "config error, hdfs_conf_file \"%s\" doesn't exist or "
                            + "can't be read!",
                    hdfsConfPath);
            return false;
        }

        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs"
                + ".DistributedFileSystem");
        conf.set("fs.hdfs.impl.disable.cache", "true");
        conf.addResource(new Path(hdfsConfPath));

        try {
            this.hdfsFS = FileSystem.get(conf);
        } catch (IOException e) {
            this.initConfigFlag = false;
            this.initErrMsg = e.getMessage();
            return false;
        }

        return true;
    }

    private boolean buildCosInfoPath() {
        String cosConfPath = "./conf/cos_info.conf";
        if (cli.hasOption(OptionsArgsName.COS_CONF_FILE)) {
            cosConfPath = cli.getOptionValue(OptionsArgsName.COS_CONF_FILE);
        }
        File confFile = new File(cosConfPath);
        if (!confFile.isFile() || !confFile.canRead()) {
            this.initConfigFlag = false;
            this.initErrMsg = String.format(
                    "config error, cos_info_file \"%s\" doesn't exist or "
                            + "can't be read!",
                    cosConfPath);
            return false;
        }

        userInfoProp = new Properties();

        FileInputStream in = null;

        try {
            in = new FileInputStream(cosConfPath);
            userInfoProp.load(in);
        } catch (IOException e) {
            this.initConfigFlag = false;
            this.initErrMsg = String.format("config error: read file %s occur"
                            + " a exception: %s",
                    cosConfPath, e.getMessage());
            return false;
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    this.initConfigFlag = false;
                    this.initErrMsg =
                            String.format("config error: close file %s occur "
                                            + "a exception: %s",
                                    cosConfPath, e.getMessage());
                    return false;
                }
            }
        }
        return true;
    }

    private String getRequiredStringParam(String key, String defaultValue)
            throws IllegalArgumentException {
        String value = null;
        value = this.userInfoProp.getProperty(key);
        if (cli.hasOption(key)) {
            value = this.cli.getOptionValue(key);
        }
        if (value == null) {
            value = defaultValue;
            if (key.compareTo(OptionsArgsName.ENDPOINT_SUFFIX) == 0) {                    // endpoint_suffix不是一个必选项
                return value;
            }
        }
        if (value == null) {
            throw new IllegalArgumentException(
                    String.format("config error. config %s is required", key));
        }
        return value.trim();
    }

    private Long formatLongStr(String key, String valueStr) throws IllegalArgumentException {
        try {
            return Long.parseLong(valueStr);
        } catch (NumberFormatException e) {
            String errMsg = String.format("config error: %s value is illeagl "
                    + "num!", key);
            throw new IllegalArgumentException(errMsg);
        }
    }

    public boolean isInitConfigFlag() {
        return initConfigFlag;
    }

    public String getInitErrMsg() {
        return initErrMsg;
    }

    public String getAppid() {
        return appid;
    }

    public String getSecretId() {
        return secretId;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public String getBucket() {
        return bucket;
    }

    public String getRegion() {
        return region;
    }

    public String getEndpointSuffix() {
        return endpointSuffix;
    }

    public String getSrcHdfsPath() {
        return srcHdfsPath;
    }

    public String getDestCosPath() {
        return destCosPath;
    }

    public String getStorageClass() {
        return storageClass;
    }

    public int getMaxTaskNum() {
        return maxTaskNum;
    }

    public int getMaxUploadPartTaskNum() {
        return maxMultiPartUploadTaskNum;
    }

    public int getPartSize() {
        if (this.partSize > 0) {
            return this.partSize;
        } else {
            return DEFAULT_PART_SIZE;
        }
    }

    public FileSystem getHdfsFS() {
        return hdfsFS;
    }

    public boolean isSkipIfLengthMatch() {
        return skipIfLengthMatch;
    }

    public boolean isForceCheckMD5Sum() {
        return this.forceCheckMD5Sum;
    }

    public boolean isDecompressHarFile() {
        return decompressHarFile;
    }

    public int getMaxRetryNum() {
        return maxRetryNum;
    }

    public long getRetryInterval() {
        return retryInterval;
    }
}
