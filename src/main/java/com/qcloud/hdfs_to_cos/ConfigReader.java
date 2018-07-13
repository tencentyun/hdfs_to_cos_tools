package com.qcloud.hdfs_to_cos;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.Path;

public class ConfigReader {

    private boolean initConfigFlag = true;
    private String initErrMsg = "";

    private long appid = 0;
    private String secretId = "";
    private String secretKey = "";
    private String bucket = "";
    private String region = "";
    private String endpointSuffix = "";
    private String srcHdfsPath = "";
    private String destCosPath = "";
    private boolean skipIfLengthMatch = false;
    private int maxTaskNum = 4;
    private int maxMultiPartUploadTaskNum = 4;
    private static final int DEFAULT_PART_SIZE = 32 * 1024 * 1024;

    private CommandLine cli = null;
    private Properties userInfoProp = null;
    private FileSystem hdfsFS = null;
    private HarFileSystem harFs = null;

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
            this.appid = formatLongStr(OptionsArgsName.APPID,
                    getRequiredStringParam(OptionsArgsName.APPID, "0")).longValue();
            this.secretId = getRequiredStringParam(OptionsArgsName.SECRET_ID, null);
            this.secretKey = getRequiredStringParam(OptionsArgsName.SECRET_KEY, null);
            this.bucket = getRequiredStringParam(OptionsArgsName.BUCKET, null);
            this.endpointSuffix = getRequiredStringParam(OptionsArgsName.ENDPOINT_SUFFIX, null);
            if (null == this.endpointSuffix) {
                this.region = getRequiredStringParam(OptionsArgsName.REGION, "");
            }
            this.srcHdfsPath = getRequiredStringParam(OptionsArgsName.HDFS_PATH, null);
            this.destCosPath = getRequiredStringParam(OptionsArgsName.COS_PATH, null);
            this.maxTaskNum = formatLongStr(OptionsArgsName.MAX_TASK_NUM,
                    getRequiredStringParam(OptionsArgsName.MAX_TASK_NUM, "4")).intValue();
            this.maxMultiPartUploadTaskNum = formatLongStr(OptionsArgsName.MAX_MULTIPART_UPLOAD_TASK_NUM,
                    getRequiredStringParam(OptionsArgsName.MAX_MULTIPART_UPLOAD_TASK_NUM, "4")).intValue();
            if (cli.hasOption(OptionsArgsName.SKIP_IF_LENGTH_MATCH)) {
                this.skipIfLengthMatch = true;
            }

        } catch (IllegalArgumentException e) {
            this.initConfigFlag = false;
            this.initErrMsg = String.format(e.getMessage());
            return;
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
                    "config error, hdfs_conf_file \"%s\" doesn't exist or can't be read!",
                    hdfsConfPath);
            return false;
        }

        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("fs.hdfs.impl.disable.cache", "true");
        conf.addResource(new Path(hdfsConfPath));

        try {
            this.hdfsFS = FileSystem.get(conf);
        } catch (IOException e) {
            this.initConfigFlag = false;
            this.initErrMsg = String.format(e.getMessage());
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
                    "config error, cos_info_file \"%s\" doesn't exist or can't be read!",
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
            this.initErrMsg = String.format("config error: read file %s occur a exception: %s",
                    cosConfPath, e.getMessage());
            return false;
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    this.initConfigFlag = false;
                    this.initErrMsg =
                            String.format("config error: close file %s occur a exception: %s",
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
            long longValue = Long.valueOf(valueStr);
            return longValue;
        } catch (NumberFormatException e) {
            String errMsg = String.format("config error: %s value is illeagl num!", key);
            throw new IllegalArgumentException(errMsg);
        }
    }

    public boolean isInitConfigFlag() {
        return initConfigFlag;
    }

    public String getInitErrMsg() {
        return initErrMsg;
    }

    public long getAppid() {
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

    public int getMaxTaskNum() {
        return maxTaskNum;
    }

    public int getMaxUploadPartTaskNum() {
        return maxMultiPartUploadTaskNum;
    }

    public int getPartSize() {
        return DEFAULT_PART_SIZE;
    }

    public FileSystem getHdfsFS() {
        return hdfsFS;
    }

    public boolean isSkipIfLengthMatch() {
        return skipIfLengthMatch;
    }


}
