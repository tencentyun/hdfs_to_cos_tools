package com.qcloud.hdfs_to_cos;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class OptionsArgsName {
    public static final String HELP =
            "h";                                      // 帮助选项
    public static final String APPID =
            "appid";                                 // appid
    public static final String BUCKET =
            "bucket";                               // bucket
    public static final String REGION =
            "region";                               // region
    public static final String ENDPOINT_SUFFIX =
            "endpoint_suffix";             // 这个是可选项
    public static final String SECRET_ID =
            "ak";                                // secretId
    public static final String SECRET_KEY =
            "sk";                               // secretKey
    public static final String HDFS_PATH =
            "hdfs_path";                         // 要迁移的HDFS的路径
    public static final String COS_PATH =
            "cos_path";                           // cos上的文件路径
    public static final String COS_CONF_FILE =
            "cos_info_file";                 // cos的配置文件路径
    public static final String HDFS_CONF_FILE =
            "hdfs_conf_file";               // hdfs的配置文件路径
    public static final String SKIP_IF_LENGTH_MATCH =
            "skip_if_len_match";      // 是否本地和COS上的文件名相同且长度一致，就跳过
    public static final String FORCE_CHECK_MD5SUM =
            "force_check_md5sum";       // 上传文件检查时，是否强制检查md5sum
    public static final String DECOMPRESS_HAR =
            "decompress_har";           // 迁移过程中，是否解压har文件
    public static final String MAX_TASK_NUM =
            "max_task_num";                   // 并发线程数
    public static final String MAX_MULTIPART_UPLOAD_TASK_NUM =
            "max_multipart_upload_task_num"; // 分块上传的线程数
    public static final String UPLOAD_PART_SIZE = "max_upload_part_size";
    public static final String MAX_RETRY_NUM = "max_retry_num"; // 失败重试的次数
    public static final String RETRY_INTERVAL = "retry_interval";
    public static final String STORAGE_CLASS = "storage_class";

    public static Options getAllSupportOption() {
        Options options = new Options();
        options.addOption(getHelpOption());
        options.addOption(getAppidOption());
        options.addOption(getBucketOption());
        options.addOption(getRegionOption());
        options.addOption(getEndpointSuffixOption());
        options.addOption(getSecretIdOption());
        options.addOption(getSecretKeyOption());
        options.addOption(getHdfsPathOption());
        options.addOption(getCosPathOption());
        options.addOption(getCosInfoFileOption());
        options.addOption(getHdfsInfoFileOption());
        options.addOption(getSkipIfLenMatch());
        options.addOption(getForceCheckMD5Sum());
        options.addOption(getDecompressHar());
        options.addOption(getMaxTaskNumOption());
        options.addOption(getMaxMultiPartUploadTaskNumOption());
        options.addOption(getPartSize());
        options.addOption(getMaxRetryNum());
        options.addOption(getRetryInterval());
        options.addOption(getStorageClass());
        return options;
    }

    public static void printHelpOption() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("hdfs_to_cos", getAllSupportOption());
    }

    public static Option getHelpOption() {
        return Option.builder(HELP).longOpt("help")
                .desc("print help message").build();
    }

    public static Option getAppidOption() {
        return Option.builder(APPID).longOpt(APPID).argName("appid").hasArg()
                .desc("the cos appid, deprecated by bucket flags").build();
    }

    public static Option getBucketOption() {
        return Option.builder(BUCKET).longOpt(BUCKET).argName("bucket_name").hasArg()
                .desc("the cos bucket, Consists of user-defined string and "
                        + "system-generated appid, like mybucket-1250000000").build();
    }

    public static Option getRegionOption() {
        return Option.builder(REGION).longOpt(REGION).argName("region").hasArg()
                .desc("the cos region. legal value cn-south, cn-east, "
                        + "cn-north, sg").build();
    }

    public static Option getEndpointSuffixOption() {
        return Option.builder(ENDPOINT_SUFFIX).longOpt(ENDPOINT_SUFFIX).argName("endpoint_suffix").hasArg()
                .desc("Custom ENDPOINT_SUFFIX, the final URL consists of "
                        + "bucket and endpoint_suffix:{bucket}"
                        + ".{endpoint_suffix} Note: If the endpoint_suffix "
                        + "option is specified, the region is automatically "
                        + "invalidated").build();
    }

    public static Option getSecretIdOption() {
        return Option.builder(SECRET_ID).argName(SECRET_ID).hasArg().desc(
                "the cos secret id")
                .build();
    }

    public static Option getMaxTaskNumOption() {
        return Option.builder(MAX_TASK_NUM).longOpt(MAX_TASK_NUM).hasArg()
                .desc("max parallel task num to upload file default 4").build();
    }

    public static Option getMaxMultiPartUploadTaskNumOption() {
        return Option.builder(MAX_MULTIPART_UPLOAD_TASK_NUM).longOpt(MAX_MULTIPART_UPLOAD_TASK_NUM).hasArg()
                .desc("max parallel multipart upload task num to upload file "
                        + "default 4").build();
    }

    public static Option getSecretKeyOption() {
        return Option.builder(SECRET_KEY).argName(SECRET_KEY).hasArg()
                .desc("the cos secret key").build();
    }

    public static Option getHdfsPathOption() {
        return Option.builder(HDFS_PATH).longOpt("hdfs_path").argName(HDFS_PATH).hasArg()
                .desc("the hdfs path").build();
    }

    public static Option getCosPathOption() {
        return Option.builder(COS_PATH).longOpt("cos_path").argName(COS_PATH).hasArg()
                .desc("the absolute cos folder path").build();
    }

    public static Option getCosInfoFileOption() {
        return Option.builder(COS_CONF_FILE).longOpt(COS_CONF_FILE).hasArg()
                .desc("the cos user info config default is ./conf/cos_info"
                        + ".conf").build();
    }

    public static Option getHdfsInfoFileOption() {
        return Option.builder(HDFS_CONF_FILE).longOpt(HDFS_CONF_FILE).hasArg()
                .desc("the hdfs info config default is ./conf/core-site.xml").build();
    }

    public static Option getSkipIfLenMatch() {
        return Option.builder(SKIP_IF_LENGTH_MATCH).longOpt(SKIP_IF_LENGTH_MATCH)
                .desc("skip upload if hadoop file length match cos").build();
    }

    public static Option getForceCheckMD5Sum() {
        return Option.builder(FORCE_CHECK_MD5SUM).longOpt(FORCE_CHECK_MD5SUM)
                .desc("Enable the check MD5Sum of a uploaded file. Default "
                        + "disable.").build();
    }

    public static Option getDecompressHar() {
        return Option.builder(DECOMPRESS_HAR).longOpt(DECOMPRESS_HAR)
                .desc("Enable the decompression of the har file during the "
                        + "transmission.").build();
    }

    public static Option getPartSize() {
        return Option.builder(UPLOAD_PART_SIZE).longOpt(UPLOAD_PART_SIZE).hasArg()
                .desc("the maximum size of a single block when use the "
                        + "multipart upload").build();
    }

    public static Option getMaxRetryNum() {
        return Option.builder(MAX_RETRY_NUM).longOpt(MAX_RETRY_NUM).hasArg()
                .desc("the maximum retry count when uploading failed.").build();
    }

    public static Option getRetryInterval() {
        return Option.builder(RETRY_INTERVAL).longOpt(RETRY_INTERVAL).hasArg()
                .desc("the interval between retries.").build();
    }

    public static Option getStorageClass() {
        return Option.builder(STORAGE_CLASS).longOpt(STORAGE_CLASS).hasArg()
                .desc("storage class as one of [STANDARD/STANDARD_IA/ARCHIVE").build();
    }
}
