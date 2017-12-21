package com.qcloud.hdfs_to_cos;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class OptionsArgsName {
    public static final String HELP = "h";
    public static final String APPID = "appid";
    public static final String BUCKET = "bucket";
    public static final String REGION = "region";
    public static final String SECRET_ID = "ak";
    public static final String SECRET_KEY = "sk";
    public static final String HDFS_PATH = "hdfs_path";
    public static final String COS_PATH = "cos_path";
    public static final String COS_CONF_FILE = "cos_info_file";
    public static final String HDFS_CONF_FILE = "hdfs_conf_file";
    public static final String SKIP_IF_LENGTH_MATCH = "skip_if_len_match";
    public static final String MAX_TASK_NUM = "max_task_num"; // 并发线程数
    public static final String MAX_MULTIPART_UPLOAD_TASK_NUM = "max_multipart_upload_task_num"; // 分块上传的线程数

    public static Options getAllSupportOption() {
        Options options = new Options();
        options.addOption(getHelpOption());
        options.addOption(getAppidOption());
        options.addOption(getBucketOption());
        options.addOption(getRegionOption());
        options.addOption(getSecretIdOption());
        options.addOption(getSecretKeyOption());
        options.addOption(getHdfsPathOption());
        options.addOption(getCosPathOption());
        options.addOption(getCosInfoFileOption());
        options.addOption(getHdfsInfoFileOption());
        options.addOption(getSkipIfLenMatch());
        return options;
    }

    public static void printHelpOption() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("hdfs_to_cos", getAllSupportOption());
    }

    public static Option getHelpOption() {
        return Option.builder(HELP).longOpt("help").desc("print help message").build();
    }

    public static Option getAppidOption() {
        return Option.builder(APPID).longOpt(APPID).argName("appid").hasArg().desc("the cos appid, deprecated by bucket flags")
                .build();
    }

    public static Option getBucketOption() {
        return Option.builder(BUCKET).longOpt(BUCKET).argName("bucket_name").hasArg()
                .desc("the cos bucket, Consists of user-defined string and system-generated appid, like mybucket-1250000000").build();
    }

    public static Option getRegionOption() {
        return Option.builder(REGION).longOpt(REGION).argName("region").hasArg()
                .desc("the cos region. legal value cn-south, cn-east, cn-north, sg").build();
    }

    public static Option getSecretIdOption() {
        return Option.builder(SECRET_ID).argName(SECRET_ID).hasArg().desc("the cos secret id")
                .build();
    }
    
    public static Option getMaxTaskNumOption() {
        return Option.builder(MAX_TASK_NUM).argName(MAX_TASK_NUM).hasArg().desc("max parallel task num to upload file default 4")
                .build();
    }
    
    public static Option getMaxMultiPartUploadTaskNumOption() {                                     
        return Option.builder(MAX_MULTIPART_UPLOAD_TASK_NUM).longOpt(MAX_MULTIPART_UPLOAD_TASK_NUM).hasArg()
                .desc("max parallel multipart upload task num to upload file default 4").build();   
    }

    public static Option getSecretKeyOption() {
        return Option.builder(SECRET_KEY).argName(SECRET_KEY).hasArg().desc("the cos secret key")
                .build();
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
                .desc("the cos user info config default is ./conf/cos_info.conf").build();
    }

    public static Option getHdfsInfoFileOption() {
        return Option.builder(HDFS_CONF_FILE).longOpt(HDFS_CONF_FILE).hasArg()
                .desc("the hdfs info config default is ./conf/core-site.xml").build();
    }

    public static Option getSkipIfLenMatch() {
        return Option.builder(SKIP_IF_LENGTH_MATCH).longOpt(SKIP_IF_LENGTH_MATCH)
                .desc("skip upload if hadoop file length match cos").build();
    }

}
