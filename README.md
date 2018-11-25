# HDFS_TO_COS


## 功能说明

HDFS_TO_COS 工具用于将 HDFS 上的数据拷贝到腾讯云对象存储（COS）上。


## 使用环境

### 系统环境
Linux 

### 软件依赖
jdk 1.7 or 1.8


## 配置方法

1. 将要同步的Hdfs集群的core-site.xml拷贝到conf中, 其中包含了NameNode的配置信息
2. 编辑配置文件cos_info.conf, 包括appid和bucket, region以及秘钥信息
3. 可以在命令行参数中指定配置文件位置, 默认读取conf目录下的cos_info.conf
4. 如果命令行参数中的参数和配置文件的重合, 则以命令行为准

注意：命令行参数中的region和endpoint_suffix选项不同时生效，endpoint_suffix是最终请求URL的后缀部分，请求的URL由bucket和endpoint共同组成。


## 使用方法

### 查看命令行支持的选项

在HDFS_TO_COS的安装目录下执行：

./hdfs_to_cos_cmd -h

### 执行复制（不跳过同名文件）
./hdfs_to_cos_cmd --hdfs_path=/tmp/hive --cos_path=/hdfs/20170224/

**Note**：如果COS上存在同名文件，则新拷贝的文件默认会覆盖COS上的同名文件

### 执行复制（跳过大小相等的同名文件）

通过指定`-skip_if_len_match`选项可以让工具跳过COS上大小相等的同名文件：
./hdfs_to_cos_cmd --hdfs_path=/tmp/hive --cos_path=/hdfs/20170224/ -skip_if_len_match

### 上传文件的MD5校验

HDFS_TO_COS工具在每上传一个文件后，默认会根据文件名和文件大小来检查COS上是否存在相同文件，尽力确保上传成功。

这里同时也提供选项`-force_check_md5sum`来标识是否开启小文件（小于128MB）的MD5校验，即只有COS文件和本地文件的MD5值相同，才认为是上传成功的。（此项的额外计算开销会较大）。


## 目录信息
conf : 配置文件, 用于存放core-site.xml和cos_info.conf
log  : 日志目录
src  : java 源程序
dep  : 编译生成的可运行的JAR包

## FAQ
1. 请确保填写的配置信息，包括秘钥信息，bucket和region信息正确，bucket由用户自定义字符串和系统生成appid数字串由中划线连接而成，如：mybucket-1250000000以及机器的时间和北京时间一致(如相差1分钟左右是正常的)，如果相差较大，请设置机器时间。
2. 请保证对于DateNode, 拷贝程序所在的机器也可以连接. 因NameNode有外网IP可以连接, 但获取的
block所在的DateNode机器是内网IP, 无法连接上, 因此建议同步程序放在Hadoop的某个节点上执行,保证对NameNode 和 DateNode皆可访问
3. 权限问题, 用当前账户使用hadoop命令下载文件,看是否正常, 再使用同步工具同步hadoop上的数据
4. 对于COS上已存在的文件, 默认进行重传覆盖，除非用户明确的指定-skip_if_len_match，当文件长度一致时则跳过上传。
5. cos path都认为是目录, 最终从HDFS上拷贝的文件都会存放在该目录下
