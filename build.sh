#!/usr/bin/env bash

mvn clean package assembly:single -Dmaven.test.skip=true

export HDFS_TO_COS_HOME=`pwd`

mv ${HDFS_TO_COS_HOME}/target/hdfs_to_cos-0.0.1-SNAPSHOT-jar-with-dependencies.jar ${HDFS_TO_COS_HOME}/dep/