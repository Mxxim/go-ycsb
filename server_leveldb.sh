#!/bin/bash

TEST_TOOL_PATH=/home/zhengbc/go-ycsb/
#TEST_TOOL_PATH=/Users/sammy/Workspace/go-ycsb/
READCOUNT=1024
FIELDLENGTH=209716
FIELDCOUNT=5

# system type
_SYSTYPE="MAC"
LEVELDB_PATH="data/leveldb"

# set environment
f_set_env(){
    case "$OSTYPE" in
      darwin*)
        echo "RUN SCRIPTS ON OSX"
        _SYSTYPE="MAC"
      ;;
      linux*)
        echo "RUN SCRIPTS ON LINUX"
        _SYSTYPE="LINUX"
      ;;
      *)
        echo "unknown: $OSTYPE"
        exit -1
      ;;
    esac
}

# set system type
f_set_env

if [ ${_SYSTYPE} = "MAC" ]; then
    LEVELDB_PATH="data/leveldb"
else
    LEVELDB_PATH="/opt/leveldb/data"
fi

######参数说明
# 1 - 数据库名称
# 2 - 测试记录文件名称
# 3 - field value字段长度
# 4 - field 个数
# 5 - sanvalue次数
# 6 - read和scan的次数
# 7 - couchbase中load的文档个数
# 8 - couchbase中write的文档个数

run() {
    storage=$1
    recordName=$2
    fieldLength=$3
    fieldCount=$4
    scanCount=$5
    OPERATIONCOUNT=$6

    load_count=$7
    write_count=$8

    if [ ! -d "bin" ];then
        make
    fi

    if [ "$storage" = "leveldb" ];then

        rm -rf $LEVELDB_PATH && mkdir -p $LEVELDB_PATH

        du -sh $LEVELDB_PATH

        # load data
        echo "LOAD $storage ($recordName) ..."
        ./bin/go-ycsb load ${storage} -P workloads/workload_WRITE > logs/${storage}_${recordName}_LOAD.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${load_count} -p recordcount=${load_count} -p leveldb.path=$LEVELDB_PATH

        # write only
        echo "WRITE $storage ($recordName)..."
        ./bin/go-ycsb run ${storage} -P workloads/workload_WRITE > logs/${storage}_${recordName}_W.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${write_count} -p recordcount=${load_count} -p leveldb.path=$LEVELDB_PATH

        du -sh $LEVELDB_PATH

        if [ ${load_count} != 85894846 -o ${load_count} != 6710886 -o ${load_count} != 131072 ];then
          # scan only
          echo "SCAN $storage ($recordName) ..."
          ./bin/go-ycsb run ${storage} -P workloads/workload_SCAN > logs/${storage}_${recordName}_S.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${OPERATIONCOUNT} -p recordcount=${OPERATIONCOUNT} -p leveldb.path=$LEVELDB_PATH
        fi


        # read only
        echo "READ $storage ($recordName) ..."
        ./bin/go-ycsb run ${storage} -P workloads/workload_READ > logs/${storage}_${recordName}_R.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${OPERATIONCOUNT} -p recordcount=${OPERATIONCOUNT} -p leveldb.path=$LEVELDB_PATH
    fi
}

echo start server_leveldb.sh ... && date

######参数说明
# 1 - 数据库名称
# 2 - 测试记录文件名称
# 3 - field value字段长度
# 4 - field 个数
# 5 - sanvalue次数
# 6 - 插入文档个数以及read和scan的次数


#### leveldb 1mb/op, total 1G
echo "================ start leveldb 1M_1G ================" && date
cd ${TEST_TOOL_PATH}
run leveldb 1M_1G $FIELDLENGTH $FIELDCOUNT 100 1024 512 512
echo "================ finish leveldb 1M_1G ================" && date
sleep 30

##### leveldb 20kb/op, total 1G
echo "================ start leveldb 20kb_1G ================" && date
cd ${TEST_TOOL_PATH}
run leveldb 20kb_1G $FIELDLENGTH $FIELDCOUNT 100 52429 26214 26215
echo "================ finish leveldb 20kb_1G ================" && date
sleep 30

###### leveldb 200b/op, total 1G
echo "================ start leveldb 200b_1G ================" && date
cd ${TEST_TOOL_PATH}
run leveldb 200b_1G $FIELDLENGTH $FIELDCOUNT 100 671089 335544 335545
echo "================ finish leveldb 200b_1G ================" && date
sleep 30


##### leveldb 1mb/op, total 16G
echo "================ start leveldb 1M_16G ================" && date
cd ${TEST_TOOL_PATH}
run leveldb 1M_16G $FIELDLENGTH $FIELDCOUNT 50 1024 8192 8192
echo "================ finish leveldb 1M_16G ================" && date
sleep 30

#### leveldb 20kb/op, total 16G
echo "================ start leveldb 20kb_16G ================" && date
cd ${TEST_TOOL_PATH}
run leveldb 20kb_16G $FIELDLENGTH $FIELDCOUNT 50 52429 419430 419431
echo "================ finish leveldb 20kb_16G ================" && date
sleep 30

##### leveldb 200b/op, total 16G
echo "================ start leveldb 200b_16G ================" && date
cd ${TEST_TOOL_PATH}
run leveldb 200b_16G $FIELDLENGTH $FIELDCOUNT 50 671089 5368709 5368709
echo "================ finish leveldb 200b_16G ================" && date
sleep 30



#### leveldb 1mb/op, total 256G
echo "================ start leveldb 1M_256G ================" && date
cd ${TEST_TOOL_PATH}
run leveldb 1M_256G $FIELDLENGTH $FIELDCOUNT 5 1024 131072 131072
echo "================ finish leveldb 1M_256G ================" && date
sleep 30







