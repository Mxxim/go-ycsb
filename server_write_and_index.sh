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


    if [ "$storage" = "mongodb" ];then

        # load data
#        if [ ${load_count} != 131072 ];then
          echo "LOAD $storage ($recordName) ..."
          ./bin/go-ycsb load ${storage} -P workloads/workload_WRITE > logs/${storage}_${recordName}_LOAD.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${load_count} -p recordcount=${load_count}
#        else
#          echo "LOAD $storage ($recordName) ..."
#          ./bin/go-ycsb load ${storage} -P workloads/workload_WRITE > logs/${storage}_${recordName}_LOAD.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${load_count} -p recordcount=${load_count} -p mongodb.indexs=field0,field1,field2,field3,field4
#        fi

        du -sh /opt/mongodb/data

        # write only
        echo "WRITE $storage ($recordName)..."
        ./bin/go-ycsb run ${storage} -P workloads/workload_WRITE > logs/${storage}_${recordName}_W.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${write_count} -p recordcount=${load_count}

        du -sh /opt/mongodb/data

#        # read only
#        echo "READ $storage ($recordName) ..."
#        ./bin/go-ycsb run ${storage} -P workloads/workload_READ > logs/${storage}_${recordName}_R.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${OPERATIONCOUNT} -p recordcount=${OPERATIONCOUNT}
#
#        # scan only
#        if [ ${load_count} != 131072 ];then
#          echo "SCAN $storage ($recordName) ..."
#          ./bin/go-ycsb run ${storage} -P workloads/workload_SCAN > logs/${storage}_${recordName}_S.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${OPERATIONCOUNT} -p recordcount=${OPERATIONCOUNT}
#        fi
        # scanvalue only with index
        echo "SCANVALUE $storage ($recordName) with index..."
        ./bin/go-ycsb run ${storage} -P workloads/workload_SCANVALUE > logs/${storage}_${recordName}_SV_WITH_INDEX.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${scanCount} -p recordcount=${OPERATIONCOUNT} -p hasIndex=true -p dropIndex=true -p dropDatabase=true

#        if [ ${load_count} != 131072 ];then
#          # scanvalue only without index
#          echo "SCANVALUE $storage ($recordName) without index..."
#          ./bin/go-ycsb run ${storage} -P workloads/workload_SCANVALUE > logs/${storage}_${recordName}_SV_WITHOUT_INDEX.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${scanCount} -p recordcount=${OPERATIONCOUNT} -p hasIndex=false -p dropIndex=false -p dropDatabase=true
#        fi

    fi

}

echo start server_1m.sh ... && date

######参数说明
# 1 - 数据库名称
# 2 - 测试记录文件名称
# 3 - field value字段长度
# 4 - field 个数
# 5 - sanvalue次数
# 6 - 插入文档个数以及read和scan的次数


##### mongodb 1mb/op, total 1G
echo "================ start mongodb 1M_1G ================" && date
cd ${TEST_TOOL_PATH}
run mongodb 1M_1G $FIELDLENGTH $FIELDCOUNT 100 1024 512 512
echo "================ finish mongodb 1M_1G ================" && date
sleep 30

###### mongodb 1mb/op, total 16G
#echo "================ start mongodb 1M_16G ================" && date
#cd ${TEST_TOOL_PATH}
#run mongodb 1M_16G $FIELDLENGTH $FIELDCOUNT 100 1024 8192 8192
#echo "================ finish mongodb 1M_16G ================" && date
#sleep 30
#
###### mongodb 1mb/op, total 256G
#echo "================ start mongodb 1M_256G ================" && date
#cd ${TEST_TOOL_PATH}
#run mongodb 1M_256G $FIELDLENGTH $FIELDCOUNT 100 1024 131072 131072
#echo "================ finish mongodb 1M_256G ================" && date
#sleep 30




