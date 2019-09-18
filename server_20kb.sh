#!/bin/bash

TEST_TOOL_PATH=/home/zhengbc/go-ycsb/

# 本脚本用于测试不同数据库的读写性能
# ./bin/go-ycsb load leveldb -P workloads/workload_WRITE > logs/leveldb_1M_2G_LOAD.txt -p insertorder=ordered -p randomizedelay=false -p fieldlength=104858 -p fieldcount=10
# ./bin/go-ycsb run leveldb -P workloads/workload_WRITE > logs/leveldb_1M_2G_W.txt -p insertorder=ordered -p randomizedelay=false -p fieldlength=104858 -p fieldcount=10
# ./bin/go-ycsb run leveldb -P workloads/workload_READ > logs/leveldb_1M_2G_R.txt -p insertorder=ordered -p randomizedelay=false -p fieldlength=104858 -p fieldcount=10
# ./bin/go-ycsb run leveldb -P workloads/workload_SCAN > logs/leveldb_1M_2G_S.txt -p insertorder=ordered -p randomizedelay=false -p fieldlength=104858 -p fieldcount=10

#
## 操作次数
#OPERATIONCOUNT=$1
#
#if [ ! -n "$1" ];then
#        OPERATIONCOUNT=1000
#fi
#
#echo "OPERATIONCOUNT is $OPERATIONCOUNT"

######参数说明
# 1 - 数据库名称
# 2 - 测试记录文件名称
# 3 - field value字段长度
# 4 - field 个数
# 5 - sanvalue次数
# 6 - 插入文档个数以及read和scan的次数
run() {
    storage=$1
    recordName=$2
    fieldLength=$3
    fieldCount=$4
    scanCount=$5
    OPERATIONCOUNT=$6

    if [ ! -d "bin" ];then
        make
    fi

    if [ "$storage" = "leveldb" ];then
        rm -rf data/ldb && mkdir -p data/ldb
    fi


    # load data
    ./bin/go-ycsb load ${storage} -P workloads/workload_WRITE > logs/${storage}_${recordName}_LOAD.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${OPERATIONCOUNT} -p recordcount=${OPERATIONCOUNT}
    # write only
    ./bin/go-ycsb run ${storage} -P workloads/workload_WRITE > logs/${storage}_${recordName}_W.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${OPERATIONCOUNT} -p recordcount=${OPERATIONCOUNT}
    # read only
    ./bin/go-ycsb run ${storage} -P workloads/workload_READ > logs/${storage}_${recordName}_R.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${OPERATIONCOUNT} -p recordcount=${OPERATIONCOUNT}
    if [ "$storage" = "leveldb" -o "$storage" = "mongodb"];then
      # scan only
      ./bin/go-ycsb run ${storage} -P workloads/workload_SCAN > logs/${storage}_${recordName}_S.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${OPERATIONCOUNT} -p recordcount=${OPERATIONCOUNT}
    fi
    # scanvalue only without index
    ./bin/go-ycsb run ${storage} -P workloads/workload_SCANVALUE > logs/${storage}_${recordName}_SV_WITHOUT_INDEX.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${scanCount} -p recordcount=${OPERATIONCOUNT} -p hasIndex=false -p dropIndex=false -p dropDatabase=false
    # scanvalue only with index
    ./bin/go-ycsb run ${storage} -P workloads/workload_SCANVALUE > logs/${storage}_${recordName}_SV_WITH_INDEX.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${scanCount} -p recordcount=${OPERATIONCOUNT} -p hasIndex=true -p dropIndex=true -p dropDatabase=true

}

echo start && date
#
#storage=$1
#recordName=$2
#fieldLength=$3
#fieldCount=$4
#scanCount=$5

##### leveldb 20kb/op, total 1G
cd ${TEST_TOOL_PATH}
run leveldb 20kb_1G 4096 5 2000 52429 && sleep 30
echo finish && date

##### mongodb 20kb/op, total 1G
cd ${TEST_TOOL_PATH}
run mongodb 20kb_1G 4096 5 2000 52429 && sleep 30
echo finish && date

##### couchbase 20kb/op, total 1G
#cd ${TEST_TOOL_PATH}
#run couchbase 20kb_1G 4096 5 2000 52429 && sleep 30
#echo finish && date


##### leveldb 20kb/op, total 16G
cd ${TEST_TOOL_PATH}
run leveldb 20kb_16G 4096 5 2000 838861 && sleep 30
echo finish && date

##### mongodb 20kb/op, total 16G
cd ${TEST_TOOL_PATH}
run mongodb 20kb_16G 4096 5 2000 838861 && sleep 30
echo finish && date

###### couchbase 20kb/op, total 16G
#cd ${TEST_TOOL_PATH}
#run couchbase 20kb_16G 4096 5 2000 838861 && sleep 30
#echo finish && date


##### leveldb 20kb/op, total 256G
cd ${TEST_TOOL_PATH}
run leveldb 20kb_256G 4096 5 2000 13421773 && sleep 30
echo finish && date

##### mongodb 20kb/op, total 256G
cd ${TEST_TOOL_PATH}
run mongodb 20kb_256G 4096 5 2000 13421773 && sleep 30
echo finish && date

##### couchbase 20kb/op, total 256G
#cd ${TEST_TOOL_PATH}
#run couchbase 20kb_256G 4096 5 2000 13421773 && sleep 30
#echo finish && date



