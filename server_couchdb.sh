#!/bin/bash

TEST_TOOL_PATH=/home/zhengbc/go-ycsb/

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

##### couchdb 1mb/op, total 1G
cd ${TEST_TOOL_PATH}
run couchdb 1M_1G 209716 5 2000 1024 && sleep 30
echo finish && date


##### couchdb 1mb/op, total 16G
cd ${TEST_TOOL_PATH}
run couchdb 1M_16G 209716 5 2000 16384 && sleep 30
echo finish && date


##### couchdb 1mb/op, total 256G
cd ${TEST_TOOL_PATH}
run couchdb 1M_256G 209716 5 2000 262144 && sleep 30
echo finish && date

##### couchdb 20kb/op, total 1G
cd ${TEST_TOOL_PATH}
run couchdb 20kb_1G 4096 5 2000 52429 && sleep 30
echo finish && date

##### couchdb 20kb/op, total 16G
cd ${TEST_TOOL_PATH}
run couchdb 20kb_16G 4096 5 2000 838861 && sleep 30
echo finish && date

##### couchdb 20kb/op, total 256G
cd ${TEST_TOOL_PATH}
run couchdb 20kb_256G 4096 5 2000 13421773 && sleep 30
echo finish && date

##### couchdb 200b/op, total 1G
cd ${TEST_TOOL_PATH}
run couchdb 200b_1G 320 5 2000 671089 && sleep 30
echo finish && date

##### couchdb 200b/op, total 16G
cd ${couchdb}
run leveldb 200b_16G 320 5 2000 10737418 && sleep 30
echo finish && date

##### couchdb 200b/op, total 256G
cd ${TEST_TOOL_PATH}
run couchdb 200b_256G 320 5 2000 171798692 && sleep 30
echo finish && date


