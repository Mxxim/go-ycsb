#!/bin/bash

TEST_TOOL_PATH=/home/zhengbc/go-ycsb/
SCANCOUNT=2000
FIELDCOUNT=5

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


    # load data
#    echo "LOAD $storage ($recordName) ..."
#    ./bin/go-ycsb load ${storage} -P workloads/workload_WRITE > logs/${storage}_${recordName}_LOAD.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${OPERATIONCOUNT} -p recordcount=${OPERATIONCOUNT}

    # write only
    echo "WRITE $storage ($recordName) ..."
    ./bin/go-ycsb run ${storage} -P workloads/workload_WRITE > logs/${storage}_${recordName}_W.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${OPERATIONCOUNT} -p recordcount=${OPERATIONCOUNT}

    # read only
    echo "READ $storage ($recordName) ..."
    ./bin/go-ycsb run ${storage} -P workloads/workload_READ > logs/${storage}_${recordName}_R.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${OPERATIONCOUNT} -p recordcount=${OPERATIONCOUNT}

    # scanvalue only without index
    echo "SCANVALUE $storage ($recordName) without index..."
    ./bin/go-ycsb run ${storage} -P workloads/workload_SCANVALUE > logs/${storage}_${recordName}_SV_WITHOUT_INDEX.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${scanCount} -p recordcount=${OPERATIONCOUNT} -p hasIndex=false -p dropIndex=false -p dropDatabase=false

    # scanvalue only with index
    echo "SCANVALUE $storage ($recordName) with index..."
    ./bin/go-ycsb run ${storage} -P workloads/workload_SCANVALUE > logs/${storage}_${recordName}_SV_WITH_INDEX.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${scanCount} -p recordcount=${OPERATIONCOUNT} -p hasIndex=true -p dropIndex=true -p dropDatabase=true

}

echo start server_couchdb.sh ... && date

######参数说明
# 1 - 数据库名称
# 2 - 测试记录文件名称
# 3 - field value字段长度
# 4 - field 个数
# 5 - sanvalue次数
# 6 - 插入文档个数以及read和scan的次数

##### couchdb 1mb/op, total 1G
echo "================ start couchdb 1M_1G ================" && date
cd ${TEST_TOOL_PATH}
run couchdb 1M_1G 100 $FIELDCOUNT $SCANCOUNT 1024
echo "================ finish couchdb 1M_1G ================" && date
sleep 30


###### couchdb 1mb/op, total 16G
#echo "================ start couchdb 1M_16G ================" && date
#cd ${TEST_TOOL_PATH}
#run couchdb 1M_16G 209716 $FIELDCOUNT $SCANCOUNT 16384
#echo "================ finish couchdb 1M_16G ================" && date
#sleep 30
#
#
###### couchdb 1mb/op, total 256G
#echo "================ start couchdb 1M_256G ================" && date
#cd ${TEST_TOOL_PATH}
#run couchdb 1M_256G 209716 $FIELDCOUNT $SCANCOUNT 262144
#echo "================ finish couchdb 1M_256G ================" && date
#sleep 30
#
###### couchdb 20kb/op, total 1G
#echo "================ start couchdb 20kb_1G ================" && date
#cd ${TEST_TOOL_PATH}
#run couchdb 20kb_1G 4096 $FIELDCOUNT $SCANCOUNT 52429
#echo "================ finish couchdb 20kb_1G ================" && date
#sleep 30
#
###### couchdb 20kb/op, total 16G
#echo "================ start couchdb 20kb_16G ================" && date
#cd ${TEST_TOOL_PATH}
#run couchdb 20kb_16G 4096 $FIELDCOUNT $SCANCOUNT 838861
#echo "================ finish couchdb 20kb_16G ================" && date
#sleep 30
#
###### couchdb 20kb/op, total 256G
#echo "================ start couchdb 20kb_256G ================" && date
#cd ${TEST_TOOL_PATH}
#run couchdb 20kb_256G 4096 $FIELDCOUNT $SCANCOUNT 13421773
#echo "================ finish couchdb 20kb_256G ================" && date
#sleep 30
#
###### couchdb 200b/op, total 1G
#echo "================ start couchdb 200b_1G ================" && date
#cd ${TEST_TOOL_PATH}
#run couchdb 200b_1G 320 $FIELDCOUNT $SCANCOUNT 671089
#echo "================ finish couchdb 200b_1G ================" && date
#sleep 30
#
###### couchdb 200b/op, total 16G
#echo "================ start couchdb 200b_16G ================" && date
#cd ${TEST_TOOL_PATH}
#run leveldb 200b_16G 320 $FIELDCOUNT $SCANCOUNT 10737418
#echo "================ finish couchdb 200b_16G ================" && date
#sleep 30
#
###### couchdb 200b/op, total 256G
#echo "================ start couchdb 200b_256G ================" && date
#cd ${TEST_TOOL_PATH}
#run couchdb 200b_256G 320 $FIELDCOUNT $SCANCOUNT 171798692
#echo "================ finish couchdb 200b_256G ================" && date
#sleep 30
#
#
