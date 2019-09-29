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

    load_count=$7
    write_count=$8



    if [ ! -d "bin" ];then
        make
    fi


    if [ "$storage" = "couchbase" ];then

        # curl -u user:password -v -X POST http://127.0.0.1:8091/nodes/self/controller/settings -d path=/opt/couchbasedb/path -d index_path=/opt/couchbasedb/index_path -d cbas_path=/opt/couchbasedb/cbas_path

        curl -u user:password -v -X POST http://127.0.0.1:8091/settings/web -d password=password -d username=user -d port=8091

        if [ ${load_count} == 85894846 -o ${load_count} == 6710886 -o ${load_count} == 131072 ];then
          # load data, 256G
          echo "LOAD $storage ($recordName) ..."
          ./bin/go-ycsb load ${storage} -P workloads/workload_WRITE > logs/${storage}_${recordName}_LOAD.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${load_count} -p recordcount=${load_count} -p couchbase.indexs=field0,field1,field2,field3,field4
        else
          echo "LOAD $storage ($recordName) ..."
          ./bin/go-ycsb load ${storage} -P workloads/workload_WRITE > logs/${storage}_${recordName}_LOAD.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${load_count} -p recordcount=${load_count}
        fi

        du -sh /opt/couchbasedb/data/

        # write only
        echo "WRITE $storage ($recordName)..."
        ./bin/go-ycsb run ${storage} -P workloads/workload_WRITE > logs/${storage}_${recordName}_W.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${write_count} -p recordcount=${load_count}

        du -sh /opt/couchbasedb/data/

        # read only
        echo "READ $storage ($recordName) ..."
        ./bin/go-ycsb run ${storage} -P workloads/workload_READ > logs/${storage}_${recordName}_R.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${OPERATIONCOUNT} -p recordcount=${OPERATIONCOUNT}

        # scanvalue only with index
        if [ ${load_count} != 85894846 -a ${load_count} != 6710886 -a ${load_count} != 131072 ];then
          echo "SCANVALUE $storage ($recordName) with index..."
          ./bin/go-ycsb run ${storage} -P workloads/workload_SCANVALUE > logs/${storage}_${recordName}_SV_WITH_INDEX.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${scanCount} -p recordcount=${OPERATIONCOUNT} -p dropIndex=true -p dropDatabase=true -p couchbase.indexs=field0,field1,field2,field3,field4
        else
          echo "SCANVALUE $storage ($recordName) with index..."
          ./bin/go-ycsb run ${storage} -P workloads/workload_SCANVALUE > logs/${storage}_${recordName}_SV_WITH_INDEX.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${scanCount} -p recordcount=${OPERATIONCOUNT} -p dropIndex=true -p dropDatabase=true
        fi
    fi
}

echo start server_couchbase.sh ... && date

######参数说明
# 1 - 数据库名称
# 2 - 测试记录文件名称
# 3 - field value字段长度
# 4 - field 个数
# 5 - sanvalue次数
# 6 - read次数


##### couchbase 200b/op, total 1G
echo "================ start couchbase 200b_1G ================" && date
cd ${TEST_TOOL_PATH}
run couchbase 200b_1G 320 $FIELDCOUNT 100 671089 335544 335545
echo "================ finish couchbase 200b_1G ================" && date
sleep 30

##### couchbase 200b/op, total 16G
echo "================ start couchbase 200b_16G ================" && date
cd ${TEST_TOOL_PATH}
run couchbase 200b_16G 320 $FIELDCOUNT 50 671089 5368709 5368709
echo "================ finish couchbase 200b_16G ================" && date
sleep 30

##### couchbase 200b/op, total 256G
echo "================ start couchbase 200b_256G ================" && date
cd ${TEST_TOOL_PATH}
run couchbase 200b_256G 320 $FIELDCOUNT 5 671089 85894846 85894846
echo "================ finish couchbase 200b_256G ================" && date
sleep 30

##### couchbase 20kb/op, total 1G
echo "================ start couchbase 20kb_1G ================" && date
cd ${TEST_TOOL_PATH}
run couchbase 20kb_1G 4096 $FIELDCOUNT 100 52429 26214 26215
echo "================ finish couchbase 20kb_1G ================" && date
sleep 30

##### couchbase 20kb/op, total 16G
echo "================ start couchbase 20kb_16G ================" && date
cd ${TEST_TOOL_PATH}
run couchbase 20kb_16G 4096 $FIELDCOUNT 50 52429 419430 419431
echo "================ finish couchbase 20kb_16G ================" && date
sleep 30

##### couchbase 20kb/op, total 256G
echo "================ start couchbase 20kb_256G ================" && date
cd ${TEST_TOOL_PATH}
run couchbase 20kb_256G 4096 $FIELDCOUNT 5 52429 6710886 6710887
echo "================ finish couchbase 20kb_256G ================" && date
sleep 30


##### couchbase 1mb/op, total 1G
echo "================ start couchbase 1M_1G ================" && date
cd ${TEST_TOOL_PATH}
run couchbase 1M_1G 209716 $FIELDCOUNT 100 1024 512 512
echo "================ finish couchbase 1M_1G ================" && date
sleep 30

#
##### couchbase 1mb/op, total 16G
echo "================ start couchbase 1M_16G ================" && date
cd ${TEST_TOOL_PATH}
run couchbase 1M_16G 209716 $FIELDCOUNT 50 1024 8192 8192
echo "================ finish couchbase 1M_16G ================" && date
sleep 30


##### couchbase 1mb/op, total 256G
echo "================ start couchbase 1M_256G ================" && date
cd ${TEST_TOOL_PATH}
run couchbase 1M_256G 209716 $FIELDCOUNT 5 1024 131072 131072
echo "================ finish couchbase 1M_256G ================" && date
sleep 30
