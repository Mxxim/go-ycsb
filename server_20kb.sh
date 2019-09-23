#!/bin/bash

TEST_TOOL_PATH=/home/zhengbc/go-ycsb/
SCANCOUNT=2000
FIELDLENGTH=4096
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
# 6 - 插入文档个数以及read和scan的次数
run() {
    storage=$1
    recordName=$2
    fieldLength=$3
    fieldCount=$4
    scanCount=$5
    OPERATIONCOUNT=$6

    couchbase_load_count=$7
    couchbase_write_count=$8

    if [ ! -d "bin" ];then
        make
    fi

    if [ "$storage" = "leveldb" ];then

        rm -rf $LEVELDB_PATH && mkdir -p $LEVELDB_PATH

        du -sh $LEVELDB_PATH

        # load data
#        echo "LOAD $storage ($recordName) ..."
#        ./bin/go-ycsb load ${storage} -P workloads/workload_WRITE > logs/${storage}_${recordName}_LOAD.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${OPERATIONCOUNT} -p recordcount=${OPERATIONCOUNT} -p leveldb.path=$LEVELDB_PATH

        # write only
        echo "WRITE $storage ($recordName) ..."
        ./bin/go-ycsb run ${storage} -P workloads/workload_WRITE > logs/${storage}_${recordName}_W.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${OPERATIONCOUNT} -p recordcount=${OPERATIONCOUNT} -p leveldb.path=$LEVELDB_PATH

        du -sh $LEVELDB_PATH

        # scan only
        echo "SCAN $storage ($recordName) ..."
        ./bin/go-ycsb run ${storage} -P workloads/workload_SCAN > logs/${storage}_${recordName}_S.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${scanCount} -p recordcount=${OPERATIONCOUNT} -p leveldb.path=$LEVELDB_PATH

        # read only
        echo "READ $storage ($recordName) ..."
        ./bin/go-ycsb run ${storage} -P workloads/workload_READ > logs/${storage}_${recordName}_R.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${OPERATIONCOUNT} -p recordcount=${OPERATIONCOUNT} -p leveldb.path=$LEVELDB_PATH

        # scanvalue only without index
#        echo "SCANVALUE $storage ($recordName) without index..."
#        ./bin/go-ycsb run ${storage} -P workloads/workload_SCANVALUE > logs/${storage}_${recordName}_SV_WITHOUT_INDEX.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${scanCount} -p recordcount=${OPERATIONCOUNT} -p hasIndex=false -p dropIndex=false -p dropDatabase=false -p leveldb.path=$LEVELDB_PATH
#
#        # scanvalue only with index
#        echo "SCANVALUE $storage ($recordName) with index..."
#        ./bin/go-ycsb run ${storage} -P workloads/workload_SCANVALUE > logs/${storage}_${recordName}_SV_WITH_INDEX.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${scanCount} -p recordcount=${OPERATIONCOUNT} -p hasIndex=true -p dropIndex=true -p dropDatabase=true -p leveldb.path=$LEVELDB_PATH
    fi


    if [ "$storage" = "mongodb" ];then

        # load data
#        echo "LOAD $storage ($recordName) ..."
#        ./bin/go-ycsb load ${storage} -P workloads/workload_WRITE > logs/${storage}_${recordName}_LOAD.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${OPERATIONCOUNT} -p recordcount=${OPERATIONCOUNT}

        du -sh /opt/mongodb/data

        # write only
        echo "WRITE $storage ($recordName) ..."
        ./bin/go-ycsb run ${storage} -P workloads/workload_WRITE > logs/${storage}_${recordName}_W.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${OPERATIONCOUNT} -p recordcount=${OPERATIONCOUNT}

        du -sh /opt/mongodb/data

        # read only
        echo "READ $storage ($recordName) ..."
        ./bin/go-ycsb run ${storage} -P workloads/workload_READ > logs/${storage}_${recordName}_R.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${OPERATIONCOUNT} -p recordcount=${OPERATIONCOUNT}

        # scan only
        echo "SCAN $storage ($recordName) ..."
        ./bin/go-ycsb run ${storage} -P workloads/workload_SCAN > logs/${storage}_${recordName}_S.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${scanCount} -p recordcount=${OPERATIONCOUNT}

        # scanvalue only with index
        echo "SCANVALUE $storage ($recordName) with index..."
        ./bin/go-ycsb run ${storage} -P workloads/workload_SCANVALUE > logs/${storage}_${recordName}_SV_WITH_INDEX.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${scanCount} -p recordcount=${OPERATIONCOUNT} -p hasIndex=true -p dropIndex=true -p dropDatabase=false

        # scanvalue only without index
        echo "SCANVALUE $storage ($recordName) without index..."
        ./bin/go-ycsb run ${storage} -P workloads/workload_SCANVALUE > logs/${storage}_${recordName}_SV_WITHOUT_INDEX.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${scanCount} -p recordcount=${OPERATIONCOUNT} -p hasIndex=false -p dropIndex=false -p dropDatabase=true
    fi

    if [ "$storage" = "couchbase" ];then

        curl -u user:password -v -X POST http://127.0.0.1:8091/nodes/self/controller/settings -d path=/opt/couchbasedb/path -d index_path=/opt/couchbasedb/index_path -d cbas_path=/opt/couchbasedb/cbas_path

        curl -u user:password -v -X POST http://127.0.0.1:8091/settings/web -d password=password -d username=user -d port=8091

        # load data
        echo "LOAD $storage ($recordName) ..."
        ./bin/go-ycsb load ${storage} -P workloads/workload_WRITE > logs/${storage}_${recordName}_LOAD.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${couchbase_load_count} -p recordcount=${couchbase_load_count}

        du -sh /opt/couchbasedb

        # write only
        echo "WRITE $storage ($recordName) ..."
        ./bin/go-ycsb run ${storage} -P workloads/workload_WRITE > logs/${storage}_${recordName}_W.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${couchbase_write_count} -p recordcount=${couchbase_write_count}

        du -sh /opt/couchbasedb

        # read only
        echo "READ $storage ($recordName) ..."
        ./bin/go-ycsb run ${storage} -P workloads/workload_READ > logs/${storage}_${recordName}_R.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${OPERATIONCOUNT} -p recordcount=${OPERATIONCOUNT}

        # scanvalue only with index
        echo "SCANVALUE $storage ($recordName) with index..."
        ./bin/go-ycsb run ${storage} -P workloads/workload_SCANVALUE > logs/${storage}_${recordName}_SV_WITH_INDEX.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${scanCount} -p recordcount=${OPERATIONCOUNT} -p hasIndex=true -p dropIndex=true -p dropDatabase=true
    fi

}

echo start server_20kb.sh ... && date

######参数说明
# 1 - 数据库名称
# 2 - 测试记录文件名称
# 3 - field value字段长度
# 4 - field 个数
# 5 - sanvalue次数
# 6 - 插入文档个数以及read和scan的次数

##### couchbase 20kb/op, total 1G
echo "================ start couchbase 20kb_1G ================" && date
cd ${TEST_TOOL_PATH}
run couchbase 20kb_1G $FIELDLENGTH $FIELDCOUNT $SCANCOUNT 52429 26214 26215
echo "================ finish couchbase 20kb_1G ================" && date
sleep 30

#### leveldb 20kb/op, total 1G
echo "================ start leveldb 20kb_1G ================" && date
cd ${TEST_TOOL_PATH}
run leveldb 20kb_1G $FIELDLENGTH $FIELDCOUNT $SCANCOUNT 52429
echo "================ finish leveldb 20kb_1G ================" && date
sleep 30

##### mongodb 20kb/op, total 1G
echo "================ start mongodb 20kb_1G ================" && date
cd ${TEST_TOOL_PATH}
run mongodb 20kb_1G $FIELDLENGTH $FIELDCOUNT $SCANCOUNT 52429
echo "================ finish mongodb 20kb_1G ================" && date
sleep 30

###### couchbase 20kb/op, total 16G
echo "================ start couchbase 20kb_16G ================" && date
cd ${TEST_TOOL_PATH}
run couchbase 20kb_16G $FIELDLENGTH $FIELDCOUNT $SCANCOUNT 838861 419430 419431
echo "================ finish couchbase 20kb_16G ================" && date
sleep 30

#### leveldb 20kb/op, total 16G
echo "================ start leveldb 20kb_16G ================" && date
cd ${TEST_TOOL_PATH}
run leveldb 20kb_16G $FIELDLENGTH $FIELDCOUNT $SCANCOUNT 838861
echo "================ finish leveldb 20kb_16G ================" && date
sleep 30

##### mongodb 20kb/op, total 16G
echo "================ start mongodb 20kb_16G ================" && date
cd ${TEST_TOOL_PATH}
run mongodb 20kb_16G $FIELDLENGTH $FIELDCOUNT $SCANCOUNT 838861
echo "================ finish mongodb 20kb_16G ================" && date
sleep 30


#### couchbase 20kb/op, total 256G
echo "================ start couchbase 20kb_256G ================" && date
cd ${TEST_TOOL_PATH}
run couchbase 20kb_256G $FIELDLENGTH $FIELDCOUNT $SCANCOUNT 13421773 6710886 6710887
echo "================ finish couchbase 20kb_256G ================" && date
sleep 30

##### leveldb 20kb/op, total 256G
echo "================ start leveldb 20kb_256G ================" && date
cd ${TEST_TOOL_PATH}
run leveldb 20kb_256G $FIELDLENGTH $FIELDCOUNT $SCANCOUNT 13421773
echo "================ finish leveldb 20kb_256G ================" && date
sleep 30

##### mongodb 20kb/op, total 256G
echo "================ start mongodb 20kb_256G ================" && date
cd ${TEST_TOOL_PATH}
run mongodb 20kb_256G $FIELDLENGTH $FIELDCOUNT $SCANCOUNT 13421773
echo "================ finish mongodb 20kb_256G ================" && date
sleep 30




