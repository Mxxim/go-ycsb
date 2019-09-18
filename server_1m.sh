#!/bin/bash

TEST_TOOL_PATH=/home/zhengbc/go-ycsb/

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

    if [ ! -d "bin" ];then
        make
    fi

    if [ "$storage" = "leveldb" ];then
        rm -rf $LEVELDB_PATH && mkdir -p $LEVELDB_PATH
    fi


    if [ "$storage" = "leveldb" ];then
        # load data
        ./bin/go-ycsb load ${storage} -P workloads/workload_WRITE > logs/${storage}_${recordName}_LOAD.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${OPERATIONCOUNT} -p recordcount=${OPERATIONCOUNT} -p leveldb.path=$LEVELDB_PATH

        # write only
        ./bin/go-ycsb run ${storage} -P workloads/workload_WRITE > logs/${storage}_${recordName}_W.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${OPERATIONCOUNT} -p recordcount=${OPERATIONCOUNT} -p leveldb.path=$LEVELDB_PATH

        # scan only
        ./bin/go-ycsb run ${storage} -P workloads/workload_SCAN > logs/${storage}_${recordName}_S.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${OPERATIONCOUNT} -p recordcount=${OPERATIONCOUNT} -p leveldb.path=$LEVELDB_PATH

        # read only
        ./bin/go-ycsb run ${storage} -P workloads/workload_READ > logs/${storage}_${recordName}_R.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${OPERATIONCOUNT} -p recordcount=${OPERATIONCOUNT} -p leveldb.path=$LEVELDB_PATH

        # scanvalue only without index
        ./bin/go-ycsb run ${storage} -P workloads/workload_SCANVALUE > logs/${storage}_${recordName}_SV_WITHOUT_INDEX.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${scanCount} -p recordcount=${OPERATIONCOUNT} -p hasIndex=false -p dropIndex=false -p dropDatabase=false -p leveldb.path=$LEVELDB_PATH

        # scanvalue only with index
        ./bin/go-ycsb run ${storage} -P workloads/workload_SCANVALUE > logs/${storage}_${recordName}_SV_WITH_INDEX.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${scanCount} -p recordcount=${OPERATIONCOUNT} -p hasIndex=true -p dropIndex=true -p dropDatabase=true -p leveldb.path=$LEVELDB_PATH
    fi


    if [ "$storage" = "mongodb" ];then
        # load data
        ./bin/go-ycsb load ${storage} -P workloads/workload_WRITE > logs/${storage}_${recordName}_LOAD.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${OPERATIONCOUNT} -p recordcount=${OPERATIONCOUNT}
        # write only
        ./bin/go-ycsb run ${storage} -P workloads/workload_WRITE > logs/${storage}_${recordName}_W.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${OPERATIONCOUNT} -p recordcount=${OPERATIONCOUNT}
        # read only
        ./bin/go-ycsb run ${storage} -P workloads/workload_READ > logs/${storage}_${recordName}_R.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${OPERATIONCOUNT} -p recordcount=${OPERATIONCOUNT}
        # scan only
        ./bin/go-ycsb run ${storage} -P workloads/workload_SCAN > logs/${storage}_${recordName}_S.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${OPERATIONCOUNT} -p recordcount=${OPERATIONCOUNT}
        # scanvalue only without index
        ./bin/go-ycsb run ${storage} -P workloads/workload_SCANVALUE > logs/${storage}_${recordName}_SV_WITHOUT_INDEX.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${scanCount} -p recordcount=${OPERATIONCOUNT} -p hasIndex=false -p dropIndex=false -p dropDatabase=false
        # scanvalue only with index
        ./bin/go-ycsb run ${storage} -P workloads/workload_SCANVALUE > logs/${storage}_${recordName}_SV_WITH_INDEX.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${scanCount} -p recordcount=${OPERATIONCOUNT} -p hasIndex=true -p dropIndex=true -p dropDatabase=true
    fi

    if [ "$storage" = "couchbase" ];then

        curl -u user:password -v -X POST http://127.0.0.1:8091/nodes/self/controller/settings -d path=/opt/couchbasedb/path -d index_path=/opt/couchbasedb/index_path -d cbas_path=/opt/couchbasedb/cbas_path

        curl -u user:password -v -X POST http://127.0.0.1:8091/settings/web -d password=password -d username=user -d port=8091

        # load data
        ./bin/go-ycsb load ${storage} -P workloads/workload_WRITE > logs/${storage}_${recordName}_LOAD.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${OPERATIONCOUNT} -p recordcount=${OPERATIONCOUNT}
        # write only
        ./bin/go-ycsb run ${storage} -P workloads/workload_WRITE > logs/${storage}_${recordName}_W.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${OPERATIONCOUNT} -p recordcount=${OPERATIONCOUNT}
        # read only
        ./bin/go-ycsb run ${storage} -P workloads/workload_READ > logs/${storage}_${recordName}_R.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${OPERATIONCOUNT} -p recordcount=${OPERATIONCOUNT}
        # scanvalue only with index
        ./bin/go-ycsb run ${storage} -P workloads/workload_SCANVALUE > logs/${storage}_${recordName}_SV_WITH_INDEX.txt -p fieldlength=${fieldLength} -p fieldcount=${fieldCount} -p operationcount=${scanCount} -p recordcount=${OPERATIONCOUNT} -p hasIndex=true -p dropIndex=true -p dropDatabase=true
    fi
}

echo start && date
#
#storage=$1
#recordName=$2
#fieldLength=$3
#fieldCount=$4
#scanCount=$5

#### leveldb 1mb/op, total 1G
cd ${TEST_TOOL_PATH}
run leveldb 1M_1G 209716 5 2000 1024 && sleep 30
echo finish && date

##### mongodb 1mb/op, total 1G
cd ${TEST_TOOL_PATH}
run mongodb 1M_1G 209716 5 2000 1024 && sleep 30
echo finish && date

#### couchbase 1mb/op, total 1G
 cd ${TEST_TOOL_PATH}
run couchbase 1M_1G 209716 5 2000 1024 && sleep 30
echo finish && date


#### leveldb 1mb/op, total 16G
cd ${TEST_TOOL_PATH}
run leveldb 1M_16G 209716 5 2000 16384 && sleep 30
echo finish && date

##### mongodb 1mb/op, total 16G
cd ${TEST_TOOL_PATH}
run mongodb 1M_16G 209716 5 2000 16384 && sleep 30
echo finish && date

#### couchbase 1mb/op, total 16G
cd ${TEST_TOOL_PATH}
run couchbase 1M_16G 209716 5 2000 16384 && sleep 30
echo finish && date


#### leveldb 1mb/op, total 256G
cd ${TEST_TOOL_PATH}
run leveldb 1M_256G 209716 5 2000 262144 && sleep 30
echo finish && date

##### mongodb 1mb/op, total 256G
cd ${TEST_TOOL_PATH}
run mongodb 1M_256G 209716 5 2000 262144 && sleep 30
echo finish && date

##### couchbase 1mb/op, total 256G
cd ${TEST_TOOL_PATH}
run couchbase 1M_256G 209716 5 2000 262144 && sleep 30
echo finish && date



