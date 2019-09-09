#!/bin/bash

TEST_TOOL_PATH=/Users/sammy/Workspace/go-ycsb/

# 本脚本用于测试不同数据库的读写性能
######参数说明
# 1 - 数据库名称
# 2 - 测试记录文件名称
# 3 - 配置文件名称
# 4 - value字段长度

rm -rf logs
mkdir logs

run() {
    storage=$1
    recordName=$2
    config=$3
    fieldLength=$4
    fieldCount=$5

    if [ ! -d "bin" ];then 
        make
    fi

    ./bin/go-ycsb load ${storage} -P workloads/${config}_W > logs/${storage}_${recordName}_W.txt -p insertorder=ordered -p randomizedelay=false -p fieldlength=${fieldLength} -p fieldcount=${fieldCount}
    ./bin/go-ycsb run ${storage} -P workloads/${config}_W > logs/${storage}_${recordName}_W.txt -p insertorder=ordered -p randomizedelay=false -p fieldlength=${fieldLength} -p fieldcount=${fieldCount}

    ./bin/go-ycsb run ${storage} -P workloads/${config}_R > logs/${storage}_${recordName}_R.txt -p insertorder=ordered -p randomizedelay=false -p fieldlength=${fieldLength} -p fieldcount=${fieldCount}

    if [ "$storage" = "filelog" ];then
        rm -rf namespaces
    fi

    if [ "$storage" = "leveldb" ];then
        rm -rf data/ldb && mkdir -p data/ldb
    fi
}

echo start && date


###### leveldb 1mb/op, total 2G
cd ${TEST_TOOL_PATH}
run leveldb 1M_2G workload_2048 104858 10 && sleep 30
echo finish && date


###### mongodb 1mb/op, total 2G
cd ${TEST_TOOL_PATH}
run mongodb 1M_2G workload_2048 104858 10 && sleep 30
echo finish && date

