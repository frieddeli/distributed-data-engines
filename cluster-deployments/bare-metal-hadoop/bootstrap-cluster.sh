#!/usr/bin/env bash
# ==============================================================================
# Multi-Node Hadoop & YARN Cluster Bootstrap Script
# ==============================================================================
set -euo pipefail

echo "[+] Initializing Hadoop cluster environment..."

# 1. Format NameNode metadata (Run ONCE during initial cluster initialization)
if [ ! -d "/home/ubuntu/hadoop_data/hdfs/namenode/current" ]; then
    echo "[+] Formatting HDFS NameNode metadata..."
    hdfs namenode -format -force
else
    echo "[!] NameNode metadata directory already exists. Skipping format."
fi

# 2. Start Distributed Filesystem Daemons (NameNode on Master, DataNode on Workers)
echo "[+] Bootstrapping HDFS daemons (start-dfs.sh)..."
"$HADOOP_HOME/sbin/start-dfs.sh"

# 3. Start YARN Resource Manager & NodeManagers
echo "[+] Bootstrapping YARN cluster daemons (start-yarn.sh)..."
"$HADOOP_HOME/sbin/start-yarn.sh"

# 4. Start MapReduce JobHistoryServer
echo "[+] Starting MapReduce JobHistoryServer daemon..."
"$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh" start historyserver

# 5. Verify local Java processes
echo "[+] Verifying running cluster processes:"
jps

echo "=============================================================================="
echo "Cluster bootstrap complete!"
echo "NameNode Web UI:        http://hadoop-master:50070"
echo "YARN ResourceManager:   http://hadoop-master:8088"
echo "JobHistoryServer Web UI: http://hadoop-master:19888"
echo "=============================================================================="
