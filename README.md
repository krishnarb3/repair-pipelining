# Repair Pipelining
Repair pipelining of failed nodes/blocks using erasure codes

_Repair pipelining implemented using sockets for file transmission and redis for message passing between coordinator and nodes_

Prerequisite:
1. Latest redis version (And cli)
2. Java

Build FAT jar:
```bash
./gradlew clean shadowJar
```

## Repair pipelining using Clay code

Start redis server:
```bash
redis-server
```

Set reference data:
```bash
redis-cli SET NUM_DATA_UNITS 4
redis-cli SET NUM_PARITY_UNITS 2
redis-cli SET CLAY_BLOCK_SIZE 32768
# For each node:
redis-cli XADD "node.info" * nodeId 0 nodeHost 127.0.0.1 nodePort 1111
```

Generate encoded blocks (Without starting helper nodes):
```bash
java -Xms128m -Xmx3072m -Denv=local -Djedis.pool.max.size=10 -Dcoordinator.ip=127.0.0.1 -Dcoordinator.local.port=1234 -Derasure.code=CLAY -Dfetch.method=normal -Dorg.slf4j.simpleLogger.defaultLogLevel=DEBUG -cp pipeline/build/libs/pipeline-1.0-SNAPSHOT-all.jar distributed.erasure.coding.pipeline.ClayCoordinatorKt
```

Start helper nodes (Change IP accordingly based on local/aws setup):
```bash
java -Dnode.local.ip=127.0.0.1 -Dcoordinator.ip=127.0.0.1 -Dnode.local.port=1111 -Dnode.local.id=0 -Djedis.pool.max.size=10 -Dorg.slf4j.simpleLogger.defaultLogLevel=DEBUG -cp pipeline/build/libs/pipeline-1.0-SNAPSHOT-all.jar distributed.erasure.coding.pipeline.ClayCodeNodeKt
```

Start repair using pipelining:
```bash
redis-cli PUBLISH coordinator.fetch "1 LP 1 pipeline" 
```

## Repair pipelining using LRC

Start redis server:
```bash
redis-server
```

Generate encoded blocks:
```bash
java -cp pipeline/build/libs/pipeline-1.0-SNAPSHOT-all.jar distributed.erasure.coding.LRCErasureCodeExampleKt
```

Delete one of the blocks: Eg: 2-LP.jpg

Start Coordinator node:
```bash
java -Denv=local -Djedis.pool.max.size=10 -Dcoordinator.ip=127.0.0.1 -Derasure.code=LRC -Dfetch.method=pipeline -Dorg.slf4j.simpleLogger.defaultLogLevel=DEBUG -cp pipeline/build/libs/pipeline-1.0-SNAPSHOT-all.jar distributed.erasure.coding.pipeline.CoordinatorKt
```

Start Helper nodes:
```bash


Note that Helper nodes can be started on any host, accordingly change main method in Coordinator.kt

Node 0:
```bash
java -Dnode.local.ip=127.0.0.1 -Dnode.local.port=4444 -Dnode.local.id=0 -Djedis.pool.max.size=10 -Dorg.slf4j.simpleLogger.defaultLogLevel=DEBUG -cp pipeline/build/libs/pipeline-1.0-SNAPSHOT-all.jar distributed.erasure.coding.pipeline.NodeImplKt
```

Node 1:
```bash
java -Dnode.local.ip=127.0.0.1 -Dnode.local.port=7777 -Dnode.local.id=1 -Djedis.pool.max.size=10 -Dorg.slf4j.simpleLogger.defaultLogLevel=DEBUG -cp pipeline/build/libs/pipeline-1.0-SNAPSHOT-all.jar distributed.erasure.coding.pipeline.NodeImplKt
```

Node 2:
```bash
java -Dnode.local.ip=127.0.0.1 -Dnode.local.port=8888 -Dnode.local.id=2 -Djedis.pool.max.size=10 -Dorg.slf4j.simpleLogger.defaultLogLevel=DEBUG -cp pipeline/build/libs/pipeline-1.0-SNAPSHOT-all.jar distributed.erasure.coding.pipeline.NodeImplKt
```

Node 3:
```bash
java -Dnode.local.ip=127.0.0.1 -Dnode.local.port=9999 -Dnode.local.id=3 -Djedis.pool.max.size=10 -Dorg.slf4j.simpleLogger.defaultLogLevel=DEBUG -cp pipeline/build/libs/pipeline-1.0-SNAPSHOT-all.jar distributed.erasure.coding.pipeline.NodeImplKt
```

Start repair of block:
```bash
redis-cli
PUBLISH coordinator "2 2-LP.jpg"
```

Rerun decode of file using blocks:
```bash
java -cp pipeline/build/libs/pipeline-1.0-SNAPSHOT-all.jar distributed.erasure.coding.LRCErasureCodeExampleKt
```
