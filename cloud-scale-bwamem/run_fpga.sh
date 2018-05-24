SPARK_DRIVER_MEMORY=32g \
/curr/pengwei/cluster_software/spark-1.6.0-bin-hadoop2.6/bin/spark-submit \
--executor-memory 48g \
--class cs.ucla.edu.bwaspark.BWAMEMSpark \
--total-executor-cores $1 \
--master spark://ubuntu.pengwei.m6.cdsc-local:7077 \
--conf spark.app.name="CS-BWAMEM:${3}" \
--conf spark.driver.cores=16 --conf spark.driver.maxResultSize=64g --conf spark.storage.memoryFraction=0.75 \
--conf spark.eventLog.enabled=true --conf spark.eventLog.dir="hdfs://ubuntu.pengwei.m6.cdsc-local:9000/user/pengwei/eventLogs" \
--conf spark.akka.threads=32 --conf spark.akka.frameSize=1024 \
--conf spark.network.timeout="600s" \
/curr/pengwei/cs_bwamem_pipeline/cloud-scale-bwamem/target/cloud-scale-bwamem-0.2.2-assembly.jar \
cs-bwamem -bfn 1 -bPSW 1 -sbatch 10 -bPSWJNI 1 -jniPath /curr/ytchen0323/shared_lib/jniNative.so \
-oChoice 2 -oPath hdfs://ubuntu.pengwei.m6.cdsc-local:9000/user/pengwei/null_fpga -localRef 1 -R "@RG	ID:HCC1954	LB:HCC1954	SM:HCC1954" -isSWExtBatched 1 -bSWExtSize $4 -FPGAAccSWExt $2 -FPGASWExtThreshold $5 \
-jniSWExtendLibPath "/curr/genomics_spark/shared_lib/jniSWExtend.so" 1 /curr/pengwei/archive/genomics/ReferenceMetadata/human_g1k_v37.fasta \
hdfs://ubuntu.pengwei.m6.cdsc-local:9000/user/pengwei/demo_input/HCC1954_8
