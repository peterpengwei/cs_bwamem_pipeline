SPARK_DRIVER_MEMORY=24g \
/cluster/spark/spark-1.5.1-bin-hadoop2.4/bin/spark-submit \
--executor-memory 48g \
--class cs.ucla.edu.bwaspark.BWAMEMSpark \
--total-executor-cores ${1} \
--master spark://10.0.1.17:7077 \
--conf spark.app.name="CS-BWAMEM:FPGA" \
--conf spark.driver.cores=20 --conf spark.driver.maxResultSize=40g --conf spark.storage.memoryFraction=0.7 \
--conf spark.eventLog.enabled=true --conf spark.eventLog.dir="hdfs://cdsc0:9000/user/pengwei/eventLogs_fpga" \
--conf spark.akka.threads=20 --conf spark.akka.frameSize=1024 \
--conf spark.ui.port=4042 \
--conf spark.network.timeout="600s" \
/curr/pengwei/demo/run/cloud-scale-bwamem/target/cloud-scale-bwamem-0.2.2-assembly.jar \
cs-bwamem -bfn 1 -bPSW 1 -sbatch 10 -bPSWJNI 1 -jniPath /curr/ytchen0323/shared_lib/jniNative.so \
-oChoice 2 -oPath hdfs://cdsc0:9000/user/pengwei/null_fpga -localRef 1 -R "@RG	ID:HCC1954	LB:HCC1954	SM:HCC1954" -isSWExtBatched 1 -bSWExtSize 32768 -FPGAAccSWExt 1 -FPGASWExtThreshold 256 \
-jniSWExtendLibPath "/curr/genomics_spark/shared_lib/jniSWExtend.so" 1 /space/scratch/ReferenceMetadata/human_g1k_v37.fasta \
hdfs://cdsc0:9000/user/pengwei/demo/output/pseudo_48
