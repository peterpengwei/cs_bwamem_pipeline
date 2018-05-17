#for i in 3 6 9 12 18
#for i in 12
#for i in 2 4 6 8 12 16 24
for i in 32 16 8 4
do
  echo "[DEMO] running CS-BWAMEM on a CPU-only cluster and an FPGA-equipped cluster (both 3 nodes, ${i} cores) simultaneously."
  echo "[DEMO] round #1"
  ## ssh m2 'pkill -f alphadata_host'
  ## ssh m3 'pkill -f alphadata_host'
  ## ssh m4 'pkill -f alphadata_host'
  time ./run_cpu.sh $i 0 CPU
  # time ./run_fpga.sh $i 1 FPGA & PIDMIX=$!
  #wait $PIDIOS
  # wait $PIDMIX
done
