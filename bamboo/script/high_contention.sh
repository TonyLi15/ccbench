for i in 1 8 16 32 64 96 120
do 
    numactl --interleave=all ./ss2pl.exe -clocks_per_us=2100 -extime=3 -max_ope=32 -rmw=0 -rratio=50 -thread_num=$i -tuple_num=100000000 -ycsb=1 -zipf_skew=0.9
done    
# numactl --interleave=all ./ss2pl.exe -clocks_per_us=2100 -extime=3 -max_ope=16 -rmw=0 -rratio=50 -thread_num=16 -tuple_num=100000000 -ycsb=1 -zipf_skew=0.8