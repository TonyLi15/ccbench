cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -DVAL_SIZE=1000 -DMASSTREE_USE=0 -DBACK_OFF=0 ..
ninja
for i in 0.0 0.2 0.4 0.6 0.8 0.99
do 
    numactl --interleave=all ./ss2pl.exe -clocks_per_us=2100 -extime=3 -max_ope=16 -rmw=0 -rratio=50 -thread_num=120 -tuple_num=1000000 -ycsb=1 -zipf_skew=$i
    # numactl --interleave=all ./silo.exe -clocks_per_us=2100 -extime=3 -max_ope=16 -rmw=0 -rratio=50 -thread_num=16 -tuple_num=1000000 -ycsb=1 -zipf_skew=$i
done