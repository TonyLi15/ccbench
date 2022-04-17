for k in 1000
# 10000 100000
do
    cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -DVAL_SIZE=$k -DMASSTREE_USE=0 -DBACK_OFF=1 ..
    ninja
    for j in 16
    # 32
    do
        for i in 1 8 16 32 64 96 120
        do 
            numactl --interleave=all ./silo.exe -clocks_per_us=2100 -extime=3 -max_ope=$j -rmw=0 -rratio=50 -thread_num=$i -tuple_num=1000000 -ycsb=1 -zipf_skew=0.9
        done
    done
done
# numactl --interleave=all ./ss2pl.exe -clocks_per_us=2100 -extime=3 -max_ope=16 -rmw=0 -rratio=50 -thread_num=16 -tuple_num=100000000 -ycsb=1 -zipf_skew=0.8
# cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -DVAL_SIZE=1000 ..
# ninja
# for j in 16
# do
# for i in 1 8 16 32 64 96 120
# do 
#     numactl --interleave=all ./ss2pl.exe -clocks_per_us=2100 -extime=3 -max_ope=$j -rmw=0 -rratio=50 -thread_num=$i -tuple_num=100000000 -ycsb=1 -zipf_skew=0.9
# done
# done