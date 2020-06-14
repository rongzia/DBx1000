cd /home/zhangrongrong/CLionProjects/DBx1000/build

# $1 为第 i 个节点的编号
# ./run 1 8, 表示第 2 个结点，8个进程

echo "node_id     : $1"
echo "process_num : $2"
start=`expr $1 \* $2`
temp=`expr $1 + 1`
temp_end=`expr $temp \* $2`
end=`expr $temp_end - 1`

echo "start : $start"
echo "end   : $end"

for i in $(seq $start $end)
do
cp main_test_instance main_test_instance$i
done

for i in $(seq $start $end)
do
./main_test_instance$i -instance_id=$i &
done
