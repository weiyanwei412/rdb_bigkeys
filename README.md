# 简介
    线上遇到redis CPU高与网卡带宽跑满的情况， 很明显的bigkey问题， 但使用一个开源的以python编写的redis RDB分析工具来分析big key， 分析<br>
    150MB的RDB文件花了一个小时， 这太慢了， 因此使用go重新写了个分析RDB文件来找出big key的工具rdb_bigkeys
    速度很快， 同样分析150MB的RDB文件， 只要1分2秒。
![rdb_bigkeys](https://github.com/GoDannyLai/rdb_bigkeys/raw/master/misc/img/time_150MB_RDB.png)

    生成的bigkey报告为CSV格式：
![rdb_bigkeys_mem](https://github.com/GoDannyLai/rdb_bigkeys/raw/master/misc/img/bigkeys_csv.png)

    使用很简单，全部就下面提到的5个参数：
        ./rdb_bigkeys --bytes 1024 --file bigkeys_6379.csv --sep 0 --sorted --threads 4 dump6379.rdb
		上述命令分析dump6379.rdb文件中大于1024bytes的KEY， 由大到小排好序， 以CSV格式把结果输出到bigkeys_6379.csv的文件中
    
