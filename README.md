# Welcome to Blockchain_Data_Analysis !
Blockchain_Data_Analysis是通过公共的rpc端点进行对区块链的数据获取的一种方法
对于想快速获取某个区块链上的数据来分析，Blockchain_Data_Analys可以帮助你！

为什么使用Blockchain_Data_Analys？
1：采用rust提供并发的高效性能
2：rpc负载均衡，完全免费（仅需要设备一定的存储和性能）
3：采用redis的优秀性能
Blockchain_Data_Analysis仅提供主体代码，如果你的需要的数据与代码不一样需要进行修改



#  配置问题
## 区块链的选择
Blockchain_Data_Analysis选择了scroll链
修改RPC_URLS中的rpc节点切换你需要的区块链rpc

## 数据的需求
Blockchain_Data_Analysis示例展示如何获取scroll链上活跃的地址数量
修改process_transaction函数和process_block函数以达到获取自己想要的数据


## 数据结构需求
Blockchain_Data_Analysis示例展示使用redis的sortedset和set数据结构方便排序和验证新旧地址信息
如果你不需要此数据结构，修改redis配置相关函数
