# 简单电影推荐系统
###流程：
####
MySQL同步ES，用户搜索、评分、收藏时埋点写入Kafka，转发到：
- Spark Streaming，实时流，基于时间窗口计算KNN
- Hive，离线流，用定时任务计算UserFC模型
