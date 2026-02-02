
# xtWindowfunctiontest
This is test code for Spark SQL window functions, intended for research and learning in big data analytics.

# Spark SQL Window Functions 实战指南本仓库专门用于演示和测试 
## Apache Spark SQL 中最核心的三个窗口函数：ROW_NUMBER()、RANK() 和 DENSE_RANK()。
通过电商订单场景的模拟数据，直观展示它们在处理重复数值时的逻辑差异。
 核心逻辑速查在处理相同数值（并列）时，三者的表现如下：
函数行为特征结果序列示例典型应用场景ROW_NUMBER()绝对连续：不论数值是否相同，行号唯一且连续递增。1, 2, 3, 4分页查询、数据去重、取第一笔订单RANK()并列跳跃：数值相同则排名相同，后续排名会跳跃。1, 2, 2, 4竞赛排名、积分排行榜DENSE_RANK()并列连续：数值相同则排名相同，后续排名保持连续。1, 2, 2, 3薪资等级计算、成绩阶梯
## 🛠️ 环境准备与数据构造首先创建测试数据库，并利用 Spark SQL 内置函数生成 30,000 条 模拟订单数据。SQL-- 1. 初始化环境
```
CREATE DATABASE IF NOT EXISTS spark_win;
USE spark_win;
```

DROP TABLE IF EXISTS orders_win;

```
-- 2. 创建订单表
CREATE TABLE orders_win (
    order_id     BIGINT,
    user_id      INT,
    category     STRING,
    amount       DECIMAL(10,2),
    order_date   DATE
) USING PARQUET;
```

```
-- 3. 构造 30,000 条模拟数据
INSERT INTO orders_win
SELECT
    id                            AS order_id,
    CAST(id % 500 AS INT)         AS user_id,          -- 模拟 500 个用户
    CASE
        WHEN id % 4 = 0 THEN 'food'
        WHEN id % 4 = 1 THEN 'book'
        WHEN id % 4 = 2 THEN 'phone'
        ELSE 'clothes'
    END                           AS category,
    CAST((rand() * 1000 + 10) AS DECIMAL(10,2)) AS amount,
    date_add('2024-01-01', id % 180) AS order_date
FROM (
    SELECT explode(sequence(1, 30000)) AS id
) t;
```
## 🔍 窗口函数测试案例1. ROW_NUMBER()：获取用户首单

```
SELECT 
    user_id, 
    order_id, 
    order_date,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY order_date) AS rn
FROM orders_win;
```
## 2. RANK()：消费金额排名（跳跃）SQL
```
SELECT 
    user_id, 
    order_id, 
    amount,
    RANK() OVER (PARTITION BY user_id ORDER BY amount DESC) AS rnk
FROM orders_win;
```
## 3. DENSE_RANK()：消费金额等级（连续）SQL
```
SELECT 
    user_id, 
    order_id, 
    amount,
    DENSE_RANK() OVER (PARTITION BY user_id ORDER BY amount DESC) AS dense_rnk
FROM orders_win;
```
## 📊 结果差异对照表

| 排序逻辑 | 相同值处理 | 序列示例 | 适用场景 |
| :--- | :--- | :--- | :--- |
| **ROW_NUMBER** | 强制唯一递增 | 1, 2, 3, 4 | 分页、取 Top 1 |
| **RANK** | 并列则跳号 | 1, 2, 2, **4** | 竞赛排名（有并列有断层） |
| **DENSE_RANK** | 并列不跳号 | 1, 2, 2, **3** | 成绩等级、薪资阶梯 |

