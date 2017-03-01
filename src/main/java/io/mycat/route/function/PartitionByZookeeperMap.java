package io.mycat.route.function;

import io.mycat.config.model.rule.RuleAlgorithm;

/**
 * 路由方式是根据字段值到子库的Map进行路由
 * <p/>
 * 控制方式是使用zookeeper实现数据迁移在mycat集群中的协调：支持锁租户和解锁租户
 * <p/>
 * zookeeper节点有三个目录：
 * 1. tenants节点，下面是所有租户到子库的映射信息
 * 2. partitioners，下面是所有活跃的partitioner（很可能处于不同的mycat实例中)
 * 3. datanodes节点，下面是所有子库到租户的映射信息。作用是为paritioner加快初始化
 * <p/>
 * 初始化流程：
 * 1. Partitioner在加载时，向zookeeper申请mycats读锁，注册自己
 * 2. 读取routes节点下的路由信息
 * <p/>
 * 锁租户流程:
 * 1. 甲方在mycats节点上加写锁，禁止新mycat实例加入
 * 2. 甲方在mycats节点下的本Partitioner节点下新建“锁租户”节点
 * 3. Paritioner收到通知，锁租户
 * 4. 在“锁租户”节点下，新建“已锁”节点
 * <p/>
 * 变更路由信息流程:
 * 1. 甲方修改routes中的路由
 * <p/>
 * 释放租户锁流程:
 * 1. 甲方在“已锁”节点下，新建“释放锁”节点，并附上租户新子库信息
 * 2. Paritioner读取变更租户信息，变更租户，释放锁。然后删除“释放锁”、“已锁”、“锁租户”节点
 * 3. 甲方检查所有Paritioner都删除节点后，释放mycats节点的写锁，允许新mycat实例
 * <p/>
 * 新租户路由信息
 * 1.
 * <p/>
 * Created by zhenghu on 17-3-1.
 */
public class PartitionByZookeeperMap extends AbstractPartitionAlgorithm implements RuleAlgorithm, ReloadFunction {

    private transient Worker worker = null;
    private transient Object reloading = false;

    private String connectionString;
    private Long timeOut;
    private String rootPath;

    public void setTimeOut(Long timeOut) {
        this.timeOut = timeOut;
    }

    public void setRootPath(String rootPath) {
        this.rootPath = rootPath;
    }

    @Override
    public void init() {
        this.reload();
    }

    @Override
    public Integer calculate(String columnValue) {
        return this.worker.calculate(columnValue);
    }

    @Override
    public void reload() {
        Worker old = this.worker;
        this.worker = new Worker(connectionString, rootPath, timeOut);
        old.close();
    }

    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    // 真实worker
    public static class Worker {

        public Worker(String zkConnection, String rootPath, Long timeout){

        }
        public Integer calculate(String columnValue) {
            return null;
        }

        void close() {

        }
    }

}
