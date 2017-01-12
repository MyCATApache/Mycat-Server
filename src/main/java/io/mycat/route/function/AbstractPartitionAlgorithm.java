package io.mycat.route.function;

import io.mycat.config.model.TableConfig;
import io.mycat.config.model.rule.RuleAlgorithm;

import java.io.Serializable;
import java.util.List;

/**
 * 路由分片函数抽象类
 * 为了实现一个默认的支持范围分片的函数 calcualteRange
 * 重写它以实现自己的范围路由规则
 * @author lxy
 *
 */
public abstract class AbstractPartitionAlgorithm implements RuleAlgorithm ,Serializable {

	@Override
	public void init() {
	}

	/**
	 * 返回所有被路由到的节点的编号
	 * 返回长度为0的数组表示所有节点都被路由（默认）
	 * 返回null表示没有节点被路由到
	 */
	@Override
	public Integer[] calculateRange(String beginValue, String endValue)  {
		return new Integer[0];
	}
	
	/**
	 * 对于存储数据按顺序存放的字段做范围路由，可以使用这个函数
	 * @param algorithm
	 * @param beginValue
	 * @param endValue
	 * @return
	 */
	public static Integer[] calculateSequenceRange(AbstractPartitionAlgorithm algorithm, String beginValue, String endValue)  {
		Integer begin = 0, end = 0;
		begin = algorithm.calculate(beginValue);
		end = algorithm.calculate(endValue);

		if(begin == null || end == null){
			return new Integer[0];
		}
		
		if (end >= begin) {
			int len = end-begin+1;
			Integer [] re = new Integer[len];
			
			for(int i =0;i<len;i++){
				re[i]=begin+i;
			}
			
			return re;
		}else{
			return new Integer[0];
		}
	}
	
	/**
	 * 
	 * 分片表所跨的节点数与分片算法分区数一致性校验
	 * @param tableConf
	 * @return 
	 * -1 if table datanode size < rule function partition size
	 * 0 if table datanode size == rule function partition size
	 * 1 if table datanode size > rule function partition size
	 */
	public final int suitableFor(TableConfig tableConf) {
		int nPartition = getPartitionNum();
		if(nPartition > 0) { // 对于有限制分区数的规则,进行检查
			int dnSize = tableConf.getDataNodes().size();
			boolean  distTable = tableConf.isDistTable();
			List tables = tableConf.getDistTables();
			if(distTable){
				if(tables.size() < nPartition){
					return  -1;
				} else if(dnSize > nPartition) {
					return 1;
				}
			}else{
				if(dnSize < nPartition) {
					return  -1;
				} else if(dnSize > nPartition) {
					return 1;
				}
			}
		}
		return 0;
	}
	
	/**
	 * 返回分区数, 返回-1表示分区数没有限制
	 * @return
	 */
	public int getPartitionNum() {
		return -1; // 表示没有限制
	}
	
}
