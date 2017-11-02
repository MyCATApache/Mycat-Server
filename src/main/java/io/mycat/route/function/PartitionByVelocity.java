package io.mycat.route.function;

import io.mycat.route.util.VelocityUtil;


/**
 * 根据Velocity模板语言，分库分表规则更加灵活，例如一共100个分库，字段中包含时间信息，取时间的月份与天，hashCode再对100取余
 * <function name="parseByVelocity" class="io.mycat.route.function.PartitionByVelocity">
 * <property name="columnName">id</property><!--id="20010222330011" partition=95 -->
 * <property name="rule"><![CDATA[
			#set($Integer=0)##
			#set($monthday=$stringUtil.substring($id,4,8))##
			#set($prefix=$monthday.hashCode()%100)##
    			$!prefix]]>
    </property>
 * </function>
 * @author yan.yan@huawei.com
 */
public class PartitionByVelocity extends AbstractPartitionAlgorithm implements RuleAlgorithm {
    //分片字段名
    private String columnName;
    //规则
    private String rule;
   

    public void setColumnName(String str) {
    	columnName = str;
    }

    public void setRule(String str) {
        rule = str;
    }

    @Override
    public void init() {

    }

    @Override
    public Integer calculate(String columnValue) {
        String partitionSubString = VelocityUtil.evalDBRule(columnName, columnValue, rule);
        return Integer.parseInt(partitionSubString);
    }
}
