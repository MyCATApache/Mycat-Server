package io.mycat.route.function;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import io.mycat.config.model.TableConfig;
import io.mycat.config.model.rule.RuleConfig;
import io.mycat.util.SplitUtil;

/**
 * 测试分片算法定义是否符合分片表的定义, 主要测试分区数是否符合分片表分片数
 * 
 * @author CrazyPig
 *
 */
public class RuleFunctionSuitTableTest {
	
	@Test
	public void testAutoPartitionByLong() {
		AutoPartitionByLong autoPartition=new AutoPartitionByLong();
		autoPartition.setMapFile("autopartition-long.txt");
		autoPartition.init(); // partition = 3
		Assert.assertEquals(3, autoPartition.getPartitionNum());
		RuleConfig rule = new RuleConfig("id", "auto-partition-long");
		rule.setRuleAlgorithm(autoPartition);
		TableConfig tableConf = new TableConfig("test", "id", true, false, -1, "dn1,dn2",
				null, rule, true, null, false, null, null, null);
		int suit1 = autoPartition.suitableFor(tableConf);
		Assert.assertEquals(-1, suit1);
		
		tableConf.getDataNodes().clear();
		tableConf.getDataNodes().addAll(Arrays.asList("dn1", "dn2", "dn3"));
		
		int suit2 = autoPartition.suitableFor(tableConf);
		Assert.assertEquals(0, suit2);
		
		tableConf.getDataNodes().clear();
		tableConf.getDataNodes().addAll(Arrays.asList("dn1", "dn2", "dn3", "dn4"));
		int suit3 = autoPartition.suitableFor(tableConf);
		Assert.assertEquals(1, suit3);
	}
	
	@Test
	public void testPartitionByDate() {
		
		PartitionByDate partition = new PartitionByDate();
		partition.setDateFormat("yyyy-MM-dd");
		partition.setsBeginDate("2014-01-01");
		partition.setsEndDate("2014-01-31");
		partition.setsPartionDay("10");
		partition.init(); // partition = 4
		Assert.assertEquals(4, partition.getPartitionNum());
		
		RuleConfig rule = new RuleConfig("col_date", "partition-date");
		rule.setRuleAlgorithm(partition);
		TableConfig tableConf = new TableConfig("test", "id", true, false, -1, "dn1,dn2,dn3",
				null, rule, true, null, false, null, null, null);
		int suit1 = partition.suitableFor(tableConf);
		
		Assert.assertEquals(-1, suit1);
		
		tableConf.getDataNodes().clear();
		tableConf.getDataNodes().addAll(Arrays.asList("dn1", "dn2", "dn3", "dn4"));
		int suit2 = partition.suitableFor(tableConf);
		Assert.assertEquals(0, suit2);
		
		tableConf.getDataNodes().clear();
		tableConf.getDataNodes().addAll(Arrays.asList("dn1", "dn2", "dn3", "dn4", "dn5"));
		int suit3 = partition.suitableFor(tableConf);
		Assert.assertEquals(1, suit3);
		
		PartitionByDate partition1 = new PartitionByDate();
		partition.setDateFormat("yyyy-MM-dd");
		partition.setsBeginDate("2014-01-01");
		partition.setsPartionDay("10");
		partition.init(); // partition no limit
		
		int suit4 = partition1.suitableFor(tableConf);
		Assert.assertEquals(0, suit4);
		
	}
	
	@Test
	public void testPartitionByHashMod() {
		
		PartitionByHashMod partition = new PartitionByHashMod();
		partition.setCount(3); // partition = 3;
		Assert.assertEquals(3, partition.getPartitionNum());
		
		RuleConfig rule = new RuleConfig("id", "partition-hash-mod");
		rule.setRuleAlgorithm(partition);
		TableConfig tableConf = new TableConfig("test", "id", true, false, -1, "dn1,dn2,dn3",
				null, rule, true, null, false, null, null, null);
		int suit1 = partition.suitableFor(tableConf);
		Assert.assertEquals(0, suit1);
		
		tableConf.getDataNodes().clear();
		tableConf.getDataNodes().addAll(Arrays.asList("dn1", "dn2", "dn3", "dn4"));
		int suit2 = partition.suitableFor(tableConf);
		Assert.assertEquals(1, suit2);
		
		tableConf.getDataNodes().clear();
		tableConf.getDataNodes().addAll(Arrays.asList("dn1", "dn2"));
		int suit3 = partition.suitableFor(tableConf);
		Assert.assertEquals(-1, suit3);
	}
	
	@Test
	public void testPartitionByRangeMod() {
		PartitionByRangeMod partition = new PartitionByRangeMod();
		partition.setMapFile("partition-range-mod.txt");
		partition.init();
		
		Assert.assertEquals(20, partition.getPartitionNum()); // partition = 20
		RuleConfig rule = new RuleConfig("id", "partition-range-mod");
		rule.setRuleAlgorithm(partition);
		TableConfig tableConf = new TableConfig("test", "id", true, false, -1, "dn$1-10",
				null, rule, true, null, false, null, null, null);
		int suit1 = partition.suitableFor(tableConf);
		Assert.assertEquals(-1, suit1);
		
		tableConf.getDataNodes().clear();
		String[] dataNodes = SplitUtil.split("dn$1-20", ',', '$', '-');
		tableConf.getDataNodes().addAll(Arrays.asList(dataNodes));
		int suit2 = partition.suitableFor(tableConf);
		Assert.assertEquals(0, suit2);
		
		tableConf.getDataNodes().clear();
		dataNodes = SplitUtil.split("dn$1-30", ',', '$', '-');
		tableConf.getDataNodes().addAll(Arrays.asList(dataNodes));
		int suit3 = partition.suitableFor(tableConf);
		Assert.assertEquals(1, suit3);
		
	}

}
