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
		
		/* 
		 * autopartition-long-dupl.txt
		 * 0-1000=0
		 * 1001-2000=1
		 * 2001-3000=0
		 * 3001-4000=1
		*/
		AutoPartitionByLong autoPartition2 = new AutoPartitionByLong();
		autoPartition2.setMapFile("autopartition-long-dupl.txt");
		autoPartition2.init();
		Assert.assertEquals(2, autoPartition2.getPartitionNum());
		RuleConfig rule2 = new RuleConfig("id", "auto-partition-long-dupl");
		rule2.setRuleAlgorithm(autoPartition2);
		TableConfig tableConf2 = new TableConfig("test2", "id", true, false, -1, "dn1,dn2",
				null, rule, true, null, false, null, null, null);
		Assert.assertEquals(0, autoPartition2.suitableFor(tableConf2));
		
		Assert.assertEquals(0, autoPartition2.calculate("500").intValue());
		Assert.assertEquals(1, autoPartition2.calculate("1500").intValue());
		Assert.assertEquals(1, autoPartition2.calculate("2000").intValue());
		Assert.assertEquals(0, autoPartition2.calculate("3000").intValue());
		Assert.assertEquals(1, autoPartition2.calculate("3001").intValue());
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
	
	@Test
	public void testPartitionByPattern() {
		PartitionByPattern partition = new PartitionByPattern();
		partition.setMapFile("partition-pattern.txt");
		partition.init();
		
		/*
		 * partition-pattern.txt
		 * 1-32=0
         * 33-64=1
         * 65-96=2
         * 97-128=3
         * 129-160=4
         * 161-192=5
         * 193-224=6
         * 225-256=7
         * 0-0=7
		 */
		
		Assert.assertEquals(8, partition.getPartitionNum());
		
	}
	
	@Test
	public void testPartitionByPrefixPattern() {
		PartitionByPrefixPattern partition = new PartitionByPrefixPattern();
		partition.setMapFile("partition_prefix_pattern.txt");
		partition.init();
		
		
		/*
		 * partition_prefix_pattern.txt
		 * 1-4=0
         * 5-8=1
         * 9-12=2
         * 13-16=3
         * 17-20=4
	     * 21-24=5
         * 25-28=6
         * 29-32=7
         * 0-0=7
		 */
		Assert.assertEquals(8, partition.getPartitionNum());
		
		RuleConfig rule = new RuleConfig("id", "partition-prefix-pattern");
		rule.setRuleAlgorithm(partition);
		TableConfig tableConf = new TableConfig("test", "id", true, false, -1, "dn1,dn2",
				null, rule, true, null, false, null, null, null);
		int suit1 = partition.suitableFor(tableConf);
		Assert.assertEquals(-1, suit1);
		
		tableConf.getDataNodes().clear();
		String[] dataNodes = SplitUtil.split("dn$1-8", ',', '$', '-');
		tableConf.getDataNodes().addAll(Arrays.asList(dataNodes));
		int suit2 = partition.suitableFor(tableConf);
		Assert.assertEquals(0, suit2);
		
		tableConf.getDataNodes().clear();
		dataNodes = SplitUtil.split("dn$1-10", ',', '$', '-');
		tableConf.getDataNodes().addAll(Arrays.asList(dataNodes));
		int suit3 = partition.suitableFor(tableConf);
		Assert.assertEquals(1, suit3);
	}

}
