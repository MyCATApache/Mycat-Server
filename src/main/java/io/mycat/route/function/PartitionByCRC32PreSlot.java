package io.mycat.route.function;

import io.mycat.config.model.SystemConfig;
import io.mycat.config.model.rule.RuleAlgorithm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

/**
 *自动迁移御用分片算法，预分slot 102400个，映射到dn上，再conf下会保存映射文件，请不要修改
 *
 * @author nange magicdoom@gmail.com
 *
 */
public class PartitionByCRC32PreSlot extends AbstractPartitionAlgorithm implements RuleAlgorithm , TableRuleAware {


	private static final Logger LOGGER = LoggerFactory.getLogger("PartitionByCRC32PreSlot");

	private static final int DEFAULT_SLOTS=102400;

	private static final Charset DEFAULT_CHARSET=Charset.forName("UTF-8");
	 private Map<Integer,Range>   rangeMap=new HashMap<>();

	private int count;

	private   Properties loadProps(String name,boolean forceNew) {
		Properties prop = new Properties();
		File file = new File(SystemConfig.getHomePath(), "conf"
				+ File.separator + name+".properties");
		if(file.exists()&&forceNew)file.delete();
		if (!file.exists()) {
			prop= genarateP();
			try (FileOutputStream out = new FileOutputStream(file)){
				prop.store(out,"WARNING   !!!Please do not modify or delete this file!!!");
			} catch (IOException e) {
				LOGGER.error("error",e);
			}
			return prop;
		}
		FileInputStream filein = null;
		try {
			filein = new FileInputStream(file);
			prop.load(filein);
		} catch (Exception e) {
			LOGGER.warn("load PartitionByCRC32PreSlotIndex err:" + e);
		} finally {
			if (filein != null) {
				try {
					filein.close();
				} catch (IOException e) {
				}
			}
		}
		return prop;
	}


	private     Properties genarateP()
	{
		int slotSize=  DEFAULT_SLOTS/count;
		Properties prop = new Properties();
		for (int i = 0; i <count; i++) {
			if(i==count-1)
			{
				prop.put(String.valueOf(i),i*slotSize+"-"+(DEFAULT_SLOTS-1));
			} else
			{
				prop.put(String.valueOf(i),i*slotSize+"-"+((i+1)*slotSize-1));
			}

		}

		return prop;
	}

	private Map<Integer,Range>    convertToMap(Properties p)
	{
		Map<Integer,Range> map=new HashMap<>();
		for (Object o : p.keySet()) {
			String k= (String) o;
			String v=p.getProperty(k) ;
			String[] vv=v.split("-");
			if(vv.length==2) {
				Range range=new Range(Integer.parseInt(vv[0]),Integer.parseInt(vv[1]));
				map.put(Integer.parseInt(k),range) ;
			} else
			{
				Range range=new Range(Integer.parseInt(vv[0]),Integer.parseInt(vv[0]));
				map.put(Integer.parseInt(k),range);
			}
		}

		return map;
	}

	@Override public void init() {

		super.init();
		if(ruleName!=null){
			Properties p=	loadProps(ruleName,false);
			rangeMap=convertToMap(p);
		}
	}

	public void reInit() {

		if(ruleName!=null){
			Properties p=	loadProps(ruleName,true);
			rangeMap=convertToMap(p);
		}
	}
	/**
	 * 节点的数量
	 * @param count
	 */
	public void setCount(int count) {
		this.count = count;
	}


	@Override
	public Integer calculate(String columnValue) {
	  if(ruleName==null) throw new RuntimeException();
		PureJavaCrc32 crc32 = new PureJavaCrc32();
		byte[] bytes = columnValue.getBytes(DEFAULT_CHARSET);
		crc32.update(bytes,0,bytes.length);
		long x = crc32.getValue();
	    int 	slot= (int) (x%DEFAULT_SLOTS);
		for (Map.Entry<Integer, Range> rangeEntry : rangeMap.entrySet()) {
			Range range=rangeEntry.getValue();
			if(slot>=range.start&&slot<=range.end){
				return rangeEntry.getKey();
			}
		}


        int slotSize=  DEFAULT_SLOTS/count;

		int index = slot / slotSize;
		if(slotSize*count!=DEFAULT_SLOTS&&index>count-1)
		{
			index=  (count - 1);
		}
		return index ;
	}

	@Override public int getPartitionNum() {
		return this.count;
	}

	private static void hashTest() throws IOException{
		PartitionByCRC32PreSlot hash=new PartitionByCRC32PreSlot();
		hash.setRuleName("test");
		hash.count=1024;//分片数

		hash.init();
		long start=System.currentTimeMillis();
		int[] bucket=new int[hash.count];
		
		Map<Integer,List<Integer>> hashed=new HashMap<>();
		
		int total=1000_0000;//数据量
		int c=0;
		for(int i=100_0000;i<total+100_0000;i++){//假设分片键从100万开始
			c++;
			int h=hash.calculate(Integer.toString(i));
			if(h>=hash.count)
			{
				System.out.println("error:"+h);
			}
			bucket[h]++;
			List<Integer> list=hashed.get(h);
			if(list==null){
				list=new ArrayList<>();
				hashed.put(h, list);
			}
			list.add(i);
		}
		System.out.println(c+"   "+total);
		double d=0;
		c=0;
		int idx=0;
		System.out.println("index    bucket   ratio");
		for(int i:bucket){
			d+=i/(double)total;
			c+=i;
			System.out.println(idx+++"  "+i+"   "+(i/(double)total));
		}
		System.out.println(d+"  "+c);

		long used=System.currentTimeMillis()-start;

		System.out.println("tps "+total*1000.0/used);
		System.out.println("****************************************************");

	}

	public static void main(String[] args) throws IOException {
		hashTest();
	}

	private String tableName;
	private String ruleName;

	@Override public void setTableName(String tableName) {
	   this.tableName=tableName;
	}

	@Override public void setRuleName(String ruleName) {
		 this.ruleName=ruleName;
	}

	public static class Range
	{
		public Range(int start, int end) {
			this.start = start;
			this.end = end;
		}

		public   int start;
		public  int end;
	}
}
