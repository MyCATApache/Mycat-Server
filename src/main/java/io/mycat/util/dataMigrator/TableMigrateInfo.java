package io.mycat.util.dataMigrator;

import com.alibaba.druid.util.JdbcUtils;
import io.mycat.route.function.AbstractPartitionAlgorithm;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;


/**
 * 表迁移信息，包括:
 * 表名、迁移前后的数据节点、表数据量、迁移前后数据分布对比
 * @author haonan108
 *
 */

public class TableMigrateInfo {

	private String schemaName;
	private String tableName;
	private List<DataNode> oldDataNodes;
	private List<DataNode> newDataNodes;
	private AtomicLong size = new AtomicLong();
	
	private List<DataNodeMigrateInfo> dataNodesDetail = new ArrayList<>();//节点间数据迁移详细信息
	
	private AbstractPartitionAlgorithm newRuleAlgorithm;
	private String column;
	
	private boolean isExpantion; //true:扩容 false：缩容
	
	private volatile boolean isError; 
	
    private StringBuffer errMessage = new StringBuffer();
    
    private String tableStructure = ""; //记录建表信息，迁移后的节点表不存在的话自动建表
    
    private Map<String,String> dnMigrateSize;
	
	public TableMigrateInfo(String schemaName, String tableName, List<DataNode> oldDataNodes,
			List<DataNode> newDataNodes, AbstractPartitionAlgorithm newRuleAlgorithm, String column) {
		super();
		this.schemaName = schemaName;
		this.tableName = tableName;
		this.oldDataNodes = oldDataNodes;
		this.newDataNodes = newDataNodes;
		this.newRuleAlgorithm = newRuleAlgorithm;
		this.column = column;
		if(newDataNodes.size()>oldDataNodes.size()){
			isExpantion = true;
		}else{
			isExpantion = false;
		}
		dnMigrateSize = new TreeMap<>(new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				return o1.compareTo(o2);
			}
		});
	}
	
	//读取表结构
	public  void setTableStructure() throws SQLException{
		DataNode dn = this.getOldDataNodes().get(0);
		Connection con = null;
		try {
			con = DataMigratorUtil.getMysqlConnection(dn);
			List<Map<String, Object>> list = DataMigratorUtil.executeQuery(con, "show create table "+tableName);
			Map<String, Object> m  = list.get(0);
			String str = m.get("Create Table").toString();
			str = str.replaceAll("CREATE TABLE", "Create Table if not exists");
			setTableStructure(str);
		} catch (SQLException e) {
			throw e;
		}finally {
			JdbcUtils.close(con);
		}
	}
	
	//缩容后，找出被移除的节点
	public List<DataNode> getRemovedDataNodes(){
		List<DataNode> list = new ArrayList<>();
		list.addAll(oldDataNodes);
		list.removeAll(newDataNodes);
		return list;
	}
	
	//扩容后，找出除旧节点以外新增加的节点
	public List<DataNode> getNewAddDataNodes(){
		List<DataNode> list = new ArrayList<>();
		list.addAll(newDataNodes);
		list.removeAll(oldDataNodes);
		return list;
	}
	
	//对新增的节点创建表：create table if not exists 
	public void createTableToNewDataNodes() throws SQLException{
		if(this.isExpantion){
			List<DataNode> newDataNodes = getNewAddDataNodes();
			for(DataNode dn:newDataNodes){
				DataMigratorUtil.createTable(dn, this.tableStructure);
			}
		}
	}
	
	//打印迁移信息
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void printMigrateInfo(){
		Map<String,String> map = new LinkedHashMap();
		map.put("tableSize", size.get()+"");
		map.put("migrate before", oldDataNodes.toString());
		map.put("migrate after", newDataNodes.toString());
		map.put("rule function", newRuleAlgorithm.getClass().getSimpleName());
		String title = getSchemaAndTableName()+" migrate info";
		System.out.println(DataMigratorUtil.printMigrateInfo(title, map, "="));
	}
	
	public void printMigrateSchedule(){
		String title = getSchemaAndTableName()+" migrate schedule";
		System.out.println(DataMigratorUtil.printMigrateInfo(title, dnMigrateSize, "->"));
	}
	
	/**
	 * 是否为扩容，true：扩容，false：缩容
	 * @return
	 */
	public boolean isExpantion(){
		return isExpantion;
	}

	public List<DataNodeMigrateInfo> getDataNodesDetail() {
		return dataNodesDetail;
	}

	public void setDataNodesDetail(List<DataNodeMigrateInfo> dataNodesDetail) {
		this.dataNodesDetail = dataNodesDetail;
	}

	public String getSchemaName() {
		return schemaName;
	}

	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public List<DataNode> getOldDataNodes() {
		return oldDataNodes;
	}

	public void setOldDataNodes(List<DataNode> oldDataNodes) {
		this.oldDataNodes = oldDataNodes;
	}

	public List<DataNode> getNewDataNodes() {
		return newDataNodes;
	}

	public void setNewDataNodes(List<DataNode> newDataNodes) {
		this.newDataNodes = newDataNodes;
	}

	public AbstractPartitionAlgorithm getNewRuleAlgorithm() {
		return newRuleAlgorithm;
	}

	public void setNewRuleAlgorithm(AbstractPartitionAlgorithm newRuleAlgorithm) {
		this.newRuleAlgorithm = newRuleAlgorithm;
	}

	public String getColumn() {
		return column;
	}

	public void setColumn(String column) {
		this.column = column;
	}
	
	public String getSchemaAndTableName(){
		return "["+schemaName+":"+tableName+"]";
	}

	public StringBuffer getErrMessage() {
		return errMessage;
	}

	public void setErrMessage(String errMessage) {
		this.errMessage = new StringBuffer(errMessage);
	}

	public AtomicLong getSize() {
		return size;
	}
	
	public void setSize(long size){
		this.size = new AtomicLong(size);
	}

	public boolean isError() {
		return isError;
	}

	public void setError(boolean isError) {
		this.isError = isError;
	}

	public String getTableStructure() {
		return tableStructure;
	}

	public void setTableStructure(String tableStructure) {
		this.tableStructure = tableStructure;
	}

	public void setSize(AtomicLong size) {
		this.size = size;
	}

	public Map<String, String> getDnMigrateSize() {
		return dnMigrateSize;
	}

	public void setDnMigrateSize(Map<String, String> dnMigrateSize) {
		this.dnMigrateSize = dnMigrateSize;
	}
	
}
