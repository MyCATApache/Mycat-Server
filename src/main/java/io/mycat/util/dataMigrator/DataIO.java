package io.mycat.util.dataMigrator;

import java.io.File;
import java.io.IOException;

/**
 * 数据导入导出接口，mysql、oracle等数据库通过实现此接口提供具体的数据导入导出功能
 * @author haonan108
 *
 */
public interface DataIO {
 
	/**
	 * 导入数据
	 * @param dn 导入到具体的数据库
	 * @param file 导入的文件
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	
    void importData(TableMigrateInfo table,DataNode dn,String tableName,File file) throws IOException, InterruptedException;
    
    /**
     * 根据条件导出迁移数据
     * @param dn 导出哪个具体的数据库
     * @param tableName 导出的表名称
     * @param export 文件导出到哪里
     * @param condion 导出文件依赖的具体条件
     * @return 
     * @throws IOException 
     * @throws InterruptedException 
     */
    File exportData(TableMigrateInfo table,DataNode dn,String tableName,File exportPath,File condion) throws IOException, InterruptedException;
	
}
