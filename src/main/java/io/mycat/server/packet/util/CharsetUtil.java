/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Any questions about this component can be directed to it's project Web address
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.server.packet.util;

import io.mycat.backend.PhysicalDBPool;
import io.mycat.backend.PhysicalDatasource;
import io.mycat.server.config.node.DBHostConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

/**
 * 该类被彻底重构，fix 掉了原来的  collationIndex 和 charset 之间对应关系的兼容性问题，
 * 比如  utf8mb4 对应的 collationIndex 有 45, 46 两个值，如果我们只配置一个 45或者46的话，
 * 那么当mysqld(my.cnf配置文件)中的配置了：collation_server=utf8mb4_bin时，而我们却仅仅
 * 值配置45的话，那么就会报错：'java.lang.RuntimeException: Unknown charsetIndex:46'
 * 如果没有配置 collation_server=utf8mb4_bin，那么collation_server就是使用的默认值，而我们却仅仅
 * 仅仅配置46，那么也会报错。所以应该要同时配置45,46两个值才是正确的。
 * 重构方法是，在 MycatServer.startup()方法在中，在 config.initDatasource(); 之前，加入
 * CharsetUtil.initCharsetAndCollation(config.getDataHosts());
 * 该方法，直接从mysqld的information_schema.collations表中获取 collationIndex 和 charset 之间对应关系，
 * 因为是从mysqld服务器获取的，所以肯定不会出现以前的兼容性问题(不同版本的mysqld，collationIndex 和 charset 对应关系不一样)。
 * @author mycat
 */
public class CharsetUtil {
    public static final Logger logger = LoggerFactory.getLogger(CharsetUtil.class);
    
    /** collationIndex 和 charsetName 的映射 */
    private static final Map<Integer,String> INDEX_TO_CHARSET = new HashMap<>();
    
    /** charsetName 到 默认collationIndex 的映射 */
    private static final Map<String, Integer> CHARSET_TO_INDEX = new HashMap<>();
    
    /** collationName 到 CharsetCollation 对象的映射 */
    private static final Map<String, CharsetCollation> COLLATION_TO_CHARSETCOLLATION = new HashMap<>();

    /**
     * 加载配置文件中 mycat中 charset-config 元素的配置的  collationIndex --> charsetName
     * @param map
     */
    public static void load(Map<String, Object> map){
        try {
        	// 加载配置文件中的 指定的  collationIndex --> charsetName
            for (String index : map.keySet()){	
            	int collationIndex = Integer.parseInt(index);
            	String charsetName = INDEX_TO_CHARSET.get(collationIndex);
                INDEX_TO_CHARSET.put(collationIndex, charsetName);
                CHARSET_TO_INDEX.put(charsetName, collationIndex);
            }
            
            logger.debug("load from mycat.xml: " + JSON.toJSONString(INDEX_TO_CHARSET));
            
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }
    
    /**
     * 根据 dataHosts 去mysqld读取 charset 和 collation 的映射关系：
     * mysql> SELECT ID,CHARACTER_SET_NAME,COLLATION_NAME,IS_DEFAULT FROM INFORMATION_SCHEMA.COLLATIONS;
	 * +-----+--------------------+--------------------------+------------+
	 * | ID  | CHARACTER_SET_NAME | COLLATION_NAME           | IS_DEFAULT |
	 * +-----+--------------------+--------------------------+------------+
	 * |   1 | big5               | big5_chinese_ci          | Yes        |
	 * |  84 | big5               | big5_bin                 |            |
	 * |   3 | dec8               | dec8_swedish_ci          | Yes        |
	 * |  69 | dec8               | dec8_bin                 |            |
     * 
     * @param dataHosts mycat.xml 配置文件中读取处理的  dataHost 元素的 map
     */
    public static void initCharsetAndCollation(Map<String, PhysicalDBPool> dataHosts){
    	if(dataHosts == null){
    		logger.error("param dataHosts is null");
    		return;
    	}
    	if(COLLATION_TO_CHARSETCOLLATION.size() > 0)	// 已经初始化
    		return;
    	
    	// 遍历 配置文件 mycat.xml 中的 dataHost 元素，直到可以成功连上mysqld，
    	// 并且获取 charset 和 collation 信息
    	for(String key : dataHosts.keySet()){
    		PhysicalDBPool pool = dataHosts.get(key);
    		if(pool != null && pool.getSource() != null){
    			PhysicalDatasource ds = pool.getSource();
    			if(ds != null && ds.getConfig() != null 
    					&& "mysql".equalsIgnoreCase(ds.getConfig().getDbType())){
    				DBHostConfig config = ds.getConfig();
    				while(!getCharsetCollationFromMysql(config)){
    					getCharsetCollationFromMysql(config);
    				}
    				return;	// 结束外层 for 循环
    			}
    		}
    	}
    	
    }
    
    public static final String getCharset(int index) {
        return INDEX_TO_CHARSET.get(index);
    }

    /**
     * 因为 每一个 charset 对应多个 collationIndex, 所以这里返回的是默认的那个 collationIndex；
     * 如果想获得确定的值 index，而非默认的index, 那么需要使用 getIndexByCollationName
     * 或者 getIndexByCharsetNameAndCollationName
     * @param charset
     * @return
     */
    public static final int getIndex(String charset) {
        if (StringUtils.isBlank(charset)) {
            return 0;
        } else {
        	if("Cp1252".equalsIgnoreCase(charset))
        		charset = "latin1";	// 参见：http://www.cp1252.com/ The windows 1252 codepage, also called Latin 1
            Integer i = CHARSET_TO_INDEX.get(charset.toLowerCase());
            return (i == null) ? 0 : i;
        }
    }
    
    /**
     * 根据 collationName 和 charset 返回 collationIndex
     * @param charset
     * @param collationName
     * @return
     */
    public static final int getIndexByCharsetNameAndCollationName(String charset, String collationName) {
    	if (StringUtils.isBlank(collationName)) {
            return 0;
        } else {
        	CharsetCollation cc = COLLATION_TO_CHARSETCOLLATION.get(collationName.toLowerCase());
        	if(cc != null && charset != null && charset.equalsIgnoreCase(cc.getCharsetName()))
        		return cc.getCollationIndex();
        	else
        		return  0;
        }
    }
    
    /**
     * 根据 collationName 返回 collationIndex, 二者是一一对应的关系
     * @param collationName
     * @return
     */
    public static final int getIndexByCollationName(String collationName) {
    	if (StringUtils.isBlank(collationName)) {
            return 0;
        } else {
        	CharsetCollation cc = COLLATION_TO_CHARSETCOLLATION.get(collationName.toLowerCase());
        	if(cc != null)
        		return cc.getCollationIndex();
        	else
        		return  0;
        }
    }
    
    private static boolean getCharsetCollationFromMysql(DBHostConfig config){
    	String sql = "SELECT ID,CHARACTER_SET_NAME,COLLATION_NAME,IS_DEFAULT FROM INFORMATION_SCHEMA.COLLATIONS";
    	try(Connection conn = getConnection(config)){
    		if(conn == null) return false;
    		
    		try(Statement statement = conn.createStatement()){
				ResultSet rs = statement.executeQuery(sql);
				while(rs != null && rs.next()){
					int collationIndex = new Long(rs.getLong(1)).intValue();
					String charsetName = rs.getString(2);
					String collationName = rs.getString(3);
					boolean isDefaultCollation = (rs.getString(4) != null 
							&& "Yes".equalsIgnoreCase(rs.getString(4))) ? true : false;
					System.out.println(charsetName + ", " + collationName + ", " + collationIndex + ", " + isDefaultCollation);
					
					INDEX_TO_CHARSET.put(collationIndex, charsetName);
					if(isDefaultCollation){	// 每一个 charsetName 对应多个collationIndex，此处选择默认的collationIndex
						CHARSET_TO_INDEX.put(charsetName, collationIndex);
					}
					
					CharsetCollation cc =  new CharsetCollation(charsetName, collationIndex, collationName, isDefaultCollation);
					COLLATION_TO_CHARSETCOLLATION.put(collationName, cc);
				}
				if(INDEX_TO_CHARSET.size() > 0)
					return true;
				return false;
			} catch (SQLException e) {
				logger.error(e.getMessage());
			}
    		
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}
    	
    	return false;
    }
    
    
    /**
     * 利用参数 DBHostConfig cfg 获得物理数据库的连接(java.sql.Connection)
     * @param cfg
     * @return
     * @throws SQLException
     */
    public static Connection getConnection(DBHostConfig cfg) throws SQLException {
    	try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
    	
    	logger.debug(JSON.toJSONString(cfg));
    	
    	String url = new StringBuffer("jdbc:mysql://").append(cfg.getUrl()).append("/mysql").toString();
		Connection connection = DriverManager.getConnection(url, cfg.getUser(), cfg.getPassword());
		
		return connection;
	}
    
}

/**
 * 该类用来表示 mysqld 数据库中 字符集、字符集支持的collation、字符集的collation的index、 字符集的默认collation 的对应关系：
 * 一个字符集一般对应(支持)多个collation，其中一个是默认的 collation，每一个 collation对应一个唯一的index,
 * collationName 和 collationIndex 一一对应，   每一个collationIndex对应到一个字符集，不同的collationIndex可以对应到相同的字符集，
 * 所以字符集 到 collationIndex 的对应不是唯一的，一个字符集对应多个 index(有一个默认的 collation的index)，
 * 而 collationIndex 到 字符集 的对应是确定的，唯一的；
 * mysqld 用 collation 的 index 来描述排序规则。
 * @author Administrator
 *
 */
class CharsetCollation {
	// mysqld支持的字符编码名称，注意这里不是java中的unicode编码的名字，
	// 二者之间的区别和联系可以参考驱动jar包中的com.mysql.jdbc.CharsetMapping源码
	private String charsetName;			
	private int collationIndex;		// collation的索引顺序
	private String collationName;	// collation 名称
	private boolean isDefaultCollation = false;	// 该collation是否是字符集的默认collation
	
	public CharsetCollation(String charsetName, int collationIndex, 
					String collationName, boolean isDefaultCollation){
		this.charsetName = charsetName;
		this.collationIndex = collationIndex;
		this.collationName = collationName;
		this.isDefaultCollation = isDefaultCollation;
	}
	
	public String getCharsetName() {
		return charsetName;
	}
	public void setCharsetName(String charsetName) {
		this.charsetName = charsetName;
	}
	public int getCollationIndex() {
		return collationIndex;
	}
	public void setCollationIndex(int collationIndex) {
		this.collationIndex = collationIndex;
	}
	public String getCollationName() {
		return collationName;
	}
	public void setCollationName(String collationName) {
		this.collationName = collationName;
	}
	public boolean isDefaultCollation() {
		return isDefaultCollation;
	}
	public void setDefaultCollation(boolean isDefaultCollation) {
		this.isDefaultCollation = isDefaultCollation;
	}
}

