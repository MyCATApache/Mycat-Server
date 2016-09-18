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

import io.mycat.MycatServer;
import io.mycat.backend.PhysicalDBPool;
import io.mycat.backend.PhysicalDatasource;
import io.mycat.server.config.node.DBHostConfig;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.google.common.util.concurrent.ListenableFuture;

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
    
    /** collationIndex 和 charsetName 的映射  */
    private static final Map<Integer,String> INDEX_TO_CHARSET = new HashMap<>();
    
    /** charsetName 到 默认collationIndex 的映射  */
    private static final Map<String, Integer> CHARSET_TO_INDEX = new HashMap<>();
    
    /** collationName 到 CharsetCollation 对象的映射  */
    private static final Map<String, CharsetCollation> COLLATION_TO_CHARSETCOLLATION = new HashMap<>();

    /**
     * 异步 初始化 charset 和 collation(根据 mycat.xml文件中的 dataHosts 去mysqld读取 charset 和 collation 的映射关系)
     * 使用异步时，应该改用 ConcurrentHashMap 
     * @param charsetConfigMap mycat.xml文件中 charset-config 元素指定的 collationIndex --> charsetName
     */
    public static void asynLoad(Map<String, PhysicalDBPool> dataHosts, Map<String, Object> charsetConfigMap){
    	MycatServer.getInstance().getListeningExecutorService().execute(new Runnable() {
			public void run() {
				CharsetUtil.load(dataHosts, charsetConfigMap);
			}
		});
    }
    
    /**
     * 同步 初始化 charset 和 collation(根据 mycat.xml文件中的 dataHosts 去mysqld读取 charset 和 collation 的映射关系)
     * @param charsetConfigMap mycat.xml文件中 charset-config 元素指定的 collationIndex 和 charsetName 映射
     */
    public static void load(Map<String, PhysicalDBPool> dataHosts, Map<String, Object> charsetConfigMap){
        try {
        	if(dataHosts != null && dataHosts.size() > 0)
        		CharsetUtil.initCharsetAndCollation(dataHosts);	// 去mysqld读取 charset 和 collation 的映射关系
        	else
        		logger.debug("param dataHosts is null");
        	
        	// 加载配置文件中的 指定的  collationIndex --> charsetName
            for (String index : charsetConfigMap.keySet()){	
            	int collationIndex = Integer.parseInt(index);
            	String charsetName = INDEX_TO_CHARSET.get(collationIndex);
            	if(StringUtils.isNotBlank(charsetName)){
            		INDEX_TO_CHARSET.put(collationIndex, charsetName);
                    CHARSET_TO_INDEX.put(charsetName, collationIndex);
            	}
            	logger.debug("load charset and collation from mycat.xml.");
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }
    
    /**
     * <pre>
     * 根据 dataHosts 去mysqld读取 charset 和 collation 的映射关系：
     * mysql> SELECT ID,CHARACTER_SET_NAME,COLLATION_NAME,IS_DEFAULT FROM INFORMATION_SCHEMA.COLLATIONS;
	 * +-----+--------------------+--------------------------+------------+
	 * | ID  | CHARACTER_SET_NAME | COLLATION_NAME           | IS_DEFAULT |
	 * +-----+--------------------+--------------------------+------------+
	 * |   1 | big5               | big5_chinese_ci          | Yes        |
	 * |  84 | big5               | big5_bin                 |            |
	 * |   3 | dec8               | dec8_swedish_ci          | Yes        |
	 * |  69 | dec8               | dec8_bin                 |            |
	 *</pre>
     */
    private static void initCharsetAndCollation(Map<String, PhysicalDBPool> dataHosts){
    	if(COLLATION_TO_CHARSETCOLLATION.size() > 0){	// 已经初始化
    		logger.debug(" charset and collation has already init ...");
    		return;
    	}
    		
    	// 先利用mycat.xml配置文件 中的 heartbeat(该配置一般是存在的)的连接信息来获得CharsetCollation，避免后面的遍历；
    	// 如果没有成功，则遍历mycat.xml配置文件 中的所有dataHost元素，来获得CharsetCollation;
    	DBHostConfig dBHostconfig = getConfigByDataHostName(dataHosts, "jdbchost");
    	if(dBHostconfig != null){
    		if(getCharsetCollationFromMysql(dBHostconfig)){
				logger.debug(" init charset and collation success...");
				return;
    		}
    	}
    	
    	// 遍历 配置文件 mycat.xml 中的 dataHost 元素，直到可以成功连上mysqld，并且获取 charset 和 collation 信息
    	for(String key : dataHosts.keySet()){
    		PhysicalDBPool pool = dataHosts.get(key);
    		if(pool != null && pool.getSource() != null){
    			PhysicalDatasource ds = pool.getSource();
    			if(ds != null && ds.getConfig() != null 
    					&& "mysql".equalsIgnoreCase(ds.getConfig().getDbType())){
    				DBHostConfig config = ds.getConfig();
    				if(getCharsetCollationFromMysql(config)){
    					logger.debug(" init charset and collation success...");
        				return;	// 结束外层 for 循环
    				}
    			}
    		}
    	}
    	logger.error(" init charset and collation from mysqld failed, please check datahost in mycat.xml."+
    				SystemUtils.LINE_SEPARATOR + 
    			" if your backend database is not mysqld, please ignore this message.");
    	
    	// 使用Mycat-server的环境中，其配置文件mycat.xml一台mysqld也没有配置，也就是后台数据库都是sqlserver或者oracle等
    	// 所以无法从mysqld中读取字符映射信息，所以只有在此种情况下使用配置文件代替，
    	// 注意配置文件因为存在时效性，可能存在兼容问题，这也是为什么从mysqld中读取，而不使用配置文件的原因；
    	getCharsetInfoFromFile();
    	logger.info(" backend database is not mysqld, read charset info from file.");
    }
    
    public static DBHostConfig getConfigByDataHostName(Map<String, PhysicalDBPool> dataHosts, String hostName){
    	PhysicalDBPool pool = dataHosts.get(hostName);
		if(pool != null && pool.getSource() != null){
			PhysicalDatasource ds = pool.getSource();
			return ds.getConfig();
		}
		return null;
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
        	Integer i = CHARSET_TO_INDEX.get(charset.toLowerCase());
        	if(i == null && "Cp1252".equalsIgnoreCase(charset) )
        		charset = "latin1";	// 参见：http://www.cp1252.com/ The windows 1252 codepage, also called Latin 1
        	
            i = CHARSET_TO_INDEX.get(charset.toLowerCase());
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
					
					INDEX_TO_CHARSET.put(collationIndex, charsetName);
					if(isDefaultCollation){	// 每一个 charsetName 对应多个collationIndex，此处选择默认的collationIndex
						CHARSET_TO_INDEX.put(charsetName, collationIndex);
					}
					
					CharsetCollation cc =  new CharsetCollation(charsetName, collationIndex, collationName, isDefaultCollation);
					COLLATION_TO_CHARSETCOLLATION.put(collationName, cc);
				}
				if(COLLATION_TO_CHARSETCOLLATION.size() > 0)	
					return true;
				return false;
			} catch (SQLException e) {
				logger.warn(e.getMessage());
			}
    		
		} catch (SQLException e) {
			logger.warn(e.getMessage());
		}
    	return false;
    }
    
    
    /**
     * 利用参数 DBHostConfig cfg 获得物理数据库的连接(java.sql.Connection)
     * 在mysqld刚启动马上启动mycat-server，该函数执行很慢。
     * 但是又不能又不能使用mysql协议来获得所要的数据，因为mysql协议中mysqld在第一次发来的handshake
     * 就指定了 "serverCharsetIndex":46，在登录之前，我们无法修改connection的字符，必须使用 serverCharsetIndex
     * 指定的字符编码完成 handshake 和登录：
     *  {"packetId":0,"packetLength":78,"protocolVersion":10,"restOfScrambleBuff":"OihYY2tvakVadV5Y",
	 *	 "seed":"YiJ+eWVsb2c=","serverCapabilities":63487,
	 *   "serverCharsetIndex":46,"serverStatus":2,"serverVersion":"NS42LjI3LWxvZw==","threadId":65}
     * 所以我们无法使用 mysql协议从mysqld获得字符信息，而JDBC协议中可以在url中指定字符集。所以只能用JDBC来获得字符信息。
     * @param cfg
     * @return
     * @throws SQLException
     */
    public static Connection getConnection(DBHostConfig cfg){
    	if(cfg == null) return null;
    	
    	String url = new StringBuffer("jdbc:mysql://").append(cfg.getUrl())
    		.append("/mysql").append("?characterEncoding=UTF-8").toString();
		Connection connection = null;
		long  millisecondsEnd2 = System.currentTimeMillis();
		try {
			Class.forName("com.mysql.jdbc.Driver");
			connection = DriverManager.getConnection(url, cfg.getUser(), cfg.getPassword());
		} catch (ClassNotFoundException | SQLException e) {
			if(e instanceof ClassNotFoundException)
				logger.error(e.getMessage());
			else
				logger.warn(e.getMessage() + " " + JSON.toJSONString(cfg));
		}
    	long  millisecondsEnd = System.currentTimeMillis();
        logger.debug(" function getConnection cost milliseconds: " + (millisecondsEnd - millisecondsEnd2));
		return connection;
	}
    
    /**
     * 从配置文件 index_to_charset.properties 读取 collationIndex 到 charsetName的映射关系，
     * 文件中的内容来源于：SELECT ID,CHARACTER_SET_NAME FROM INFORMATION_SCHEMA.COLLATIONS order by id;
     * 
     * 从配置文件charset_to_default_index.properties中读取charsetName到默认的collationIndex的映射关系，
     * 文件内容来源于：SELECT CHARACTER_SET_NAME,ID FROM INFORMATION_SCHEMA.COLLATIONS where Default='Yes';
     * 
     * 如果存在兼容性问题，请按照上面给出的方式更新那两个文件即可。
     * 
     * 只有在所有数据库都是 非 mysql数据库时，才需要使用到该函数。
     */
    public static void getCharsetInfoFromFile(){
    	Properties pros = new Properties();
    	try {
    		pros.load(CharsetUtil.class.getClassLoader().getResourceAsStream("index_to_charset.properties"));
    		Iterator<Entry<Object, Object>> it = pros.entrySet().iterator();  
            while (it.hasNext()) {  
                Entry<Object, Object> entry = it.next();  
                Object key = entry.getKey();
                Object value = entry.getValue();  
                INDEX_TO_CHARSET.put(Integer.parseInt(key.toString()), value.toString());
            }  
//            System.out.println(JSON.toJSONString(INDEX_TO_CHARSET));
            
            pros.clear();
            pros.load(CharsetUtil.class.getClassLoader().getResourceAsStream("charset_to_default_index.properties"));
            it = pros.entrySet().iterator();  
            while (it.hasNext()) {  
                Entry<Object, Object> entry = it.next();  
                Object key = entry.getKey();
                Object value = entry.getValue();  
                CHARSET_TO_INDEX.put(key.toString(), Integer.parseInt(value.toString()));
            }  
//            System.out.println(JSON.toJSONString(CHARSET_TO_INDEX));
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
    
    public static void main(String[] args){
    	getCharsetInfoFromFile();
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

