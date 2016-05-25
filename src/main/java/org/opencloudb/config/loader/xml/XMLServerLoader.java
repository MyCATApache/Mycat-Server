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
package org.opencloudb.config.loader.xml;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.config.Versions;
import org.opencloudb.config.model.ClusterConfig;
import org.opencloudb.config.model.QuarantineConfig;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.config.model.UserConfig;
import org.opencloudb.config.util.ConfigException;
import org.opencloudb.config.util.ConfigUtil;
import org.opencloudb.config.util.ParameterMapping;
import org.opencloudb.util.DecryptUtil;
import org.opencloudb.util.SplitUtil;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.alibaba.druid.wall.WallConfig;

/**
 * @author mycat
 */
@SuppressWarnings("unchecked")
public class XMLServerLoader {
    private final SystemConfig system;
    private final Map<String, UserConfig> users;
    private final QuarantineConfig quarantine;
    private ClusterConfig cluster;

    public XMLServerLoader() {
        this.system = new SystemConfig();
        this.users = new HashMap<String, UserConfig>();
        this.quarantine = new QuarantineConfig();
        this.load();
    }

    public SystemConfig getSystem() {
        return system;
    }

    public Map<String, UserConfig> getUsers() {
        return (Map<String, UserConfig>) (users.isEmpty() ? Collections.emptyMap() : Collections.unmodifiableMap(users));
    }

    public QuarantineConfig getQuarantine() {
        return quarantine;
    }

    public ClusterConfig getCluster() {
        return cluster;
    }

    private void load() {
        InputStream dtd = null;
        InputStream xml = null;
        try {
            dtd = XMLServerLoader.class.getResourceAsStream("/server.dtd");
            xml = XMLServerLoader.class.getResourceAsStream("/server.xml");
            Element root = ConfigUtil.getDocument(dtd, xml).getDocumentElement();
            loadSystem(root);
            loadUsers(root);
            this.cluster = new ClusterConfig(root, system.getServerPort());
            loadQuarantine(root);
        } catch (ConfigException e) {
            throw e;
        } catch (Exception e) {
            throw new ConfigException(e);
        } finally {
            if (dtd != null) {
                try {
                    dtd.close();
                } catch (IOException e) {
                }
            }
            if (xml != null) {
                try {
                    xml.close();
                } catch (IOException e) {
                }
            }
        }
    }

    private void loadQuarantine(Element root) throws IllegalAccessException, InvocationTargetException {
        NodeList list = root.getElementsByTagName("host");
        Map<String, List<UserConfig>> whitehost = new HashMap<String, List<UserConfig>>();

        for (int i = 0, n = list.getLength(); i < n; i++) {
            Node node = list.item(i);
            if (node instanceof Element) {
                Element e = (Element) node;
                String host = e.getAttribute("host").trim();
                String userStr = e.getAttribute("user").trim();
                if (this.quarantine.existsHost(host)) {
                    throw new ConfigException("host duplicated : " + host);
                }
                String []users = userStr.split(",");
                List<UserConfig> userConfigs = new ArrayList<UserConfig>();
                for(String user : users){
                	UserConfig uc = this.users.get(user);
                    if (null == uc) {
                        throw new ConfigException("[user: " + user + "] doesn't exist in [host: " + host + "]");
                    }
                    if (uc.getSchemas() == null || uc.getSchemas().size() == 0) {
                        throw new ConfigException("[host: " + host + "] contains one root privileges user: " + user);
                    }
                    userConfigs.add(uc);
                }
                whitehost.put(host, userConfigs);
            }
        }
        quarantine.setWhitehost(whitehost);
        WallConfig wallConfig = new WallConfig();
        NodeList blacklist = root.getElementsByTagName("blacklist");
        for (int i = 0, n = blacklist.getLength(); i < n; i++) {
            Node node = blacklist.item(i);
            if (node instanceof Element) {
            	Element e = (Element) node;
             	String check = e.getAttribute("check");
             	if (null != check) {
             		quarantine.setCheck(Boolean.valueOf(check));
				}

                Map<String, Object> props = ConfigUtil.loadElements((Element) node);
                ParameterMapping.mapping(wallConfig, props);
            }
        }
        quarantine.setWallConfig(wallConfig);
        quarantine.init();
        
    }

    private void loadUsers(Element root) {
        NodeList list = root.getElementsByTagName("user");
        for (int i = 0, n = list.getLength(); i < n; i++) {
            Node node = list.item(i);
            if (node instanceof Element) {
                Element e = (Element) node;
                String name = e.getAttribute("name");
                UserConfig user = new UserConfig();
                Map<String, Object> props = ConfigUtil.loadElements(e);
                String password = (String)props.get("password");
                String usingDecrypt = (String)props.get("usingDecrypt");
                String passwordDecrypt = DecryptUtil.mycatDecrypt(usingDecrypt,name,password);
                user.setName(name);
                user.setPassword(passwordDecrypt);
                user.setEncryptPassword(password);
				
				String benchmark = (String) props.get("benchmark");
				if(null != benchmark) {
					user.setBenchmark( Integer.parseInt(benchmark) );
				}
				
				String benchmarkSmsTel = (String) props.get("benchmarkSmsTel");
				if(null != benchmarkSmsTel) {
					user.setBenchmarkSmsTel( benchmarkSmsTel );
				}
				
				String readOnly = (String) props.get("readOnly");
				if (null != readOnly) {
					user.setReadOnly(Boolean.valueOf(readOnly));
				}
				
				String schemas = (String) props.get("schemas");
                if (schemas != null) {
                    String[] strArray = SplitUtil.split(schemas, ',', true);
                    user.setSchemas(new HashSet<String>(Arrays.asList(strArray)));
                }
                if (users.containsKey(name)) {
                    throw new ConfigException("user " + name + " duplicated!");
                }
                users.put(name, user);
            }
        }
    }

    private void loadSystem(Element root) throws IllegalAccessException, InvocationTargetException {
        NodeList list = root.getElementsByTagName("system");
        for (int i = 0, n = list.getLength(); i < n; i++) {
            Node node = list.item(i);
            if (node instanceof Element) {
                Map<String, Object> props = ConfigUtil.loadElements((Element) node);
                ParameterMapping.mapping(system, props);
            }
        }

        if (system.getFakeMySQLVersion() != null) {
            boolean validVersion = false;
            String majorMySQLVersion = system.getFakeMySQLVersion();
            /*
             * 注意！！！ 目前MySQL官方主版本号仍然是5.x, 以后万一前面的大版本号变成2位数字，
             * 比如 10.x...,下面获取主版本的代码要做修改
             */
            majorMySQLVersion = majorMySQLVersion.substring(0, majorMySQLVersion.indexOf(".", 2));
            for (String ver : SystemConfig.MySQLVersions) {
                // 这里只是比较mysql前面的大版本号
                if (majorMySQLVersion.equals(ver)) {
                    validVersion = true;
                }
            }

            if (validVersion) {
                Versions.setServerVersion(system.getFakeMySQLVersion());
            } else {
                throw new ConfigException("The specified MySQL Version (" + system.getFakeMySQLVersion()
                        + ") is not valid.");
            }
        }
    }

}
