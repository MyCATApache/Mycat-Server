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
import java.sql.SQLSyntaxErrorException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.opencloudb.config.model.rule.RuleConfig;
import org.opencloudb.config.model.rule.TableRuleConfig;
import org.opencloudb.config.util.ConfigException;
import org.opencloudb.config.util.ConfigUtil;
import org.opencloudb.config.util.ParameterMapping;
import org.opencloudb.route.function.AbstractPartitionAlgorithm;
import org.opencloudb.util.SplitUtil;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * @author mycat
 */
@SuppressWarnings("unchecked")
public class XMLRuleLoader {
	private final static String DEFAULT_DTD = "/rule.dtd";
	private final static String DEFAULT_XML = "/rule.xml";

	private final Map<String, TableRuleConfig> tableRules;
	// private final Set<RuleConfig> rules;
	private final Map<String, AbstractPartitionAlgorithm> functions;

	public XMLRuleLoader(String ruleFile) {
		// this.rules = new HashSet<RuleConfig>();
		this.tableRules = new HashMap<String, TableRuleConfig>();
		this.functions = new HashMap<String, AbstractPartitionAlgorithm>();
		load(DEFAULT_DTD, ruleFile == null ? DEFAULT_XML : ruleFile);
	}

	public XMLRuleLoader() {
		this(null);
	}

	public Map<String, TableRuleConfig> getTableRules() {
		return (Map<String, TableRuleConfig>) (tableRules.isEmpty() ? Collections
				.emptyMap() : tableRules);
	}

	

	
	private void load(String dtdFile, String xmlFile) {
		InputStream dtd = null;
		InputStream xml = null;
		try {
			dtd = XMLRuleLoader.class.getResourceAsStream(dtdFile);
			xml = XMLRuleLoader.class.getResourceAsStream(xmlFile);
			Element root = ConfigUtil.getDocument(dtd, xml)
					.getDocumentElement();
			loadFunctions(root);
			loadTableRules(root);
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

	private void loadTableRules(Element root) throws SQLSyntaxErrorException {
		NodeList list = root.getElementsByTagName("tableRule");
		for (int i = 0, n = list.getLength(); i < n; ++i) {
			Node node = list.item(i);
			if (node instanceof Element) {
				Element e = (Element) node;
				String name = e.getAttribute("name");
				if (tableRules.containsKey(name)) {
					throw new ConfigException("table rule " + name
							+ " duplicated!");
				}
				NodeList ruleNodes = e.getElementsByTagName("rule");
				int length = ruleNodes.getLength();
				if (length > 1) {
					throw new ConfigException("only one rule can defined :"
							+ name);
				}
				RuleConfig rule = loadRule((Element) ruleNodes.item(0));
				String funName = rule.getFunctionName();
				AbstractPartitionAlgorithm func = functions.get(funName);
				if (func == null) {
					throw new ConfigException("can't find function of name :"
							+ funName);
				}
				rule.setRuleAlgorithm(func);
				tableRules.put(name, new TableRuleConfig(name, rule));
			}
		}
	}

	private RuleConfig loadRule(Element element) throws SQLSyntaxErrorException {
		Element columnsEle = ConfigUtil.loadElement(element, "columns");
		String column = columnsEle.getTextContent();
		String[] columns = SplitUtil.split(column, ',', true);
		if (columns.length > 1) {
			throw new ConfigException("table rule coulmns has multi values:"
					+ columnsEle.getTextContent());
		}
		Element algorithmEle = ConfigUtil.loadElement(element, "algorithm");
		String algorithm = algorithmEle.getTextContent();
		return new RuleConfig(column.toUpperCase(), algorithm);
	}

	private void loadFunctions(Element root) throws ClassNotFoundException,
			InstantiationException, IllegalAccessException,
			InvocationTargetException {
		NodeList list = root.getElementsByTagName("function");
		for (int i = 0, n = list.getLength(); i < n; ++i) {
			Node node = list.item(i);
			if (node instanceof Element) {
				Element e = (Element) node;
				String name = e.getAttribute("name");
				if (functions.containsKey(name)) {
					throw new ConfigException("rule function " + name
							+ " duplicated!");
				}
				String clazz = e.getAttribute("class");
				AbstractPartitionAlgorithm function = createFunction(name, clazz);
				ParameterMapping.mapping(function, ConfigUtil.loadElements(e));
				function.init();
				functions.put(name, function);
			}
		}
	}

	private AbstractPartitionAlgorithm createFunction(String name, String clazz)
			throws ClassNotFoundException, InstantiationException,
			IllegalAccessException, InvocationTargetException {
		Class<?> clz = Class.forName(clazz);
		if (!AbstractPartitionAlgorithm.class.isAssignableFrom(clz)) {
			throw new IllegalArgumentException("rule function must implements "
					+ AbstractPartitionAlgorithm.class.getName() + ", name=" + name);
		}
		return (AbstractPartitionAlgorithm) clz.newInstance();
	}

}