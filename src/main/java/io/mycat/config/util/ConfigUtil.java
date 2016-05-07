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
package io.mycat.config.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.EntityResolver;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import io.mycat.util.StringUtil;

/**
 * @author mycat
 */
public class ConfigUtil {

    public static String filter(String text) {
        return filter(text, System.getProperties());
    }

    public static String filter(String text, Properties properties) {
        StringBuilder s = new StringBuilder();
        int cur = 0;
        int textLen = text.length();
        int propStart = -1;
        int propStop = -1;
        String propName = null;
        String propValue = null;
        for (; cur < textLen; cur = propStop + 1) {
            propStart = text.indexOf("${", cur);
            if (propStart < 0) {
                break;
            }
            s.append(text.substring(cur, propStart));
            propStop = text.indexOf("}", propStart);
            if (propStop < 0) {
                throw new ConfigException("Unterminated property: " + text.substring(propStart));
            }
            propName = text.substring(propStart + 2, propStop);
            propValue = properties.getProperty(propName);
            if (propValue == null) {
                s.append("${").append(propName).append('}');
            } else {
                s.append(propValue);
            }
        }
        return s.append(text.substring(cur)).toString();
    }

    public static Document getDocument(final InputStream dtd, InputStream xml) throws ParserConfigurationException,
            SAXException, IOException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setValidating(true);
        factory.setNamespaceAware(false);
        DocumentBuilder builder = factory.newDocumentBuilder();
        builder.setEntityResolver(new EntityResolver() {
            @Override
            public InputSource resolveEntity(String publicId, String systemId) {
                return new InputSource(dtd);
            }
        });
        builder.setErrorHandler(new ErrorHandler() {
            @Override
            public void warning(SAXParseException e) {
            }

            @Override
            public void error(SAXParseException e) throws SAXException {
                throw e;
            }

            @Override
            public void fatalError(SAXParseException e) throws SAXException {
                throw e;
            }
        });
        return builder.parse(xml);
    }

    public static Map<String, Object> loadAttributes(Element e) {
        Map<String, Object> map = new HashMap<String, Object>();
        NamedNodeMap nm = e.getAttributes();
        for (int j = 0; j < nm.getLength(); j++) {
            Node n = nm.item(j);
            if (n instanceof Attr) {
                Attr attr = (Attr) n;
                map.put(attr.getName(), attr.getNodeValue());
            }
        }
        return map;
    }

    public static Element loadElement(Element parent, String tagName) {
        NodeList nodeList = parent.getElementsByTagName(tagName);
        if (nodeList.getLength() > 1) {
            throw new ConfigException(tagName + " elements length  over one!");
        }
        if (nodeList.getLength() == 1) {
            return (Element) nodeList.item(0);
        } else {
            return null;
        }
    }

    /**
     * 获取节点下所有property
     * @param parent
     * @return key-value property键值对
     */
    public static Map<String, Object> loadElements(Element parent) {
        Map<String, Object> map = new HashMap<String, Object>();
        NodeList children = parent.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node node = children.item(i);
            if (node instanceof Element) {
                Element e = (Element) node;
                String name = e.getNodeName();
                //获取property
                if ("property".equals(name)) {
                    String key = e.getAttribute("name");
                    NodeList nl = e.getElementsByTagName("bean");
                    if (nl.getLength() == 0) {
                        String value = e.getTextContent();
                        map.put(key, StringUtil.isEmpty(value) ? null : value.trim());
                    } else {
                        map.put(key, loadBean((Element) nl.item(0)));
                    }
                }
            }
        }
        return map;
    }

    public static BeanConfig loadBean(Element parent, String tagName) {
        NodeList nodeList = parent.getElementsByTagName(tagName);
        if (nodeList.getLength() > 1) {
            throw new ConfigException(tagName + " elements length over one!");
        }
        return loadBean((Element) nodeList.item(0));
    }

    public static BeanConfig loadBean(Element e) {
        if (e == null) {
            return null;
        }
        BeanConfig bean = new BeanConfig();
        bean.setName(e.getAttribute("name"));
        Element element = loadElement(e, "className");
        if (element != null) {
            bean.setClassName(element.getTextContent());
        } else {
            bean.setClassName(e.getAttribute("class"));
        }
        bean.setParams(loadElements(e));
        return bean;
    }

}