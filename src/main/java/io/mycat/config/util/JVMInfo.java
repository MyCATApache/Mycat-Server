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

import java.lang.reflect.Field;
import java.text.AttributedString;
import java.util.HashMap;
import java.util.Map;

/**
 * @author mycat
 */
public class JVMInfo {
    private static final float DEFAULT_JAVA_VERSION = 1.3f;
    private static final boolean reverseFieldOrder;
    private static final float majorJavaVersion = getMajorJavaVersion(System.getProperty("java.specification.version"));

    private ReflectionProvider reflectionProvider;
    private Map<String, Class<?>> loaderCache = new HashMap<String, Class<?>>();

    static {
        boolean reverse = false;
        final Field[] fields = AttributedString.class.getDeclaredFields();
        for (int i = 0; i < fields.length; i++) {
            if (fields[i].getName().equals("text")) {
                reverse = i > 3;
            }
        }
        reverseFieldOrder = reverse;
    }

    /**
     * Parses the java version system property to determine the major java
     * version, ie 1.x
     * 
     * @param javaVersion
     *            the system property 'java.specification.version'
     * @return A float of the form 1.x
     */
    public static final float getMajorJavaVersion(String javaVersion) {
        try {
            return Float.parseFloat(javaVersion.substring(0, 3));
        } catch (NumberFormatException e) {
            // Some JVMs may not conform to the x.y.z java.version format
            return DEFAULT_JAVA_VERSION;
        }
    }

    public static boolean is14() {
        return majorJavaVersion >= 1.4f;
    }

    public static boolean is15() {
        return majorJavaVersion >= 1.5f;
    }

    public static boolean is16() {
        return majorJavaVersion >= 1.6f;
    }

    private static boolean isSun() {
        return System.getProperty("java.vm.vendor").indexOf("Sun") != -1;
    }

    private static boolean isApple() {
        return System.getProperty("java.vm.vendor").indexOf("Apple") != -1;
    }

    private static boolean isHPUX() {
        return System.getProperty("java.vm.vendor").indexOf("Hewlett-Packard Company") != -1;
    }

    private static boolean isIBM() {
        return System.getProperty("java.vm.vendor").indexOf("IBM") != -1;
    }

    private static boolean isBlackdown() {
        return System.getProperty("java.vm.vendor").indexOf("Blackdown") != -1;
    }

    /*
     * Support for sun.misc.Unsafe and sun.reflect.ReflectionFactory is present
     * in JRockit versions R25.1.0 and later, both 1.4.2 and 5.0 (and in future
     * 6.0 builds).
     */
    private static boolean isBEAWithUnsafeSupport() {
        // This property should be "BEA Systems, Inc."
        if (System.getProperty("java.vm.vendor").indexOf("BEA") != -1) {

            /*
             * Recent 1.4.2 and 5.0 versions of JRockit have a java.vm.version
             * string starting with the "R" JVM version number, i.e.
             * "R26.2.0-38-57237-1.5.0_06-20060209..."
             */
            String vmVersion = System.getProperty("java.vm.version");
            if (vmVersion.startsWith("R")) {
                /*
                 * Wecould also check that it's R26 or later, but that is
                 * implicitly true
                 */
                return true;
            }

            /*
             * For older JRockit versions we can check java.vm.info. JRockit
             * 1.4.2 R24 -> "Native Threads, GC strategy: parallel" and JRockit
             * 5.0 R25 -> "R25.2.0-28".
             */
            String vmInfo = System.getProperty("java.vm.info");
            if (vmInfo != null) {
                // R25.1 or R25.2 supports Unsafe, other versions do not
                return (vmInfo.startsWith("R25.1") || vmInfo.startsWith("R25.2"));
            }
        }
        // If non-BEA, or possibly some very old JRockit version
        return false;
    }

    public Class<?> loadClass(String name) {
        try {
            Class<?> clazz = loaderCache.get(name);
            if (clazz == null) {
                clazz = Class.forName(name, false, getClass().getClassLoader());
                loaderCache.put(name, clazz);
            }
            return clazz;
        } catch (ClassNotFoundException e) {
            return null;
        }
    }

    public synchronized ReflectionProvider getReflectionProvider() {
        if (reflectionProvider == null) {
            reflectionProvider = new ReflectionProvider();
        }
        return reflectionProvider;
    }

    protected boolean canUseSun14ReflectionProvider() {
        return (isSun() || isApple() || isHPUX() || isIBM() || isBlackdown() || isBEAWithUnsafeSupport()) && is14()
                && loadClass("sun.misc.Unsafe") != null;
    }

    public static boolean reverseFieldDefinition() {
        return reverseFieldOrder;
    }

    public static void main(String[] args) {
        System.out.println(majorJavaVersion);
    }

}