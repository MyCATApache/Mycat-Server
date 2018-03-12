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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author mycat
 */
public class FieldDictionary {

    private final Map<String, Map<String, Field>> nameCache = Collections
            .synchronizedMap(new HashMap<String, Map<String, Field>>());
    private final Map<String, Map<FieldKey, Field>> keyCache = Collections
            .synchronizedMap(new HashMap<String, Map<FieldKey, Field>>());

    /**
     * Returns an iterator for all serializable fields for some class
     * 
     * @param cls
     *            the class you are interested on
     * @return an iterator for its serializable fields
     */
    public Iterator<Field> serializableFieldsFor(Class<?> cls) {
        return buildMap(cls, true).values().iterator();
    }

    /**
     * Returns an specific field of some class. If definedIn is null, it searchs
     * for the field named 'name' inside the class cls. If definedIn is
     * different than null, tries to find the specified field name in the
     * specified class cls which should be defined in class definedIn (either
     * equals cls or a one of it's superclasses)
     * 
     * @param cls
     *            the class where the field is to be searched
     * @param name
     *            the field name
     * @param definedIn
     *            the superclass (or the class itself) of cls where the field
     *            was defined
     * @return the field itself
     */
    public Field field(Class<?> cls, String name, Class<?> definedIn) {
        Map<?, Field> fields = buildMap(cls, definedIn != null);
        Field field = fields.get(definedIn != null ? new FieldKey(name, definedIn, 0) : name);
        if (field == null) {
            throw new ObjectAccessException("No such field " + cls.getName() + "." + name);
        } else {
            return field;
        }
    }

    private Map<?, Field> buildMap(Class<?> cls, boolean tupleKeyed) {
        final String clsName = cls.getName();
        if (!nameCache.containsKey(clsName)) {
            synchronized (keyCache) {
                if (!nameCache.containsKey(clsName)) { // double check
                    final Map<String, Field> keyedByFieldName = new HashMap<String, Field>();
                    final Map<FieldKey, Field> keyedByFieldKey = new OrderRetainingMap<FieldKey, Field>();
                    while (!Object.class.equals(cls)) {
                        Field[] fields = cls.getDeclaredFields();
                        if (JVMInfo.reverseFieldDefinition()) {
                            for (int i = fields.length >> 1; i-- > 0;) {
                                final int idx = fields.length - i - 1;
                                final Field field = fields[i];
                                fields[i] = fields[idx];
                                fields[idx] = field;
                            }
                        }
                        for (int i = 0; i < fields.length; i++) {
                            Field field = fields[i];
                            field.setAccessible(true);
                            if (!keyedByFieldName.containsKey(field.getName())) {
                                keyedByFieldName.put(field.getName(), field);
                            }
                            keyedByFieldKey.put(new FieldKey(field.getName(), field.getDeclaringClass(), i), field);
                        }
                        cls = cls.getSuperclass();
                    }
                    nameCache.put(clsName, keyedByFieldName);
                    keyCache.put(clsName, keyedByFieldKey);
                }
            }
        }
        return tupleKeyed ? keyCache.get(clsName) : nameCache.get(clsName);
    }

    /**
     * @author mycat
     */
    private static class FieldKey {

        private String fieldName;
        private Class<?> declaringClass;
        private Integer depth;
        private int order;

        public FieldKey(String fieldName, Class<?> declaringClass, int order) {
            this.fieldName = fieldName;
            this.declaringClass = declaringClass;
            this.order = order;
            Class<?> c = declaringClass;
            int i = 0;
            while (c.getSuperclass() != null) {
                i++;
                c = c.getSuperclass();
            }
            //不用构造器创建Integer，用静态方法节省时间和空间，因为depth是不可变变量
            depth = Integer.valueOf(i);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof FieldKey)) {
                return false;
            }

            final FieldKey fieldKey = (FieldKey) o;

            if (declaringClass != null ? !declaringClass.equals(fieldKey.declaringClass)
                    : fieldKey.declaringClass != null) {
                return false;
            }

            if (fieldName != null ? !fieldName.equals(fieldKey.fieldName) : fieldKey.fieldName != null) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result;
            result = (fieldName != null ? fieldName.hashCode() : 0);
            result = 29 * result + (declaringClass != null ? declaringClass.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "FieldKey{" + "order=" + order + ", writer=" + depth + ", declaringClass=" + declaringClass
                    + ", fieldName='" + fieldName + "'" + "}";
        }

    }

}