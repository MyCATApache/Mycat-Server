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
package io.mycat.util;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author mycat
 */
public final class ObjectUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(ObjectUtil.class);


    public static Object getStaticFieldValue(String className,String fieldName)
    {
        Class clazz = null;
        try
        {
            clazz = Class.forName(className);
           Field field = clazz.getField(fieldName);
             if(field!=null) {
                 return field.get(null);
             }
        } catch (ClassNotFoundException e)
        {
            //LOGGER.error("getStaticFieldValue", e);
        } catch (NoSuchFieldException e)
        {
           // LOGGER.error("getStaticFieldValue", e);
        } catch (IllegalAccessException e)
        {
          //  LOGGER.error("getStaticFieldValue", e);
        }
        return null;
    }

    
	public static Object copyObject(Object object) {
		ByteArrayOutputStream b = new ByteArrayOutputStream();
		ObjectOutputStream s = null;
		try {
			s = new ObjectOutputStream(b);
			s.writeObject(object);
			ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(b.toByteArray()));
			return ois.readObject();
		} catch (IOException e) {
            throw new RuntimeException(e);
		} catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
		}

	}
	
    /**
     * 递归地比较两个数组是否相同，支持多维数组。
     * <p>
     * 如果比较的对象不是数组，则此方法的结果同<code>ObjectUtil.equals</code>。
     * </p>
     * 
     * @param array1
     *            数组1
     * @param array2
     *            数组2
     * @return 如果相等, 则返回<code>true</code>
     */
    public static boolean equals(Object array1, Object array2) {
        if (array1 == array2) {
            return true;
        }

        if ((array1 == null) || (array2 == null)) {
            return false;
        }

        Class<? extends Object> clazz = array1.getClass();

        if (!clazz.equals(array2.getClass())) {
            return false;
        }

        if (!clazz.isArray()) {
            return array1.equals(array2);
        }

        // array1和array2为同类型的数组
        if (array1 instanceof long[]) {
            long[] longArray1 = (long[]) array1;
            long[] longArray2 = (long[]) array2;

            if (longArray1.length != longArray2.length) {
                return false;
            }

            for (int i = 0; i < longArray1.length; i++) {
                if (longArray1[i] != longArray2[i]) {
                    return false;
                }
            }

            return true;
        } else if (array1 instanceof int[]) {
            int[] intArray1 = (int[]) array1;
            int[] intArray2 = (int[]) array2;

            if (intArray1.length != intArray2.length) {
                return false;
            }

            for (int i = 0; i < intArray1.length; i++) {
                if (intArray1[i] != intArray2[i]) {
                    return false;
                }
            }

            return true;
        } else if (array1 instanceof short[]) {
            short[] shortArray1 = (short[]) array1;
            short[] shortArray2 = (short[]) array2;

            if (shortArray1.length != shortArray2.length) {
                return false;
            }

            for (int i = 0; i < shortArray1.length; i++) {
                if (shortArray1[i] != shortArray2[i]) {
                    return false;
                }
            }

            return true;
        } else if (array1 instanceof byte[]) {
            byte[] byteArray1 = (byte[]) array1;
            byte[] byteArray2 = (byte[]) array2;

            if (byteArray1.length != byteArray2.length) {
                return false;
            }

            for (int i = 0; i < byteArray1.length; i++) {
                if (byteArray1[i] != byteArray2[i]) {
                    return false;
                }
            }

            return true;
        } else if (array1 instanceof double[]) {
            double[] doubleArray1 = (double[]) array1;
            double[] doubleArray2 = (double[]) array2;

            if (doubleArray1.length != doubleArray2.length) {
                return false;
            }

            for (int i = 0; i < doubleArray1.length; i++) {
                if (Double.doubleToLongBits(doubleArray1[i]) != Double.doubleToLongBits(doubleArray2[i])) {
                    return false;
                }
            }

            return true;
        } else if (array1 instanceof float[]) {
            float[] floatArray1 = (float[]) array1;
            float[] floatArray2 = (float[]) array2;

            if (floatArray1.length != floatArray2.length) {
                return false;
            }

            for (int i = 0; i < floatArray1.length; i++) {
                if (Float.floatToIntBits(floatArray1[i]) != Float.floatToIntBits(floatArray2[i])) {
                    return false;
                }
            }

            return true;
        } else if (array1 instanceof boolean[]) {
            boolean[] booleanArray1 = (boolean[]) array1;
            boolean[] booleanArray2 = (boolean[]) array2;

            if (booleanArray1.length != booleanArray2.length) {
                return false;
            }

            for (int i = 0; i < booleanArray1.length; i++) {
                if (booleanArray1[i] != booleanArray2[i]) {
                    return false;
                }
            }

            return true;
        } else if (array1 instanceof char[]) {
            char[] charArray1 = (char[]) array1;
            char[] charArray2 = (char[]) array2;

            if (charArray1.length != charArray2.length) {
                return false;
            }

            for (int i = 0; i < charArray1.length; i++) {
                if (charArray1[i] != charArray2[i]) {
                    return false;
                }
            }

            return true;
        } else {
            Object[] objectArray1 = (Object[]) array1;
            Object[] objectArray2 = (Object[]) array2;

            if (objectArray1.length != objectArray2.length) {
                return false;
            }

            for (int i = 0; i < objectArray1.length; i++) {
                if (!equals(objectArray1[i], objectArray2[i])) {
                    return false;
                }
            }

            return true;
        }
    }


    public static void copyProperties(Object fromObj, Object toObj) {
        Class<? extends Object> fromClass = fromObj.getClass();
        Class<? extends Object> toClass = toObj.getClass();

        try {
            BeanInfo fromBean = Introspector.getBeanInfo(fromClass);
            BeanInfo toBean = Introspector.getBeanInfo(toClass);

            PropertyDescriptor[] toPd = toBean.getPropertyDescriptors();
            List<PropertyDescriptor> fromPd = Arrays.asList(fromBean
                    .getPropertyDescriptors());

            for (PropertyDescriptor propertyDescriptor : toPd) {
                propertyDescriptor.getDisplayName();
                PropertyDescriptor pd = fromPd.get(fromPd
                        .indexOf(propertyDescriptor));
                if (pd.getDisplayName().equals(
                        propertyDescriptor.getDisplayName())
                        && !pd.getDisplayName().equals("class")
                        && propertyDescriptor.getWriteMethod() != null) {
                        propertyDescriptor.getWriteMethod().invoke(toObj, pd.getReadMethod().invoke(fromObj, null));
                }

            }
        } catch (IntrospectionException e) {
          throw  new RuntimeException(e);
        } catch (IllegalArgumentException e) {
            throw  new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw  new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw  new RuntimeException(e);
        }
    }
}
