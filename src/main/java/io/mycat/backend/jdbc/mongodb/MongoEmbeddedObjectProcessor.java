package io.mycat.backend.jdbc.mongodb;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 处理从MongoDB中获取的内嵌对象(Embeedded Object|SubDocument)，将MongoDB对象转换为对应的Java对象。
 * <hr/>
 * 支持：
 * <ul>
 * <li>{@link ObjectId}</li>
 * <li>基本类型</li>
 * <li>枚举</li>
 * <li>内嵌对象</li>
 * <li>内嵌数组</li>
 * </ul>
 * eg.<br/>
 * public class A{<br/>
 * &nbsp; private ObjectId _id;<br/>
 * &nbsp; private String name;<br/>
 * &nbsp; private Integer age;<br/>
 * &nbsp; private B b;<br/>
 * &nbsp; private Address[] addresses;<br/>
 * &nbsp; private String[] someCode;<br/>
 * &nbsp; ...<br/>
 * }
 * <p>
 * 不支持：
 * <ul>
 * <li>第一层的内嵌集合类型</li>
 * </ul>
 * eg.<br/>
 * public class A{<br/>
 * &nbsp; private ObjectId _id;<br/>
 * &nbsp; private String name;<br/>
 * &nbsp; private Integer age;<br/>
 * &nbsp; private B b;<br/>
 * &nbsp; private List&lt;Address> addresses;<br/>
 * &nbsp; private Set&lt;String> someCode;<br/>
 * &nbsp; ...<br/>
 * }
 * <br/>
 * 第一次拿不到范型，所以addresses、someCode不支持，直接返回null。B对象里的则没问题。<br/>
 *
 * @author liuxinsi
 * @mail akalxs@gmail.com
 */
public class MongoEmbeddedObjectProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(MongoEmbeddedObjectProcessor.class);

    /**
     * 将传入的值<code>value</code>转换成对应的类型<code>type</code>返回。
     *
     * @param columnLabel 列名
     * @param value       值
     * @param type        对应的类型
     * @return 转换后的对象
     */
    public static Object valueMapper(String columnLabel, Object value, Class<?> type) {
        if (value == null) {
            return null;
        }

        // mongodb _id field
        if (type.isAssignableFrom(ObjectId.class)
                && (value instanceof ObjectId || value instanceof String)) {
            return new ObjectId(value.toString());
        }

        // enum
        if (type.isEnum()) {
            return value.toString();
        }

        // embedded collection，内嵌集合
        if ((type.isAssignableFrom(List.class) || type.isAssignableFrom(Set.class))
                && value instanceof BasicDBList) {
            // TODO 拿不到范型，list没法转
            LOG.debug("column:[{}],type:[{}]为内嵌列表,无法获取范型类,无法映射.return null.", columnLabel, type);
            return null;
        }

        // embedded object，内嵌对象
        if (value instanceof BasicDBObject) {
            BasicDBObject dbObj = (BasicDBObject) value;
            return beanMapper(dbObj, type);
        }

        // embedded array,内嵌数组
        if (type.isArray() && value instanceof BasicDBList) {
            BasicDBList basicDBList = (BasicDBList) value;
            return arrayMapper(basicDBList, type);
        }

        LOG.debug("column:[{}],type:[{}] unsupported type yet.return null", columnLabel, type);
        return null;
    }

    /**
     * 加载<code>clazzToMapper</code>下所有field。
     *
     * @param clazzToMapper class
     * @return filed map，k=field name，v=field
     */
    private static Map<String, Field> loadFields(Class<?> clazzToMapper) {
        Map<String, Field> fieldMap = new HashMap<>();
        Field[] fields = clazzToMapper.getDeclaredFields();
        for (Field field : fields) {
            field.setAccessible(true);
            fieldMap.put(field.getName(), field);
        }
        return fieldMap;
    }

    /**
     * 获取<code>field</code>字段的范型类。
     *
     * @param field field
     * @return null 如果没有获取到或异常。
     */
    private static Class<?> getParameterizedClass(Field field) {
        Type type = field.getGenericType();
        String parameterizedType;
        if (type instanceof ParameterizedType) {
            ParameterizedType pt = (ParameterizedType) type;
            if (pt.getActualTypeArguments() == null || pt.getActualTypeArguments().length == 0) {
                return null;
            }
            parameterizedType = pt.getActualTypeArguments()[0].toString();
        } else {
            return null;
        }

        Class<?> clazz;
        try {
            clazz = Class.forName(parameterizedType);
        } catch (ClassNotFoundException e) {
            LOG.warn("获取field:{}的范型异常。", field.getName(), e);
            return null;
        }
        return clazz;
    }

    /**
     * 根据字段<code>field</code>类型创建对应的集合类。<br/>
     * <b>仅支持List、Set。</b>
     *
     * @param field field
     * @param size  集合初始大小
     * @return 对应集合的实现类
     */
    private static Collection<Object> createCollection(Field field, int size) {
        Class<?> fieldType = field.getType();
        Collection<Object> collection = null;
        if (fieldType.isAssignableFrom(List.class)) {
            collection = new ArrayList<>(size);
        } else if (fieldType.isAssignableFrom(Set.class)) {
            collection = new HashSet<>(size);
        }
        return collection;
    }

    /**
     * 将mongodb的数据对象<code>dbObj</code>转换成对应类型<code>clazzToMapper</code>的对象。<br/>
     * key=fieldName。
     *
     * @param dbObj         mongodb数据对象
     * @param clazzToMapper 目标对象类
     * @return 转换后的对象
     */
    private static Object beanMapper(BasicDBObject dbObj, Class<?> clazzToMapper) {
        // load all field
        Map<String, Field> fieldMap = loadFields(clazzToMapper);

        // 将dbObj中的数据映射到beanMap中，如数据包含BasicDBObject则递归映射为对应的bean
        // k=dbObj中的字段名，v=dbObj中对应的值或对象
        Map<String, Object> beanMap = new HashMap<>();
        for (String s : dbObj.keySet()) {
            Object o = dbObj.get(s);
            // 嵌套对象
            if (o instanceof BasicDBObject) {
                Field field = fieldMap.get(s);
                o = beanMapper((BasicDBObject) o, field.getType());

                // 钳套对象列表
            } else if (o instanceof BasicDBList) {
                Field field = fieldMap.get(s);
                // 获取对应的范型
                Class<?> parameterizedClazz = getParameterizedClass(field);

                BasicDBList basicDBs = (BasicDBList) o;

                Collection<Object> collection = createCollection(field, basicDBs.size());
                for (Object basicDbObj : basicDBs) {
                    // 基本类型
                    if (parameterizedClazz.isPrimitive()) {
                        collection.add(basicDbObj);
                    } else if (parameterizedClazz.getName().startsWith("java.lang")) {
                        collection.add(basicDbObj);
                    } else {
                        // 对象类型
                        collection.add(beanMapper((BasicDBObject) basicDbObj, parameterizedClazz));
                    }
                }
                o = collection;
            }

            beanMap.put(s, o);
        }

        // create
        Object instance;
        try {
            instance = clazzToMapper.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            LOG.warn("实例化:[{}]对象异常.", clazzToMapper, e);
            return null;
        }

        // 赋值
        Set<String> fieldNames = fieldMap.keySet();
        for (String fieldName : fieldNames) {
            if (beanMap.containsKey(fieldName)) {
                Field field = fieldMap.get(fieldName);
                Object value = beanMap.get(fieldName);

                try {
                    field.set(instance, value);
                } catch (IllegalAccessException e) {
                    // 应该不会报
                    LOG.error("为字段:[{}]设置值异常",
                            fieldName, e);
                }
            }
        }
        return instance;
    }

    /**
     * 将mongodb的数据对象列表<code>basicDBList</code>转换成对应类型<code>arrayClass</code>的数组。<br/>
     * 基本类型直接转换，对象类型使用 {@link #beanMapper(BasicDBObject, Class)}。
     *
     * @param basicDBList mongodb数据对象列表
     * @param arrayClass  目标数组对象类
     * @return 转换后的数组对象
     * @see MongoEmbeddedObjectProcessor#beanMapper(BasicDBObject, Class)
     */
    private static Object arrayMapper(BasicDBList basicDBList, Class<?> arrayClass) {
        // 具体类
        Class<?> clazzToMapper;
        try {
            clazzToMapper = Class.forName(arrayClass.getName()
                    .replace("[L", "")
                    .replace(";", ""));
        } catch (ClassNotFoundException e) {
            LOG.warn("实例化:[{}]对象异常.", arrayClass, e);
            return null;
        }

        // 创建对应的数组
        Object array = Array.newInstance(clazzToMapper, basicDBList.size());

        // 数组赋值
        int i = 0;
        for (Object basicDbObj : basicDBList) {
            Object value;
            // 基本类型
            if (clazzToMapper.isPrimitive()) {
                value = basicDbObj;
            } else if (clazzToMapper.getName().startsWith("java.lang")) {
                value = basicDbObj;
            } else {
                // 对象类型
                value = beanMapper((BasicDBObject) basicDbObj, clazzToMapper);
            }

            Array.set(array, i, value);
            i++;
        }
        return array;
    }
}
