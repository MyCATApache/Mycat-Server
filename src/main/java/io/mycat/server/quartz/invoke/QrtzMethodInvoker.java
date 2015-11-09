package io.mycat.server.quartz.invoke;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;

import org.apache.commons.lang.ClassUtils;

public class QrtzMethodInvoker {

	public static boolean invoke(String clazzName, String methodName, Object... params) {
		Class<?> clazz = null;
		Object bean = null;
		Method method = null;

		try {
			// 反射获取相关class
			clazz = ClassUtils.getClass(clazzName);
			bean = clazz.newInstance();

			// 如果传入 args 为null, 证明只有一个参数,将遍历获取方法
			if (params == null) {
				Method[] methods = clazz.getDeclaredMethods();
				for (Method m : methods) {
					if (m.getName().equals(methodName.trim())) {
						Parameter[] parameters = m.getParameters();
						for (Parameter parameter : parameters) {
							Class<?> param = parameter.getType();
							method = ClassUtils.getPublicMethod(clazz, methodName, new Class<?>[] { param });
							method.invoke(bean, (Object) baseWrapDefaultValue(param));
							return true;
						}
					}
				}
			}
			
			// 判断是否传入参数
			if (params==null||params.length == 0) {
				method = clazz.getMethod(methodName);
				method.invoke(bean);
				return true;
			}

			Class<?>[] paramTypes = new Class<?>[params.length];
			for (int i = 0; i < paramTypes.length; i++) {
				if (params[i] == null) {
					paramTypes[i] = null;
					continue;
				}
				paramTypes[i] = params[i].getClass();
			}
			method = clazz.getMethod(methodName, paramTypes);
			method.invoke(bean, params);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	public static Object baseWrapDefaultValue(Class<?> clazz) {
		String clazzName = clazz.getName();
		if (clazzName.equals("int")) {
			return 0;
		}
		if (clazzName.equals("double")) {
			return 0.0;
		}
		if (clazzName.equals("float")) {
			return 0.0;
		}
		if (clazzName.equals("boolean")) {
			return false;
		}
		if (clazzName.equals("byte")) {
			return 0;
		}
		return null;

	}
}
