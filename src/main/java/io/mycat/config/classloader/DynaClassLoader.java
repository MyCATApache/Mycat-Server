package io.mycat.config.classloader;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

/**
 * used for mycat's catlet class loader ,catlet's class file is stored in
 * Mycat_home/catlet dir
 * 
 * @author wuzhih
 * 
 */
public class DynaClassLoader {
	private static final Logger LOGGER = LoggerFactory.getLogger("DynaClassLoader");
	/** key- class full name */
	private static Map<String, DynaClass> loadedDynaClassMap = new ConcurrentHashMap<String, DynaClass>();
	private final String extClassHome;
	private final MyDynaClassLoader myClassLoader;
	private final long classCheckMilis;

	public DynaClassLoader(String extClassHome, int classCheckSeconds) {
		super();
		this.extClassHome = extClassHome;
		classCheckMilis = classCheckSeconds * 1000L;
		myClassLoader = new MyDynaClassLoader();
		LOGGER.info("dyna class load from " + extClassHome
				+ ",and auto check for class file modified every "
				+ classCheckSeconds + " seconds");
	}

	public Object getInstanceofClass(String className) throws Exception {
		DynaClass dynaClass = loadedDynaClassMap.get(className);
		boolean needReload = (dynaClass == null || (dynaClass
				.needReloadClass(classCheckMilis) && checkChanged(dynaClass)));
		Class<?> newClass = null;
		if (needReload) {
			newClass = myClassLoader.loadClass(className);
			dynaClass = loadedDynaClassMap.get(className);
		} else {
			newClass = dynaClass.realClass;
		}

		if (dynaClass != null) {
			Object val = dynaClass.classObj;
			if (val == null) {
				val = dynaClass.realClass.newInstance();
				dynaClass.classObj = val;

			}
			return val;
		} else {
			return newClass.newInstance();
		}
	}

	/**
	 * 加载某个类的字节码
	 * 
	 * @param c
	 * @return
	 * @throws IOException
	 */
	private static byte[] loadFile(String path) throws IOException {
		BufferedInputStream in = null;
		try {
			in = new BufferedInputStream(new FileInputStream(path));
			byte[] readed = new byte[1024 * 4];
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			int count = 0;
			while ((count = in.read(readed)) != -1) {
				out.write(readed, 0, count);
			}
			return out.toByteArray();
		} finally {
			if (in != null) {
				in.close();
			}
		}
	}

	private boolean checkChanged(DynaClass dynaClass) throws IOException {
		boolean isChanged = false;
		File f = new File(dynaClass.filePath);
		if (f.exists()) {
			long newTime = f.lastModified();
			long oldTime = dynaClass.lastModified;
			if (oldTime != newTime) {
				// need reload
				dynaClass.lastModified = newTime;
				dynaClass.classObj = null;
				dynaClass.realClass = null;
				isChanged = true;
			}
		}
		return isChanged;
	}

	class MyDynaClassLoader extends ClassLoader {
		public MyDynaClassLoader() {
		}

		public MyDynaClassLoader(ClassLoader parentLoader) {
			super(parentLoader);
		}

		/**
		 * 加载某个类
		 * 
		 * @param c
		 * @return
		 * @throws ClassNotFoundException
		 * @throws IOException
		 */
		public Class<?> loadClass(String name) throws ClassNotFoundException {
			if (name.startsWith("java") || name.startsWith("sun")
					|| name.startsWith("org.opencloudb")) {
				return super.loadClass(name);
			}
			DynaClass dynaClass = loadedDynaClassMap.get(name);
			if (dynaClass != null) {
				if (dynaClass.realClass != null) {
					return dynaClass.realClass;
				}
			} else {
				try {
					dynaClass = searchFile(extClassHome, name);
				} catch (Exception e) {
					LOGGER.error("SearchFileError", e);
				}
			}

			if (dynaClass == null) {
				return super.loadClass(name);
			} else {
				LOGGER.info("load class from file "+dynaClass.filePath);
				Class<?> cNew = null;
				if (dynaClass.isJar) {
					cNew =dynaClass.realClass;
				}
				else {
				   byte[] content;
				   try {
					 content = loadFile(dynaClass.filePath);
				   } catch (IOException e) {
					 throw new ClassNotFoundException(e.toString());
				   }
				   cNew = super.defineClass(name, content, 0,content.length);
				   dynaClass.realClass = cNew;
				}
				dynaClass.classObj = null;
				loadedDynaClassMap.put(name, dynaClass);
				return cNew;
			}

		}

		private DynaClass searchFile(String classpath, String fileName) throws Exception {
			DynaClass dynCls = null;
			String path = fileName.replace('.', File.separatorChar) + ".class";
			System.out.println("class " + classpath + " file " + path);
			File f = new File(classpath, path);
			if (f.isFile()) {
				String theName = f.getPath();
				System.out.println("found " + theName);

				dynCls = new DynaClass(f.getPath());
				dynCls.lastModified = f.lastModified();
				return dynCls;
			}
			else {
				path = fileName.replace('.', File.separatorChar) + ".jar";
				//classpath="D:\\code\\mycat\\Mycat-Server\\catlet\\";
				System.out.println("jar " + classpath + " file " + path);
				f = new File(classpath, path);
				if (f.isFile()) {
				  try {
					 dynCls = new DynaClass(f.getPath());
					 dynCls.lastModified = f.lastModified();					 
				     dynCls.realClass=JarLoader.loadJar(classpath+"/"+path,fileName);	
					 dynCls.isJar=true;
					 return dynCls;
				  }
				  catch(Exception err) {
					  return null;
				  }

				}
				return null;
			}
			
		}

	}

	public void clearUnUsedClass() {
		long deadTime = System.currentTimeMillis() - 30 * 60 * 1000L;
		Iterator<Map.Entry<String, DynaClass>> itor = loadedDynaClassMap
				.entrySet().iterator();
		while (itor.hasNext()) {
			Map.Entry<String, DynaClass> entry = itor.next();
			DynaClass dyCls = entry.getValue();
			if (dyCls.lastModified < deadTime) {
				LOGGER.info("clear unused catlet " + entry.getKey());
				dyCls.clear();
				itor.remove();
			}
		}
	}
}

class DynaClass {
	public final String filePath;
	public volatile long lastModified;
	public Class<?> realClass;
	public Object classObj;
    public boolean isJar=false;
	public boolean needReloadClass(long classCheckMilis) {
		if (lastModified + classCheckMilis < System.currentTimeMillis()) {
			return true;
		} else {
			return false;
		}
	}

	public void clear() {
		this.realClass = null;
		this.classObj = null;

	}

	public DynaClass(String filePath) {
		super();
		this.filePath = filePath;
	}
}
