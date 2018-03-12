package io.mycat.config.classloader;


import java.util.jar.*;
import java.lang.reflect.*;
import java.net.URL;
import java.net.URLClassLoader;
import java.io.*;
import java.util.*;

public class JarLoader {
	  /** Unpack a jar file into a directory. */
	  public static void unJar(File jarFile, File toDir) throws IOException {
	    JarFile jar = new JarFile(jarFile);
	    try {
	      Enumeration entries = jar.entries();
	      while (entries.hasMoreElements()) {
	        JarEntry entry = (JarEntry)entries.nextElement();
	        if (!entry.isDirectory()) {
	          InputStream in = jar.getInputStream(entry);
	          try {
	            File file = new File(toDir, entry.getName());
	            if (!file.getParentFile().mkdirs() && !file.getParentFile().isDirectory()) {

	                throw new IOException("Mkdirs failed to create " + 
	                                      file.getParentFile().toString());

	            }
	            OutputStream out = new FileOutputStream(file);
	            try {
	              byte[] buffer = new byte[8192];
	              int i;
	              while ((i = in.read(buffer)) != -1) {
	                out.write(buffer, 0, i);
	              }
	            } finally {
	              out.close();
	            }
	          } finally {
	            in.close();
	          }
	        }
	      }
	    } finally {
	      jar.close();
	    }
	  }
	  
	  public static Class<?> loadJar(String fileName,String mainJavaclass) throws Exception {

		    File file = new File(fileName);
		    String mainClassName = null;

		    JarFile jarFile;
		    try {
		      jarFile = new JarFile(fileName);
		    } catch(IOException io) {
		      throw new IOException("Error opening jar: " + fileName);
		    }

		    Manifest manifest = jarFile.getManifest();
		    if (manifest != null) {
		      mainClassName = manifest.getMainAttributes().getValue("Main-Class");
		    }
		    jarFile.close();

		    if (mainClassName == null) {
		      mainClassName = mainJavaclass;
		    }
		    mainClassName = mainClassName.replaceAll("/", ".");

		    File tmpDir = new File(System.getProperty("java.io.tmpdir"));
		    tmpDir.mkdirs();
		    if (!tmpDir.isDirectory()) { 
		    	System.out.println("Mkdirs failed to create " + tmpDir);
		    }
		    final File workDir = File.createTempFile("unjar", "", tmpDir);
		    workDir.delete();
		    workDir.mkdirs();
		    if (!workDir.isDirectory()) {
		    	System.out.println("Mkdirs failed to create " + workDir);
		    }

		    Runtime.getRuntime().addShutdownHook(new Thread() {
		        public void run() {
		          try {
		            fullyDelete(workDir);
		          } catch (IOException e) {
		          }
		        }
		      });

		    unJar(file, workDir);
		    
		    ArrayList<URL> classPath = new ArrayList<URL>();
		    classPath.add(new File(workDir+"/").toURL());
		    classPath.add(file.toURL());
		    classPath.add(new File(workDir, "classes/").toURL());
		    File[] libs = new File(workDir, "lib").listFiles();
		    if (libs != null) {
		      for (int i = 0; i < libs.length; i++) {
		        classPath.add(libs[i].toURL());
		      }
		    }
		    
		    ClassLoader loader =	 new URLClassLoader(classPath.toArray(new URL[0]));

		    Thread.currentThread().setContextClassLoader(loader);
		    Class<?> mainClass = Class.forName(mainClassName, true, loader);
		    return mainClass;
		  }
	  
	  public static boolean fullyDelete(File dir) throws IOException {
		    if (!fullyDeleteContents(dir)) {
		      return false;
		    }
		    return dir.delete();
		  }

		  /**
		   * Delete the contents of a directory, not the directory itself.  If
		   * we return false, the directory may be partially-deleted.
		   */
		  public static boolean fullyDeleteContents(File dir) throws IOException {
		    boolean deletionSucceeded = true;
		    File contents[] = dir.listFiles();
		    if (contents != null) {
		      for (int i = 0; i < contents.length; i++) {
		        if (contents[i].isFile()) {
		          if (!contents[i].delete()) {
		            deletionSucceeded = false;
		            continue; // continue deletion of other files/dirs under dir
		          }
		        } else {
		          //try deleting the directory
		          // this might be a symlink
		          boolean b = false;
		          b = contents[i].delete();
		          if (b){
		            //this was indeed a symlink or an empty directory
		            continue;
		          }
		          // if not an empty directory or symlink let
		          // fullydelete handle it.
		          if (!fullyDelete(contents[i])) {
		            deletionSucceeded = false;
		            continue; // continue deletion of other files/dirs under dir
		          }
		        }
		      }
		    }
		    return deletionSucceeded;
		  }		  	  
}
