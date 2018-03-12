package io.mycat.classload;

import org.junit.Assert;
import org.junit.Test;

import io.mycat.config.classloader.DynaClassLoader;

public class TestDynClassLoad {

	@Test
	public void testLoadClass() throws Exception {
		String path=this.getClass().getResource("/").getPath();
		String clsName="demo.test.TestClass1";
		System.out.println("class load path "+path);
		DynaClassLoader loader =new DynaClassLoader(path,1);
		Object obj=loader.getInstanceofClass(clsName);
		Assert.assertEquals(obj.getClass().getSimpleName(),"TestClass1");
		Assert.assertEquals(true,loader.getInstanceofClass(clsName)==obj);
	
	}

	
}
