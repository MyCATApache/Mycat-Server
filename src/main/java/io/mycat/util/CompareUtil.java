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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompareUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompareUtil.class);
	public static int compareInt(int l,int r){	 
		  
		  if(l > r){
			  return 1;
		  }else if(l < r){
			  return -1;
		  }else{
			  return 0;
		  }
		  
	  }
	  
	  public static int compareDouble(double l,double r){
		  
		  if(l > r){
			  return 1;
		  }else if(l < r){
			  return -1;
		  }else{
			  return 0;
		  }
		  
	  } 
	  public static int compareFloat(float l,float r){
		  
		  if(l > r){
			  return 1;
		  }else if(l < r){
			  return -1;
		  }else{
			  return 0;
		  }
		  
	  }
	  
	  public static int compareLong(long l,long r){ 
//		  System.out.println(l + "      " +  r);
		  if(l > r){
			  return 1;
		  }else if(l < r){
			  return -1;
		  }else{
			  return 0;
		  }
		  
	  }
	  
	  public static int compareString(String l,String r){
//		  return compareStringForChinese(l,r);
		  if(l == null) {
			  return -1;
		  }
		   else if(r == null) {
			  return 1;
		  }
		  return l.compareTo(r); 
	  }
	  
	  public static int compareChar(char l,char r){
		  
		  if(l > r){
			  return 1;
		  }else if(l < r){
			  return -1;
		  }else{
			  return 0;
		  }
		  
	  }
	  
	  public static int compareUtilDate(Object left,Object right){
		  
		  java.util.Date l = (java.util.Date)left;
		  java.util.Date r = (java.util.Date)right;
		  
		  return l.compareTo(r);
		  
	  }
	  
	  public static int compareSqlDate(Object left,Object right){
		  
		  java.sql.Date l = (java.sql.Date)left;
		  java.sql.Date r = (java.sql.Date)right;
		  
		  return l.compareTo(r);
		  
	  }
	  
	  
    private static int compareStringForChinese(String s1, String s2) {
        String m_s1 = null, m_s2 = null;
        try {
            //先将两字符串编码成GBK
            m_s1 = new String(s1.getBytes("GB2312"), "GBK");
            m_s2 = new String(s2.getBytes("GB2312"), "GBK");
        } catch (Exception ex) {
            LOGGER.error("compareStringForChineseError", ex);
            return s1.compareTo(s2);
        }
        int res = chineseCompareTo(m_s1, m_s2);
        
        //              System.out.println("比较：" + s1 + " | " + s2 + "==== Result: " + res);
        return res;
    }
 
	//获取一个汉字/字母的Char值
	private static int getCharCode(String s){
          if (s==null || s.length()==0) {
			  return -1;//保护代码
		  }
          byte [] b = s.getBytes();
          int value = 0;
          //保证取第一个字符（汉字或者英文）
          for (int i = 0; i < b.length && i <= 2; i ++){
                 value = value*100+b[i];
          }
          if(value < 0){
        	  value += 100000;
          }
          
          return value;
   }
	 
	//比较两个字符串
	private static int chineseCompareTo(String s1, String s2){
		int len1 = s1.length();
		int len2 = s2.length();
 
		int n = Math.min(len1, len2);
 
		for (int i = 0; i < n; i ++){
			int s1_code = getCharCode(s1.charAt(i) + "");
			int s2_code = getCharCode(s2.charAt(i) + "");
			if (s1_code != s2_code){
				return s1_code - s2_code;
			}
     	}
        return len1 - len2;
   }
}