package demo.test;

import java.util.Arrays;

public class ByteArrayToHexArray {
	public static String buqi(String hexStr){
		if(hexStr.length() == 1){
			return "0"+hexStr;
		}
		return hexStr;
	}
	public static void main(String[] args) {
//		byte [] array = {19, 0, 0, 8, 4, 49, 53, 53, 57, 1, 49, 4, 49, 49, 49, 53, 5, 52, 52, 51, 49, 55, -5};		
		byte [] array =	{4, 0, 0, 0, 2, 100, 98, 50};
		for(byte b : array){
			 int a=b&0xff;  
	         System.out.print( buqi(Integer.toHexString(b)) + ":");
		}
		
		System.out.println("");
		for(byte b : array){
			 int a=b&0xff;  
	         System.out.print( new String(new byte[]{b}) + "  ");
		}
		
	}
}
