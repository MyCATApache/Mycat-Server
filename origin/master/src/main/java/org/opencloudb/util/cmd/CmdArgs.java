package org.opencloudb.util.cmd;

import java.util.HashMap;
import java.util.Map;

/**
 * -host=192.168.1.1:8080
 * -
 * @author me
 *
 */
public class CmdArgs {
	private static final CmdArgs cmdArgs=new CmdArgs();
	
	private Map<String,String> args;
	
	private CmdArgs(){
		args=new HashMap<>();
	}
	
	
	public static CmdArgs getInstance(String[] args){
		Map<String,String> cmdArgs=CmdArgs.cmdArgs.args;
		for(int i=0,l=args.length;i<l;i++){
			String arg=args[i].trim();
			int split=arg.indexOf('=');
			cmdArgs.put(arg.substring(1,split), arg.substring(split+1));
		}
		return CmdArgs.cmdArgs;
	}
	
	public String getString(String name){
		return args.get(name);
	}
	public int getInt(String name){
		return Integer.parseInt(getString(name));
	}
	public long getLong(String name){
		return Long.parseLong(getString(name));
	}
	public boolean getBoolean(String name){
		return Boolean.parseBoolean(getString(name));
	}
}
