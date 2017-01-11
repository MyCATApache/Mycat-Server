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

package org.opencloudb.trace;

import org.apache.log4j.Logger;
import org.opencloudb.backend.BackendConnection;
import org.opencloudb.net.FrontendConnection;

/**
 * <p>
 * A component is used to trace mycat execution deeply, which includes basic trace 
 *and frontend-or-backend connection trace, for debug intention and our high qualified 
 *mycat server.
 * </p>
 * 
 * <p>
 * The tracer is disabled by default, and you can enable it by specifying
 *the system property {@code -Dmycat.trace.enabled=true} and setting log4j
 *debug level.
 * </p>
 * 
 * @author little-pan
 * @since 2017-01-11
 */
public final class Tracer {
	final static Logger LOGGER = Logger.getLogger(Tracer.class);
	
	final static boolean trace,     traceStack,  useBuffer;
	final static int     maxBuffer, initBuffer,  stackMaxDeep;
	final static String  lineSep,   stackPrompt, stackIndent;
	
	final static Object[] zeroArgs = new Object[0];
	
	private final static ThreadLocal<StringBuilder> localBuffer = new ThreadLocal<StringBuilder>(){
		@Override
		protected StringBuilder initialValue(){
			return (new StringBuilder(initBuffer));
		}
	};
	
	static{
		trace = Boolean.parseBoolean(System.getProperty("mycat.trace.enabled", "false"));
		
		traceStack  = Boolean.parseBoolean(System.getProperty("mycat.trace.stack", "true"));
		stackPrompt = System.getProperty("mycat.trace.stackPrompt", ">");
		stackIndent = System.getProperty("mycat.trace.stackIndent", " ");
		stackMaxDeep= Integer.parseInt(System.getProperty("mycat.trace.stackMaxDeep", "20"));
				
		useBuffer = Boolean.parseBoolean(System.getProperty("mycat.trace.useBuffer", "true"));
		maxBuffer = Integer.parseInt(System.getProperty("mycat.trace.maxBuffer", "4096"));
		initBuffer= Integer.parseInt(System.getProperty("mycat.trace.initBuffer", "512"));
		
		lineSep = System.getProperty("line.separator");
	}
	
	private Tracer(){
		// noop
	}
	
	public final static void trace(final String format, final Object ...args){
		if(trace && LOGGER.isDebugEnabled()){
			trace0(2, null, format, args);
		}
	}
	
	public final static void trace(final String tag, final String format, final Object ...args){
		if(trace && LOGGER.isDebugEnabled()){
			trace0(2, tag, format, args);
		}
	}
	
	public final static void trace(final FrontendConnection fronend){
		if(trace && LOGGER.isDebugEnabled()){
			trace0(2, fronendTag(fronend), fronend+"");
		}
	}
	
	public final static void trace(final FrontendConnection fronend, final String format, final Object ...args){
		if(trace && LOGGER.isDebugEnabled()){
			trace0(2, fronendTag(fronend), format, args);
		}
	}
	
	private final static String fronendTag(final FrontendConnection fronend){
		return (String.format("fronend#%x-%d", fronend.hashCode(), fronend.getId()));
	}
	
	public final static void trace(final BackendConnection backend){
		if(trace && LOGGER.isDebugEnabled()){
			trace0(2, backendTag(backend), backend+"");
		}
	}
	
	public final static void trace(final BackendConnection backend, final String format, final Object ...args){
		if(trace && LOGGER.isDebugEnabled()){
			trace0(2, backendTag(backend), format, args);
		}
	}
	
	private final static String backendTag(final BackendConnection backend){
		return (String.format("backend#%x-%d", backend.hashCode(), backend.getId()));
	}
	
	final static void trace0(final int traces, 
			final String tag, final String format, final Object ...args){
		final StringBuilder buf = acquireBuffer();
		try{
			// tag: optional
			if(tag != null){
				buf.append('[').append(tag).append(']').append(' ');
			}
			buf.append((args == null) ? format: (String.format(format, args)));
			if(traceStack){
				final Thread curThread = Thread.currentThread();
				final String thrName   = curThread.getName();
				final StackTraceElement[] stack = curThread.getStackTrace();
				final int size = stack.length, maxDeep = stackMaxDeep;
				final int base = traces + 1/* ignored: trace(), getStackTrace() */;
				for(int i = size, k = 0; i > base; ++k){
					buf.append(lineSep);
					if(tag != null){
						buf.append('[').append(tag).append(']');
					}
					buf.append('[').append(thrName).append(']');
					if(k == maxDeep){
						buf.append('<').append(".........").append(i-base).append(" levels omitted");
						break;
					}
					final StackTraceElement e = stack[--i];
					final int w = (size - (i+1))<<1;
					for(int j = 0; j < w; ++j){
						buf.append(stackIndent);
					}
					buf.append(stackPrompt).append(' ').append(e);
				}
			}
			LOGGER.debug(buf);
		}finally{
			releaseBuffer(buf);
		}
	}
	
	private final static StringBuilder acquireBuffer(){
		if(useBuffer){
			return (localBuffer.get());
		}
		return (new StringBuilder());
	}
	
	private final static void releaseBuffer(final StringBuilder buf){
		buf.setLength(0);
		if(buf.capacity() > maxBuffer){
			localBuffer.remove();
		}
	}
	
	private final static void testDeeper(){
		trace("Tracer", "test");
		trace("Tracer", "test: s0 = %d, s1 = %d", 1, 2);
		trace("Tracer", "test: cur= %s", new java.util.Date());
		trace((String)null, "test");
		trace("test: JAN = %.2f, FEB = %.2f", 2017.01, 2017.02);
		trace("test");
	}
	
	private final static void test(){
		testDeeper();
	}
	
	public static void main(String args[]){
		test();
	}
	
}
