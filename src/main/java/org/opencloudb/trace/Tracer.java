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

import java.lang.reflect.Method;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;
import org.opencloudb.backend.BackendConnection;
import org.opencloudb.net.AbstractConnection;
import org.opencloudb.net.FrontendConnection;
import org.opencloudb.server.ServerConnection;

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
	
	final static boolean trace,     traceStack,  useBuffer,
						 buffer,    fronend,     backend,
						 tx;
	final static int     maxBuffer, initBuffer,  stackMaxDeep;
	final static String  lineSep,   stackPrompt, stackIndent;
	
	final static Method address;
	
	
	private final static ThreadLocal<StringBuilder> localBuffer = new ThreadLocal<StringBuilder>(){
		@Override
		protected StringBuilder initialValue(){
			return (new StringBuilder(initBuffer));
		}
	};
	
	static{
		trace   = Boolean.parseBoolean(System.getProperty("mycat.trace.enabled", "false"));
		buffer  = Boolean.parseBoolean(System.getProperty("mycat.trace.buffer",  "false"));
		fronend = Boolean.parseBoolean(System.getProperty("mycat.trace.fronend", "false"));
		backend = Boolean.parseBoolean(System.getProperty("mycat.trace.backend", "false"));
		tx      = Boolean.parseBoolean(System.getProperty("mycat.trace.tx", "true"));
		if(trace && buffer){
			final ByteBuffer dbuf = ByteBuffer.allocateDirect(1);
			Method addr = null;
			try{
				addr = dbuf.getClass().getDeclaredMethod("address");
				addr.setAccessible(true);
			}catch(final Exception e){
				addr = null;
			}
			address = addr;
		}else{
			address = null;
		}
		
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
	
	public final static void trace(final ByteBuffer buffer){
		if(trace && Tracer.buffer && LOGGER.isDebugEnabled()){
			trace0(2, bufferTag(buffer), 
				"pos = %x, rem = %x, lim = %x", 
					buffer.position(), buffer.remaining(), buffer.limit());
		}
	}
	
	public final static void trace(final ByteBuffer buffer, final String format, final Object ...args){
		if(trace && Tracer.buffer && LOGGER.isDebugEnabled()){
			trace0(2, bufferTag(buffer), format, args);
		}
	}
	
	private final static String bufferTag(final ByteBuffer buffer){
		if(buffer == null){
			return ("buffer#null");
		}
		// ByteBuffer hash-code related to content, so that it same when
		//the buffer block size same. We can get the buffer address or array
		//hash code as buffer hash code for tracing.
		// @since 2017-01-14 little-pan
		final String heap;
		long hash = 0L;
		if(buffer.isDirect()){
			heap = "oheap";
			try{
				if(address != null){
					hash = (Long)address.invoke(buffer);
				}
			}catch(final Exception e){
				// ignore
			}
		}else{
			heap = "heap";
			final byte[] array = buffer.array();
			hash = array.hashCode();
		}
		return (String.format("%s#%x-%x", heap, hash, buffer.capacity()));
	}
	
	public final static void traceTx(final ServerConnection connection) {
		if(trace && Tracer.tx && LOGGER.isDebugEnabled()){
			trace0(2, txTag(connection), connection + "");
		}
	}
	
	public final static void traceTx(final ServerConnection connection, final String format, final Object ...args) {
		if(trace && Tracer.tx && LOGGER.isDebugEnabled()){
			trace0(2, txTag(connection), format, args);
		}
	}
	
	private final static String txTag(final ServerConnection connection) {
		if(connection == null){
			return ("tx#null");
		}
		return (String.format("tx#%x-%x", connection.hashCode(), connection.getTxLocalId()));
	}

	public final static void traceCnxn(final AbstractConnection connection){
		if(trace && LOGGER.isDebugEnabled()){
			if( (connection instanceof BackendConnection  && Tracer.backend == false) 
					|| 
				(connection instanceof FrontendConnection && Tracer.fronend == false)
			){
				return;
			}
			trace0(2, connectTag(connection), connection+"");
		}
	}
	
	public final static void traceCnxn(final AbstractConnection connection, final String format, final Object ...args){
		if(trace && LOGGER.isDebugEnabled()){
			if( (connection instanceof BackendConnection  && Tracer.backend == false) 
					|| 
				(connection instanceof FrontendConnection && Tracer.fronend == false)
			){
				return;
			}
			trace0(2, connectTag(connection), format, args);
		}
	}
	
	private final static String connectTag(final AbstractConnection connection){
		final String tag;
		if(connection instanceof BackendConnection){
			tag = backendTag((BackendConnection)connection);
		}else if(connection instanceof FrontendConnection){
			tag = fronendTag((FrontendConnection)connection);
		}else if(connection == null){
			tag = "connect#null";
		}else{
			tag = String.format("connect#%x-%d", 
				connection.hashCode(), connection.getId());
		}
		return tag;
	}
	
	public final static void trace(final FrontendConnection fronend){
		if(trace && Tracer.fronend && LOGGER.isDebugEnabled()){
			trace0(2, fronendTag(fronend), fronend+"");
		}
	}
	
	public final static void trace(final FrontendConnection fronend, final String format, final Object ...args){
		if(trace && Tracer.fronend && LOGGER.isDebugEnabled()){
			trace0(2, fronendTag(fronend), format, args);
		}
	}
	
	private final static String fronendTag(final FrontendConnection fronend){
		if(fronend == null){
			return "fronend#null";
		}
		return (String.format("fronend#%x-%d", fronend.hashCode(), fronend.getId()));
	}
	
	public final static void trace(final BackendConnection backend){
		if(trace && Tracer.backend && LOGGER.isDebugEnabled()){
			trace0(2, backendTag(backend), backend+"");
		}
	}
	
	public final static void trace(final BackendConnection backend, final String format, final Object ...args){
		if(trace && Tracer.backend && LOGGER.isDebugEnabled()){
			trace0(2, backendTag(backend), format, args);
		}
	}
	
	private final static String backendTag(final BackendConnection backend){
		if(backend == null){
			return "backend#null";
		}
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
