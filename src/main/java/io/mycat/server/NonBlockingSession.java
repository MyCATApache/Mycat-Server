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
package io.mycat.server;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.MycatServer;
import io.mycat.backend.BackendConnection;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.backend.mysql.nio.handler.CommitNodeHandler;
import io.mycat.backend.mysql.nio.handler.KillConnectionHandler;
import io.mycat.backend.mysql.nio.handler.LockTablesHandler;
import io.mycat.backend.mysql.nio.handler.MiddlerResultHandler;
import io.mycat.backend.mysql.nio.handler.MultiNodeCoordinator;
import io.mycat.backend.mysql.nio.handler.MultiNodeQueryHandler;
import io.mycat.backend.mysql.nio.handler.RollbackNodeHandler;
import io.mycat.backend.mysql.nio.handler.RollbackReleaseHandler;
import io.mycat.backend.mysql.nio.handler.SingleNodeHandler;
import io.mycat.backend.mysql.nio.handler.UnLockTablesHandler;
import io.mycat.config.ErrorCode;
import io.mycat.config.MycatConfig;
import io.mycat.net.FrontendConnection;
import io.mycat.net.mysql.OkPacket;
import io.mycat.route.RouteResultset;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.parser.ServerParse;
import io.mycat.server.sqlcmd.SQLCmdConstant;

/**
 * @author mycat
 * @author mycat
 */
public class NonBlockingSession implements Session {

    public static final Logger LOGGER = LoggerFactory.getLogger(NonBlockingSession.class);

    private final ServerConnection source;
    //huangyiming add 避免出现jdk版本冲突
    private final ConcurrentMap<RouteResultsetNode, BackendConnection> target;
    // life-cycle: each sql execution
    private volatile SingleNodeHandler singleNodeHandler;
    private volatile MultiNodeQueryHandler multiNodeHandler;
    private volatile RollbackNodeHandler rollbackHandler;
    private final MultiNodeCoordinator multiNodeCoordinator;
    private final CommitNodeHandler commitHandler;
    private volatile String xaTXID;

   //huangyiming 
  	private  volatile boolean canClose = true;
  	
  	private volatile MiddlerResultHandler  middlerResultHandler;
    private boolean prepared;

    public NonBlockingSession(ServerConnection source) {
        this.source = source;
        this.target = new ConcurrentHashMap<RouteResultsetNode, BackendConnection>(2, 0.75f);
        multiNodeCoordinator = new MultiNodeCoordinator(this);
        commitHandler = new CommitNodeHandler(this);
    }

    @Override
    public ServerConnection getSource() {
        return source;
    }

    @Override
    public int getTargetCount() {
        return target.size();
    }

    public Set<RouteResultsetNode> getTargetKeys() {
        return target.keySet();
    }

    public BackendConnection getTarget(RouteResultsetNode key) {
        return target.get(key);
    }

    public Map<RouteResultsetNode, BackendConnection> getTargetMap() {
        return this.target;
    }

    public BackendConnection removeTarget(RouteResultsetNode key) {
        return target.remove(key);
    }
    
    @Override
    public void execute(RouteResultset rrs, int type) {

        // clear prev execute resources
        clearHandlesResources();
        if (LOGGER.isDebugEnabled()) {
            StringBuilder s = new StringBuilder();
            LOGGER.debug(s.append(source).append(rrs).toString() + " rrs ");
        }

        // 检查路由结果是否为空
        RouteResultsetNode[] nodes = rrs.getNodes();
        if (nodes == null || nodes.length == 0 || nodes[0].getName() == null || nodes[0].getName().equals("")) {
            source.writeErrMessage(ErrorCode.ER_NO_DB_ERROR,
                    "No dataNode found ,please check tables defined in schema:" + source.getSchema());
            return;
        }
        boolean autocommit = source.isAutocommit();
        final int initCount = target.size();
        if (nodes.length == 1) {
            singleNodeHandler = new SingleNodeHandler(rrs, this);
            if (this.isPrepared()) {
                singleNodeHandler.setPrepared(true);
            }

            try {
                if(initCount > 1){
                    checkDistriTransaxAndExecute(rrs,1,autocommit);
                }else{
                    singleNodeHandler.execute();
                }
            } catch (Exception e) {
                LOGGER.warn(new StringBuilder().append(source).append(rrs).toString(), e);
                source.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.toString());
            }

        } else {

            multiNodeHandler = new MultiNodeQueryHandler(type, rrs, autocommit, this);
            if (this.isPrepared()) {
                multiNodeHandler.setPrepared(true);
            }
            try {
                if(((type == ServerParse.DELETE || type == ServerParse.INSERT || type == ServerParse.UPDATE) && !rrs.isGlobalTable() && nodes.length > 1)||initCount > 1) {
                    checkDistriTransaxAndExecute(rrs,2,autocommit);
                } else {
                    multiNodeHandler.execute();
                }
            } catch (Exception e) {
                LOGGER.warn(new StringBuilder().append(source).append(rrs).toString(), e);
                source.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.toString());
            }
        }

        if (this.isPrepared()) {
            this.setPrepared(false);
        }
    }

    private void checkDistriTransaxAndExecute(RouteResultset rrs, int type,boolean autocommit) throws Exception {
        switch(MycatServer.getInstance().getConfig().getSystem().getHandleDistributedTransactions()) {
            case 1:
                source.writeErrMessage(ErrorCode.ER_NOT_ALLOWED_COMMAND, "Distributed transaction is disabled!");
                if(!autocommit){
                    source.setTxInterrupt("Distributed transaction is disabled!");
                }
                break;
            case 2:
                LOGGER.warn("Distributed transaction detected! RRS:" + rrs);
                if(type == 1){
                    singleNodeHandler.execute();
                }
                else{
                    multiNodeHandler.execute();
                }
                break;
            default:
                if(type == 1){
                    singleNodeHandler.execute();
                }
                else{
                    multiNodeHandler.execute();
                }
        }
    }

    private void checkDistriTransaxAndExecute() {
        if(!isALLGlobal()){
            switch(MycatServer.getInstance().getConfig().getSystem().getHandleDistributedTransactions()) {
                case 1:
                    source.writeErrMessage(ErrorCode.ER_NOT_ALLOWED_COMMAND, "Distributed transaction is disabled!Please rollback!");
                    source.setTxInterrupt("Distributed transaction is disabled!");
                    break;
                case 2:
                    multiNodeCoordinator.executeBatchNodeCmd(SQLCmdConstant.COMMIT_CMD);
                    LOGGER.warn("Distributed transaction detected! Targets:" + target);
                    break;
                default:
                    multiNodeCoordinator.executeBatchNodeCmd(SQLCmdConstant.COMMIT_CMD);

            }
        } else {
            multiNodeCoordinator.executeBatchNodeCmd(SQLCmdConstant.COMMIT_CMD);
        }
    }

    public void commit() {
        final int initCount = target.size();
        if (initCount <= 0) {
            ByteBuffer buffer = source.allocate();
            buffer = source.writeToBuffer(OkPacket.OK, buffer);
            source.write(buffer);
            /* 1. 如果开启了 xa 事务 */
            if(getXaTXID()!=null){
				setXATXEnabled(false);
			}
            /* 2. preAcStates 为true,事务结束后,需要设置为true。preAcStates 为ac上一个状态    */
            if(source.isPreAcStates()&&!source.isAutocommit()){
            	source.setAutocommit(true);
            }
            return;
        } else if (initCount == 1) {
        	//huangyiming add 避免出现jdk版本冲突
            BackendConnection con = target.values().iterator().next();
            commitHandler.commit(con);
        } else {

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("multi node commit to send ,total " + initCount);
            }
            checkDistriTransaxAndExecute();
        }

    }

    private boolean isALLGlobal(){
        for(RouteResultsetNode routeResultsetNode:target.keySet()){
            if(routeResultsetNode.getSource()==null){
                return false;
            }
            else if(!routeResultsetNode.getSource().isGlobalTable()){
                return false;
            }
        }
        return true;
    }

    public void rollback() {
        final int initCount = target.size();
        if (initCount <= 0) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("no session bound connections found ,no need send rollback cmd ");
            }
            ByteBuffer buffer = source.allocate();
            buffer = source.writeToBuffer(OkPacket.OK, buffer);
            source.write(buffer);
            /* 1. 如果开启了 xa 事务 */
            if(getXaTXID()!=null){
				setXATXEnabled(false);
			}
            /* 2. preAcStates 为true,事务结束后,需要设置为true。preAcStates 为ac上一个状态    */
            if(source.isPreAcStates()&&!source.isAutocommit()){
            	source.setAutocommit(true);
            }
            return;
        }

        rollbackHandler = new RollbackNodeHandler(this);
        rollbackHandler.rollback();
    }

	/**
	 * 执行lock tables语句方法
	 * @author songdabin
	 * @date 2016-7-9
	 * @param rrs
	 */
	public void lockTable(RouteResultset rrs) {
		// 检查路由结果是否为空
		RouteResultsetNode[] nodes = rrs.getNodes();
		if (nodes == null || nodes.length == 0 || nodes[0].getName() == null
				|| nodes[0].getName().equals("")) {
			source.writeErrMessage(ErrorCode.ER_NO_DB_ERROR,
					"No dataNode found ,please check tables defined in schema:"
							+ source.getSchema());
			return;
		}
		LockTablesHandler handler = new LockTablesHandler(this, rrs);
		source.setLocked(true);
		try {
			handler.execute();
		} catch (Exception e) {
			LOGGER.warn(new StringBuilder().append(source).append(rrs).toString(), e);
			source.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.toString());
		}
	}

	/**
	 * 执行unlock tables语句方法
	 * @author songdabin
	 * @date 2016-7-9
	 * @param sql
	 */
	public void unLockTable(String sql) {
		UnLockTablesHandler handler = new UnLockTablesHandler(this, this.source.isAutocommit(), sql);
		handler.execute();
	}
	
    @Override
    public void cancel(FrontendConnection sponsor) {

    }

    /**
     * {@link ServerConnection#isClosed()} must be true before invoking this
     */
    public void terminate() {
        for (BackendConnection node : target.values()) {
            node.close("client closed ");
        }
        target.clear();
        clearHandlesResources();
    }

    public void closeAndClearResources(String reason) {
        for (BackendConnection node : target.values()) {
            node.close(reason);
        }
        target.clear();
        clearHandlesResources();
    }

    public void releaseConnectionIfSafe(BackendConnection conn, boolean debug,
                                        boolean needRollback) {
        RouteResultsetNode node = (RouteResultsetNode) conn.getAttachment();

        if (node != null) {
        	/*  分表 在
        	 *    1. 没有开启事务
        	 *    2. 读取走的从节点
        	 *    3. 没有执行过更新sql
        	 *    也需要释放连接
        	 */
//            if (node.isDisctTable()) {
//                return;
//            }
            if (MycatServer.getInstance().getConfig().getSystem().isStrictTxIsolation()) {
                // 如果是严格隔离级别模式的话,不考虑是否已经执行了modifiedSql,直接不释放连接
                if ((!this.source.isAutocommit() && !conn.isFromSlaveDB()) || this.source.isLocked()) {
                    return;
                }
            } else {
                if ((this.source.isAutocommit() || conn.isFromSlaveDB()
                             || !conn.isModifiedSQLExecuted()) && !this.source.isLocked()) {
                    releaseConnection((RouteResultsetNode) conn.getAttachment(), LOGGER.isDebugEnabled(),
                            needRollback);
                }
            }
        }
    }

    public void releaseConnection(RouteResultsetNode rrn, boolean debug,
                                  final boolean needRollback) {

        BackendConnection c = target.remove(rrn);
        if (c != null) {
            if (debug) {
                LOGGER.debug("release connection " + c);
            }
            if (c.getAttachment() != null) {
                c.setAttachment(null);
            }
            if (!c.isClosedOrQuit()) {
                if (c.isAutocommit()) {
                    c.release();
                } else
                //if (needRollback)
                {
                    c.setResponseHandler(new RollbackReleaseHandler());
                    c.rollback();
                }
                //else {
				//	c.release();
				//}
            }
        }
    }

    public void releaseConnections(final boolean needRollback) {
        boolean debug = LOGGER.isDebugEnabled();
        
        for (RouteResultsetNode rrn : target.keySet()) {
            releaseConnection(rrn, debug, needRollback);
        }
    }

    public void releaseConnection(BackendConnection con) {
        Iterator<Entry<RouteResultsetNode, BackendConnection>> itor = target
                .entrySet().iterator();
        while (itor.hasNext()) {
            BackendConnection theCon = itor.next().getValue();
            if (theCon == con) {
                itor.remove();
                con.release();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("realse connection " + con);
                }
                break;
            }
        }

    }

    /**
     * @return previous bound connection
     */
    public BackendConnection bindConnection(RouteResultsetNode key,
                                            BackendConnection conn) {
        // System.out.println("bind connection "+conn+
        // " to key "+key.getName()+" on sesion "+this);
        return target.put(key, conn);
    }
    
    public boolean tryExistsCon(final BackendConnection conn, RouteResultsetNode node) {
        if (conn == null) {
            return false;
        }

        boolean canReUse = false;
        // conn 是 slave db 的，并且 路由结果显示，本次sql可以重用该 conn
        if (conn.isFromSlaveDB() && (node.canRunnINReadDB(getSource().isAutocommit())
                && (node.getRunOnSlave() == null || node.getRunOnSlave()))) {
            canReUse = true;
        }

        // conn 是 master db 的，并且路由结果显示，本次sql可以重用该conn
        if (!conn.isFromSlaveDB() && (node.getRunOnSlave() == null || !node.getRunOnSlave())) {
            canReUse = true;
        }

        if (canReUse) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("found connections in session to use " + conn
                        + " for " + node);
            }
            conn.setAttachment(node);
            return true;
        } else {
            // slavedb connection and can't use anymore ,release it
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("release slave connection,can't be used in trasaction  "
                        + conn + " for " + node);
            }
            releaseConnection(node, LOGGER.isDebugEnabled(), false);
        }
        return false;
    }

//	public boolean tryExistsCon(final BackendConnection conn,
//			RouteResultsetNode node) {
//
//		if (conn == null) {
//			return false;
//		}
//		if (!conn.isFromSlaveDB()
//				|| node.canRunnINReadDB(getSource().isAutocommit())) {
//			if (LOGGER.isDebugEnabled()) {
//				LOGGER.debug("found connections in session to use " + conn
//						+ " for " + node);
//			}
//			conn.setAttachment(node);
//			return true;
//		} else {
//			// slavedb connection and can't use anymore ,release it
//			if (LOGGER.isDebugEnabled()) {
//				LOGGER.debug("release slave connection,can't be used in trasaction  "
//						+ conn + " for " + node);
//			}
//			releaseConnection(node, LOGGER.isDebugEnabled(), false);
//		}
//		return false;
//	}

    protected void kill() {
        boolean hooked = false;
        AtomicInteger count = null;
        Map<RouteResultsetNode, BackendConnection> killees = null;
        for (RouteResultsetNode node : target.keySet()) {
            BackendConnection c = target.get(node);
            if (c != null) {
                if (!hooked) {
                    hooked = true;
                    killees = new HashMap<RouteResultsetNode, BackendConnection>();
                    count = new AtomicInteger(0);
                }
                killees.put(node, c);
                count.incrementAndGet();
            }
        }
        if (hooked) {
            for (Entry<RouteResultsetNode, BackendConnection> en : killees
                    .entrySet()) {
                KillConnectionHandler kill = new KillConnectionHandler(
                        en.getValue(), this);
                MycatConfig conf = MycatServer.getInstance().getConfig();
                PhysicalDBNode dn = conf.getDataNodes().get(
                        en.getKey().getName());
                try {
                    dn.getConnectionFromSameSource(null, true, en.getValue(),
                            kill, en.getKey());
                } catch (Exception e) {
                    LOGGER.error(
                            "get killer connection failed for " + en.getKey(),
                            e);
                    kill.connectionError(e, null);
                }
            }
        }
    }

    private void clearHandlesResources() {
        SingleNodeHandler singleHander = singleNodeHandler;
        if (singleHander != null) {
            singleHander.clearResources();
            singleNodeHandler = null;
        }
        MultiNodeQueryHandler multiHandler = multiNodeHandler;
        if (multiHandler != null) {
            multiHandler.clearResources();
            multiNodeHandler = null;
        }
    }

    public void clearResources(final boolean needRollback) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("clear session resources " + this);
        }
        this.releaseConnections(needRollback);
        clearHandlesResources();
    }

    public boolean closed() {
        return source.isClosed();
    }

    private String genXATXID() {
        return MycatServer.getInstance().getXATXIDGLOBAL();
    }

    public void setXATXEnabled(boolean xaTXEnabled) {

        if (xaTXEnabled) {
        	LOGGER.info("XA Transaction enabled ,con " + this.getSource());
        	if(this.xaTXID == null){
        		xaTXID = genXATXID();
        	}
        }else{
        	LOGGER.info("XA Transaction disabled ,con " + this.getSource());
        	this.xaTXID = null;
        }
    }

    public String getXaTXID() {
        return xaTXID;
    }

    public boolean isPrepared() {
        return prepared;
    }

    public void setPrepared(boolean prepared) {
        this.prepared = prepared;
    }


	public boolean isCanClose() {
		return canClose;
	}

	public void setCanClose(boolean canClose) {
		this.canClose = canClose;
	}

	public MiddlerResultHandler getMiddlerResultHandler() {
		return middlerResultHandler;
	}

	public void setMiddlerResultHandler(MiddlerResultHandler middlerResultHandler) {
		this.middlerResultHandler = middlerResultHandler;
	}

    public void setAutoCommitStatus() {
		/* 1.  事务结束后,xa事务结束    */
		if(this.getXaTXID()!=null){
			this.setXATXEnabled(false);
		}
		/* 2. preAcStates 为true,事务结束后,需要设置为true。preAcStates 为ac上一个状态    */
		if(this.getSource().isPreAcStates()&&!this.getSource().isAutocommit()){
			this.getSource().setAutocommit(true);
        }
		this.getSource().clearTxInterrupt();

    }
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		StringBuilder sb = new StringBuilder();
		for (BackendConnection backCon : target.values()) {
			sb.append(backCon).append("\r\n");
		}
		return sb.toString();
	}
}
