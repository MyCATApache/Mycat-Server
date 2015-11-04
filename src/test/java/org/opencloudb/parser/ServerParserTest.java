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
package org.opencloudb.parser;

import org.junit.Assert;
import org.junit.Test;
import org.opencloudb.server.parser.ServerParse;
import org.opencloudb.server.parser.ServerParseSelect;
import org.opencloudb.server.parser.ServerParseSet;
import org.opencloudb.server.parser.ServerParseShow;
import org.opencloudb.server.parser.ServerParseStart;

/**
 * @author mycat
 */
public class ServerParserTest {

    @Test
    public void testIsBegin() {
        Assert.assertEquals(ServerParse.BEGIN, ServerParse.parse("begin"));
        Assert.assertEquals(ServerParse.BEGIN, ServerParse.parse("BEGIN"));
        Assert.assertEquals(ServerParse.BEGIN, ServerParse.parse("BegIn"));
    }

    @Test
    public void testIsCommit() {
        Assert.assertEquals(ServerParse.COMMIT, ServerParse.parse("commit"));
        Assert.assertEquals(ServerParse.COMMIT, ServerParse.parse("COMMIT"));
        Assert.assertEquals(ServerParse.COMMIT, ServerParse.parse("cOmmiT "));
    }
    

    @Test
    public void testComment() {
        Assert.assertEquals(ServerParse.MYSQL_CMD_COMMENT, ServerParse.parse("/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */"));
        Assert.assertEquals(ServerParse.MYSQL_CMD_COMMENT, ServerParse.parse("/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */"));
        Assert.assertEquals(ServerParse.MYSQL_CMD_COMMENT, ServerParse.parse("/*!40101 SET @saved_cs_client     = @@character_set_client */"));
   
        Assert.assertEquals(ServerParse.MYSQL_COMMENT, ServerParse.parse("/*SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */"));
        Assert.assertEquals(ServerParse.MYSQL_COMMENT, ServerParse.parse("/*SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */"));
        Assert.assertEquals(ServerParse.MYSQL_COMMENT, ServerParse.parse("/*SET @saved_cs_client     = @@character_set_client */"));
    }

    @Test
    public void testMycatComment() {
        Assert.assertEquals(ServerParse.SELECT, 0xff & ServerParse.parse("/*#mycat:schema=DN1*/SELECT ..."));
        Assert.assertEquals(ServerParse.UPDATE, 0xff & ServerParse.parse("/*#mycat: schema = DN1 */ UPDATE ..."));
        Assert.assertEquals(ServerParse.DELETE, 0xff & ServerParse.parse("/*#mycat: sql = SELECT id FROM user */ DELETE ..."));
    }

    @Test
    public void testOldMycatComment() {
        Assert.assertEquals(ServerParse.SELECT, 0xff & ServerParse.parse("/*!mycat:schema=DN1*/SELECT ..."));
        Assert.assertEquals(ServerParse.UPDATE, 0xff & ServerParse.parse("/*!mycat: schema = DN1 */ UPDATE ..."));
        Assert.assertEquals(ServerParse.DELETE, 0xff & ServerParse.parse("/*!mycat: sql = SELECT id FROM user */ DELETE ..."));
    }

    @Test
    public void testIsDelete() {
        Assert.assertEquals(ServerParse.DELETE, ServerParse.parse("delete ..."));
        Assert.assertEquals(ServerParse.DELETE, ServerParse.parse("DELETE ..."));
        Assert.assertEquals(ServerParse.DELETE, ServerParse.parse("DeletE ..."));
    }

    @Test
    public void testIsInsert() {
        Assert.assertEquals(ServerParse.INSERT, ServerParse.parse("insert ..."));
        Assert.assertEquals(ServerParse.INSERT, ServerParse.parse("INSERT ..."));
        Assert.assertEquals(ServerParse.INSERT, ServerParse.parse("InserT ..."));
    }

    @Test
    public void testIsReplace() {
        Assert.assertEquals(ServerParse.REPLACE, ServerParse.parse("replace ..."));
        Assert.assertEquals(ServerParse.REPLACE, ServerParse.parse("REPLACE ..."));
        Assert.assertEquals(ServerParse.REPLACE, ServerParse.parse("rEPLACe ..."));
    }

    @Test
    public void testIsRollback() {
        Assert.assertEquals(ServerParse.ROLLBACK, ServerParse.parse("rollback"));
        Assert.assertEquals(ServerParse.ROLLBACK, ServerParse.parse("ROLLBACK"));
        Assert.assertEquals(ServerParse.ROLLBACK, ServerParse.parse("rolLBACK "));
    }

    @Test
    public void testIsSelect() {
        Assert.assertEquals(ServerParse.SELECT, 0xff & ServerParse.parse("select ..."));
        Assert.assertEquals(ServerParse.SELECT, 0xff & ServerParse.parse("SELECT ..."));
        Assert.assertEquals(ServerParse.SELECT, 0xff & ServerParse.parse("sELECt ..."));
    }

    @Test
    public void testIsSet() {
        Assert.assertEquals(ServerParse.SET, 0xff & ServerParse.parse("set ..."));
        Assert.assertEquals(ServerParse.SET, 0xff & ServerParse.parse("SET ..."));
        Assert.assertEquals(ServerParse.SET, 0xff & ServerParse.parse("sEt ..."));
    }

    @Test
    public void testIsShow() {
        Assert.assertEquals(ServerParse.SHOW, 0xff & ServerParse.parse("show ..."));
        Assert.assertEquals(ServerParse.SHOW, 0xff & ServerParse.parse("SHOW ..."));
        Assert.assertEquals(ServerParse.SHOW, 0xff & ServerParse.parse("sHOw ..."));
    }

    @Test
    public void testIsStart() {
        Assert.assertEquals(ServerParse.START, 0xff & ServerParse.parse("start ..."));
        Assert.assertEquals(ServerParse.START, 0xff & ServerParse.parse("START ..."));
        Assert.assertEquals(ServerParse.START, 0xff & ServerParse.parse("stART ..."));
    }

    @Test
    public void testIsUpdate() {
        Assert.assertEquals(ServerParse.UPDATE, ServerParse.parse("update ..."));
        Assert.assertEquals(ServerParse.UPDATE, ServerParse.parse("UPDATE ..."));
        Assert.assertEquals(ServerParse.UPDATE, ServerParse.parse("UPDate ..."));
    }

    @Test
    public void testIsShowDatabases() {
        Assert.assertEquals(ServerParseShow.DATABASES, ServerParseShow.parse("show databases", 4));
        Assert.assertEquals(ServerParseShow.DATABASES, ServerParseShow.parse("SHOW DATABASES", 4));
        Assert.assertEquals(ServerParseShow.DATABASES, ServerParseShow.parse("SHOW databases ", 4));
    }

    @Test
    public void testIsShowDataSources() {
        Assert.assertEquals(ServerParseShow.DATASOURCES, ServerParseShow.parse("show datasources", 4));
        Assert.assertEquals(ServerParseShow.DATASOURCES, ServerParseShow.parse("SHOW DATASOURCES", 4));
        Assert.assertEquals(ServerParseShow.DATASOURCES, ServerParseShow.parse("  SHOW   DATASOURCES  ", 6));
    }

    @Test
    public void testShowMycatStatus() {
        Assert.assertEquals(ServerParseShow.MYCAT_STATUS, ServerParseShow.parse("show mycat_status", 4));
        Assert.assertEquals(ServerParseShow.MYCAT_STATUS, ServerParseShow.parse("show mycat_status ", 4));
        Assert.assertEquals(ServerParseShow.MYCAT_STATUS, ServerParseShow.parse(" SHOW MYCAT_STATUS", " SHOW".length()));
        Assert.assertEquals(ServerParseShow.OTHER, ServerParseShow.parse(" show mycat_statu", " SHOW".length()));
        Assert.assertEquals(ServerParseShow.OTHER, ServerParseShow.parse(" show mycat_status2", " SHOW".length()));
        Assert.assertEquals(ServerParseShow.OTHER, ServerParseShow.parse("Show mycat_status2 ", "SHOW".length()));
    }

    @Test
    public void testShowMycatCluster() {
        Assert.assertEquals(ServerParseShow.MYCAT_CLUSTER, ServerParseShow.parse("show mycat_cluster", 4));
        Assert.assertEquals(ServerParseShow.MYCAT_CLUSTER, ServerParseShow.parse("Show mycat_CLUSTER ", 4));
        Assert.assertEquals(ServerParseShow.MYCAT_CLUSTER, ServerParseShow.parse(" show  MYCAT_cluster", 5));
        Assert.assertEquals(ServerParseShow.OTHER, ServerParseShow.parse(" show mycat_clust", 5));
        Assert.assertEquals(ServerParseShow.OTHER, ServerParseShow.parse(" show mycat_cluster2", 5));
        Assert.assertEquals(ServerParseShow.OTHER, ServerParseShow.parse("Show mycat_cluster9 ", 4));
    }

    @Test
    public void testIsShowOther() {
        Assert.assertEquals(ServerParseShow.OTHER, ServerParseShow.parse("show ...", 4));
        Assert.assertEquals(ServerParseShow.OTHER, ServerParseShow.parse("SHOW ...", 4));
        Assert.assertEquals(ServerParseShow.OTHER, ServerParseShow.parse("SHOW ... ", 4));
    }

    @Test
    public void testIsSetAutocommitOn() {
        Assert.assertEquals(ServerParseSet.AUTOCOMMIT_ON, ServerParseSet.parse("set autocommit=1", 3));
        Assert.assertEquals(ServerParseSet.AUTOCOMMIT_ON, ServerParseSet.parse("set autoCOMMIT = 1", 3));
        Assert.assertEquals(ServerParseSet.AUTOCOMMIT_ON, ServerParseSet.parse("SET AUTOCOMMIT=on", 3));
        Assert.assertEquals(ServerParseSet.AUTOCOMMIT_ON, ServerParseSet.parse("set autoCOMMIT = ON", 3));
    }

    @Test
    public void testIsSetAutocommitOff() {
        Assert.assertEquals(ServerParseSet.AUTOCOMMIT_OFF, ServerParseSet.parse("set autocommit=0", 3));
        Assert.assertEquals(ServerParseSet.AUTOCOMMIT_OFF, ServerParseSet.parse("SET AUTOCOMMIT= 0", 3));
        Assert.assertEquals(ServerParseSet.AUTOCOMMIT_OFF, ServerParseSet.parse("set autoCOMMIT =OFF", 3));
        Assert.assertEquals(ServerParseSet.AUTOCOMMIT_OFF, ServerParseSet.parse("set autoCOMMIT = off", 3));
    }

    @Test
    public void testIsSetNames() {
        Assert.assertEquals(ServerParseSet.NAMES, 0xff & ServerParseSet.parse("set names utf8", 3));
        Assert.assertEquals(ServerParseSet.NAMES, 0xff & ServerParseSet.parse("SET NAMES UTF8", 3));
        Assert.assertEquals(ServerParseSet.NAMES, 0xff & ServerParseSet.parse("set NAMES utf8", 3));
    }

    @Test
    public void testIsCharacterSetResults() {
        Assert.assertEquals(ServerParseSet.CHARACTER_SET_RESULTS,
                0xff & ServerParseSet.parse("SET character_set_results  = NULL", 3));
        Assert.assertEquals(ServerParseSet.CHARACTER_SET_RESULTS,
                0xff & ServerParseSet.parse("SET CHARACTER_SET_RESULTS= NULL", 3));
        Assert.assertEquals(ServerParseSet.CHARACTER_SET_RESULTS,
                0xff & ServerParseSet.parse("Set chARActer_SET_RESults =  NULL", 3));
        Assert.assertEquals(ServerParseSet.CHARACTER_SET_CONNECTION,
                0xff & ServerParseSet.parse("Set chARActer_SET_Connection =  NULL", 3));
        Assert.assertEquals(ServerParseSet.CHARACTER_SET_CLIENT,
                0xff & ServerParseSet.parse("Set chARActer_SET_client =  NULL", 3));
    }

    @Test
    public void testIsSetOther() {
        Assert.assertEquals(ServerParseSet.OTHER, ServerParseSet.parse("set ...", 3));
        Assert.assertEquals(ServerParseSet.OTHER, ServerParseSet.parse("SET ...", 3));
        Assert.assertEquals(ServerParseSet.OTHER, ServerParseSet.parse("sEt ...", 3));
    }

    @Test
    public void testIsKill() {
        Assert.assertEquals(ServerParse.KILL, 0xff & ServerParse.parse(" kill  ..."));
        Assert.assertEquals(ServerParse.KILL, 0xff & ServerParse.parse("kill 111111 ..."));
        Assert.assertEquals(ServerParse.KILL, 0xff & ServerParse.parse("KILL  1335505632"));
    }

    @Test
    public void testIsKillQuery() {
        Assert.assertEquals(ServerParse.KILL_QUERY, 0xff & ServerParse.parse(" kill query ..."));
        Assert.assertEquals(ServerParse.KILL_QUERY, 0xff & ServerParse.parse("kill   query 111111 ..."));
        Assert.assertEquals(ServerParse.KILL_QUERY, 0xff & ServerParse.parse("KILL QUERY 1335505632"));
    }

    @Test
    public void testIsSavepoint() {
        Assert.assertEquals(ServerParse.SAVEPOINT, ServerParse.parse(" savepoint  ..."));
        Assert.assertEquals(ServerParse.SAVEPOINT, ServerParse.parse("SAVEPOINT "));
        Assert.assertEquals(ServerParse.SAVEPOINT, ServerParse.parse(" SAVEpoint   a"));
    }

    @Test
    public void testIsUse() {
        Assert.assertEquals(ServerParse.USE, 0xff & ServerParse.parse(" use  ..."));
        Assert.assertEquals(ServerParse.USE, 0xff & ServerParse.parse("USE "));
        Assert.assertEquals(ServerParse.USE, 0xff & ServerParse.parse(" Use   a"));
    }

    @Test
    public void testIsStartTransaction() {
        Assert.assertEquals(ServerParseStart.TRANSACTION, ServerParseStart.parse(" start transaction  ...", 6));
        Assert.assertEquals(ServerParseStart.TRANSACTION, ServerParseStart.parse("START TRANSACTION", 5));
        Assert.assertEquals(ServerParseStart.TRANSACTION, ServerParseStart.parse(" staRT   TRANSaction  ", 6));
    }

    @Test
    public void testIsSelectVersionComment() {
        Assert.assertEquals(ServerParseSelect.VERSION_COMMENT,
                ServerParseSelect.parse(" select @@version_comment  ", 7));
        Assert.assertEquals(ServerParseSelect.VERSION_COMMENT, ServerParseSelect.parse("SELECT @@VERSION_COMMENT", 6));
        Assert.assertEquals(ServerParseSelect.VERSION_COMMENT,
                ServerParseSelect.parse(" selECT    @@VERSION_comment  ", 7));
    }

    @Test
    public void testIsSelectVersion() {
        Assert.assertEquals(ServerParseSelect.VERSION, ServerParseSelect.parse(" select version ()  ", 7));
        Assert.assertEquals(ServerParseSelect.VERSION, ServerParseSelect.parse("SELECT VERSION(  )", 6));
        Assert.assertEquals(ServerParseSelect.VERSION, ServerParseSelect.parse(" selECT    VERSION()  ", 7));
    }

    @Test
    public void testIsSelectDatabase() {
        Assert.assertEquals(ServerParseSelect.DATABASE, ServerParseSelect.parse(" select database()  ", 7));
        Assert.assertEquals(ServerParseSelect.DATABASE, ServerParseSelect.parse("SELECT DATABASE()", 6));
        Assert.assertEquals(ServerParseSelect.DATABASE, ServerParseSelect.parse(" selECT    DATABASE()  ", 7));
    }

    @Test
    public void testIsSelectUser() {
        Assert.assertEquals(ServerParseSelect.USER, ServerParseSelect.parse(" select user()  ", 7));
        Assert.assertEquals(ServerParseSelect.USER, ServerParseSelect.parse("SELECT USER()", 6));
        Assert.assertEquals(ServerParseSelect.USER, ServerParseSelect.parse(" selECT    USER()  ", 7));
    }

    @Test
    public void testTxReadUncommitted() {
        Assert.assertEquals(ServerParseSet.TX_READ_UNCOMMITTED,
                ServerParseSet.parse("  SET SESSION TRANSACTION ISOLATION LEVEL READ  UNCOMMITTED  ", "  SET".length()));
        Assert.assertEquals(ServerParseSet.TX_READ_UNCOMMITTED,
                ServerParseSet.parse(" set session transaction isolation level read  uncommitted  ", " SET".length()));
        Assert.assertEquals(ServerParseSet.TX_READ_UNCOMMITTED,
                ServerParseSet.parse(" set session transaCTION ISOLATION LEvel read  uncommitteD ", " SET".length()));
    }

    @Test
    public void testTxReadCommitted() {
        Assert.assertEquals(ServerParseSet.TX_READ_COMMITTED,
                ServerParseSet.parse("  SET SESSION TRANSACTION ISOLATION LEVEL READ  COMMITTED  ", "  SET".length()));
        Assert.assertEquals(ServerParseSet.TX_READ_COMMITTED,
                ServerParseSet.parse(" set session transaction isolation level read  committed  ", " SET".length()));
        Assert.assertEquals(ServerParseSet.TX_READ_COMMITTED,
                ServerParseSet.parse(" set session transaCTION ISOLATION LEVel read  committed ", " SET".length()));
    }

    @Test
    public void testTxRepeatedRead() {
        Assert.assertEquals(ServerParseSet.TX_REPEATED_READ,
                ServerParseSet.parse("  SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE   READ  ", "  SET".length()));
        Assert.assertEquals(ServerParseSet.TX_REPEATED_READ,
                ServerParseSet.parse(" set session transaction isolation level repeatable   read  ", " SET".length()));
        Assert.assertEquals(ServerParseSet.TX_REPEATED_READ,
                ServerParseSet.parse(" set session transaction isOLATION LEVEL REPEatable   read ", " SET".length()));
    }

    @Test
    public void testTxSerializable() {
        Assert.assertEquals(ServerParseSet.TX_SERIALIZABLE,
                ServerParseSet.parse("  SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE  ", "  SET".length()));
        Assert.assertEquals(ServerParseSet.TX_SERIALIZABLE,
                ServerParseSet.parse(" set session transaction   isolation level serializable  ", " SET".length()));
        Assert.assertEquals(ServerParseSet.TX_SERIALIZABLE,
                ServerParseSet.parse(" set session   transaction  isOLATION LEVEL SERIAlizable ", " SET".length()));
    }

    @Test
    public void testIdentity() {
        String stmt = "select @@identity";
        int indexAfterLastInsertIdFunc = ServerParseSelect.indexAfterIdentity(stmt, stmt.indexOf('i'));
        Assert.assertEquals(stmt.length(), indexAfterLastInsertIdFunc);
        Assert.assertEquals(ServerParseSelect.IDENTITY, ServerParseSelect.parse(stmt, 6));
        stmt = "select  @@identity as id";
        Assert.assertEquals(ServerParseSelect.IDENTITY, ServerParseSelect.parse(stmt, 6));
        stmt = "select  @@identitY  id";
        Assert.assertEquals(ServerParseSelect.IDENTITY, ServerParseSelect.parse(stmt, 6));
        stmt = "select  /*foo*/@@identitY  id";
        Assert.assertEquals(ServerParseSelect.IDENTITY, ServerParseSelect.parse(stmt, 6));
        stmt = "select/*foo*/ @@identitY  id";
        Assert.assertEquals(ServerParseSelect.IDENTITY, ServerParseSelect.parse(stmt, 6));
        stmt = "select/*foo*/ @@identitY As id";
        Assert.assertEquals(ServerParseSelect.IDENTITY, ServerParseSelect.parse(stmt, 6));

        stmt = "select  @@identity ,";
        Assert.assertEquals(ServerParseSelect.OTHER, ServerParseSelect.parse(stmt, 6));
        stmt = "select  @@identity as, ";
        Assert.assertEquals(ServerParseSelect.OTHER, ServerParseSelect.parse(stmt, 6));
        stmt = "select  @@identity as id  , ";
        Assert.assertEquals(ServerParseSelect.OTHER, ServerParseSelect.parse(stmt, 6));
        stmt = "select  @@identity ass id   ";
        Assert.assertEquals(ServerParseSelect.OTHER, ServerParseSelect.parse(stmt, 6));

    }

    @Test
    public void testLastInsertId() {
        String stmt = " last_insert_iD()";
        int indexAfterLastInsertIdFunc = ServerParseSelect.indexAfterLastInsertIdFunc(stmt, stmt.indexOf('l'));
        Assert.assertEquals(stmt.length(), indexAfterLastInsertIdFunc);
        stmt = " last_insert_iD ()";
        indexAfterLastInsertIdFunc = ServerParseSelect.indexAfterLastInsertIdFunc(stmt, stmt.indexOf('l'));
        Assert.assertEquals(stmt.length(), indexAfterLastInsertIdFunc);
        stmt = " last_insert_iD ( /**/ )";
        indexAfterLastInsertIdFunc = ServerParseSelect.indexAfterLastInsertIdFunc(stmt, stmt.indexOf('l'));
        Assert.assertEquals(stmt.length(), indexAfterLastInsertIdFunc);
        stmt = " last_insert_iD (  )  ";
        indexAfterLastInsertIdFunc = ServerParseSelect.indexAfterLastInsertIdFunc(stmt, stmt.indexOf('l'));
        Assert.assertEquals(stmt.lastIndexOf(')') + 1, indexAfterLastInsertIdFunc);
        stmt = " last_insert_id(  )";
        indexAfterLastInsertIdFunc = ServerParseSelect.indexAfterLastInsertIdFunc(stmt, stmt.indexOf('l'));
        Assert.assertEquals(stmt.lastIndexOf(')') + 1, indexAfterLastInsertIdFunc);
        stmt = "last_iNsert_id(  ) ";
        indexAfterLastInsertIdFunc = ServerParseSelect.indexAfterLastInsertIdFunc(stmt, stmt.indexOf('l'));
        Assert.assertEquals(stmt.lastIndexOf(')') + 1, indexAfterLastInsertIdFunc);
        stmt = " last_insert_iD";
        indexAfterLastInsertIdFunc = ServerParseSelect.indexAfterLastInsertIdFunc(stmt, stmt.indexOf('l'));
        Assert.assertEquals(-1, indexAfterLastInsertIdFunc);
        stmt = " last_insert_i     ";
        indexAfterLastInsertIdFunc = ServerParseSelect.indexAfterLastInsertIdFunc(stmt, stmt.indexOf('l'));
        Assert.assertEquals(-1, indexAfterLastInsertIdFunc);
        stmt = " last_insert_i    d ";
        indexAfterLastInsertIdFunc = ServerParseSelect.indexAfterLastInsertIdFunc(stmt, stmt.indexOf('l'));
        Assert.assertEquals(-1, indexAfterLastInsertIdFunc);
        stmt = " last_insert_id (     ";
        indexAfterLastInsertIdFunc = ServerParseSelect.indexAfterLastInsertIdFunc(stmt, stmt.indexOf('l'));
        Assert.assertEquals(-1, indexAfterLastInsertIdFunc);
        stmt = " last_insert_id(  d)     ";
        indexAfterLastInsertIdFunc = ServerParseSelect.indexAfterLastInsertIdFunc(stmt, stmt.indexOf('l'));
        Assert.assertEquals(-1, indexAfterLastInsertIdFunc);
        stmt = " last_insert_id(  ) d    ";
        indexAfterLastInsertIdFunc = ServerParseSelect.indexAfterLastInsertIdFunc(stmt, stmt.indexOf('l'));
        Assert.assertEquals(stmt.lastIndexOf(')') + 1, indexAfterLastInsertIdFunc);
        stmt = " last_insert_id(d)";
        indexAfterLastInsertIdFunc = ServerParseSelect.indexAfterLastInsertIdFunc(stmt, stmt.indexOf('l'));
        Assert.assertEquals(-1, indexAfterLastInsertIdFunc);
        stmt = " last_insert_id(#\r\nd) ";
        indexAfterLastInsertIdFunc = ServerParseSelect.indexAfterLastInsertIdFunc(stmt, stmt.indexOf('l'));
        Assert.assertEquals(-1, indexAfterLastInsertIdFunc);
        stmt = " last_insert_id(#\n\r) ";
        indexAfterLastInsertIdFunc = ServerParseSelect.indexAfterLastInsertIdFunc(stmt, stmt.indexOf('l'));
        Assert.assertEquals(stmt.lastIndexOf(')') + 1, indexAfterLastInsertIdFunc);
        stmt = " last_insert_id (#\n\r)";
        indexAfterLastInsertIdFunc = ServerParseSelect.indexAfterLastInsertIdFunc(stmt, stmt.indexOf('l'));
        Assert.assertEquals(stmt.lastIndexOf(')') + 1, indexAfterLastInsertIdFunc);
        stmt = " last_insert_id(#\n\r)";
        indexAfterLastInsertIdFunc = ServerParseSelect.indexAfterLastInsertIdFunc(stmt, stmt.indexOf('l'));
        Assert.assertEquals(stmt.lastIndexOf(')') + 1, indexAfterLastInsertIdFunc);

        stmt = "select last_insert_id(#\n\r)";
        Assert.assertEquals(ServerParseSelect.LAST_INSERT_ID, ServerParseSelect.parse(stmt, 6));
        stmt = "select last_insert_id(#\n\r) as id";
        Assert.assertEquals(ServerParseSelect.LAST_INSERT_ID, ServerParseSelect.parse(stmt, 6));
        stmt = "select last_insert_id(#\n\r) as `id`";
        Assert.assertEquals(ServerParseSelect.LAST_INSERT_ID, ServerParseSelect.parse(stmt, 6));
        stmt = "select last_insert_id(#\n\r) as 'id'";
        Assert.assertEquals(ServerParseSelect.LAST_INSERT_ID, ServerParseSelect.parse(stmt, 6));
        stmt = "select last_insert_id(#\n\r)  id";
        Assert.assertEquals(ServerParseSelect.LAST_INSERT_ID, ServerParseSelect.parse(stmt, 6));
        stmt = "select last_insert_id(#\n\r)  `id`";
        Assert.assertEquals(ServerParseSelect.LAST_INSERT_ID, ServerParseSelect.parse(stmt, 6));
        stmt = "select last_insert_id(#\n\r)  'id'";
        Assert.assertEquals(ServerParseSelect.LAST_INSERT_ID, ServerParseSelect.parse(stmt, 6));
        stmt = "select last_insert_id(#\n\r) a";
        Assert.assertEquals(ServerParseSelect.LAST_INSERT_ID, ServerParseSelect.parse(stmt, 6));
        // NOTE: this should be invalid, ignore this bug
        stmt = "select last_insert_id(#\n\r) as";
        Assert.assertEquals(ServerParseSelect.LAST_INSERT_ID, ServerParseSelect.parse(stmt, 6));
        stmt = "select last_insert_id(#\n\r) asd";
        Assert.assertEquals(ServerParseSelect.LAST_INSERT_ID, ServerParseSelect.parse(stmt, 6));
        // NOTE: this should be invalid, ignore this bug
        stmt = "select last_insert_id(#\n\r) as 777";
        Assert.assertEquals(ServerParseSelect.LAST_INSERT_ID, ServerParseSelect.parse(stmt, 6));
        // NOTE: this should be invalid, ignore this bug
        stmt = "select last_insert_id(#\n\r)  777";
        Assert.assertEquals(ServerParseSelect.LAST_INSERT_ID, ServerParseSelect.parse(stmt, 6));
        stmt = "select last_insert_id(#\n\r)as `77``7`";
        Assert.assertEquals(ServerParseSelect.LAST_INSERT_ID, ServerParseSelect.parse(stmt, 6));
        stmt = "select last_insert_id(#\n\r)ass";
        Assert.assertEquals(ServerParseSelect.LAST_INSERT_ID, ServerParseSelect.parse(stmt, 6));
        stmt = "select last_insert_id(#\n\r)as 'a'";
        Assert.assertEquals(ServerParseSelect.LAST_INSERT_ID, ServerParseSelect.parse(stmt, 6));
        stmt = "select last_insert_id(#\n\r)as 'a\\''";
        Assert.assertEquals(ServerParseSelect.LAST_INSERT_ID, ServerParseSelect.parse(stmt, 6));
        stmt = "select last_insert_id(#\n\r)as 'a'''";
        Assert.assertEquals(ServerParseSelect.LAST_INSERT_ID, ServerParseSelect.parse(stmt, 6));
        stmt = "select last_insert_id(#\n\r)as 'a\"'";
        Assert.assertEquals(ServerParseSelect.LAST_INSERT_ID, ServerParseSelect.parse(stmt, 6));
        stmt = "   select last_insert_id(#\n\r) As 'a\"'";
        Assert.assertEquals(ServerParseSelect.LAST_INSERT_ID, ServerParseSelect.parse(stmt, 9));

        stmt = "select last_insert_id(#\n\r)as 'a\"\\'";
        Assert.assertEquals(ServerParseSelect.OTHER, ServerParseSelect.parse(stmt, 6));
        stmt = "select last_insert_id(#\n\r)as `77``7` ,";
        Assert.assertEquals(ServerParseSelect.OTHER, ServerParseSelect.parse(stmt, 6));
        stmt = "select last_insert_id(#\n\r)as `77`7`";
        Assert.assertEquals(ServerParseSelect.OTHER, ServerParseSelect.parse(stmt, 6));
        stmt = "select last_insert_id(#\n\r) as,";
        Assert.assertEquals(ServerParseSelect.OTHER, ServerParseSelect.parse(stmt, 6));
        stmt = "select last_insert_id(#\n\r) ass a";
        Assert.assertEquals(ServerParseSelect.OTHER, ServerParseSelect.parse(stmt, 6));
        stmt = "select last_insert_id(#\n\r) as 'a";
        Assert.assertEquals(ServerParseSelect.OTHER, ServerParseSelect.parse(stmt, 6));
    }

}