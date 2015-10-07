package io.mycat.server.parser;

import io.mycat.server.parser.MycatServerParse.SqlType;
import io.mycat.server.parser.antlr.MycatBaseListener;
import io.mycat.server.parser.antlr.MycatLexer;
import io.mycat.server.parser.antlr.MycatParser;
import io.mycat.server.parser.antlr.MycatParser.Begin_sqlContext;
import io.mycat.server.parser.antlr.MycatParser.Call_sqlContext;
import io.mycat.server.parser.antlr.MycatParser.Clear_msqlContext;
import io.mycat.server.parser.antlr.MycatParser.Commit_sqlContext;
import io.mycat.server.parser.antlr.MycatParser.Ddl_sqlContext;
import io.mycat.server.parser.antlr.MycatParser.Delete_sqlContext;
import io.mycat.server.parser.antlr.MycatParser.Describe_sqlContext;
import io.mycat.server.parser.antlr.MycatParser.Explain_sqlContext;
import io.mycat.server.parser.antlr.MycatParser.Help_sqlContext;
import io.mycat.server.parser.antlr.MycatParser.Insert_sqlContext;
import io.mycat.server.parser.antlr.MycatParser.Kill_msqlContext;
import io.mycat.server.parser.antlr.MycatParser.Kill_query_sqlContext;
import io.mycat.server.parser.antlr.MycatParser.Kill_sqlContext;
import io.mycat.server.parser.antlr.MycatParser.Load_data_infile_sqlContext;
import io.mycat.server.parser.antlr.MycatParser.Mysql_comment_sqlContext;
import io.mycat.server.parser.antlr.MycatParser.Offline_msqlContext;
import io.mycat.server.parser.antlr.MycatParser.Online_msqlContext;
import io.mycat.server.parser.antlr.MycatParser.Reload_msqlContext;
import io.mycat.server.parser.antlr.MycatParser.Replace_sqlContext;
import io.mycat.server.parser.antlr.MycatParser.Rollback_msqlContext;
import io.mycat.server.parser.antlr.MycatParser.Rollback_sqlContext;
import io.mycat.server.parser.antlr.MycatParser.Savepoint_sqlContext;
import io.mycat.server.parser.antlr.MycatParser.Select_sqlContext;
import io.mycat.server.parser.antlr.MycatParser.Set_sqlContext;
import io.mycat.server.parser.antlr.MycatParser.Show_msqlContext;
import io.mycat.server.parser.antlr.MycatParser.Show_sqlContext;
import io.mycat.server.parser.antlr.MycatParser.Start_sqlContext;
import io.mycat.server.parser.antlr.MycatParser.Stop_msqlContext;
import io.mycat.server.parser.antlr.MycatParser.Switch_msqlContext;
import io.mycat.server.parser.antlr.MycatParser.Update_sqlContext;
import io.mycat.server.parser.antlr.MycatParser.Use_sqlContext;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;

/***************
 * Mycat Server sql type parse server
 * 
 * @author Coollf
 *
 */
public class MycatServerParse {

	public static class SqlType {

		// mysql sql types
		public static final int OTHER = -1;
		public static final int BEGIN = 1;
		public static final int COMMIT = 2;
		public static final int DELETE = 3;
		public static final int INSERT = 4;
		public static final int REPLACE = 5;
		public static final int ROLLBACK = 6;
		public static final int SELECT = 7;
		public static final int SET = 8;
		public static final int SHOW = 9;
		public static final int START = 10;
		public static final int UPDATE = 11;
		public static final int KILL = 12;
		public static final int SAVEPOINT = 13;
		public static final int USE = 14;
		public static final int EXPLAIN = 15;
		public static final int KILL_QUERY = 16;
		public static final int HELP = 17;
		public static final int MYSQL_CMD_COMMENT = 18;
		public static final int MYSQL_COMMENT = 19;
		public static final int CALL = 20;
		public static final int DESCRIBE = 21;
		public static final int LOAD_DATA_INFILE_SQL = 99;
		public static final int DDL = 100;

		// Mycat manager sql types
		public static final int MGR_SHOW = 10003;
		public static final int MGR_SWITCH = 10004;
		public static final int MGR_KILL_CONN = 10005;
		public static final int MGR_STOP = 10006;
		public static final int MGR_RELOAD = 10007;
		public static final int MGR_ROLLBACK = 10008;
		public static final int MGR_OFFLINE = 10009;
		public static final int MGR_ONLINE = 10010;
		public static final int MGR_CLEAR = 10011;
		public static final int MGR_CONFIGFILE = 10012;
		public static final int MGR_LOGFILE = 10013;

	}

	public static int parse(String stmt) {
		ANTLRInputStream in = new ANTLRInputStream(stmt);
		MycatLexer lexer = new MycatLexer(in);
		CommonTokenStream tokens = new CommonTokenStream(lexer);
		MycatParser parser = new MycatParser(tokens);
		MycatSqlTypeListener listener = new MycatSqlTypeListener();
		parser.addParseListener(listener);
		parser.mycat();
		return listener.getSqlType();
	}
}

class MycatSqlTypeListener extends MycatBaseListener {
	private int sqlType = SqlType.OTHER;

	public int getSqlType() {
		return sqlType;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.mycat.server.parser.antlr.MycatBaseListener#exitHelp_sql(io.mycat
	 * .server.parser.antlr.MycatParser.Help_sqlContext)
	 */
	@Override
	public void exitHelp_sql(Help_sqlContext ctx) {
		this.sqlType = SqlType.HELP;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * io.mycat.server.parser.antlr.MycatBaseListener#exitStart_sql(io.mycat
	 * .server.parser.antlr.MycatParser.Start_sqlContext)
	 */
	@Override
	public void exitStart_sql(Start_sqlContext ctx) {
		this.sqlType = SqlType.START;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.mycat.server.parser.antlr.MycatBaseListener#exitShow_sql(io.mycat
	 * .server.parser.antlr.MycatParser.Show_sqlContext)
	 */
	@Override
	public void exitShow_sql(Show_sqlContext ctx) {
		this.sqlType = SqlType.SHOW;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.mycat.server.parser.antlr.MycatBaseListener#exitCommit_sql(io.
	 * mycat.server.parser.antlr.MycatParser.Commit_sqlContext)
	 */
	@Override
	public void exitCommit_sql(Commit_sqlContext ctx) {
		this.sqlType = SqlType.COMMIT;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.mycat.server.parser.antlr.MycatBaseListener#exitDdl_sql(io.mycat
	 * .server.parser.antlr.MycatParser.Ddl_sqlContext)
	 */
	@Override
	public void exitDdl_sql(Ddl_sqlContext ctx) {
		this.sqlType = SqlType.DDL;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.mycat.server.parser.antlr.MycatBaseListener#exitExplain_sql(io
	 * .mycat.server.parser.antlr.MycatParser.Explain_sqlContext)
	 */
	@Override
	public void exitExplain_sql(Explain_sqlContext ctx) {
		this.sqlType = SqlType.EXPLAIN;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.mycat.server.parser.antlr.MycatBaseListener#exitReplace_sql(io
	 * .mycat.server.parser.antlr.MycatParser.Replace_sqlContext)
	 */
	@Override
	public void exitReplace_sql(Replace_sqlContext ctx) {
		this.sqlType = SqlType.REPLACE;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.mycat.server.parser.antlr.MycatBaseListener#exitDescribe_sql(io
	 * .mycat.server.parser.antlr.MycatParser.Describe_sqlContext)
	 */
	@Override
	public void exitDescribe_sql(Describe_sqlContext ctx) {
		this.sqlType = SqlType.DESCRIBE;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * io.mycat.server.parser.antlr.MycatBaseListener#exitBegin_sql(io.mycat
	 * .server.parser.antlr.MycatParser.Begin_sqlContext)
	 */
	@Override
	public void exitBegin_sql(Begin_sqlContext ctx) {
		this.sqlType = SqlType.BEGIN;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.mycat.server.parser.antlr.MycatBaseListener#exitDelete_sql(io.
	 * mycat.server.parser.antlr.MycatParser.Delete_sqlContext)
	 */
	@Override
	public void exitDelete_sql(Delete_sqlContext ctx) {
		this.sqlType = SqlType.DELETE;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.mycat.server.parser.antlr.MycatBaseListener#exitSavepoint_sql(
	 * io.mycat.server.parser.antlr.MycatParser.Savepoint_sqlContext)
	 */
	@Override
	public void exitSavepoint_sql(Savepoint_sqlContext ctx) {
		this.sqlType = SqlType.SAVEPOINT;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.mycat.server.parser.antlr.MycatBaseListener#exitUse_sql(io.mycat
	 * .server.parser.antlr.MycatParser.Use_sqlContext)
	 */
	@Override
	public void exitUse_sql(Use_sqlContext ctx) {
		this.sqlType = SqlType.USE;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.mycat.server.parser.antlr.MycatBaseListener#exitCall_sql(io.mycat
	 * .server.parser.antlr.MycatParser.Call_sqlContext)
	 */
	@Override
	public void exitCall_sql(Call_sqlContext ctx) {
		this.sqlType = SqlType.CALL;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.mycat.server.parser.antlr.MycatBaseListener#exitSelect_sql(io.
	 * mycat.server.parser.antlr.MycatParser.Select_sqlContext)
	 */
	@Override
	public void exitSelect_sql(Select_sqlContext ctx) {
		this.sqlType = SqlType.SELECT;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.mycat.server.parser.antlr.MycatBaseListener#exitKill_query_sql
	 * (io.mycat.server.parser.antlr.MycatParser.Kill_query_sqlContext)
	 */
	@Override
	public void exitKill_query_sql(Kill_query_sqlContext ctx) {
		this.sqlType = SqlType.KILL_QUERY;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.mycat.server.parser.antlr.MycatBaseListener#exitKill_sql(io.mycat
	 * .server.parser.antlr.MycatParser.Kill_sqlContext)
	 */
	@Override
	public void exitKill_sql(Kill_sqlContext ctx) {
		this.sqlType = SqlType.KILL;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.mycat.server.parser.antlr.MycatBaseListener#exitSet_sql(io.mycat
	 * .server.parser.antlr.MycatParser.Set_sqlContext)
	 */
	@Override
	public void exitSet_sql(Set_sqlContext ctx) {
		this.sqlType = SqlType.SET;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.mycat.server.parser.antlr.MycatBaseListener#exitUpdate_sql(io.
	 * mycat.server.parser.antlr.MycatParser.Update_sqlContext)
	 */
	@Override
	public void exitUpdate_sql(Update_sqlContext ctx) {
		this.sqlType = SqlType.UPDATE;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.mycat.server.parser.antlr.MycatBaseListener#exitInsert_sql(io.
	 * mycat.server.parser.antlr.MycatParser.Insert_sqlContext)
	 */
	@Override
	public void exitInsert_sql(Insert_sqlContext ctx) {
		this.sqlType = SqlType.INSERT;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.mycat.server.parser.antlr.MycatBaseListener#exitRollback_sql(io
	 * .mycat.server.parser.antlr.MycatParser.Rollback_sqlContext)
	 */
	@Override
	public void exitRollback_sql(Rollback_sqlContext ctx) {
		this.sqlType = SqlType.ROLLBACK;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * io.mycat.server.parser.antlr.MycatBaseListener#exitLoad_data_infile_sql (
	 * io.mycat.server.parser.antlr.MycatParser.Load_data_infile_sqlContext)
	 */
	@Override
	public void exitLoad_data_infile_sql(Load_data_infile_sqlContext ctx) {
		this.sqlType = SqlType.LOAD_DATA_INFILE_SQL;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.mycat.server.parser.antlr.MycatBaseListener#exitMysql_comment_sql
	 * (io.mycat.server.parser.antlr.MycatParser.Mysql_comment_sqlContext)
	 */
	@Override
	public void exitMysql_comment_sql(Mysql_comment_sqlContext ctx) {
		this.sqlType = SqlType.MYSQL_COMMENT;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.mycat.server.parser.antlr.MycatBaseListener#exitReload_msql(io
	 * .mycat.server.parser.antlr.MycatParser.Reload_msqlContext)
	 */
	@Override
	public void exitReload_msql(Reload_msqlContext ctx) {
		this.sqlType = SqlType.MGR_RELOAD;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * io.mycat.server.parser.antlr.MycatBaseListener#exitShow_msql(io.mycat
	 * .server.parser.antlr.MycatParser.Show_msqlContext)
	 */
	@Override
	public void exitShow_msql(Show_msqlContext ctx) {
		this.sqlType = SqlType.MGR_SHOW;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.mycat.server.parser.antlr.MycatBaseListener#exitSwitch_msql(io
	 * .mycat.server.parser.antlr.MycatParser.Switch_msqlContext)
	 */
	@Override
	public void exitSwitch_msql(Switch_msqlContext ctx) {
		this.sqlType = SqlType.MGR_SWITCH;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * io.mycat.server.parser.antlr.MycatBaseListener#exitStop_msql(io.mycat
	 * .server.parser.antlr.MycatParser.Stop_msqlContext)
	 */
	@Override
	public void exitStop_msql(Stop_msqlContext ctx) {
		this.sqlType = SqlType.MGR_STOP;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * io.mycat.server.parser.antlr.MycatBaseListener#exitKill_msql(io.mycat
	 * .server.parser.antlr.MycatParser.Kill_msqlContext)
	 */
	@Override
	public void exitKill_msql(Kill_msqlContext ctx) {
		this.sqlType = SqlType.MGR_KILL_CONN;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.mycat.server.parser.antlr.MycatBaseListener#exitRollback_msql(
	 * io.mycat.server.parser.antlr.MycatParser.Rollback_msqlContext)
	 */
	@Override
	public void exitRollback_msql(Rollback_msqlContext ctx) {
		this.sqlType = SqlType.MGR_ROLLBACK;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.mycat.server.parser.antlr.MycatBaseListener#exitClear_msql(io.
	 * mycat.server.parser.antlr.MycatParser.Clear_msqlContext)
	 */
	@Override
	public void exitClear_msql(Clear_msqlContext ctx) {
		this.sqlType = SqlType.MGR_CLEAR;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.mycat.server.parser.antlr.MycatBaseListener#exitOnline_msql(io
	 * .mycat.server.parser.antlr.MycatParser.Online_msqlContext)
	 */
	@Override
	public void exitOnline_msql(Online_msqlContext ctx) {
		this.sqlType = SqlType.MGR_ONLINE;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.mycat.server.parser.antlr.MycatBaseListener#exitOffline_msql(io
	 * .mycat.server.parser.antlr.MycatParser.Offline_msqlContext)
	 */
	@Override
	public void exitOffline_msql(Offline_msqlContext ctx) {
		this.sqlType = SqlType.MGR_OFFLINE;
	}

}
