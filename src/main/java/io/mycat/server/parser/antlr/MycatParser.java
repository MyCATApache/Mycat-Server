// Generated from C:\Users\Coollf\git\Mycat-Server\src\main\java\io\mycat\server\parser\antlr\Mycat.g4 by ANTLR 4.1
package io.mycat.server.parser.antlr;
import java.util.List;

import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.atn.ATN;
import org.antlr.v4.runtime.atn.ATNSimulator;
import org.antlr.v4.runtime.atn.ParserATNSimulator;
import org.antlr.v4.runtime.atn.PredictionContextCache;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;
import org.antlr.v4.runtime.tree.TerminalNode;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class MycatParser extends Parser {
	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		SET=1, CLEAR=2, WHERE=3, SHOW=4, KILL=5, RELOAD=6, ROLLBACK=7, SWITCH=8, 
		STOP=9, OFFLINE=10, ONLINE=11, BEGIN=12, COMMIT=13, DELETE=14, INSERT=15, 
		REPLACE=16, SELECT=17, START=18, UPDATE=19, SAVEPOINT=20, EXPLAIN=21, 
		USE=22, CREATE=23, TRUNCATE=24, DROP=25, QUERY=26, HELP=27, CALL=28, DESC=29, 
		DESCRIBE=30, LOAD=31, DATA=32, INFILE=33, MSQL_SYS_ID=34, NAMESPACE_ID=35, 
		EQN=36, COMMA=37, DOT=38, ASTERISK=39, RPAREN=40, LPAREN=41, RBRACK=42, 
		LBRACK=43, PLUS=44, MINUS=45, NEGATION=46, VERTBAR=47, BITAND=48, POWER_OP=49, 
		ID=50, NUM=51, STRING=52, COMMENT_LINE=53, COMMENT_PIECE=54, WS=55;
	public static final String[] tokenNames = {
		"<INVALID>", "SET", "CLEAR", "WHERE", "SHOW", "KILL", "RELOAD", "ROLLBACK", 
		"SWITCH", "STOP", "OFFLINE", "ONLINE", "BEGIN", "COMMIT", "DELETE", "INSERT", 
		"REPLACE", "SELECT", "START", "UPDATE", "SAVEPOINT", "EXPLAIN", "USE", 
		"CREATE", "TRUNCATE", "DROP", "QUERY", "HELP", "CALL", "DESC", "DESCRIBE", 
		"LOAD", "DATA", "INFILE", "MSQL_SYS_ID", "NAMESPACE_ID", "'='", "','", 
		"'.'", "'*'", "')'", "'('", "']'", "'['", "'+'", "'-'", "'~'", "'|'", 
		"'&'", "'^'", "ID", "NUM", "STRING", "COMMENT_LINE", "COMMENT_PIECE", 
		"WS"
	};
	public static final int
		RULE_mycat = 0, RULE_msql = 1, RULE_sql = 2, RULE_show_msql = 3, RULE_clear_msql = 4, 
		RULE_kill_msql = 5, RULE_reload_msql = 6, RULE_rollback_msql = 7, RULE_switch_msql = 8, 
		RULE_stop_msql = 9, RULE_offline_msql = 10, RULE_online_msql = 11, RULE_show_sql = 12, 
		RULE_begin_sql = 13, RULE_commit_sql = 14, RULE_rollback_sql = 15, RULE_delete_sql = 16, 
		RULE_insert_sql = 17, RULE_replace_sql = 18, RULE_select_sql = 19, RULE_kill_sql = 20, 
		RULE_set_sql = 21, RULE_start_sql = 22, RULE_update_sql = 23, RULE_savepoint_sql = 24, 
		RULE_use_sql = 25, RULE_explain_sql = 26, RULE_ddl_sql = 27, RULE_kill_query_sql = 28, 
		RULE_help_sql = 29, RULE_call_sql = 30, RULE_describe_sql = 31, RULE_load_data_infile_sql = 32, 
		RULE_mysql_comment_sql = 33;
	public static final String[] ruleNames = {
		"mycat", "msql", "sql", "show_msql", "clear_msql", "kill_msql", "reload_msql", 
		"rollback_msql", "switch_msql", "stop_msql", "offline_msql", "online_msql", 
		"show_sql", "begin_sql", "commit_sql", "rollback_sql", "delete_sql", "insert_sql", 
		"replace_sql", "select_sql", "kill_sql", "set_sql", "start_sql", "update_sql", 
		"savepoint_sql", "use_sql", "explain_sql", "ddl_sql", "kill_query_sql", 
		"help_sql", "call_sql", "describe_sql", "load_data_infile_sql", "mysql_comment_sql"
	};

	@Override
	public String getGrammarFileName() { return "Mycat.g4"; }

	@Override
	public String[] getTokenNames() { return tokenNames; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public ATN getATN() { return _ATN; }

	public MycatParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}
	public static class MycatContext extends ParserRuleContext {
		public SqlContext sql() {
			return getRuleContext(SqlContext.class,0);
		}
		public MsqlContext msql() {
			return getRuleContext(MsqlContext.class,0);
		}
		public MycatContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_mycat; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).enterMycat(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).exitMycat(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MycatVisitor ) return ((MycatVisitor<? extends T>)visitor).visitMycat(this);
			else return visitor.visitChildren(this);
		}
	}

	public final MycatContext mycat() throws RecognitionException {
		MycatContext _localctx = new MycatContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_mycat);
		try {
			setState(70);
			switch ( getInterpreter().adaptivePredict(_input,0,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(68); msql();
				}
				break;

			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(69); sql();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class MsqlContext extends ParserRuleContext {
		public Offline_msqlContext offline_msql() {
			return getRuleContext(Offline_msqlContext.class,0);
		}
		public Kill_msqlContext kill_msql() {
			return getRuleContext(Kill_msqlContext.class,0);
		}
		public Switch_msqlContext switch_msql() {
			return getRuleContext(Switch_msqlContext.class,0);
		}
		public Clear_msqlContext clear_msql() {
			return getRuleContext(Clear_msqlContext.class,0);
		}
		public Rollback_msqlContext rollback_msql() {
			return getRuleContext(Rollback_msqlContext.class,0);
		}
		public Show_msqlContext show_msql() {
			return getRuleContext(Show_msqlContext.class,0);
		}
		public Stop_msqlContext stop_msql() {
			return getRuleContext(Stop_msqlContext.class,0);
		}
		public Online_msqlContext online_msql() {
			return getRuleContext(Online_msqlContext.class,0);
		}
		public Reload_msqlContext reload_msql() {
			return getRuleContext(Reload_msqlContext.class,0);
		}
		public MsqlContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_msql; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).enterMsql(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).exitMsql(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MycatVisitor ) return ((MycatVisitor<? extends T>)visitor).visitMsql(this);
			else return visitor.visitChildren(this);
		}
	}

	public final MsqlContext msql() throws RecognitionException {
		MsqlContext _localctx = new MsqlContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_msql);
		try {
			setState(81);
			switch ( getInterpreter().adaptivePredict(_input,1,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(72); show_msql();
				}
				break;

			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(73); clear_msql();
				}
				break;

			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(74); kill_msql();
				}
				break;

			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(75); reload_msql();
				}
				break;

			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(76); rollback_msql();
				}
				break;

			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(77); switch_msql();
				}
				break;

			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(78); stop_msql();
				}
				break;

			case 8:
				enterOuterAlt(_localctx, 8);
				{
				setState(79); offline_msql();
				}
				break;

			case 9:
				enterOuterAlt(_localctx, 9);
				{
				setState(80); online_msql();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SqlContext extends ParserRuleContext {
		public Replace_sqlContext replace_sql() {
			return getRuleContext(Replace_sqlContext.class,0);
		}
		public Commit_sqlContext commit_sql() {
			return getRuleContext(Commit_sqlContext.class,0);
		}
		public Show_sqlContext show_sql() {
			return getRuleContext(Show_sqlContext.class,0);
		}
		public Select_sqlContext select_sql() {
			return getRuleContext(Select_sqlContext.class,0);
		}
		public Use_sqlContext use_sql() {
			return getRuleContext(Use_sqlContext.class,0);
		}
		public Update_sqlContext update_sql() {
			return getRuleContext(Update_sqlContext.class,0);
		}
		public Describe_sqlContext describe_sql() {
			return getRuleContext(Describe_sqlContext.class,0);
		}
		public Kill_query_sqlContext kill_query_sql() {
			return getRuleContext(Kill_query_sqlContext.class,0);
		}
		public Ddl_sqlContext ddl_sql() {
			return getRuleContext(Ddl_sqlContext.class,0);
		}
		public Delete_sqlContext delete_sql() {
			return getRuleContext(Delete_sqlContext.class,0);
		}
		public Set_sqlContext set_sql() {
			return getRuleContext(Set_sqlContext.class,0);
		}
		public Start_sqlContext start_sql() {
			return getRuleContext(Start_sqlContext.class,0);
		}
		public Begin_sqlContext begin_sql() {
			return getRuleContext(Begin_sqlContext.class,0);
		}
		public Rollback_sqlContext rollback_sql() {
			return getRuleContext(Rollback_sqlContext.class,0);
		}
		public Load_data_infile_sqlContext load_data_infile_sql() {
			return getRuleContext(Load_data_infile_sqlContext.class,0);
		}
		public Call_sqlContext call_sql() {
			return getRuleContext(Call_sqlContext.class,0);
		}
		public Explain_sqlContext explain_sql() {
			return getRuleContext(Explain_sqlContext.class,0);
		}
		public Kill_sqlContext kill_sql() {
			return getRuleContext(Kill_sqlContext.class,0);
		}
		public Help_sqlContext help_sql() {
			return getRuleContext(Help_sqlContext.class,0);
		}
		public Savepoint_sqlContext savepoint_sql() {
			return getRuleContext(Savepoint_sqlContext.class,0);
		}
		public Insert_sqlContext insert_sql() {
			return getRuleContext(Insert_sqlContext.class,0);
		}
		public SqlContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sql; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).enterSql(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).exitSql(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MycatVisitor ) return ((MycatVisitor<? extends T>)visitor).visitSql(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SqlContext sql() throws RecognitionException {
		SqlContext _localctx = new SqlContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_sql);
		try {
			setState(104);
			switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(83); begin_sql();
				}
				break;

			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(84); commit_sql();
				}
				break;

			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(85); rollback_sql();
				}
				break;

			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(86); delete_sql();
				}
				break;

			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(87); insert_sql();
				}
				break;

			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(88); replace_sql();
				}
				break;

			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(89); select_sql();
				}
				break;

			case 8:
				enterOuterAlt(_localctx, 8);
				{
				setState(90); set_sql();
				}
				break;

			case 9:
				enterOuterAlt(_localctx, 9);
				{
				setState(91); show_sql();
				}
				break;

			case 10:
				enterOuterAlt(_localctx, 10);
				{
				setState(92); start_sql();
				}
				break;

			case 11:
				enterOuterAlt(_localctx, 11);
				{
				setState(93); update_sql();
				}
				break;

			case 12:
				enterOuterAlt(_localctx, 12);
				{
				setState(94); kill_sql();
				}
				break;

			case 13:
				enterOuterAlt(_localctx, 13);
				{
				setState(95); savepoint_sql();
				}
				break;

			case 14:
				enterOuterAlt(_localctx, 14);
				{
				setState(96); use_sql();
				}
				break;

			case 15:
				enterOuterAlt(_localctx, 15);
				{
				setState(97); explain_sql();
				}
				break;

			case 16:
				enterOuterAlt(_localctx, 16);
				{
				setState(98); kill_query_sql();
				}
				break;

			case 17:
				enterOuterAlt(_localctx, 17);
				{
				setState(99); help_sql();
				}
				break;

			case 18:
				enterOuterAlt(_localctx, 18);
				{
				setState(100); call_sql();
				}
				break;

			case 19:
				enterOuterAlt(_localctx, 19);
				{
				setState(101); describe_sql();
				}
				break;

			case 20:
				enterOuterAlt(_localctx, 20);
				{
				setState(102); load_data_infile_sql();
				}
				break;

			case 21:
				enterOuterAlt(_localctx, 21);
				{
				setState(103); ddl_sql();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Show_msqlContext extends ParserRuleContext {
		public TerminalNode EQN() { return getToken(MycatParser.EQN, 0); }
		public TerminalNode WHERE() { return getToken(MycatParser.WHERE, 0); }
		public List<TerminalNode> ID() { return getTokens(MycatParser.ID); }
		public TerminalNode MSQL_SYS_ID() { return getToken(MycatParser.MSQL_SYS_ID, 0); }
		public TerminalNode SHOW() { return getToken(MycatParser.SHOW, 0); }
		public TerminalNode ID(int i) {
			return getToken(MycatParser.ID, i);
		}
		public Show_msqlContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_show_msql; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).enterShow_msql(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).exitShow_msql(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MycatVisitor ) return ((MycatVisitor<? extends T>)visitor).visitShow_msql(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Show_msqlContext show_msql() throws RecognitionException {
		Show_msqlContext _localctx = new Show_msqlContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_show_msql);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(106); match(SHOW);
			setState(107); match(MSQL_SYS_ID);
			setState(112);
			switch ( getInterpreter().adaptivePredict(_input,3,_ctx) ) {
			case 1:
				{
				setState(108); match(WHERE);
				setState(109); match(ID);
				setState(110); match(EQN);
				setState(111); match(ID);
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Clear_msqlContext extends ParserRuleContext {
		public TerminalNode EQN() { return getToken(MycatParser.EQN, 0); }
		public TerminalNode WHERE() { return getToken(MycatParser.WHERE, 0); }
		public List<TerminalNode> ID() { return getTokens(MycatParser.ID); }
		public TerminalNode MSQL_SYS_ID() { return getToken(MycatParser.MSQL_SYS_ID, 0); }
		public TerminalNode CLEAR() { return getToken(MycatParser.CLEAR, 0); }
		public TerminalNode NUM() { return getToken(MycatParser.NUM, 0); }
		public TerminalNode ID(int i) {
			return getToken(MycatParser.ID, i);
		}
		public Clear_msqlContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_clear_msql; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).enterClear_msql(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).exitClear_msql(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MycatVisitor ) return ((MycatVisitor<? extends T>)visitor).visitClear_msql(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Clear_msqlContext clear_msql() throws RecognitionException {
		Clear_msqlContext _localctx = new Clear_msqlContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_clear_msql);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(114); match(CLEAR);
			setState(115); match(MSQL_SYS_ID);
			setState(116); match(WHERE);
			setState(117); match(ID);
			setState(118); match(EQN);
			setState(119);
			_la = _input.LA(1);
			if ( !(_la==ID || _la==NUM) ) {
			_errHandler.recoverInline(this);
			}
			consume();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Kill_msqlContext extends ParserRuleContext {
		public List<TerminalNode> COMMA() { return getTokens(MycatParser.COMMA); }
		public List<TerminalNode> ID() { return getTokens(MycatParser.ID); }
		public TerminalNode KILL() { return getToken(MycatParser.KILL, 0); }
		public TerminalNode MSQL_SYS_ID() { return getToken(MycatParser.MSQL_SYS_ID, 0); }
		public TerminalNode COMMA(int i) {
			return getToken(MycatParser.COMMA, i);
		}
		public TerminalNode ID(int i) {
			return getToken(MycatParser.ID, i);
		}
		public Kill_msqlContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_kill_msql; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).enterKill_msql(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).exitKill_msql(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MycatVisitor ) return ((MycatVisitor<? extends T>)visitor).visitKill_msql(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Kill_msqlContext kill_msql() throws RecognitionException {
		Kill_msqlContext _localctx = new Kill_msqlContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_kill_msql);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(121); match(KILL);
			setState(122); match(MSQL_SYS_ID);
			setState(123); match(ID);
			setState(128);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,4,_ctx);
			while ( _alt!=2 && _alt!=-1 ) {
				if ( _alt==1 ) {
					{
					{
					setState(124); match(COMMA);
					setState(125); match(ID);
					}
					} 
				}
				setState(130);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,4,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Reload_msqlContext extends ParserRuleContext {
		public TerminalNode MSQL_SYS_ID() { return getToken(MycatParser.MSQL_SYS_ID, 0); }
		public TerminalNode RELOAD() { return getToken(MycatParser.RELOAD, 0); }
		public Reload_msqlContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_reload_msql; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).enterReload_msql(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).exitReload_msql(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MycatVisitor ) return ((MycatVisitor<? extends T>)visitor).visitReload_msql(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Reload_msqlContext reload_msql() throws RecognitionException {
		Reload_msqlContext _localctx = new Reload_msqlContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_reload_msql);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(131); match(RELOAD);
			setState(132); match(MSQL_SYS_ID);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Rollback_msqlContext extends ParserRuleContext {
		public TerminalNode ROLLBACK() { return getToken(MycatParser.ROLLBACK, 0); }
		public TerminalNode MSQL_SYS_ID() { return getToken(MycatParser.MSQL_SYS_ID, 0); }
		public Rollback_msqlContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rollback_msql; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).enterRollback_msql(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).exitRollback_msql(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MycatVisitor ) return ((MycatVisitor<? extends T>)visitor).visitRollback_msql(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Rollback_msqlContext rollback_msql() throws RecognitionException {
		Rollback_msqlContext _localctx = new Rollback_msqlContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_rollback_msql);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(134); match(ROLLBACK);
			setState(135); match(MSQL_SYS_ID);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Switch_msqlContext extends ParserRuleContext {
		public TerminalNode NAMESPACE_ID() { return getToken(MycatParser.NAMESPACE_ID, 0); }
		public TerminalNode MSQL_SYS_ID() { return getToken(MycatParser.MSQL_SYS_ID, 0); }
		public TerminalNode SWITCH() { return getToken(MycatParser.SWITCH, 0); }
		public Switch_msqlContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_switch_msql; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).enterSwitch_msql(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).exitSwitch_msql(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MycatVisitor ) return ((MycatVisitor<? extends T>)visitor).visitSwitch_msql(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Switch_msqlContext switch_msql() throws RecognitionException {
		Switch_msqlContext _localctx = new Switch_msqlContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_switch_msql);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(137); match(SWITCH);
			setState(138); match(MSQL_SYS_ID);
			setState(139); match(NAMESPACE_ID);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Stop_msqlContext extends ParserRuleContext {
		public TerminalNode NAMESPACE_ID() { return getToken(MycatParser.NAMESPACE_ID, 0); }
		public TerminalNode STOP() { return getToken(MycatParser.STOP, 0); }
		public TerminalNode MSQL_SYS_ID() { return getToken(MycatParser.MSQL_SYS_ID, 0); }
		public Stop_msqlContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_stop_msql; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).enterStop_msql(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).exitStop_msql(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MycatVisitor ) return ((MycatVisitor<? extends T>)visitor).visitStop_msql(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Stop_msqlContext stop_msql() throws RecognitionException {
		Stop_msqlContext _localctx = new Stop_msqlContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_stop_msql);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(141); match(STOP);
			setState(142); match(MSQL_SYS_ID);
			setState(143); match(NAMESPACE_ID);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Offline_msqlContext extends ParserRuleContext {
		public TerminalNode OFFLINE() { return getToken(MycatParser.OFFLINE, 0); }
		public Offline_msqlContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_offline_msql; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).enterOffline_msql(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).exitOffline_msql(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MycatVisitor ) return ((MycatVisitor<? extends T>)visitor).visitOffline_msql(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Offline_msqlContext offline_msql() throws RecognitionException {
		Offline_msqlContext _localctx = new Offline_msqlContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_offline_msql);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(145); match(OFFLINE);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Online_msqlContext extends ParserRuleContext {
		public TerminalNode ONLINE() { return getToken(MycatParser.ONLINE, 0); }
		public Online_msqlContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_online_msql; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).enterOnline_msql(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).exitOnline_msql(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MycatVisitor ) return ((MycatVisitor<? extends T>)visitor).visitOnline_msql(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Online_msqlContext online_msql() throws RecognitionException {
		Online_msqlContext _localctx = new Online_msqlContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_online_msql);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(147); match(ONLINE);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Show_sqlContext extends ParserRuleContext {
		public TerminalNode NAMESPACE_ID() { return getToken(MycatParser.NAMESPACE_ID, 0); }
		public TerminalNode ID() { return getToken(MycatParser.ID, 0); }
		public TerminalNode SHOW() { return getToken(MycatParser.SHOW, 0); }
		public Show_sqlContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_show_sql; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).enterShow_sql(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).exitShow_sql(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MycatVisitor ) return ((MycatVisitor<? extends T>)visitor).visitShow_sql(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Show_sqlContext show_sql() throws RecognitionException {
		Show_sqlContext _localctx = new Show_sqlContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_show_sql);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(149); match(SHOW);
			setState(150);
			_la = _input.LA(1);
			if ( !(_la==NAMESPACE_ID || _la==ID) ) {
			_errHandler.recoverInline(this);
			}
			consume();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Begin_sqlContext extends ParserRuleContext {
		public TerminalNode BEGIN() { return getToken(MycatParser.BEGIN, 0); }
		public Begin_sqlContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_begin_sql; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).enterBegin_sql(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).exitBegin_sql(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MycatVisitor ) return ((MycatVisitor<? extends T>)visitor).visitBegin_sql(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Begin_sqlContext begin_sql() throws RecognitionException {
		Begin_sqlContext _localctx = new Begin_sqlContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_begin_sql);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(152); match(BEGIN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Commit_sqlContext extends ParserRuleContext {
		public TerminalNode COMMIT() { return getToken(MycatParser.COMMIT, 0); }
		public Commit_sqlContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_commit_sql; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).enterCommit_sql(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).exitCommit_sql(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MycatVisitor ) return ((MycatVisitor<? extends T>)visitor).visitCommit_sql(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Commit_sqlContext commit_sql() throws RecognitionException {
		Commit_sqlContext _localctx = new Commit_sqlContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_commit_sql);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(154); match(COMMIT);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Rollback_sqlContext extends ParserRuleContext {
		public TerminalNode NAMESPACE_ID() { return getToken(MycatParser.NAMESPACE_ID, 0); }
		public TerminalNode ID() { return getToken(MycatParser.ID, 0); }
		public TerminalNode ROLLBACK() { return getToken(MycatParser.ROLLBACK, 0); }
		public Rollback_sqlContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rollback_sql; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).enterRollback_sql(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).exitRollback_sql(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MycatVisitor ) return ((MycatVisitor<? extends T>)visitor).visitRollback_sql(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Rollback_sqlContext rollback_sql() throws RecognitionException {
		Rollback_sqlContext _localctx = new Rollback_sqlContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_rollback_sql);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(156); match(ROLLBACK);
			setState(158);
			switch ( getInterpreter().adaptivePredict(_input,5,_ctx) ) {
			case 1:
				{
				setState(157);
				_la = _input.LA(1);
				if ( !(_la==NAMESPACE_ID || _la==ID) ) {
				_errHandler.recoverInline(this);
				}
				consume();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Delete_sqlContext extends ParserRuleContext {
		public TerminalNode DELETE() { return getToken(MycatParser.DELETE, 0); }
		public Delete_sqlContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_delete_sql; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).enterDelete_sql(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).exitDelete_sql(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MycatVisitor ) return ((MycatVisitor<? extends T>)visitor).visitDelete_sql(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Delete_sqlContext delete_sql() throws RecognitionException {
		Delete_sqlContext _localctx = new Delete_sqlContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_delete_sql);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(160); match(DELETE);
			setState(164);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,6,_ctx);
			while ( _alt!=1 && _alt!=-1 ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(161);
					matchWildcard();
					}
					} 
				}
				setState(166);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,6,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Insert_sqlContext extends ParserRuleContext {
		public TerminalNode INSERT() { return getToken(MycatParser.INSERT, 0); }
		public Insert_sqlContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_insert_sql; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).enterInsert_sql(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).exitInsert_sql(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MycatVisitor ) return ((MycatVisitor<? extends T>)visitor).visitInsert_sql(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Insert_sqlContext insert_sql() throws RecognitionException {
		Insert_sqlContext _localctx = new Insert_sqlContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_insert_sql);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(167); match(INSERT);
			setState(171);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,7,_ctx);
			while ( _alt!=1 && _alt!=-1 ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(168);
					matchWildcard();
					}
					} 
				}
				setState(173);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,7,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Replace_sqlContext extends ParserRuleContext {
		public TerminalNode REPLACE() { return getToken(MycatParser.REPLACE, 0); }
		public Replace_sqlContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_replace_sql; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).enterReplace_sql(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).exitReplace_sql(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MycatVisitor ) return ((MycatVisitor<? extends T>)visitor).visitReplace_sql(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Replace_sqlContext replace_sql() throws RecognitionException {
		Replace_sqlContext _localctx = new Replace_sqlContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_replace_sql);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(174); match(REPLACE);
			setState(178);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,8,_ctx);
			while ( _alt!=1 && _alt!=-1 ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(175);
					matchWildcard();
					}
					} 
				}
				setState(180);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,8,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Select_sqlContext extends ParserRuleContext {
		public TerminalNode SELECT() { return getToken(MycatParser.SELECT, 0); }
		public Select_sqlContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_select_sql; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).enterSelect_sql(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).exitSelect_sql(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MycatVisitor ) return ((MycatVisitor<? extends T>)visitor).visitSelect_sql(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Select_sqlContext select_sql() throws RecognitionException {
		Select_sqlContext _localctx = new Select_sqlContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_select_sql);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(181); match(SELECT);
			setState(185);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,9,_ctx);
			while ( _alt!=1 && _alt!=-1 ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(182);
					matchWildcard();
					}
					} 
				}
				setState(187);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,9,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Kill_sqlContext extends ParserRuleContext {
		public TerminalNode NAMESPACE_ID() { return getToken(MycatParser.NAMESPACE_ID, 0); }
		public TerminalNode ID() { return getToken(MycatParser.ID, 0); }
		public TerminalNode KILL() { return getToken(MycatParser.KILL, 0); }
		public Kill_sqlContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_kill_sql; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).enterKill_sql(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).exitKill_sql(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MycatVisitor ) return ((MycatVisitor<? extends T>)visitor).visitKill_sql(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Kill_sqlContext kill_sql() throws RecognitionException {
		Kill_sqlContext _localctx = new Kill_sqlContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_kill_sql);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(188); match(KILL);
			setState(189);
			_la = _input.LA(1);
			if ( !(_la==NAMESPACE_ID || _la==ID) ) {
			_errHandler.recoverInline(this);
			}
			consume();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Set_sqlContext extends ParserRuleContext {
		public TerminalNode SET() { return getToken(MycatParser.SET, 0); }
		public Set_sqlContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_set_sql; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).enterSet_sql(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).exitSet_sql(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MycatVisitor ) return ((MycatVisitor<? extends T>)visitor).visitSet_sql(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Set_sqlContext set_sql() throws RecognitionException {
		Set_sqlContext _localctx = new Set_sqlContext(_ctx, getState());
		enterRule(_localctx, 42, RULE_set_sql);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(191); match(SET);
			setState(195);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,10,_ctx);
			while ( _alt!=1 && _alt!=-1 ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(192);
					matchWildcard();
					}
					} 
				}
				setState(197);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,10,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Start_sqlContext extends ParserRuleContext {
		public TerminalNode START() { return getToken(MycatParser.START, 0); }
		public Start_sqlContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_start_sql; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).enterStart_sql(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).exitStart_sql(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MycatVisitor ) return ((MycatVisitor<? extends T>)visitor).visitStart_sql(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Start_sqlContext start_sql() throws RecognitionException {
		Start_sqlContext _localctx = new Start_sqlContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_start_sql);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(198); match(START);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Update_sqlContext extends ParserRuleContext {
		public TerminalNode UPDATE() { return getToken(MycatParser.UPDATE, 0); }
		public Update_sqlContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_update_sql; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).enterUpdate_sql(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).exitUpdate_sql(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MycatVisitor ) return ((MycatVisitor<? extends T>)visitor).visitUpdate_sql(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Update_sqlContext update_sql() throws RecognitionException {
		Update_sqlContext _localctx = new Update_sqlContext(_ctx, getState());
		enterRule(_localctx, 46, RULE_update_sql);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(200); match(UPDATE);
			setState(204);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,11,_ctx);
			while ( _alt!=1 && _alt!=-1 ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(201);
					matchWildcard();
					}
					} 
				}
				setState(206);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,11,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Savepoint_sqlContext extends ParserRuleContext {
		public TerminalNode NAMESPACE_ID() { return getToken(MycatParser.NAMESPACE_ID, 0); }
		public TerminalNode ID() { return getToken(MycatParser.ID, 0); }
		public TerminalNode SAVEPOINT() { return getToken(MycatParser.SAVEPOINT, 0); }
		public Savepoint_sqlContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_savepoint_sql; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).enterSavepoint_sql(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).exitSavepoint_sql(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MycatVisitor ) return ((MycatVisitor<? extends T>)visitor).visitSavepoint_sql(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Savepoint_sqlContext savepoint_sql() throws RecognitionException {
		Savepoint_sqlContext _localctx = new Savepoint_sqlContext(_ctx, getState());
		enterRule(_localctx, 48, RULE_savepoint_sql);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(207); match(SAVEPOINT);
			setState(208);
			_la = _input.LA(1);
			if ( !(_la==NAMESPACE_ID || _la==ID) ) {
			_errHandler.recoverInline(this);
			}
			consume();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Use_sqlContext extends ParserRuleContext {
		public TerminalNode NAMESPACE_ID() { return getToken(MycatParser.NAMESPACE_ID, 0); }
		public TerminalNode USE() { return getToken(MycatParser.USE, 0); }
		public TerminalNode ID() { return getToken(MycatParser.ID, 0); }
		public Use_sqlContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_use_sql; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).enterUse_sql(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).exitUse_sql(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MycatVisitor ) return ((MycatVisitor<? extends T>)visitor).visitUse_sql(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Use_sqlContext use_sql() throws RecognitionException {
		Use_sqlContext _localctx = new Use_sqlContext(_ctx, getState());
		enterRule(_localctx, 50, RULE_use_sql);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(210); match(USE);
			setState(211);
			_la = _input.LA(1);
			if ( !(_la==NAMESPACE_ID || _la==ID) ) {
			_errHandler.recoverInline(this);
			}
			consume();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Explain_sqlContext extends ParserRuleContext {
		public TerminalNode EXPLAIN() { return getToken(MycatParser.EXPLAIN, 0); }
		public Explain_sqlContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_explain_sql; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).enterExplain_sql(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).exitExplain_sql(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MycatVisitor ) return ((MycatVisitor<? extends T>)visitor).visitExplain_sql(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Explain_sqlContext explain_sql() throws RecognitionException {
		Explain_sqlContext _localctx = new Explain_sqlContext(_ctx, getState());
		enterRule(_localctx, 52, RULE_explain_sql);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(213); match(EXPLAIN);
			setState(217);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,12,_ctx);
			while ( _alt!=1 && _alt!=-1 ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(214);
					matchWildcard();
					}
					} 
				}
				setState(219);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,12,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Ddl_sqlContext extends ParserRuleContext {
		public TerminalNode CREATE() { return getToken(MycatParser.CREATE, 0); }
		public TerminalNode DROP() { return getToken(MycatParser.DROP, 0); }
		public TerminalNode TRUNCATE() { return getToken(MycatParser.TRUNCATE, 0); }
		public Ddl_sqlContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ddl_sql; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).enterDdl_sql(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).exitDdl_sql(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MycatVisitor ) return ((MycatVisitor<? extends T>)visitor).visitDdl_sql(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Ddl_sqlContext ddl_sql() throws RecognitionException {
		Ddl_sqlContext _localctx = new Ddl_sqlContext(_ctx, getState());
		enterRule(_localctx, 54, RULE_ddl_sql);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(220);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << CREATE) | (1L << TRUNCATE) | (1L << DROP))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			consume();
			setState(224);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,13,_ctx);
			while ( _alt!=1 && _alt!=-1 ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(221);
					matchWildcard();
					}
					} 
				}
				setState(226);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,13,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Kill_query_sqlContext extends ParserRuleContext {
		public TerminalNode NAMESPACE_ID() { return getToken(MycatParser.NAMESPACE_ID, 0); }
		public TerminalNode QUERY() { return getToken(MycatParser.QUERY, 0); }
		public TerminalNode ID() { return getToken(MycatParser.ID, 0); }
		public TerminalNode KILL() { return getToken(MycatParser.KILL, 0); }
		public Kill_query_sqlContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_kill_query_sql; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).enterKill_query_sql(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).exitKill_query_sql(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MycatVisitor ) return ((MycatVisitor<? extends T>)visitor).visitKill_query_sql(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Kill_query_sqlContext kill_query_sql() throws RecognitionException {
		Kill_query_sqlContext _localctx = new Kill_query_sqlContext(_ctx, getState());
		enterRule(_localctx, 56, RULE_kill_query_sql);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(227); match(KILL);
			setState(228); match(QUERY);
			setState(229);
			_la = _input.LA(1);
			if ( !(_la==NAMESPACE_ID || _la==ID) ) {
			_errHandler.recoverInline(this);
			}
			consume();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Help_sqlContext extends ParserRuleContext {
		public TerminalNode HELP() { return getToken(MycatParser.HELP, 0); }
		public Help_sqlContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_help_sql; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).enterHelp_sql(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).exitHelp_sql(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MycatVisitor ) return ((MycatVisitor<? extends T>)visitor).visitHelp_sql(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Help_sqlContext help_sql() throws RecognitionException {
		Help_sqlContext _localctx = new Help_sqlContext(_ctx, getState());
		enterRule(_localctx, 58, RULE_help_sql);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(231); match(HELP);
			setState(235);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,14,_ctx);
			while ( _alt!=1 && _alt!=-1 ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(232);
					matchWildcard();
					}
					} 
				}
				setState(237);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,14,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Call_sqlContext extends ParserRuleContext {
		public TerminalNode CALL() { return getToken(MycatParser.CALL, 0); }
		public Call_sqlContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_call_sql; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).enterCall_sql(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).exitCall_sql(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MycatVisitor ) return ((MycatVisitor<? extends T>)visitor).visitCall_sql(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Call_sqlContext call_sql() throws RecognitionException {
		Call_sqlContext _localctx = new Call_sqlContext(_ctx, getState());
		enterRule(_localctx, 60, RULE_call_sql);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(238); match(CALL);
			setState(242);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,15,_ctx);
			while ( _alt!=1 && _alt!=-1 ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(239);
					matchWildcard();
					}
					} 
				}
				setState(244);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,15,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Describe_sqlContext extends ParserRuleContext {
		public TerminalNode DESC() { return getToken(MycatParser.DESC, 0); }
		public TerminalNode DESCRIBE() { return getToken(MycatParser.DESCRIBE, 0); }
		public Describe_sqlContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_describe_sql; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).enterDescribe_sql(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).exitDescribe_sql(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MycatVisitor ) return ((MycatVisitor<? extends T>)visitor).visitDescribe_sql(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Describe_sqlContext describe_sql() throws RecognitionException {
		Describe_sqlContext _localctx = new Describe_sqlContext(_ctx, getState());
		enterRule(_localctx, 62, RULE_describe_sql);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(245);
			_la = _input.LA(1);
			if ( !(_la==DESC || _la==DESCRIBE) ) {
			_errHandler.recoverInline(this);
			}
			consume();
			setState(249);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,16,_ctx);
			while ( _alt!=1 && _alt!=-1 ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(246);
					matchWildcard();
					}
					} 
				}
				setState(251);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,16,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Load_data_infile_sqlContext extends ParserRuleContext {
		public TerminalNode INFILE() { return getToken(MycatParser.INFILE, 0); }
		public TerminalNode DATA() { return getToken(MycatParser.DATA, 0); }
		public TerminalNode LOAD() { return getToken(MycatParser.LOAD, 0); }
		public Load_data_infile_sqlContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_load_data_infile_sql; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).enterLoad_data_infile_sql(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).exitLoad_data_infile_sql(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MycatVisitor ) return ((MycatVisitor<? extends T>)visitor).visitLoad_data_infile_sql(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Load_data_infile_sqlContext load_data_infile_sql() throws RecognitionException {
		Load_data_infile_sqlContext _localctx = new Load_data_infile_sqlContext(_ctx, getState());
		enterRule(_localctx, 64, RULE_load_data_infile_sql);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(252); match(LOAD);
			setState(253); match(DATA);
			setState(254); match(INFILE);
			setState(258);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,17,_ctx);
			while ( _alt!=1 && _alt!=-1 ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(255);
					matchWildcard();
					}
					} 
				}
				setState(260);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,17,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Mysql_comment_sqlContext extends ParserRuleContext {
		public Mysql_comment_sqlContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_mysql_comment_sql; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).enterMysql_comment_sql(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MycatListener ) ((MycatListener)listener).exitMysql_comment_sql(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MycatVisitor ) return ((MycatVisitor<? extends T>)visitor).visitMysql_comment_sql(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Mysql_comment_sqlContext mysql_comment_sql() throws RecognitionException {
		Mysql_comment_sqlContext _localctx = new Mysql_comment_sqlContext(_ctx, getState());
		enterRule(_localctx, 66, RULE_mysql_comment_sql);
		try {
			enterOuterAlt(_localctx, 1);
			{
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static final String _serializedATN =
		"\3\uacf5\uee8c\u4f5d\u8b0d\u4a45\u78bd\u1b2f\u3378\39\u010a\4\2\t\2\4"+
		"\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
		"\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\3\2\3\2\5\2I\n\2\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3"+
		"\5\3T\n\3\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4"+
		"\3\4\3\4\3\4\3\4\3\4\3\4\5\4k\n\4\3\5\3\5\3\5\3\5\3\5\3\5\5\5s\n\5\3\6"+
		"\3\6\3\6\3\6\3\6\3\6\3\6\3\7\3\7\3\7\3\7\3\7\7\7\u0081\n\7\f\7\16\7\u0084"+
		"\13\7\3\b\3\b\3\b\3\t\3\t\3\t\3\n\3\n\3\n\3\n\3\13\3\13\3\13\3\13\3\f"+
		"\3\f\3\r\3\r\3\16\3\16\3\16\3\17\3\17\3\20\3\20\3\21\3\21\5\21\u00a1\n"+
		"\21\3\22\3\22\7\22\u00a5\n\22\f\22\16\22\u00a8\13\22\3\23\3\23\7\23\u00ac"+
		"\n\23\f\23\16\23\u00af\13\23\3\24\3\24\7\24\u00b3\n\24\f\24\16\24\u00b6"+
		"\13\24\3\25\3\25\7\25\u00ba\n\25\f\25\16\25\u00bd\13\25\3\26\3\26\3\26"+
		"\3\27\3\27\7\27\u00c4\n\27\f\27\16\27\u00c7\13\27\3\30\3\30\3\31\3\31"+
		"\7\31\u00cd\n\31\f\31\16\31\u00d0\13\31\3\32\3\32\3\32\3\33\3\33\3\33"+
		"\3\34\3\34\7\34\u00da\n\34\f\34\16\34\u00dd\13\34\3\35\3\35\7\35\u00e1"+
		"\n\35\f\35\16\35\u00e4\13\35\3\36\3\36\3\36\3\36\3\37\3\37\7\37\u00ec"+
		"\n\37\f\37\16\37\u00ef\13\37\3 \3 \7 \u00f3\n \f \16 \u00f6\13 \3!\3!"+
		"\7!\u00fa\n!\f!\16!\u00fd\13!\3\"\3\"\3\"\3\"\7\"\u0103\n\"\f\"\16\"\u0106"+
		"\13\"\3#\3#\3#\16\u00a6\u00ad\u00b4\u00bb\u00c5\u00ce\u00db\u00e2\u00ed"+
		"\u00f4\u00fb\u0104$\2\4\6\b\n\f\16\20\22\24\26\30\32\34\36 \"$&(*,.\60"+
		"\62\64\668:<>@BD\2\6\3\2\64\65\4\2%%\64\64\3\2\31\33\3\2\37 \u0113\2H"+
		"\3\2\2\2\4S\3\2\2\2\6j\3\2\2\2\bl\3\2\2\2\nt\3\2\2\2\f{\3\2\2\2\16\u0085"+
		"\3\2\2\2\20\u0088\3\2\2\2\22\u008b\3\2\2\2\24\u008f\3\2\2\2\26\u0093\3"+
		"\2\2\2\30\u0095\3\2\2\2\32\u0097\3\2\2\2\34\u009a\3\2\2\2\36\u009c\3\2"+
		"\2\2 \u009e\3\2\2\2\"\u00a2\3\2\2\2$\u00a9\3\2\2\2&\u00b0\3\2\2\2(\u00b7"+
		"\3\2\2\2*\u00be\3\2\2\2,\u00c1\3\2\2\2.\u00c8\3\2\2\2\60\u00ca\3\2\2\2"+
		"\62\u00d1\3\2\2\2\64\u00d4\3\2\2\2\66\u00d7\3\2\2\28\u00de\3\2\2\2:\u00e5"+
		"\3\2\2\2<\u00e9\3\2\2\2>\u00f0\3\2\2\2@\u00f7\3\2\2\2B\u00fe\3\2\2\2D"+
		"\u0107\3\2\2\2FI\5\4\3\2GI\5\6\4\2HF\3\2\2\2HG\3\2\2\2I\3\3\2\2\2JT\5"+
		"\b\5\2KT\5\n\6\2LT\5\f\7\2MT\5\16\b\2NT\5\20\t\2OT\5\22\n\2PT\5\24\13"+
		"\2QT\5\26\f\2RT\5\30\r\2SJ\3\2\2\2SK\3\2\2\2SL\3\2\2\2SM\3\2\2\2SN\3\2"+
		"\2\2SO\3\2\2\2SP\3\2\2\2SQ\3\2\2\2SR\3\2\2\2T\5\3\2\2\2Uk\5\34\17\2Vk"+
		"\5\36\20\2Wk\5 \21\2Xk\5\"\22\2Yk\5$\23\2Zk\5&\24\2[k\5(\25\2\\k\5,\27"+
		"\2]k\5\32\16\2^k\5.\30\2_k\5\60\31\2`k\5*\26\2ak\5\62\32\2bk\5\64\33\2"+
		"ck\5\66\34\2dk\5:\36\2ek\5<\37\2fk\5> \2gk\5@!\2hk\5B\"\2ik\58\35\2jU"+
		"\3\2\2\2jV\3\2\2\2jW\3\2\2\2jX\3\2\2\2jY\3\2\2\2jZ\3\2\2\2j[\3\2\2\2j"+
		"\\\3\2\2\2j]\3\2\2\2j^\3\2\2\2j_\3\2\2\2j`\3\2\2\2ja\3\2\2\2jb\3\2\2\2"+
		"jc\3\2\2\2jd\3\2\2\2je\3\2\2\2jf\3\2\2\2jg\3\2\2\2jh\3\2\2\2ji\3\2\2\2"+
		"k\7\3\2\2\2lm\7\6\2\2mr\7$\2\2no\7\5\2\2op\7\64\2\2pq\7&\2\2qs\7\64\2"+
		"\2rn\3\2\2\2rs\3\2\2\2s\t\3\2\2\2tu\7\4\2\2uv\7$\2\2vw\7\5\2\2wx\7\64"+
		"\2\2xy\7&\2\2yz\t\2\2\2z\13\3\2\2\2{|\7\7\2\2|}\7$\2\2}\u0082\7\64\2\2"+
		"~\177\7\'\2\2\177\u0081\7\64\2\2\u0080~\3\2\2\2\u0081\u0084\3\2\2\2\u0082"+
		"\u0080\3\2\2\2\u0082\u0083\3\2\2\2\u0083\r\3\2\2\2\u0084\u0082\3\2\2\2"+
		"\u0085\u0086\7\b\2\2\u0086\u0087\7$\2\2\u0087\17\3\2\2\2\u0088\u0089\7"+
		"\t\2\2\u0089\u008a\7$\2\2\u008a\21\3\2\2\2\u008b\u008c\7\n\2\2\u008c\u008d"+
		"\7$\2\2\u008d\u008e\7%\2\2\u008e\23\3\2\2\2\u008f\u0090\7\13\2\2\u0090"+
		"\u0091\7$\2\2\u0091\u0092\7%\2\2\u0092\25\3\2\2\2\u0093\u0094\7\f\2\2"+
		"\u0094\27\3\2\2\2\u0095\u0096\7\r\2\2\u0096\31\3\2\2\2\u0097\u0098\7\6"+
		"\2\2\u0098\u0099\t\3\2\2\u0099\33\3\2\2\2\u009a\u009b\7\16\2\2\u009b\35"+
		"\3\2\2\2\u009c\u009d\7\17\2\2\u009d\37\3\2\2\2\u009e\u00a0\7\t\2\2\u009f"+
		"\u00a1\t\3\2\2\u00a0\u009f\3\2\2\2\u00a0\u00a1\3\2\2\2\u00a1!\3\2\2\2"+
		"\u00a2\u00a6\7\20\2\2\u00a3\u00a5\13\2\2\2\u00a4\u00a3\3\2\2\2\u00a5\u00a8"+
		"\3\2\2\2\u00a6\u00a7\3\2\2\2\u00a6\u00a4\3\2\2\2\u00a7#\3\2\2\2\u00a8"+
		"\u00a6\3\2\2\2\u00a9\u00ad\7\21\2\2\u00aa\u00ac\13\2\2\2\u00ab\u00aa\3"+
		"\2\2\2\u00ac\u00af\3\2\2\2\u00ad\u00ae\3\2\2\2\u00ad\u00ab\3\2\2\2\u00ae"+
		"%\3\2\2\2\u00af\u00ad\3\2\2\2\u00b0\u00b4\7\22\2\2\u00b1\u00b3\13\2\2"+
		"\2\u00b2\u00b1\3\2\2\2\u00b3\u00b6\3\2\2\2\u00b4\u00b5\3\2\2\2\u00b4\u00b2"+
		"\3\2\2\2\u00b5\'\3\2\2\2\u00b6\u00b4\3\2\2\2\u00b7\u00bb\7\23\2\2\u00b8"+
		"\u00ba\13\2\2\2\u00b9\u00b8\3\2\2\2\u00ba\u00bd\3\2\2\2\u00bb\u00bc\3"+
		"\2\2\2\u00bb\u00b9\3\2\2\2\u00bc)\3\2\2\2\u00bd\u00bb\3\2\2\2\u00be\u00bf"+
		"\7\7\2\2\u00bf\u00c0\t\3\2\2\u00c0+\3\2\2\2\u00c1\u00c5\7\3\2\2\u00c2"+
		"\u00c4\13\2\2\2\u00c3\u00c2\3\2\2\2\u00c4\u00c7\3\2\2\2\u00c5\u00c6\3"+
		"\2\2\2\u00c5\u00c3\3\2\2\2\u00c6-\3\2\2\2\u00c7\u00c5\3\2\2\2\u00c8\u00c9"+
		"\7\24\2\2\u00c9/\3\2\2\2\u00ca\u00ce\7\25\2\2\u00cb\u00cd\13\2\2\2\u00cc"+
		"\u00cb\3\2\2\2\u00cd\u00d0\3\2\2\2\u00ce\u00cf\3\2\2\2\u00ce\u00cc\3\2"+
		"\2\2\u00cf\61\3\2\2\2\u00d0\u00ce\3\2\2\2\u00d1\u00d2\7\26\2\2\u00d2\u00d3"+
		"\t\3\2\2\u00d3\63\3\2\2\2\u00d4\u00d5\7\30\2\2\u00d5\u00d6\t\3\2\2\u00d6"+
		"\65\3\2\2\2\u00d7\u00db\7\27\2\2\u00d8\u00da\13\2\2\2\u00d9\u00d8\3\2"+
		"\2\2\u00da\u00dd\3\2\2\2\u00db\u00dc\3\2\2\2\u00db\u00d9\3\2\2\2\u00dc"+
		"\67\3\2\2\2\u00dd\u00db\3\2\2\2\u00de\u00e2\t\4\2\2\u00df\u00e1\13\2\2"+
		"\2\u00e0\u00df\3\2\2\2\u00e1\u00e4\3\2\2\2\u00e2\u00e3\3\2\2\2\u00e2\u00e0"+
		"\3\2\2\2\u00e39\3\2\2\2\u00e4\u00e2\3\2\2\2\u00e5\u00e6\7\7\2\2\u00e6"+
		"\u00e7\7\34\2\2\u00e7\u00e8\t\3\2\2\u00e8;\3\2\2\2\u00e9\u00ed\7\35\2"+
		"\2\u00ea\u00ec\13\2\2\2\u00eb\u00ea\3\2\2\2\u00ec\u00ef\3\2\2\2\u00ed"+
		"\u00ee\3\2\2\2\u00ed\u00eb\3\2\2\2\u00ee=\3\2\2\2\u00ef\u00ed\3\2\2\2"+
		"\u00f0\u00f4\7\36\2\2\u00f1\u00f3\13\2\2\2\u00f2\u00f1\3\2\2\2\u00f3\u00f6"+
		"\3\2\2\2\u00f4\u00f5\3\2\2\2\u00f4\u00f2\3\2\2\2\u00f5?\3\2\2\2\u00f6"+
		"\u00f4\3\2\2\2\u00f7\u00fb\t\5\2\2\u00f8\u00fa\13\2\2\2\u00f9\u00f8\3"+
		"\2\2\2\u00fa\u00fd\3\2\2\2\u00fb\u00fc\3\2\2\2\u00fb\u00f9\3\2\2\2\u00fc"+
		"A\3\2\2\2\u00fd\u00fb\3\2\2\2\u00fe\u00ff\7!\2\2\u00ff\u0100\7\"\2\2\u0100"+
		"\u0104\7#\2\2\u0101\u0103\13\2\2\2\u0102\u0101\3\2\2\2\u0103\u0106\3\2"+
		"\2\2\u0104\u0105\3\2\2\2\u0104\u0102\3\2\2\2\u0105C\3\2\2\2\u0106\u0104"+
		"\3\2\2\2\u0107\u0108\3\2\2\2\u0108E\3\2\2\2\24HSjr\u0082\u00a0\u00a6\u00ad"+
		"\u00b4\u00bb\u00c5\u00ce\u00db\u00e2\u00ed\u00f4\u00fb\u0104";
	public static final ATN _ATN =
		ATNSimulator.deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}