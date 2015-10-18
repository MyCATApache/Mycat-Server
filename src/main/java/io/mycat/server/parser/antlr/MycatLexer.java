// Generated from C:\Users\Coollf\git\Mycat-Server\src\main\java\io\mycat\server\parser\antlr\Mycat.g4 by ANTLR 4.1
package io.mycat.server.parser.antlr;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.atn.ATN;
import org.antlr.v4.runtime.atn.ATNSimulator;
import org.antlr.v4.runtime.atn.LexerATNSimulator;
import org.antlr.v4.runtime.atn.PredictionContextCache;
import org.antlr.v4.runtime.dfa.DFA;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class MycatLexer extends Lexer {
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
	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	public static final String[] tokenNames = {
		"<INVALID>",
		"SET", "CLEAR", "WHERE", "SHOW", "KILL", "RELOAD", "ROLLBACK", "SWITCH", 
		"STOP", "OFFLINE", "ONLINE", "BEGIN", "COMMIT", "DELETE", "INSERT", "REPLACE", 
		"SELECT", "START", "UPDATE", "SAVEPOINT", "EXPLAIN", "USE", "CREATE", 
		"TRUNCATE", "DROP", "QUERY", "HELP", "CALL", "DESC", "DESCRIBE", "LOAD", 
		"DATA", "INFILE", "MSQL_SYS_ID", "NAMESPACE_ID", "'='", "','", "'.'", 
		"'*'", "')'", "'('", "']'", "'['", "'+'", "'-'", "'~'", "'|'", "'&'", 
		"'^'", "ID", "NUM", "STRING", "COMMENT_LINE", "COMMENT_PIECE", "WS"
	};
	public static final String[] ruleNames = {
		"SET", "CLEAR", "WHERE", "SHOW", "KILL", "RELOAD", "ROLLBACK", "SWITCH", 
		"STOP", "OFFLINE", "ONLINE", "BEGIN", "COMMIT", "DELETE", "INSERT", "REPLACE", 
		"SELECT", "START", "UPDATE", "SAVEPOINT", "EXPLAIN", "USE", "CREATE", 
		"TRUNCATE", "DROP", "QUERY", "HELP", "CALL", "DESC", "DESCRIBE", "LOAD", 
		"DATA", "INFILE", "MSQL_SYS_ID", "NAMESPACE_ID", "EQN", "COMMA", "DOT", 
		"ASTERISK", "RPAREN", "LPAREN", "RBRACK", "LBRACK", "PLUS", "MINUS", "NEGATION", 
		"VERTBAR", "BITAND", "POWER_OP", "ID", "NUM", "STRING", "COMMENT_LINE", 
		"COMMENT_PIECE", "WS"
	};


	public MycatLexer(CharStream input) {
		super(input);
		_interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@Override
	public String getGrammarFileName() { return "Mycat.g4"; }

	@Override
	public String[] getTokenNames() { return tokenNames; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String[] getModeNames() { return modeNames; }

	@Override
	public ATN getATN() { return _ATN; }

	@Override
	public void action(RuleContext _localctx, int ruleIndex, int actionIndex) {
		switch (ruleIndex) {
		case 52: COMMENT_LINE_action((RuleContext)_localctx, actionIndex); break;

		case 53: COMMENT_PIECE_action((RuleContext)_localctx, actionIndex); break;

		case 54: WS_action((RuleContext)_localctx, actionIndex); break;
		}
	}
	private void COMMENT_PIECE_action(RuleContext _localctx, int actionIndex) {
		switch (actionIndex) {
		case 1: skip();  break;
		}
	}
	private void WS_action(RuleContext _localctx, int actionIndex) {
		switch (actionIndex) {
		case 2: skip();  break;
		}
	}
	private void COMMENT_LINE_action(RuleContext _localctx, int actionIndex) {
		switch (actionIndex) {
		case 0: skip();  break;
		}
	}

	public static final String _serializedATN =
		"\3\uacf5\uee8c\u4f5d\u8b0d\u4a45\u78bd\u1b2f\u3378\29\u01c0\b\1\4\2\t"+
		"\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
		",\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64\t"+
		"\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\3\2\3\2\3\2\3\2\3\3\3\3\3\3\3"+
		"\3\3\3\3\3\3\4\3\4\3\4\3\4\3\4\3\4\3\5\3\5\3\5\3\5\3\5\3\6\3\6\3\6\3\6"+
		"\3\6\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3"+
		"\t\3\t\3\t\3\t\3\t\3\t\3\t\3\n\3\n\3\n\3\n\3\n\3\13\3\13\3\13\3\13\3\13"+
		"\3\13\3\13\3\13\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\5\f\u00bc"+
		"\n\f\3\r\3\r\3\r\3\r\3\r\3\r\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\17\3"+
		"\17\3\17\3\17\3\17\3\17\3\17\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\21\3"+
		"\21\3\21\3\21\3\21\3\21\3\21\3\21\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3"+
		"\23\3\23\3\23\3\23\3\23\3\23\3\24\3\24\3\24\3\24\3\24\3\24\3\24\3\25\3"+
		"\25\3\25\3\25\3\25\3\25\3\25\3\25\3\25\3\25\3\26\3\26\3\26\3\26\3\26\3"+
		"\26\3\26\3\26\3\27\3\27\3\27\3\27\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3"+
		"\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\32\3\32\3\32\3\32\3\32\3"+
		"\33\3\33\3\33\3\33\3\33\3\33\3\34\3\34\3\34\3\34\3\34\5\34\u012b\n\34"+
		"\3\35\3\35\3\35\3\35\3\35\3\36\3\36\3\36\3\36\3\36\3\37\3\37\3\37\3\37"+
		"\3\37\3\37\3\37\3\37\3\37\3 \3 \3 \3 \3 \3!\3!\3!\3!\3!\3\"\3\"\3\"\3"+
		"\"\3\"\3\"\3\"\3#\3#\3#\3#\3#\5#\u0156\n#\3$\3$\3$\6$\u015b\n$\r$\16$"+
		"\u015c\3%\3%\3&\3&\3\'\3\'\3(\3(\3)\3)\3*\3*\3+\3+\3,\3,\3-\3-\3.\3.\3"+
		"/\3/\3\60\3\60\3\61\3\61\3\62\3\62\3\63\3\63\7\63\u017d\n\63\f\63\16\63"+
		"\u0180\13\63\3\64\6\64\u0183\n\64\r\64\16\64\u0184\3\64\3\64\6\64\u0189"+
		"\n\64\r\64\16\64\u018a\5\64\u018d\n\64\3\65\3\65\7\65\u0191\n\65\f\65"+
		"\16\65\u0194\13\65\3\65\3\65\3\66\3\66\3\66\5\66\u019b\n\66\3\66\7\66"+
		"\u019e\n\66\f\66\16\66\u01a1\13\66\3\66\5\66\u01a4\n\66\3\66\3\66\5\66"+
		"\u01a8\n\66\3\66\3\66\3\67\3\67\3\67\3\67\7\67\u01b0\n\67\f\67\16\67\u01b3"+
		"\13\67\3\67\3\67\3\67\3\67\3\67\38\68\u01bb\n8\r8\168\u01bc\38\38\4\u0192"+
		"\u01b19\3\3\1\5\4\1\7\5\1\t\6\1\13\7\1\r\b\1\17\t\1\21\n\1\23\13\1\25"+
		"\f\1\27\r\1\31\16\1\33\17\1\35\20\1\37\21\1!\22\1#\23\1%\24\1\'\25\1)"+
		"\26\1+\27\1-\30\1/\31\1\61\32\1\63\33\1\65\34\1\67\35\19\36\1;\37\1= "+
		"\1?!\1A\"\1C#\1E$\1G%\1I&\1K\'\1M(\1O)\1Q*\1S+\1U,\1W-\1Y.\1[/\1]\60\1"+
		"_\61\1a\62\1c\63\1e\64\1g\65\1i\66\1k\67\2m8\3o9\4\3\2\37\4\2UUuu\4\2"+
		"GGgg\4\2VVvv\4\2EEee\4\2NNnn\4\2CCcc\4\2TTtt\4\2YYyy\4\2JJjj\4\2QQqq\4"+
		"\2MMmm\4\2KKkk\4\2FFff\4\2DDdd\4\2RRrr\4\2HHhh\4\2PPpp\4\2IIii\4\2OOo"+
		"o\4\2WWww\4\2XXxx\4\2ZZzz\4\2SSss\4\2[[{{\4\2\60\60<<\6\2C\\aac|\u4e02"+
		"\u9fa7\7\2\62;C\\aac|\u4e02\u9fa7\4\2\f\f\17\17\6\2\13\f\17\17\"\"\u3002"+
		"\u3002\u01ce\2\3\3\2\2\2\2\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2"+
		"\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2\21\3\2\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2"+
		"\27\3\2\2\2\2\31\3\2\2\2\2\33\3\2\2\2\2\35\3\2\2\2\2\37\3\2\2\2\2!\3\2"+
		"\2\2\2#\3\2\2\2\2%\3\2\2\2\2\'\3\2\2\2\2)\3\2\2\2\2+\3\2\2\2\2-\3\2\2"+
		"\2\2/\3\2\2\2\2\61\3\2\2\2\2\63\3\2\2\2\2\65\3\2\2\2\2\67\3\2\2\2\29\3"+
		"\2\2\2\2;\3\2\2\2\2=\3\2\2\2\2?\3\2\2\2\2A\3\2\2\2\2C\3\2\2\2\2E\3\2\2"+
		"\2\2G\3\2\2\2\2I\3\2\2\2\2K\3\2\2\2\2M\3\2\2\2\2O\3\2\2\2\2Q\3\2\2\2\2"+
		"S\3\2\2\2\2U\3\2\2\2\2W\3\2\2\2\2Y\3\2\2\2\2[\3\2\2\2\2]\3\2\2\2\2_\3"+
		"\2\2\2\2a\3\2\2\2\2c\3\2\2\2\2e\3\2\2\2\2g\3\2\2\2\2i\3\2\2\2\2k\3\2\2"+
		"\2\2m\3\2\2\2\2o\3\2\2\2\3q\3\2\2\2\5u\3\2\2\2\7{\3\2\2\2\t\u0081\3\2"+
		"\2\2\13\u0086\3\2\2\2\r\u008b\3\2\2\2\17\u0092\3\2\2\2\21\u009b\3\2\2"+
		"\2\23\u00a2\3\2\2\2\25\u00a7\3\2\2\2\27\u00bb\3\2\2\2\31\u00bd\3\2\2\2"+
		"\33\u00c3\3\2\2\2\35\u00ca\3\2\2\2\37\u00d1\3\2\2\2!\u00d8\3\2\2\2#\u00e0"+
		"\3\2\2\2%\u00e7\3\2\2\2\'\u00ed\3\2\2\2)\u00f4\3\2\2\2+\u00fe\3\2\2\2"+
		"-\u0106\3\2\2\2/\u010a\3\2\2\2\61\u0111\3\2\2\2\63\u011a\3\2\2\2\65\u011f"+
		"\3\2\2\2\67\u012a\3\2\2\29\u012c\3\2\2\2;\u0131\3\2\2\2=\u0136\3\2\2\2"+
		"?\u013f\3\2\2\2A\u0144\3\2\2\2C\u0149\3\2\2\2E\u0150\3\2\2\2G\u0157\3"+
		"\2\2\2I\u015e\3\2\2\2K\u0160\3\2\2\2M\u0162\3\2\2\2O\u0164\3\2\2\2Q\u0166"+
		"\3\2\2\2S\u0168\3\2\2\2U\u016a\3\2\2\2W\u016c\3\2\2\2Y\u016e\3\2\2\2["+
		"\u0170\3\2\2\2]\u0172\3\2\2\2_\u0174\3\2\2\2a\u0176\3\2\2\2c\u0178\3\2"+
		"\2\2e\u017a\3\2\2\2g\u0182\3\2\2\2i\u018e\3\2\2\2k\u019a\3\2\2\2m\u01ab"+
		"\3\2\2\2o\u01ba\3\2\2\2qr\t\2\2\2rs\t\3\2\2st\t\4\2\2t\4\3\2\2\2uv\t\5"+
		"\2\2vw\t\6\2\2wx\t\3\2\2xy\t\7\2\2yz\t\b\2\2z\6\3\2\2\2{|\t\t\2\2|}\t"+
		"\n\2\2}~\t\3\2\2~\177\t\b\2\2\177\u0080\t\3\2\2\u0080\b\3\2\2\2\u0081"+
		"\u0082\t\2\2\2\u0082\u0083\t\n\2\2\u0083\u0084\t\13\2\2\u0084\u0085\t"+
		"\t\2\2\u0085\n\3\2\2\2\u0086\u0087\t\f\2\2\u0087\u0088\t\r\2\2\u0088\u0089"+
		"\t\6\2\2\u0089\u008a\t\6\2\2\u008a\f\3\2\2\2\u008b\u008c\t\b\2\2\u008c"+
		"\u008d\t\3\2\2\u008d\u008e\t\6\2\2\u008e\u008f\t\13\2\2\u008f\u0090\t"+
		"\7\2\2\u0090\u0091\t\16\2\2\u0091\16\3\2\2\2\u0092\u0093\t\b\2\2\u0093"+
		"\u0094\t\13\2\2\u0094\u0095\t\6\2\2\u0095\u0096\t\6\2\2\u0096\u0097\t"+
		"\17\2\2\u0097\u0098\t\7\2\2\u0098\u0099\t\5\2\2\u0099\u009a\t\f\2\2\u009a"+
		"\20\3\2\2\2\u009b\u009c\t\2\2\2\u009c\u009d\t\t\2\2\u009d\u009e\t\r\2"+
		"\2\u009e\u009f\t\4\2\2\u009f\u00a0\t\5\2\2\u00a0\u00a1\t\n\2\2\u00a1\22"+
		"\3\2\2\2\u00a2\u00a3\t\2\2\2\u00a3\u00a4\t\4\2\2\u00a4\u00a5\t\13\2\2"+
		"\u00a5\u00a6\t\20\2\2\u00a6\24\3\2\2\2\u00a7\u00a8\t\13\2\2\u00a8\u00a9"+
		"\t\21\2\2\u00a9\u00aa\t\21\2\2\u00aa\u00ab\t\6\2\2\u00ab\u00ac\t\r\2\2"+
		"\u00ac\u00ad\t\22\2\2\u00ad\u00ae\t\3\2\2\u00ae\26\3\2\2\2\u00af\u00b0"+
		"\7q\2\2\u00b0\u00b1\7p\2\2\u00b1\u00b2\7n\2\2\u00b2\u00b3\7k\2\2\u00b3"+
		"\u00b4\7p\2\2\u00b4\u00bc\7g\2\2\u00b5\u00b6\7Q\2\2\u00b6\u00b7\7P\2\2"+
		"\u00b7\u00b8\7N\2\2\u00b8\u00b9\7K\2\2\u00b9\u00ba\7P\2\2\u00ba\u00bc"+
		"\7G\2\2\u00bb\u00af\3\2\2\2\u00bb\u00b5\3\2\2\2\u00bc\30\3\2\2\2\u00bd"+
		"\u00be\t\17\2\2\u00be\u00bf\t\3\2\2\u00bf\u00c0\t\23\2\2\u00c0\u00c1\t"+
		"\r\2\2\u00c1\u00c2\t\22\2\2\u00c2\32\3\2\2\2\u00c3\u00c4\t\5\2\2\u00c4"+
		"\u00c5\t\13\2\2\u00c5\u00c6\t\24\2\2\u00c6\u00c7\t\24\2\2\u00c7\u00c8"+
		"\t\r\2\2\u00c8\u00c9\t\4\2\2\u00c9\34\3\2\2\2\u00ca\u00cb\t\16\2\2\u00cb"+
		"\u00cc\t\3\2\2\u00cc\u00cd\t\6\2\2\u00cd\u00ce\t\3\2\2\u00ce\u00cf\t\4"+
		"\2\2\u00cf\u00d0\t\3\2\2\u00d0\36\3\2\2\2\u00d1\u00d2\t\r\2\2\u00d2\u00d3"+
		"\t\22\2\2\u00d3\u00d4\t\2\2\2\u00d4\u00d5\t\3\2\2\u00d5\u00d6\t\b\2\2"+
		"\u00d6\u00d7\t\4\2\2\u00d7 \3\2\2\2\u00d8\u00d9\t\b\2\2\u00d9\u00da\t"+
		"\3\2\2\u00da\u00db\t\20\2\2\u00db\u00dc\t\6\2\2\u00dc\u00dd\t\7\2\2\u00dd"+
		"\u00de\t\5\2\2\u00de\u00df\t\3\2\2\u00df\"\3\2\2\2\u00e0\u00e1\t\2\2\2"+
		"\u00e1\u00e2\t\3\2\2\u00e2\u00e3\t\6\2\2\u00e3\u00e4\t\3\2\2\u00e4\u00e5"+
		"\t\5\2\2\u00e5\u00e6\t\4\2\2\u00e6$\3\2\2\2\u00e7\u00e8\t\2\2\2\u00e8"+
		"\u00e9\t\4\2\2\u00e9\u00ea\t\7\2\2\u00ea\u00eb\t\b\2\2\u00eb\u00ec\t\4"+
		"\2\2\u00ec&\3\2\2\2\u00ed\u00ee\t\25\2\2\u00ee\u00ef\t\20\2\2\u00ef\u00f0"+
		"\t\16\2\2\u00f0\u00f1\t\7\2\2\u00f1\u00f2\t\4\2\2\u00f2\u00f3\t\3\2\2"+
		"\u00f3(\3\2\2\2\u00f4\u00f5\t\2\2\2\u00f5\u00f6\t\7\2\2\u00f6\u00f7\t"+
		"\26\2\2\u00f7\u00f8\t\3\2\2\u00f8\u00f9\t\20\2\2\u00f9\u00fa\t\13\2\2"+
		"\u00fa\u00fb\t\r\2\2\u00fb\u00fc\t\22\2\2\u00fc\u00fd\t\4\2\2\u00fd*\3"+
		"\2\2\2\u00fe\u00ff\t\3\2\2\u00ff\u0100\t\27\2\2\u0100\u0101\t\20\2\2\u0101"+
		"\u0102\t\6\2\2\u0102\u0103\t\7\2\2\u0103\u0104\t\r\2\2\u0104\u0105\t\22"+
		"\2\2\u0105,\3\2\2\2\u0106\u0107\t\25\2\2\u0107\u0108\t\2\2\2\u0108\u0109"+
		"\t\3\2\2\u0109.\3\2\2\2\u010a\u010b\t\5\2\2\u010b\u010c\t\b\2\2\u010c"+
		"\u010d\t\3\2\2\u010d\u010e\t\7\2\2\u010e\u010f\t\4\2\2\u010f\u0110\t\3"+
		"\2\2\u0110\60\3\2\2\2\u0111\u0112\t\4\2\2\u0112\u0113\t\b\2\2\u0113\u0114"+
		"\t\25\2\2\u0114\u0115\t\22\2\2\u0115\u0116\t\5\2\2\u0116\u0117\t\7\2\2"+
		"\u0117\u0118\t\4\2\2\u0118\u0119\t\3\2\2\u0119\62\3\2\2\2\u011a\u011b"+
		"\t\16\2\2\u011b\u011c\t\b\2\2\u011c\u011d\t\13\2\2\u011d\u011e\t\20\2"+
		"\2\u011e\64\3\2\2\2\u011f\u0120\t\30\2\2\u0120\u0121\t\25\2\2\u0121\u0122"+
		"\t\3\2\2\u0122\u0123\t\b\2\2\u0123\u0124\t\31\2\2\u0124\66\3\2\2\2\u0125"+
		"\u012b\7A\2\2\u0126\u0127\t\n\2\2\u0127\u0128\t\3\2\2\u0128\u0129\t\6"+
		"\2\2\u0129\u012b\t\20\2\2\u012a\u0125\3\2\2\2\u012a\u0126\3\2\2\2\u012b"+
		"8\3\2\2\2\u012c\u012d\t\5\2\2\u012d\u012e\t\7\2\2\u012e\u012f\t\6\2\2"+
		"\u012f\u0130\t\6\2\2\u0130:\3\2\2\2\u0131\u0132\t\16\2\2\u0132\u0133\t"+
		"\3\2\2\u0133\u0134\t\2\2\2\u0134\u0135\t\5\2\2\u0135<\3\2\2\2\u0136\u0137"+
		"\t\16\2\2\u0137\u0138\t\3\2\2\u0138\u0139\t\2\2\2\u0139\u013a\t\5\2\2"+
		"\u013a\u013b\t\b\2\2\u013b\u013c\t\r\2\2\u013c\u013d\t\17\2\2\u013d\u013e"+
		"\t\3\2\2\u013e>\3\2\2\2\u013f\u0140\t\6\2\2\u0140\u0141\t\13\2\2\u0141"+
		"\u0142\t\7\2\2\u0142\u0143\t\16\2\2\u0143@\3\2\2\2\u0144\u0145\t\16\2"+
		"\2\u0145\u0146\t\7\2\2\u0146\u0147\t\4\2\2\u0147\u0148\t\7\2\2\u0148B"+
		"\3\2\2\2\u0149\u014a\t\r\2\2\u014a\u014b\t\22\2\2\u014b\u014c\t\21\2\2"+
		"\u014c\u014d\t\r\2\2\u014d\u014e\t\6\2\2\u014e\u014f\t\3\2\2\u014fD\3"+
		"\2\2\2\u0150\u0151\7B\2\2\u0151\u0152\7B\2\2\u0152\u0155\3\2\2\2\u0153"+
		"\u0156\5e\63\2\u0154\u0156\5G$\2\u0155\u0153\3\2\2\2\u0155\u0154\3\2\2"+
		"\2\u0156F\3\2\2\2\u0157\u015a\5e\63\2\u0158\u0159\t\32\2\2\u0159\u015b"+
		"\5e\63\2\u015a\u0158\3\2\2\2\u015b\u015c\3\2\2\2\u015c\u015a\3\2\2\2\u015c"+
		"\u015d\3\2\2\2\u015dH\3\2\2\2\u015e\u015f\7?\2\2\u015fJ\3\2\2\2\u0160"+
		"\u0161\7.\2\2\u0161L\3\2\2\2\u0162\u0163\7\60\2\2\u0163N\3\2\2\2\u0164"+
		"\u0165\7,\2\2\u0165P\3\2\2\2\u0166\u0167\7+\2\2\u0167R\3\2\2\2\u0168\u0169"+
		"\7*\2\2\u0169T\3\2\2\2\u016a\u016b\7_\2\2\u016bV\3\2\2\2\u016c\u016d\7"+
		"]\2\2\u016dX\3\2\2\2\u016e\u016f\7-\2\2\u016fZ\3\2\2\2\u0170\u0171\7/"+
		"\2\2\u0171\\\3\2\2\2\u0172\u0173\7\u0080\2\2\u0173^\3\2\2\2\u0174\u0175"+
		"\7~\2\2\u0175`\3\2\2\2\u0176\u0177\7(\2\2\u0177b\3\2\2\2\u0178\u0179\7"+
		"`\2\2\u0179d\3\2\2\2\u017a\u017e\t\33\2\2\u017b\u017d\t\34\2\2\u017c\u017b"+
		"\3\2\2\2\u017d\u0180\3\2\2\2\u017e\u017c\3\2\2\2\u017e\u017f\3\2\2\2\u017f"+
		"f\3\2\2\2\u0180\u017e\3\2\2\2\u0181\u0183\4\62;\2\u0182\u0181\3\2\2\2"+
		"\u0183\u0184\3\2\2\2\u0184\u0182\3\2\2\2\u0184\u0185\3\2\2\2\u0185\u018c"+
		"\3\2\2\2\u0186\u0188\7\60\2\2\u0187\u0189\4\62;\2\u0188\u0187\3\2\2\2"+
		"\u0189\u018a\3\2\2\2\u018a\u0188\3\2\2\2\u018a\u018b\3\2\2\2\u018b\u018d"+
		"\3\2\2\2\u018c\u0186\3\2\2\2\u018c\u018d\3\2\2\2\u018dh\3\2\2\2\u018e"+
		"\u0192\7)\2\2\u018f\u0191\13\2\2\2\u0190\u018f\3\2\2\2\u0191\u0194\3\2"+
		"\2\2\u0192\u0193\3\2\2\2\u0192\u0190\3\2\2\2\u0193\u0195\3\2\2\2\u0194"+
		"\u0192\3\2\2\2\u0195\u0196\7)\2\2\u0196j\3\2\2\2\u0197\u0198\7/\2\2\u0198"+
		"\u019b\7/\2\2\u0199\u019b\7%\2\2\u019a\u0197\3\2\2\2\u019a\u0199\3\2\2"+
		"\2\u019b\u019f\3\2\2\2\u019c\u019e\n\35\2\2\u019d\u019c\3\2\2\2\u019e"+
		"\u01a1\3\2\2\2\u019f\u019d\3\2\2\2\u019f\u01a0\3\2\2\2\u01a0\u01a7\3\2"+
		"\2\2\u01a1\u019f\3\2\2\2\u01a2\u01a4\7\17\2\2\u01a3\u01a2\3\2\2\2\u01a3"+
		"\u01a4\3\2\2\2\u01a4\u01a5\3\2\2\2\u01a5\u01a8\7\f\2\2\u01a6\u01a8\7\2"+
		"\2\3\u01a7\u01a3\3\2\2\2\u01a7\u01a6\3\2\2\2\u01a8\u01a9\3\2\2\2\u01a9"+
		"\u01aa\b\66\2\2\u01aal\3\2\2\2\u01ab\u01ac\7\61\2\2\u01ac\u01ad\7,\2\2"+
		"\u01ad\u01b1\3\2\2\2\u01ae\u01b0\13\2\2\2\u01af\u01ae\3\2\2\2\u01b0\u01b3"+
		"\3\2\2\2\u01b1\u01b2\3\2\2\2\u01b1\u01af\3\2\2\2\u01b2\u01b4\3\2\2\2\u01b3"+
		"\u01b1\3\2\2\2\u01b4\u01b5\7,\2\2\u01b5\u01b6\7\61\2\2\u01b6\u01b7\3\2"+
		"\2\2\u01b7\u01b8\b\67\3\2\u01b8n\3\2\2\2\u01b9\u01bb\t\36\2\2\u01ba\u01b9"+
		"\3\2\2\2\u01bb\u01bc\3\2\2\2\u01bc\u01ba\3\2\2\2\u01bc\u01bd\3\2\2\2\u01bd"+
		"\u01be\3\2\2\2\u01be\u01bf\b8\4\2\u01bfp\3\2\2\2\22\2\u00bb\u012a\u0155"+
		"\u015c\u017e\u0184\u018a\u018c\u0192\u019a\u019f\u01a3\u01a7\u01b1\u01bc";
	public static final ATN _ATN =
		ATNSimulator.deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}