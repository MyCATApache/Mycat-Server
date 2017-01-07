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
package io.mycat.server.parser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.mycat.route.parser.util.ParseUtil;

/**
 * @author mycat
 */
public final class ServerParse {

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
	public static final int EXPLAIN2 = 151;
	public static final int KILL_QUERY = 16;
	public static final int HELP = 17;
	public static final int MYSQL_CMD_COMMENT = 18;
	public static final int MYSQL_COMMENT = 19;
	public static final int CALL = 20;
	public static final int DESCRIBE = 21;
	public static final int LOCK = 22;
	public static final int UNLOCK = 23;
    public static final int LOAD_DATA_INFILE_SQL = 99;
    public static final int DDL = 100;


	public static final int MIGRATE  = 203;
    private static final  Pattern pattern = Pattern.compile("(load)+\\s+(data)+\\s+\\w*\\s*(infile)+",Pattern.CASE_INSENSITIVE);
    private static final  Pattern callPattern = Pattern.compile("\\w*\\;\\s*\\s*(call)+\\s+\\w*\\s*",Pattern.CASE_INSENSITIVE);

    public static int parse(String stmt) {
		int length = stmt.length();
		//FIX BUG FOR SQL SUCH AS /XXXX/SQL
		int rt = -1;
		for (int i = 0; i < length; ++i) {
			switch (stmt.charAt(i)) {
			case ' ':
			case '\t':
			case '\r':
			case '\n':
				continue;
			case '/':
				// such as /*!40101 SET character_set_client = @saved_cs_client
				// */;
				if (i == 0 && stmt.charAt(1) == '*' && stmt.charAt(2) == '!' && stmt.charAt(length - 2) == '*'
						&& stmt.charAt(length - 1) == '/') {
					return MYSQL_CMD_COMMENT;
				}
			case '#':
				i = ParseUtil.comment(stmt, i);
				if (i + 1 == length) {
					return MYSQL_COMMENT;
				}
				continue;
			case 'A':
			case 'a':
				rt = aCheck(stmt, i);
				if (rt != OTHER) {
					return rt;
				}
				continue;
			case 'B':
			case 'b':
				rt = beginCheck(stmt, i);
				if (rt != OTHER) {
					return rt;
				}
				continue;
			case 'C':
			case 'c':
				rt = commitOrCallCheckOrCreate(stmt, i);
				if (rt != OTHER) {
					return rt;
				}
				continue;
			case 'D':
			case 'd':
				rt = deleteOrdCheck(stmt, i);
				if (rt != OTHER) {
					return rt;
				}
				continue;
			case 'E':
			case 'e':
				rt = explainCheck(stmt, i);
				if (rt != OTHER) {
					return rt;
				}
				continue;
			case 'I':
			case 'i':
				rt = insertCheck(stmt, i);
				if (rt != OTHER) {
					return rt;
				}
				continue;
				case 'M':
				case 'm':
					rt = migrateCheck(stmt, i);
					if (rt != OTHER) {
						return rt;
					}
					continue;
			case 'R':
			case 'r':
				rt = rCheck(stmt, i);
				if (rt != OTHER) {
					return rt;
				}
				continue;
			case 'S':
			case 's':
				rt = sCheck(stmt, i);
				if (rt != OTHER) {
					return rt;
				}
				continue;
			case 'T':
			case 't':
				rt = tCheck(stmt, i);
				if (rt != OTHER) {
					return rt;
				}
				continue;
			case 'U':
			case 'u':
				rt = uCheck(stmt, i);
				if (rt != OTHER) {
					return rt;
				}
				continue;
			case 'K':
			case 'k':
				rt = killCheck(stmt, i);
				if (rt != OTHER) {
					return rt;
				}
				continue;
			case 'H':
			case 'h':
				rt = helpCheck(stmt, i);
				if (rt != OTHER) {
					return rt;
				}
				continue;
			case 'L':
			case 'l':
				rt = lCheck(stmt, i);
				if (rt != OTHER) {
					return rt;
				}
				continue;
			default:
				continue;
			}
		}
		return OTHER;
	}


	static int lCheck(String stmt, int offset) {
		if (stmt.length() > offset + 3) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			if ((c1 == 'O' || c1 == 'o') && (c2 == 'A' || c2 == 'a')
					&& (c3 == 'D' || c3 == 'd')) {
				Matcher matcher = pattern.matcher(stmt);
				return matcher.find() ? LOAD_DATA_INFILE_SQL : OTHER;
			} else if ((c1 == 'O' || c1 == 'o') && (c2 == 'C' || c2 == 'c')
					&& (c3 == 'K' || c3 == 'k')){
				return LOCK;
			}
		}

		return OTHER;
	}

	private static int migrateCheck(String stmt, int offset) {
		if (stmt.length() > offset + 7) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			char c4 = stmt.charAt(++offset);
			char c5 = stmt.charAt(++offset);
			char c6 = stmt.charAt(++offset);


			if ((c1 == 'i' || c1 == 'I')
					&& (c2 == 'g' || c2 == 'G')
					&& (c3 == 'r' || c3 == 'R')
					&& (c4 == 'a' || c4 == 'A')
					&& (c5 == 't' || c5 == 'T')
					&& (c6 == 'e' || c6 == 'E'))
					{
				return MIGRATE;
			}
		}
		return OTHER;
	}
	//truncate
	private static int tCheck(String stmt, int offset) {
		if (stmt.length() > offset + 7) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			char c4 = stmt.charAt(++offset);
			char c5 = stmt.charAt(++offset);
			char c6 = stmt.charAt(++offset);
			char c7 = stmt.charAt(++offset);
			char c8 = stmt.charAt(++offset);

			if ((c1 == 'R' || c1 == 'r')
					&& (c2 == 'U' || c2 == 'u')
					&& (c3 == 'N' || c3 == 'n')
					&& (c4 == 'C' || c4 == 'c')
					&& (c5 == 'A' || c5 == 'a')
					&& (c6 == 'T' || c6 == 't')
					&& (c7 == 'E' || c7 == 'e')
					&& (c8 == ' ' || c8 == '\t' || c8 == '\r' || c8 == '\n')) {
				return DDL;
			}
		}
		return OTHER;
	}
	//alter table/view/...
	private static int aCheck(String stmt, int offset) {
		if (stmt.length() > offset + 4) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			char c4 = stmt.charAt(++offset);
			char c5 = stmt.charAt(++offset);
			if ((c1 == 'L' || c1 == 'l')
					&& (c2 == 'T' || c2 == 't')
					&& (c3 == 'E' || c3 == 'e')
					&& (c4 == 'R' || c4 == 'r')
					&& (c5 == ' ' || c5 == '\t' || c5 == '\r' || c5 == '\n')) {
				return DDL;
			}
		}
		return OTHER;
	}
	//create table/view/...
	private static int createCheck(String stmt, int offset) {
		if (stmt.length() > offset + 5) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			char c4 = stmt.charAt(++offset);
			char c5 = stmt.charAt(++offset);
			char c6 = stmt.charAt(++offset);
			if ((c1 == 'R' || c1 == 'r')
					&& (c2 == 'E' || c2 == 'e')
					&& (c3 == 'A' || c3 == 'a')
					&& (c4 == 'T' || c4 == 't')
					&& (c5 == 'E' || c5 == 'e')
					&& (c6 == ' ' || c6 == '\t' || c6 == '\r' || c6 == '\n')) {
				return DDL;
			}
		}
		return OTHER;
	}
	//drop
	private static int dropCheck(String stmt, int offset) {
		if (stmt.length() > offset + 3) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			char c4 = stmt.charAt(++offset);
			if ((c1 == 'R' || c1 == 'r')
					&& (c2 == 'O' || c2 == 'o')
					&& (c3 == 'P' || c3 == 'p')
					&& (c4 == ' ' || c4 == '\t' || c4 == '\r' || c4 == '\n')) {
				return DDL;
			}
		}
		return OTHER;
	}
	// delete or drop
    static int deleteOrdCheck(String stmt, int offset){
    	int sqlType = OTHER;
		switch (stmt.charAt((offset + 1))) {
		case 'E':
		case 'e':
			sqlType = dCheck(stmt, offset);
			break;
		case 'R':
		case 'r':
			sqlType = dropCheck(stmt, offset);
			break;
		default:
			sqlType = OTHER;
		}
		return sqlType;
    }
	// HELP' '
	static int helpCheck(String stmt, int offset) {
		if (stmt.length() > offset + "ELP ".length()) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			if ((c1 == 'E' || c1 == 'e') && (c2 == 'L' || c2 == 'l')
					&& (c3 == 'P' || c3 == 'p')) {
				return (offset << 8) | HELP;
			}
		}
		return OTHER;
	}

	// EXPLAIN' '
	static int explainCheck(String stmt, int offset) {

		if (stmt.length() > offset + "XPLAIN ".length()) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			char c4 = stmt.charAt(++offset);
			char c5 = stmt.charAt(++offset);
			char c6 = stmt.charAt(++offset);
			char c7 = stmt.charAt(++offset);
			if ((c1 == 'X' || c1 == 'x') && (c2 == 'P' || c2 == 'p')
					&& (c3 == 'L' || c3 == 'l') && (c4 == 'A' || c4 == 'a')
					&& (c5 == 'I' || c5 == 'i') && (c6 == 'N' || c6 == 'n')
					&& (c7 == ' ' || c7 == '\t' || c7 == '\r' || c7 == '\n')) {
				return (offset << 8) | EXPLAIN;
			}
		}
		if(stmt != null && stmt.toLowerCase().startsWith("explain2")){
			return (offset << 8) | EXPLAIN2;
		}
		return OTHER;
	}

	// KILL' '
	static int killCheck(String stmt, int offset) {
		if (stmt.length() > offset + "ILL ".length()) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			char c4 = stmt.charAt(++offset);
			if ((c1 == 'I' || c1 == 'i') && (c2 == 'L' || c2 == 'l')
					&& (c3 == 'L' || c3 == 'l')
					&& (c4 == ' ' || c4 == '\t' || c4 == '\r' || c4 == '\n')) {
				while (stmt.length() > ++offset) {
					switch (stmt.charAt(offset)) {
					case ' ':
					case '\t':
					case '\r':
					case '\n':
						continue;
					case 'Q':
					case 'q':
						return killQueryCheck(stmt, offset);
					default:
						return (offset << 8) | KILL;
					}
				}
				return OTHER;
			}
		}
		return OTHER;
	}

	// KILL QUERY' '
	static int killQueryCheck(String stmt, int offset) {
		if (stmt.length() > offset + "UERY ".length()) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			char c4 = stmt.charAt(++offset);
			char c5 = stmt.charAt(++offset);
			if ((c1 == 'U' || c1 == 'u') && (c2 == 'E' || c2 == 'e')
					&& (c3 == 'R' || c3 == 'r') && (c4 == 'Y' || c4 == 'y')
					&& (c5 == ' ' || c5 == '\t' || c5 == '\r' || c5 == '\n')) {
				while (stmt.length() > ++offset) {
					switch (stmt.charAt(offset)) {
					case ' ':
					case '\t':
					case '\r':
					case '\n':
						continue;
					default:
						return (offset << 8) | KILL_QUERY;
					}
				}
				return OTHER;
			}
		}
		return OTHER;
	}

	// BEGIN
	static int beginCheck(String stmt, int offset) {
		if (stmt.length() > offset + 4) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			char c4 = stmt.charAt(++offset);
			if ((c1 == 'E' || c1 == 'e')
					&& (c2 == 'G' || c2 == 'g')
					&& (c3 == 'I' || c3 == 'i')
					&& (c4 == 'N' || c4 == 'n')
					&& (stmt.length() == ++offset || ParseUtil.isEOF(stmt
							.charAt(offset)))) {
				return BEGIN;
			}
		}
		return OTHER;
	}

	// COMMIT
	static int commitCheck(String stmt, int offset) {
		if (stmt.length() > offset + 5) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			char c4 = stmt.charAt(++offset);
			char c5 = stmt.charAt(++offset);
			if ((c1 == 'O' || c1 == 'o')
					&& (c2 == 'M' || c2 == 'm')
					&& (c3 == 'M' || c3 == 'm')
					&& (c4 == 'I' || c4 == 'i')
					&& (c5 == 'T' || c5 == 't')
					&& (stmt.length() == ++offset || ParseUtil.isEOF(stmt
							.charAt(offset)))) {
				return COMMIT;
			}
		}

		return OTHER;
	}

	// CALL
	static int callCheck(String stmt, int offset) {
		if (stmt.length() > offset + 3) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			if ((c1 == 'A' || c1 == 'a') && (c2 == 'L' || c2 == 'l')
					&& (c3 == 'L' || c3 == 'l')) {
				return CALL;
			}
		}

		return OTHER;
	}

	static int commitOrCallCheckOrCreate(String stmt, int offset) {
		int sqlType = OTHER;
		switch (stmt.charAt((offset + 1))) {
		case 'O':
		case 'o':
			sqlType = commitCheck(stmt, offset);
			break;
		case 'A':
		case 'a':
			sqlType = callCheck(stmt, offset);
			break;
		case 'R':
		case 'r':
			sqlType = createCheck(stmt, offset);
			break;
		default:
			sqlType = OTHER;
		}
		return sqlType;
	}

	// DESCRIBE or desc or DELETE' '
	static int dCheck(String stmt, int offset) {
		if (stmt.length() > offset + 4) {
			int res = describeCheck(stmt, offset);
			if (res == DESCRIBE) {
				return res;
			}
		}
		// continue check
		if (stmt.length() > offset + 6) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			char c4 = stmt.charAt(++offset);
			char c5 = stmt.charAt(++offset);
			char c6 = stmt.charAt(++offset);
			if ((c1 == 'E' || c1 == 'e') && (c2 == 'L' || c2 == 'l')
					&& (c3 == 'E' || c3 == 'e') && (c4 == 'T' || c4 == 't')
					&& (c5 == 'E' || c5 == 'e')
					&& (c6 == ' ' || c6 == '\t' || c6 == '\r' || c6 == '\n')) {
				return DELETE;
			}
		}
		return OTHER;
	}

	// DESCRIBE' ' 或 desc' '
	static int describeCheck(String stmt, int offset) {
		//desc
		if (stmt.length() > offset + 4) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			char c4 = stmt.charAt(++offset);
			if ((c1 == 'E' || c1 == 'e') && (c2 == 'S' || c2 == 's')
					&& (c3 == 'C' || c3 == 'c')
					&& (c4 == ' ' || c4 == '\t' || c4 == '\r' || c4 == '\n')) {
				return DESCRIBE;
			}
			//describe
			if (stmt.length() > offset + 4) {
				char c5 = stmt.charAt(++offset);
				char c6 = stmt.charAt(++offset);
				char c7 = stmt.charAt(++offset);
				char c8 = stmt.charAt(++offset);
				if ((c1 == 'E' || c1 == 'e') && (c2 == 'S' || c2 == 's')
						&& (c3 == 'C' || c3 == 'c') && (c4 == 'R' || c4 == 'r')
						&& (c5 == 'I' || c5 == 'i') && (c6 == 'B' || c6 == 'b')
						&& (c7 == 'E' || c7 == 'e')
						&& (c8 == ' ' || c8 == '\t' || c8 == '\r' || c8 == '\n')) {
					return DESCRIBE;
				}
			}
		}
		return OTHER;
	}

	// INSERT' '
	static int insertCheck(String stmt, int offset) {
		if (stmt.length() > offset + 6) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			char c4 = stmt.charAt(++offset);
			char c5 = stmt.charAt(++offset);
			char c6 = stmt.charAt(++offset);
			if ((c1 == 'N' || c1 == 'n') && (c2 == 'S' || c2 == 's')
					&& (c3 == 'E' || c3 == 'e') && (c4 == 'R' || c4 == 'r')
					&& (c5 == 'T' || c5 == 't')
					&& (c6 == ' ' || c6 == '\t' || c6 == '\r' || c6 == '\n')) {
				return INSERT;
			}
		}
		return OTHER;
	}

	static int rCheck(String stmt, int offset) {
		if (stmt.length() > ++offset) {
			switch (stmt.charAt(offset)) {
			case 'E':
			case 'e':
				return replaceCheck(stmt, offset);
			case 'O':
			case 'o':
				return rollabckCheck(stmt, offset);
			default:
				return OTHER;
			}
		}
		return OTHER;
	}

	// REPLACE' '
	static int replaceCheck(String stmt, int offset) {
		if (stmt.length() > offset + 6) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			char c4 = stmt.charAt(++offset);
			char c5 = stmt.charAt(++offset);
			char c6 = stmt.charAt(++offset);
			if ((c1 == 'P' || c1 == 'p') && (c2 == 'L' || c2 == 'l')
					&& (c3 == 'A' || c3 == 'a') && (c4 == 'C' || c4 == 'c')
					&& (c5 == 'E' || c5 == 'e')
					&& (c6 == ' ' || c6 == '\t' || c6 == '\r' || c6 == '\n')) {
				return REPLACE;
			}
		}
		return OTHER;
	}

	// ROLLBACK
	static int rollabckCheck(String stmt, int offset) {
		if (stmt.length() > offset + 6) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			char c4 = stmt.charAt(++offset);
			char c5 = stmt.charAt(++offset);
			char c6 = stmt.charAt(++offset);
			if ((c1 == 'L' || c1 == 'l')
					&& (c2 == 'L' || c2 == 'l')
					&& (c3 == 'B' || c3 == 'b')
					&& (c4 == 'A' || c4 == 'a')
					&& (c5 == 'C' || c5 == 'c')
					&& (c6 == 'K' || c6 == 'k')
					&& (stmt.length() == ++offset || ParseUtil.isEOF(stmt
							.charAt(offset)))) {
				return ROLLBACK;
			}
		}
		return OTHER;
	}

	static int sCheck(String stmt, int offset) {
		if (stmt.length() > ++offset) {
			switch (stmt.charAt(offset)) {
			case 'A':
			case 'a':
				return savepointCheck(stmt, offset);
			case 'E':
			case 'e':
				return seCheck(stmt, offset);
			case 'H':
			case 'h':
				return showCheck(stmt, offset);
			case 'T':
			case 't':
				return startCheck(stmt, offset);
			default:
				return OTHER;
			}
		}
		return OTHER;
	}

	// SAVEPOINT
	static int savepointCheck(String stmt, int offset) {
		if (stmt.length() > offset + 8) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			char c4 = stmt.charAt(++offset);
			char c5 = stmt.charAt(++offset);
			char c6 = stmt.charAt(++offset);
			char c7 = stmt.charAt(++offset);
			char c8 = stmt.charAt(++offset);
			if ((c1 == 'V' || c1 == 'v') && (c2 == 'E' || c2 == 'e')
					&& (c3 == 'P' || c3 == 'p') && (c4 == 'O' || c4 == 'o')
					&& (c5 == 'I' || c5 == 'i') && (c6 == 'N' || c6 == 'n')
					&& (c7 == 'T' || c7 == 't')
					&& (c8 == ' ' || c8 == '\t' || c8 == '\r' || c8 == '\n')) {
				return SAVEPOINT;
			}
		}
		return OTHER;
	}

	static int seCheck(String stmt, int offset) {
		if (stmt.length() > ++offset) {
			switch (stmt.charAt(offset)) {
			case 'L':
			case 'l':
				return selectCheck(stmt, offset);
			case 'T':
			case 't':
				if (stmt.length() > ++offset) {
//支持一下语句
//  /*!mycat: sql=SELECT * FROM test where id=99 */set @pin=1;
//                    call p_test(@pin,@pout);
//                    select @pout;
                    if(stmt.startsWith("/*!mycat:")||stmt.startsWith("/*#mycat:")||stmt.startsWith("/*mycat:"))
                    {
                        Matcher matcher = callPattern.matcher(stmt);
                        if (matcher.find()) {
							return CALL;
						}
                    }

					char c = stmt.charAt(offset);
					if (c == ' ' || c == '\r' || c == '\n' || c == '\t'
							|| c == '/' || c == '#') {
						return (offset << 8) | SET;
					}
				}
				return OTHER;
			default:
				return OTHER;
			}
		}
		return OTHER;
	}

	// SELECT' '
	static int selectCheck(String stmt, int offset) {
		if (stmt.length() > offset + 4) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			char c4 = stmt.charAt(++offset);
			if ((c1 == 'E' || c1 == 'e')
					&& (c2 == 'C' || c2 == 'c')
					&& (c3 == 'T' || c3 == 't')
					&& (c4 == ' ' || c4 == '\t' || c4 == '\r' || c4 == '\n'
							|| c4 == '/' || c4 == '#')) {
				return (offset << 8) | SELECT;
			}
		}
		return OTHER;
	}

	// SHOW' '
	static int showCheck(String stmt, int offset) {
		if (stmt.length() > offset + 3) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			if ((c1 == 'O' || c1 == 'o') && (c2 == 'W' || c2 == 'w')
					&& (c3 == ' ' || c3 == '\t' || c3 == '\r' || c3 == '\n')) {
				return (offset << 8) | SHOW;
			}
		}
		return OTHER;
	}

	// START' '
	static int startCheck(String stmt, int offset) {
		if (stmt.length() > offset + 4) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			char c4 = stmt.charAt(++offset);
			if ((c1 == 'A' || c1 == 'a') && (c2 == 'R' || c2 == 'r')
					&& (c3 == 'T' || c3 == 't')
					&& (c4 == ' ' || c4 == '\t' || c4 == '\r' || c4 == '\n')) {
				return (offset << 8) | START;
			}
		}
		return OTHER;
	}

	// UPDATE' ' | USE' '
	static int uCheck(String stmt, int offset) {
		if (stmt.length() > ++offset) {
			switch (stmt.charAt(offset)) {
			case 'P':
			case 'p':
				if (stmt.length() > offset + 5) {
					char c1 = stmt.charAt(++offset);
					char c2 = stmt.charAt(++offset);
					char c3 = stmt.charAt(++offset);
					char c4 = stmt.charAt(++offset);
					char c5 = stmt.charAt(++offset);
					if ((c1 == 'D' || c1 == 'd')
							&& (c2 == 'A' || c2 == 'a')
							&& (c3 == 'T' || c3 == 't')
							&& (c4 == 'E' || c4 == 'e')
							&& (c5 == ' ' || c5 == '\t' || c5 == '\r' || c5 == '\n')) {
						return UPDATE;
					}
				}
				break;
			case 'S':
			case 's':
				if (stmt.length() > offset + 2) {
					char c1 = stmt.charAt(++offset);
					char c2 = stmt.charAt(++offset);
					if ((c1 == 'E' || c1 == 'e')
							&& (c2 == ' ' || c2 == '\t' || c2 == '\r' || c2 == '\n')) {
						return (offset << 8) | USE;
					}
				}
				break;
			case 'N':
			case 'n':
				if (stmt.length() > offset + 5) {
					char c1 = stmt.charAt(++offset);
					char c2 = stmt.charAt(++offset);
					char c3 = stmt.charAt(++offset);
					char c4 = stmt.charAt(++offset);
					char c5 = stmt.charAt(++offset);
					if ((c1 == 'L' || c1 == 'l')
							&& (c2 == 'O' || c2 == 'o')
							&& (c3 == 'C' || c3 == 'c')
							&& (c4 == 'K' || c4 == 'k')
							&& (c5 == ' ' || c5 == '\t' || c5 == '\r' || c5 == '\n')) {
						return UNLOCK;
					}
				}
				break;
			default:
				return OTHER;
			}
		}
		return OTHER;
	}

}
