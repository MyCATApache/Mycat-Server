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

import io.mycat.route.parser.util.ParseUtil;

/**
 * @author mycat
 */
public final class ServerParseSet {

	public static final int OTHER = -1;
	public static final int AUTOCOMMIT_ON = 1;
	public static final int AUTOCOMMIT_OFF = 2;
	public static final int TX_READ_UNCOMMITTED = 3;
	public static final int TX_READ_COMMITTED = 4;
	public static final int TX_REPEATED_READ = 5;
	public static final int TX_SERIALIZABLE = 6;
	public static final int NAMES = 7;
	public static final int CHARACTER_SET_CLIENT = 8;
	public static final int CHARACTER_SET_CONNECTION = 9;
	public static final int CHARACTER_SET_RESULTS = 10;
	public static final int XA_FLAG_ON = 11;
	public static final int XA_FLAG_OFF = 12;

	public static int parse(String stmt, int offset) {
		int i = offset;
		for (; i < stmt.length(); i++) {
			switch (stmt.charAt(i)) {
			case ' ':
			case '\r':
			case '\n':
			case '\t':
				continue;
			case '/':
			case '#':
				i = ParseUtil.comment(stmt, i);
				continue;
			case 'A':
			case 'a':
				return autocommit(stmt, i);
			case 'C':
			case 'c':
				return characterSet(stmt, i, 0);
			case 'N':
			case 'n':
				return names(stmt, i);
			case 'S':
			case 's':
				return session(stmt, i);
			case 'T':
			case 't':
				return transaction(stmt, i);
			case 'X':
			case 'x':
				return xaFlag(stmt, i);
			default:
				return OTHER;
			}
		}
		return OTHER;
	}

	// set xa=1
	private static int xaFlag(String stmt, int offset) {
		if (stmt.length() > offset + 1) {
			char c1 = stmt.charAt(++offset);
			char c2=stmt.charAt(++offset);
			if ((c1 == 'A' || c1 == 'a')
					&&( c2== ' '||c2=='=')) {
				int value = autocommitValue(stmt, offset);
				if (value == AUTOCOMMIT_ON) {
					return XA_FLAG_ON;
				} else if (value == AUTOCOMMIT_OFF) {
					return XA_FLAG_OFF;
				} else {
					return OTHER;
				}

			}
		}
		return OTHER;
	}

	// SET AUTOCOMMIT(' '=)
	private static int autocommit(String stmt, int offset) {
		if (stmt.length() > offset + 9) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			char c4 = stmt.charAt(++offset);
			char c5 = stmt.charAt(++offset);
			char c6 = stmt.charAt(++offset);
			char c7 = stmt.charAt(++offset);
			char c8 = stmt.charAt(++offset);
			char c9 = stmt.charAt(++offset);
			if ((c1 == 'U' || c1 == 'u') && (c2 == 'T' || c2 == 't')
					&& (c3 == 'O' || c3 == 'o') && (c4 == 'C' || c4 == 'c')
					&& (c5 == 'O' || c5 == 'o') && (c6 == 'M' || c6 == 'm')
					&& (c7 == 'M' || c7 == 'm') && (c8 == 'I' || c8 == 'i')
					&& (c9 == 'T' || c9 == 't')) {
				while (stmt.length() > ++offset) {
					switch (stmt.charAt(offset)) {
					case ' ':
					case '\r':
					case '\n':
					case '\t':
						continue;
					case '=':
						return autocommitValue(stmt, offset);
					default:
						return OTHER;
					}
				}
			}
		}
		return OTHER;
	}

	private static int autocommitValue(String stmt, int offset) {
		for (;;) {
			offset++;
			if (stmt.length() <= offset) {
				return OTHER;
			}
			switch (stmt.charAt(offset)) {
			case ' ':
			case '\r':
			case '\n':
			case '\t':
				continue;
			case '1':
				if (stmt.length() == ++offset
						|| ParseUtil.isEOF(stmt.charAt(offset))) {
					return AUTOCOMMIT_ON;
				} else {
					return OTHER;
				}
			case '0':
				if (stmt.length() == ++offset
						|| ParseUtil.isEOF(stmt.charAt(offset))) {
					return AUTOCOMMIT_OFF;
				} else {
					return OTHER;
				}
			case 'O':
			case 'o':
				return autocommitOn(stmt, offset);
			default:
				return OTHER;
			}
		}
	}

	private static int autocommitOn(String stmt, int offset) {
		if (stmt.length() > ++offset) {
			switch (stmt.charAt(offset)) {
			case 'N':
			case 'n':
				if (stmt.length() == ++offset
						|| ParseUtil.isEOF(stmt.charAt(offset))) {
					return AUTOCOMMIT_ON;
				} else {
					return OTHER;
				}
			case 'F':
			case 'f':
				return autocommitOff(stmt, offset);
			default:
				return OTHER;
			}
		}
		return OTHER;
	}

	// SET AUTOCOMMIT = OFF
	private static int autocommitOff(String stmt, int offset) {
		if (stmt.length() > ++offset) {
			switch (stmt.charAt(offset)) {
			case 'F':
			case 'f':
				if (stmt.length() == ++offset
						|| ParseUtil.isEOF(stmt.charAt(offset))) {
					return AUTOCOMMIT_OFF;
				} else {
					return OTHER;
				}
			default:
				return OTHER;
			}
		}
		return OTHER;
	}

	// SET NAMES' '
	private static int names(String stmt, int offset) {
		if (stmt.length() > offset + 5) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			char c4 = stmt.charAt(++offset);
			if ((c1 == 'A' || c1 == 'a') && (c2 == 'M' || c2 == 'm')
					&& (c3 == 'E' || c3 == 'e') && (c4 == 'S' || c4 == 's')
					&& stmt.charAt(++offset) == ' ') {
				return (offset << 8) | NAMES;
			}
		}
		return OTHER;
	}

	// SET CHARACTER_SET_
	private static int characterSet(String stmt, int offset, int depth) {
		if (stmt.length() > offset + 14) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			char c4 = stmt.charAt(++offset);
			char c5 = stmt.charAt(++offset);
			char c6 = stmt.charAt(++offset);
			char c7 = stmt.charAt(++offset);
			char c8 = stmt.charAt(++offset);
			char c9 = stmt.charAt(++offset);
			char c10 = stmt.charAt(++offset);
			char c11 = stmt.charAt(++offset);
			char c12 = stmt.charAt(++offset);
			char c13 = stmt.charAt(++offset);
			char c14 = stmt.charAt(++offset);
			if ((c1 == 'H' || c1 == 'h') && (c2 == 'A' || c2 == 'a')
					&& (c3 == 'R' || c3 == 'r') && (c4 == 'A' || c4 == 'a')
					&& (c5 == 'C' || c5 == 'c') && (c6 == 'T' || c6 == 't')
					&& (c7 == 'E' || c7 == 'e') && (c8 == 'R' || c8 == 'r')
					&& (c9 == '_') && (c10 == 'S' || c10 == 's')
					&& (c11 == 'E' || c11 == 'e') && (c12 == 'T' || c12 == 't')
					&& (c13 == '_')) {
				switch (c14) {
				case 'R':
				case 'r':
					return characterSetResults(stmt, offset);
				case 'C':
				case 'c':
					return characterSetC(stmt, offset);
				default:
					return OTHER;
				}
			}
		}
		return OTHER;
	}

	// SET CHARACTER_SET_RESULTS =
	private static int characterSetResults(String stmt, int offset) {
		if (stmt.length() > offset + 6) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			char c4 = stmt.charAt(++offset);
			char c5 = stmt.charAt(++offset);
			char c6 = stmt.charAt(++offset);
			if ((c1 == 'E' || c1 == 'e') && (c2 == 'S' || c2 == 's')
					&& (c3 == 'U' || c3 == 'u') && (c4 == 'L' || c4 == 'l')
					&& (c5 == 'T' || c5 == 't') && (c6 == 'S' || c6 == 's')) {
				while (stmt.length() > ++offset) {
					switch (stmt.charAt(offset)) {
					case ' ':
					case '\r':
					case '\n':
					case '\t':
						continue;
					case '=':
						while (stmt.length() > ++offset) {
							switch (stmt.charAt(offset)) {
							case ' ':
							case '\r':
							case '\n':
							case '\t':
								continue;
							default:
								return (offset << 8) | CHARACTER_SET_RESULTS;
							}
						}
						return OTHER;
					default:
						return OTHER;
					}
				}
			}
		}
		return OTHER;
	}

	// SET CHARACTER_SET_C
	private static int characterSetC(String stmt, int offset) {
		if (stmt.length() > offset + 1) {
			char c1 = stmt.charAt(++offset);
			switch (c1) {
			case 'o':
			case 'O':
				return characterSetConnection(stmt, offset);
			case 'l':
			case 'L':
				return characterSetClient(stmt, offset);
			default:
				return OTHER;
			}
		}
		return OTHER;
	}

	// SET CHARACTER_SET_CONNECTION =
	private static int characterSetConnection(String stmt, int offset) {
		if (stmt.length() > offset + 8) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			char c4 = stmt.charAt(++offset);
			char c5 = stmt.charAt(++offset);
			char c6 = stmt.charAt(++offset);
			char c7 = stmt.charAt(++offset);
			char c8 = stmt.charAt(++offset);
			if ((c1 == 'N' || c1 == 'n') && (c2 == 'N' || c2 == 'n')
					&& (c3 == 'E' || c3 == 'e') && (c4 == 'C' || c4 == 'c')
					&& (c5 == 'T' || c5 == 't') && (c6 == 'I' || c6 == 'i')
					&& (c7 == 'O' || c7 == 'o') && (c8 == 'N' || c8 == 'n')) {
				while (stmt.length() > ++offset) {
					switch (stmt.charAt(offset)) {
					case ' ':
					case '\r':
					case '\n':
					case '\t':
						continue;
					case '=':
						while (stmt.length() > ++offset) {
							switch (stmt.charAt(offset)) {
							case ' ':
							case '\r':
							case '\n':
							case '\t':
								continue;
							default:
								return (offset << 8) | CHARACTER_SET_CONNECTION;
							}
						}
						return OTHER;
					default:
						return OTHER;
					}
				}
			}
		}
		return OTHER;
	}

	// SET CHARACTER_SET_CLIENT =
	private static int characterSetClient(String stmt, int offset) {
		if (stmt.length() > offset + 4) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			char c4 = stmt.charAt(++offset);
			if ((c1 == 'I' || c1 == 'i') && (c2 == 'E' || c2 == 'e')
					&& (c3 == 'N' || c3 == 'n') && (c4 == 'T' || c4 == 't')) {
				while (stmt.length() > ++offset) {
					switch (stmt.charAt(offset)) {
					case ' ':
					case '\r':
					case '\n':
					case '\t':
						continue;
					case '=':
						while (stmt.length() > ++offset) {
							switch (stmt.charAt(offset)) {
							case ' ':
							case '\r':
							case '\n':
							case '\t':
								continue;
							default:
								return (offset << 8) | CHARACTER_SET_CLIENT;
							}
						}
						return OTHER;
					default:
						return OTHER;
					}
				}
			}
		}
		return OTHER;
	}

	// SET SESSION' '
	private static int session(String stmt, int offset) {
		if (stmt.length() > offset + 7) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			char c4 = stmt.charAt(++offset);
			char c5 = stmt.charAt(++offset);
			char c6 = stmt.charAt(++offset);
			if ((c1 == 'E' || c1 == 'e') && (c2 == 'S' || c2 == 's')
					&& (c3 == 'S' || c3 == 's') && (c4 == 'I' || c4 == 'i')
					&& (c5 == 'O' || c5 == 'o') && (c6 == 'N' || c6 == 'n')
					&& stmt.charAt(++offset) == ' ') {
//				for (;;) {
					while (stmt.length() > ++offset) {
						switch (stmt.charAt(offset)) {
						case ' ':
						case '\r':
						case '\n':
						case '\t':
							continue;
						case 'T':
						case 't':
							return transaction(stmt, offset);
						default:
							return OTHER;
						}
					}
//					return OTHER;
//				}
			}
		}
		return OTHER;
	}

	// SET [SESSION] TRANSACTION ISOLATION LEVEL
	private static int transaction(String stmt, int offset) {
		if (stmt.length() > offset + 11) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			char c4 = stmt.charAt(++offset);
			char c5 = stmt.charAt(++offset);
			char c6 = stmt.charAt(++offset);
			char c7 = stmt.charAt(++offset);
			char c8 = stmt.charAt(++offset);
			char c9 = stmt.charAt(++offset);
			char c10 = stmt.charAt(++offset);
			if ((c1 == 'R' || c1 == 'r') && (c2 == 'A' || c2 == 'a')
					&& (c3 == 'N' || c3 == 'n') && (c4 == 'S' || c4 == 's')
					&& (c5 == 'A' || c5 == 'a') && (c6 == 'C' || c6 == 'c')
					&& (c7 == 'T' || c7 == 't') && (c8 == 'I' || c8 == 'i')
					&& (c9 == 'O' || c9 == 'o') && (c10 == 'N' || c10 == 'n')
					&& stmt.charAt(++offset) == ' ') {
//				for (;;) {
					while (stmt.length() > ++offset) {
						switch (stmt.charAt(offset)) {
						case ' ':
						case '\r':
						case '\n':
						case '\t':
							continue;
						case 'I':
						case 'i':
							return isolation(stmt, offset);
						default:
							return OTHER;
						}
					}
//					return OTHER;
//				}
			}
		}
		return OTHER;
	}

	// SET [SESSION] TRANSACTION ISOLATION LEVEL
	private static int isolation(String stmt, int offset) {
		if (stmt.length() > offset + 9) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			char c4 = stmt.charAt(++offset);
			char c5 = stmt.charAt(++offset);
			char c6 = stmt.charAt(++offset);
			char c7 = stmt.charAt(++offset);
			char c8 = stmt.charAt(++offset);
			if ((c1 == 'S' || c1 == 's') && (c2 == 'O' || c2 == 'o')
					&& (c3 == 'L' || c3 == 'l') && (c4 == 'A' || c4 == 'a')
					&& (c5 == 'T' || c5 == 't') && (c6 == 'I' || c6 == 'i')
					&& (c7 == 'O' || c7 == 'o') && (c8 == 'N' || c8 == 'n')
					&& stmt.charAt(++offset) == ' ') {
//				for (;;) {
					while (stmt.length() > ++offset) {
						switch (stmt.charAt(offset)) {
						case ' ':
						case '\r':
						case '\n':
						case '\t':
							continue;
						case 'L':
						case 'l':
							return level(stmt, offset);
						default:
							return OTHER;
						}
					}
//					return OTHER;
//				}
			}
		}
		return OTHER;
	}

	// SET [SESSION] TRANSACTION ISOLATION LEVEL' '
	private static int level(String stmt, int offset) {
		if (stmt.length() > offset + 5) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			char c4 = stmt.charAt(++offset);
			if ((c1 == 'E' || c1 == 'e') && (c2 == 'V' || c2 == 'v')
					&& (c3 == 'E' || c3 == 'e') && (c4 == 'L' || c4 == 'l')
					&& stmt.charAt(++offset) == ' ') {
//				for (;;) {
					while (stmt.length() > ++offset) {
						switch (stmt.charAt(offset)) {
						case ' ':
						case '\r':
						case '\n':
						case '\t':
							continue;
						case 'R':
						case 'r':
							return rCheck(stmt, offset);
						case 'S':
						case 's':
							return serializable(stmt, offset);
						default:
							return OTHER;
						}
					}
//					return OTHER;
//				}
			}
		}
		return OTHER;
	}

	// SET [SESSION] TRANSACTION ISOLATION LEVEL SERIALIZABLE
	private static int serializable(String stmt, int offset) {
		if (stmt.length() > offset + 11) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			char c4 = stmt.charAt(++offset);
			char c5 = stmt.charAt(++offset);
			char c6 = stmt.charAt(++offset);
			char c7 = stmt.charAt(++offset);
			char c8 = stmt.charAt(++offset);
			char c9 = stmt.charAt(++offset);
			char c10 = stmt.charAt(++offset);
			char c11 = stmt.charAt(++offset);
			if ((c1 == 'E' || c1 == 'e')
					&& (c2 == 'R' || c2 == 'r')
					&& (c3 == 'I' || c3 == 'i')
					&& (c4 == 'A' || c4 == 'a')
					&& (c5 == 'L' || c5 == 'l')
					&& (c6 == 'I' || c6 == 'i')
					&& (c7 == 'Z' || c7 == 'z')
					&& (c8 == 'A' || c8 == 'a')
					&& (c9 == 'B' || c9 == 'b')
					&& (c10 == 'L' || c10 == 'l')
					&& (c11 == 'E' || c11 == 'e')
					&& (stmt.length() == ++offset || ParseUtil.isEOF(stmt
							.charAt(offset)))) {
				return TX_SERIALIZABLE;
			}
		}
		return OTHER;
	}

	// READ' '|REPEATABLE
	private static int rCheck(String stmt, int offset) {
		if (stmt.length() > ++offset) {
			switch (stmt.charAt(offset)) {
			case 'E':
			case 'e':
				return eCheck(stmt, offset);
			default:
				return OTHER;
			}
		}
		return OTHER;
	}

	// READ' '|REPEATABLE
	private static int eCheck(String stmt, int offset) {
		if (stmt.length() > ++offset) {
			switch (stmt.charAt(offset)) {
			case 'A':
			case 'a':
				return aCheck(stmt, offset);
			case 'P':
			case 'p':
				return pCheck(stmt, offset);
			default:
				return OTHER;
			}
		}
		return OTHER;
	}

	// READ' '
	private static int aCheck(String stmt, int offset) {
		if (stmt.length() > offset + 2) {
			char c1 = stmt.charAt(++offset);
			if ((c1 == 'D' || c1 == 'd') && stmt.charAt(++offset) == ' ') {
//				for (;;) {
					while (stmt.length() > ++offset) {
						switch (stmt.charAt(offset)) {
						case ' ':
						case '\r':
						case '\n':
						case '\t':
							continue;
						case 'C':
						case 'c':
							return committed(stmt, offset);
						case 'U':
						case 'u':
							return uncommitted(stmt, offset);
						default:
							return OTHER;
						}
					}
//					return OTHER;
//				}
			}
		}
		return OTHER;
	}

	// COMMITTED
	private static int committed(String stmt, int offset) {
		if (stmt.length() > offset + 8) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			char c4 = stmt.charAt(++offset);
			char c5 = stmt.charAt(++offset);
			char c6 = stmt.charAt(++offset);
			char c7 = stmt.charAt(++offset);
			char c8 = stmt.charAt(++offset);
			if ((c1 == 'O' || c1 == 'o')
					&& (c2 == 'M' || c2 == 'm')
					&& (c3 == 'M' || c3 == 'm')
					&& (c4 == 'I' || c4 == 'i')
					&& (c5 == 'T' || c5 == 't')
					&& (c6 == 'T' || c6 == 't')
					&& (c7 == 'E' || c7 == 'e')
					&& (c8 == 'D' || c8 == 'd')
					&& (stmt.length() == ++offset || ParseUtil.isEOF(stmt
							.charAt(offset)))) {
				return TX_READ_COMMITTED;
			}
		}
		return OTHER;
	}

	// UNCOMMITTED
	private static int uncommitted(String stmt, int offset) {
		if (stmt.length() > offset + 10) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			char c4 = stmt.charAt(++offset);
			char c5 = stmt.charAt(++offset);
			char c6 = stmt.charAt(++offset);
			char c7 = stmt.charAt(++offset);
			char c8 = stmt.charAt(++offset);
			char c9 = stmt.charAt(++offset);
			char c10 = stmt.charAt(++offset);
			if ((c1 == 'N' || c1 == 'n')
					&& (c2 == 'C' || c2 == 'c')
					&& (c3 == 'O' || c3 == 'o')
					&& (c4 == 'M' || c4 == 'm')
					&& (c5 == 'M' || c5 == 'm')
					&& (c6 == 'I' || c6 == 'i')
					&& (c7 == 'T' || c7 == 't')
					&& (c8 == 'T' || c8 == 't')
					&& (c9 == 'E' || c9 == 'e')
					&& (c10 == 'D' || c10 == 'd')
					&& (stmt.length() == ++offset || ParseUtil.isEOF(stmt
							.charAt(offset)))) {
				return TX_READ_UNCOMMITTED;
			}
		}
		return OTHER;
	}

	// REPEATABLE
	private static int pCheck(String stmt, int offset) {
		if (stmt.length() > offset + 8) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			char c4 = stmt.charAt(++offset);
			char c5 = stmt.charAt(++offset);
			char c6 = stmt.charAt(++offset);
			char c7 = stmt.charAt(++offset);
			if ((c1 == 'E' || c1 == 'e') && (c2 == 'A' || c2 == 'a')
					&& (c3 == 'T' || c3 == 't') && (c4 == 'A' || c4 == 'a')
					&& (c5 == 'B' || c5 == 'b') && (c6 == 'L' || c6 == 'l')
					&& (c7 == 'E' || c7 == 'e') && stmt.charAt(++offset) == ' ') {
//				for (;;) {
					while (stmt.length() > ++offset) {
						switch (stmt.charAt(offset)) {
						case ' ':
						case '\r':
						case '\n':
						case '\t':
							continue;
						case 'R':
						case 'r':
							return prCheck(stmt, offset);
						default:
							return OTHER;
						}
					}
//					return OTHER;
//				}
			}
		}
		return OTHER;
	}

	// READ
	private static int prCheck(String stmt, int offset) {
		if (stmt.length() > offset + 3) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			if ((c1 == 'E' || c1 == 'e')
					&& (c2 == 'A' || c2 == 'a')
					&& (c3 == 'D' || c3 == 'd')
					&& (stmt.length() == ++offset || ParseUtil.isEOF(stmt
							.charAt(offset)))) {
				return TX_REPEATED_READ;
			}
		}
		return OTHER;
	}

}