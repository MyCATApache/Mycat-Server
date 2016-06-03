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

import io.mycat.route.parser.util.CharTypes;
import io.mycat.route.parser.util.ParseUtil;

/**
 * @author mycat
 */
public final class ServerParseSelect {

	public static final int OTHER = -1;
	public static final int VERSION_COMMENT = 1;
	public static final int DATABASE = 2;
	public static final int USER = 3;
	public static final int LAST_INSERT_ID = 4;
	public static final int IDENTITY = 5;
	public static final int VERSION = 6;
	public static final int SESSION_INCREMENT = 7;
	public static final int SESSION_ISOLATION = 8;

    public static final int SELECT_VAR_ALL = 9;

	public static final int SESSION_TX_READ_ONLY = 10;

	private static final char[] _VERSION_COMMENT = "VERSION_COMMENT"
			.toCharArray();
	private static final char[] _IDENTITY = "IDENTITY".toCharArray();
	private static final char[] _LAST_INSERT_ID = "LAST_INSERT_ID"
			.toCharArray();
	private static final char[] _DATABASE = "DATABASE()".toCharArray();
	private static final char[] _CURRENT_USER = "CURRENT_USER()".toCharArray();

	public static int parse(String stmt, int offset) {
		int i = offset;
		for (; i < stmt.length(); ++i) {
			switch (stmt.charAt(i)) {
			case ' ':
				continue;
			case '/':
			case '#':
				i = ParseUtil.comment(stmt, i);
				continue;
			case '@':
				return select2Check(stmt, i);
			case 'D':
			case 'd':
				return databaseCheck(stmt, i);
			case 'L':
			case 'l':
				return lastInsertCheck(stmt, i);
			case 'U':
			case 'u':
				return userCheck(stmt, i);
			case 'C':
			case 'c':
				return currentUserCheck(stmt, i);
			case 'V':
			case 'v':
				return versionCheck(stmt, i);
			default:
				return OTHER;
			}
		}
		return OTHER;
	}

	/**
	 * SELECT @@session.auto_increment_increment
	 * 
	 * @param stmt
	 * @param offset
	 * @return
	 */
	private static int sessionVarCheck(String stmt, int offset) {
        String s = stmt.substring(offset).toLowerCase();
        if (s.startsWith("session.auto_increment_increment")) {
            if(s.contains("@@"))
            {
             return    SELECT_VAR_ALL;
            }
			return SESSION_INCREMENT;
		} else if (s
				.startsWith("session.tx_isolation")) {
			return SESSION_ISOLATION;
		}
		else if (s
				.startsWith("session.tx_read_only")) {
			return SESSION_TX_READ_ONLY;
		}
		else {
			return OTHER;
		}
	}

	// SELECT VERSION
	private static int versionCheck(String stmt, int offset) {
		if (stmt.length() > offset + "ERSION".length()) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			char c4 = stmt.charAt(++offset);
			char c5 = stmt.charAt(++offset);
			char c6 = stmt.charAt(++offset);
			if ((c1 == 'E' || c1 == 'e') && (c2 == 'R' || c2 == 'r')
					&& (c3 == 'S' || c3 == 's') && (c4 == 'I' || c4 == 'i')
					&& (c5 == 'O' || c5 == 'o') && (c6 == 'N' || c6 == 'n')) {
				while (stmt.length() > ++offset) {
					switch (stmt.charAt(offset)) {
					case ' ':
					case '\t':
					case '\r':
					case '\n':
						continue;
					case '(':
						return versionParenthesisCheck(stmt, offset);
					default:
						return OTHER;
					}
				}
			}
		}
		return OTHER;
	}

	// SELECT VERSION (
	private static int versionParenthesisCheck(String stmt, int offset) {
		while (stmt.length() > ++offset) {
			switch (stmt.charAt(offset)) {
			case ' ':
			case '\t':
			case '\r':
			case '\n':
				continue;
			case ')':
				while (stmt.length() > ++offset) {
					switch (stmt.charAt(offset)) {
					case ' ':
					case '\t':
					case '\r':
					case '\n':
						continue;
					default:
						return OTHER;
					}
				}
				return VERSION;
			default:
				return OTHER;
			}
		}
		return OTHER;
	}

	/**
	 * <code>SELECT LAST_INSERT_ID() AS id, </code>
	 * 
	 * @param offset
	 *            index of 'i', offset == stmt.length() is possible
	 * @return index of ','. return stmt.length() is possible. -1 if not alias
	 */
	private static int skipAlias(String stmt, int offset) {
		offset = ParseUtil.move(stmt, offset, 0);
		if (offset >= stmt.length()) {
			return offset;
		}
		switch (stmt.charAt(offset)) {
		case '\'':
			return skipString(stmt, offset);
		case '"':
			return skipString2(stmt, offset);
		case '`':
			return skipIdentifierEscape(stmt, offset);
		default:
			if (CharTypes.isIdentifierChar(stmt.charAt(offset))) {
				for (; offset < stmt.length()
						&& CharTypes.isIdentifierChar(stmt.charAt(offset)); ++offset) {
				}
				return offset;
			}
		}
		return -1;
	}

	/**
	 * <code>`abc`d</code>
	 * 
	 * @param offset
	 *            index of first <code>`</code>
	 * @return index of 'd'. return stmt.length() is possible. -1 if string
	 *         invalid
	 */
	private static int skipIdentifierEscape(String stmt, int offset) {
		for (++offset; offset < stmt.length(); ++offset) {
			if (stmt.charAt(offset) == '`'
					&& (++offset >= stmt.length() || stmt.charAt(offset) != '`')) {
					return offset;
			}
		}
		return -1;
	}

	/**
	 * <code>"abc"d</code>
	 * 
	 * @param offset
	 *            index of first <code>"</code>
	 * @return index of 'd'. return stmt.length() is possible. -1 if string
	 *         invalid
	 */
	private static int skipString2(String stmt, int offset) {
		int state = 0;
		for (++offset; offset < stmt.length(); ++offset) {
			char c = stmt.charAt(offset);
			switch (state) {
			case 0:
				switch (c) {
				case '\\':
					state = 1;
					break;
				case '"':
					state = 2;
					break;
				}
				break;
			case 1:
				state = 0;
				break;
			case 2:
				switch (c) {
				case '"':
					state = 0;
					break;
				default:
					return offset;
				}
				break;
			}
		}
		if (offset == stmt.length() && state == 2) {
			return stmt.length();
		}
		return -1;
	}

	/**
	 * <code>'abc'd</code>
	 * 
	 * @param offset
	 *            index of first <code>'</code>
	 * @return index of 'd'. return stmt.length() is possible. -1 if string
	 *         invalid
	 */
	private static int skipString(String stmt, int offset) {
		int state = 0;
		for (++offset; offset < stmt.length(); ++offset) {
			char c = stmt.charAt(offset);
			switch (state) {
			case 0:
				switch (c) {
				case '\\':
					state = 1;
					break;
				case '\'':
					state = 2;
					break;
				}
				break;
			case 1:
				state = 0;
				break;
			case 2:
				switch (c) {
				case '\'':
					state = 0;
					break;
				default:
					return offset;
				}
				break;
			}
		}
		if (offset == stmt.length() && state == 2) {
			return stmt.length();
		}
		return -1;
	}

	/**
	 * <code>SELECT LAST_INSERT_ID() AS id</code>
	 * 
	 * @param offset
	 *            index of first ' ' after LAST_INSERT_ID(), offset ==
	 *            stmt.length() is possible
	 * @return index of 'i'. return stmt.length() is possible
	 */
	public static int skipAs(String stmt, int offset) {
		offset = ParseUtil.move(stmt, offset, 0);
		if (stmt.length() > offset + "AS".length()
				&& (stmt.charAt(offset) == 'A' || stmt.charAt(offset) == 'a')
				&& (stmt.charAt(offset + 1) == 'S' || stmt.charAt(offset + 1) == 's')
				&& (stmt.charAt(offset + 2) == ' '
						|| stmt.charAt(offset + 2) == '\r'
						|| stmt.charAt(offset + 2) == '\n'
						|| stmt.charAt(offset + 2) == '\t'
						|| stmt.charAt(offset + 2) == '/' || stmt
						.charAt(offset + 2) == '#')) {
			offset = ParseUtil.move(stmt, offset + 2, 0);
		}
		return offset;
	}

	/**
	 * @param offset
	 *            <code>stmt.charAt(offset) == first 'L' OR 'l'</code>
	 * @return index after LAST_INSERT_ID(), might equals to length. -1 if not
	 *         LAST_INSERT_ID
	 */
	public static int indexAfterLastInsertIdFunc(String stmt, int offset) {
		if (stmt.length() >= offset + "LAST_INSERT_ID()".length()
				&& ParseUtil.compare(stmt, offset, _LAST_INSERT_ID)) {
				offset = ParseUtil.move(stmt, offset + _LAST_INSERT_ID.length,
						0);
				if (offset + 1 < stmt.length() && stmt.charAt(offset) == '(') {
					offset = ParseUtil.move(stmt, offset + 1, 0);
					if (offset < stmt.length() && stmt.charAt(offset) == ')') {
						return ++offset;
					}
				}
		}
		return -1;
	}

	/**
	 * @param offset
	 *            <code>stmt.charAt(offset) == first '`' OR 'i' OR 'I' OR '\'' OR '"'</code>
	 * @return index after identity or `identity` or "identity" or 'identity',
	 *         might equals to length. -1 if not identity or `identity` or
	 *         "identity" or 'identity'
	 */
	public static int indexAfterIdentity(String stmt, int offset) {
		char first = stmt.charAt(offset);
		switch (first) {
		case '`':
		case '\'':
		case '"':
			if (stmt.length() < offset + "identity".length() + 2) {
				return -1;
			}
			if (stmt.charAt(offset + "identity".length() + 1) != first) {
				return -1;
			}
			++offset;
			break;
		case 'i':
		case 'I':
			if (stmt.length() < offset + "identity".length()) {
				return -1;
			}
			break;
		default:
			return -1;
		}
		if (ParseUtil.compare(stmt, offset, _IDENTITY)) {
			offset += _IDENTITY.length;
			switch (first) {
			case '`':
			case '\'':
			case '"':
				return ++offset;
			}
			return offset;
		}
		return -1;
	}

	/**
	 * SELECT LAST_INSERT_ID()
	 */
	static int lastInsertCheck(String stmt, int offset) {
		offset = indexAfterLastInsertIdFunc(stmt, offset);
		if (offset < 0) {
			return OTHER;
		}
		offset = skipAs(stmt, offset);
		offset = skipAlias(stmt, offset);
		if (offset < 0) {
			return OTHER;
		}
		offset = ParseUtil.move(stmt, offset, 0);
		if (offset < stmt.length()) {
			return OTHER;
		}
		return LAST_INSERT_ID;
	}

	/**
	 * select @@identity<br/>
	 * select @@identiTy aS iD
	 */
	static int identityCheck(String stmt, int offset) {
		offset = indexAfterIdentity(stmt, offset);
		if (offset < 0) {
			return OTHER;
		}
		offset = skipAs(stmt, offset);
		offset = skipAlias(stmt, offset);
		if (offset < 0) {
			return OTHER;
		}
		offset = ParseUtil.move(stmt, offset, 0);
		if (offset < stmt.length()) {
			return OTHER;
		}
		return IDENTITY;
	}

	static int select2Check(String stmt, int offset) {
		if (stmt.length() > ++offset && stmt.charAt(offset) == '@'
				&& stmt.length() > ++offset) {
				switch (stmt.charAt(offset)) {
				case 'V':
				case 'v':
					return versionCommentCheck(stmt, offset);
				case 'i':
				case 'I':
					return identityCheck(stmt, offset);
				case 's':
				case 'S':
					return sessionVarCheck(stmt, offset);
				default:
					return OTHER;
				}
		}
		return OTHER;
	}

	/**
	 * SELECT DATABASE()
	 */
	static int databaseCheck(String stmt, int offset) {
		int length = offset + _DATABASE.length;
		if (stmt.length() >= length
				&& ParseUtil.compare(stmt, offset, _DATABASE)) {
			if (stmt.length() > length && stmt.charAt(length) != ' ') {
				return OTHER;
			} else {
				return DATABASE;
			}
		}
		return OTHER;
	}

	/**
	 * SELECT USER()
	 */
	static int userCheck(String stmt, int offset) {
		if (stmt.length() > offset + 5) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			char c4 = stmt.charAt(++offset);
			char c5 = stmt.charAt(++offset);
			if ((c1 == 'S' || c1 == 's')
					&& (c2 == 'E' || c2 == 'e')
					&& (c3 == 'R' || c3 == 'r')
					&& (c4 == '(')
					&& (c5 == ')')
					&& (stmt.length() == ++offset || ParseUtil.isEOF(stmt
							.charAt(offset)))) {
				return USER;
			}
		}
		return OTHER;
	}

	/**
	 * SELECT USER()
	 */
	static int currentUserCheck(String stmt, int offset) {
		int length = offset + _CURRENT_USER.length;
		if (stmt.length() >= length
				&& ParseUtil.compare(stmt, offset, _CURRENT_USER)) {
				if (stmt.length() > length && stmt.charAt(length) != ' ') {
					return OTHER;
				}
				return USER;
		}
		return OTHER;
	}

	/**
	 * SELECT @@VERSION_COMMENT
	 */
	static int versionCommentCheck(String stmt, int offset) {
		int length = offset + _VERSION_COMMENT.length;
		if (stmt.length() >= length
				&& ParseUtil.compare(stmt, offset, _VERSION_COMMENT)) {
			if (stmt.length() > length && stmt.charAt(length) != ' ') {
				return OTHER;
			} else {
				return VERSION_COMMENT;
			}
		}
		return OTHER;
	}

}
