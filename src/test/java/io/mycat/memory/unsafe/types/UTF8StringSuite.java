package io.mycat.memory.unsafe.types;

/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/


import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;

import static org.junit.Assert.*;


public class UTF8StringSuite {

  private static void checkBasic(String str, int len) throws UnsupportedEncodingException {
    UTF8String s1 = UTF8String.fromString(str);
    UTF8String s2 = UTF8String.fromBytes(str.getBytes("utf8"));
    assertEquals(s1.numChars(), len);
    assertEquals(s2.numChars(), len);

    assertEquals(s1.toString(), str);
    assertEquals(s2.toString(), str);
    assertEquals(s1, s2);

    assertEquals(s1.hashCode(), s2.hashCode());

    assertEquals(0, s1.compareTo(s2));

    assertTrue(s1.contains(s2));
    assertTrue(s2.contains(s1));
    assertTrue(s1.startsWith(s1));
    assertTrue(s1.endsWith(s1));
  }

  @Test
  public void basicTest() throws UnsupportedEncodingException {
    checkBasic("", 0);
    checkBasic("hello", 5);
    checkBasic("大 千 世 界", 7);
  }

  @Test
  public void emptyStringTest() {
    assertEquals(UTF8String.EMPTY_UTF8, UTF8String.fromString(""));
    assertEquals(UTF8String.EMPTY_UTF8, UTF8String.fromBytes(new byte[0]));
    assertEquals(0, UTF8String.EMPTY_UTF8.numChars());
    assertEquals(0, UTF8String.EMPTY_UTF8.numBytes());
  }

  @Test
  public void prefix() {
    assertTrue(UTF8String.fromString("a").getPrefix() - UTF8String.fromString("b").getPrefix() < 0);
    assertTrue(UTF8String.fromString("ab").getPrefix() - UTF8String.fromString("b").getPrefix() < 0);
    assertTrue(
      UTF8String.fromString("abbbbbbbbbbbasdf").getPrefix() - UTF8String.fromString("bbbbbbbbbbbbasdf").getPrefix() < 0);
    assertTrue(UTF8String.fromString("").getPrefix() - UTF8String.fromString("a").getPrefix() < 0);
    assertTrue(UTF8String.fromString("你好").getPrefix() - UTF8String.fromString("世界").getPrefix() > 0);

    byte[] buf1 = {1, 2, 3, 4, 5, 6, 7, 8, 9};
    byte[] buf2 = {1, 2, 3};
    UTF8String str1 = UTF8String.fromBytes(buf1, 0, 3);
    UTF8String str2 = UTF8String.fromBytes(buf1, 0, 8);
    UTF8String str3 = UTF8String.fromBytes(buf2);
    assertTrue(str1.getPrefix() - str2.getPrefix() < 0);
    assertEquals(str1.getPrefix(), str3.getPrefix());
  }

  @Test
  public void compareTo() {
    assertTrue(UTF8String.fromString("").compareTo(UTF8String.fromString("a")) < 0);
    assertTrue(UTF8String.fromString("abc").compareTo(UTF8String.fromString("ABC")) > 0);
    assertTrue(UTF8String.fromString("abc0").compareTo(UTF8String.fromString("abc")) > 0);
    assertTrue(UTF8String.fromString("abcabcabc").compareTo(UTF8String.fromString("abcabcabc")) == 0);
    assertTrue(UTF8String.fromString("aBcabcabc").compareTo(UTF8String.fromString("Abcabcabc")) > 0);
    assertTrue(UTF8String.fromString("Abcabcabc").compareTo(UTF8String.fromString("abcabcabC")) < 0);
    assertTrue(UTF8String.fromString("abcabcabc").compareTo(UTF8String.fromString("abcabcabC")) > 0);

    assertTrue(UTF8String.fromString("abc").compareTo(UTF8String.fromString("世界")) < 0);
    assertTrue(UTF8String.fromString("你好").compareTo(UTF8String.fromString("世界")) > 0);
    assertTrue(UTF8String.fromString("你好123").compareTo(UTF8String.fromString("你好122")) > 0);
  }

  protected static void testUpperandLower(String upper, String lower) {
    UTF8String us = UTF8String.fromString(upper);
    UTF8String ls = UTF8String.fromString(lower);
    assertEquals(ls, us.toLowerCase());
    assertEquals(us, ls.toUpperCase());
    assertEquals(us, us.toUpperCase());
    assertEquals(ls, ls.toLowerCase());
  }

  @Test
  public void upperAndLower() {
    testUpperandLower("", "");
    testUpperandLower("0123456", "0123456");
    testUpperandLower("ABCXYZ", "abcxyz");
    testUpperandLower("ЀЁЂѺΏỀ", "ѐёђѻώề");
    testUpperandLower("大千世界 数据砖头", "大千世界 数据砖头");
  }

  @Test
  public void titleCase() {
    assertEquals(UTF8String.fromString(""), UTF8String.fromString("").toTitleCase());
    assertEquals(UTF8String.fromString("Ab Bc Cd"), UTF8String.fromString("ab bc cd").toTitleCase());
    assertEquals(UTF8String.fromString("Ѐ Ё Ђ Ѻ Ώ Ề"), UTF8String.fromString("ѐ ё ђ ѻ ώ ề").toTitleCase());
    assertEquals(UTF8String.fromString("大千世界 数据砖头"), UTF8String.fromString("大千世界 数据砖头").toTitleCase());
  }

  @Test
  public void concatTest() {
    assertEquals(UTF8String.EMPTY_UTF8, UTF8String.concat());
    assertNull(UTF8String.concat((UTF8String) null));
    assertEquals(UTF8String.EMPTY_UTF8, UTF8String.concat(UTF8String.EMPTY_UTF8));
    assertEquals(UTF8String.fromString("ab"), UTF8String.concat(UTF8String.fromString("ab")));
    assertEquals(UTF8String.fromString("ab"), UTF8String.concat(UTF8String.fromString("a"), UTF8String.fromString("b")));
    assertEquals(UTF8String.fromString("abc"), UTF8String.concat(UTF8String.fromString("a"), UTF8String.fromString("b"), UTF8String.fromString("c")));
    assertNull(UTF8String.concat(UTF8String.fromString("a"), null, UTF8String.fromString("c")));
    assertNull(UTF8String.concat(UTF8String.fromString("a"), null, null));
    assertNull(UTF8String.concat(null, null, null));
    assertEquals(UTF8String.fromString("数据砖头"), UTF8String.concat(UTF8String.fromString("数据"), UTF8String.fromString("砖头")));
  }

  @Test
  public void concatWsTest() {
    // Returns null if the separator is null
    assertNull(UTF8String.concatWs(null, (UTF8String) null));
    assertNull(UTF8String.concatWs(null, UTF8String.fromString("a")));

    // If separator is null, concatWs should skip all null inputs and never return null.
    UTF8String sep = UTF8String.fromString("哈哈");
    assertEquals(
      UTF8String.EMPTY_UTF8,
      UTF8String.concatWs(sep, UTF8String.EMPTY_UTF8));
    assertEquals(
      UTF8String.fromString("ab"),
      UTF8String.concatWs(sep, UTF8String.fromString("ab")));
    assertEquals(
      UTF8String.fromString("a哈哈b"),
      UTF8String.concatWs(sep, UTF8String.fromString("a"), UTF8String.fromString("b")));
    assertEquals(
      UTF8String.fromString("a哈哈b哈哈c"),
      UTF8String.concatWs(sep, UTF8String.fromString("a"), UTF8String.fromString("b"), UTF8String.fromString("c")));
    assertEquals(
      UTF8String.fromString("a哈哈c"),
      UTF8String.concatWs(sep, UTF8String.fromString("a"), null, UTF8String.fromString("c")));
    assertEquals(
      UTF8String.fromString("a"),
      UTF8String.concatWs(sep, UTF8String.fromString("a"), null, null));
    assertEquals(
      UTF8String.EMPTY_UTF8,
      UTF8String.concatWs(sep, null, null, null));
    assertEquals(
      UTF8String.fromString("数据哈哈砖头"),
      UTF8String.concatWs(sep, UTF8String.fromString("数据"), UTF8String.fromString("砖头")));
  }

  @Test
  public void contains() {
    assertTrue(UTF8String.EMPTY_UTF8.contains(UTF8String.EMPTY_UTF8));
    assertTrue(UTF8String.fromString("hello").contains(UTF8String.fromString("ello")));
    assertFalse(UTF8String.fromString("hello").contains(UTF8String.fromString("vello")));
    assertFalse(UTF8String.fromString("hello").contains(UTF8String.fromString("hellooo")));
    assertTrue(UTF8String.fromString("大千世界").contains(UTF8String.fromString("千世界")));
    assertFalse(UTF8String.fromString("大千世界").contains(UTF8String.fromString("世千")));
    assertFalse(UTF8String.fromString("大千世界").contains(UTF8String.fromString("大千世界好")));
  }

  @Test
  public void startsWith() {
    assertTrue(UTF8String.EMPTY_UTF8.startsWith(UTF8String.EMPTY_UTF8));
    assertTrue(UTF8String.fromString("hello").startsWith(UTF8String.fromString("hell")));
    assertFalse(UTF8String.fromString("hello").startsWith(UTF8String.fromString("ell")));
    assertFalse(UTF8String.fromString("hello").startsWith(UTF8String.fromString("hellooo")));
    assertTrue(UTF8String.fromString("数据砖头").startsWith(UTF8String.fromString("数据")));
    assertFalse(UTF8String.fromString("大千世界").startsWith(UTF8String.fromString("千")));
    assertFalse(UTF8String.fromString("大千世界").startsWith(UTF8String.fromString("大千世界好")));
  }

  @Test
  public void endsWith() {
    assertTrue(UTF8String.EMPTY_UTF8.endsWith(UTF8String.EMPTY_UTF8));
    assertTrue(UTF8String.fromString("hello").endsWith(UTF8String.fromString("ello")));
    assertFalse(UTF8String.fromString("hello").endsWith(UTF8String.fromString("ellov")));
    assertFalse(UTF8String.fromString("hello").endsWith(UTF8String.fromString("hhhello")));
    assertTrue(UTF8String.fromString("大千世界").endsWith(UTF8String.fromString("世界")));
    assertFalse(UTF8String.fromString("大千世界").endsWith(UTF8String.fromString("世")));
    assertFalse(UTF8String.fromString("数据砖头").endsWith(UTF8String.fromString("我的数据砖头")));
  }

  @Test
  public void substring() {
    assertEquals(UTF8String.EMPTY_UTF8, UTF8String.fromString("hello").substring(0, 0));
    assertEquals(UTF8String.fromString("el"), UTF8String.fromString("hello").substring(1, 3));
    assertEquals(UTF8String.fromString("数"), UTF8String.fromString("数据砖头").substring(0, 1));
    assertEquals(UTF8String.fromString("据砖"), UTF8String.fromString("数据砖头").substring(1, 3));
    assertEquals(UTF8String.fromString("头"), UTF8String.fromString("数据砖头").substring(3, 5));
    assertEquals(UTF8String.fromString("ߵ梷"), UTF8String.fromString("ߵ梷").substring(0, 2));
  }

  @Test
  public void trims() {
    assertEquals(UTF8String.fromString("hello"), UTF8String.fromString("  hello ").trim());
    assertEquals(UTF8String.fromString("hello "), UTF8String.fromString("  hello ").trimLeft());
    assertEquals(UTF8String.fromString("  hello"), UTF8String.fromString("  hello ").trimRight());

    assertEquals(UTF8String.EMPTY_UTF8, UTF8String.fromString("  ").trim());
    assertEquals(UTF8String.EMPTY_UTF8, UTF8String.fromString("  ").trimLeft());
    assertEquals(UTF8String.EMPTY_UTF8, UTF8String.fromString("  ").trimRight());

    assertEquals(UTF8String.fromString("数据砖头"), UTF8String.fromString("  数据砖头 ").trim());
    assertEquals(UTF8String.fromString("数据砖头 "), UTF8String.fromString("  数据砖头 ").trimLeft());
    assertEquals(UTF8String.fromString("  数据砖头"), UTF8String.fromString("  数据砖头 ").trimRight());

    assertEquals(UTF8String.fromString("数据砖头"), UTF8String.fromString("数据砖头").trim());
    assertEquals(UTF8String.fromString("数据砖头"), UTF8String.fromString("数据砖头").trimLeft());
    assertEquals(UTF8String.fromString("数据砖头"), UTF8String.fromString("数据砖头").trimRight());
  }

  @Test
  public void indexOf() {
    assertEquals(0, UTF8String.EMPTY_UTF8.indexOf(UTF8String.EMPTY_UTF8, 0));
    assertEquals(-1, UTF8String.EMPTY_UTF8.indexOf(UTF8String.fromString("l"), 0));
    assertEquals(0, UTF8String.fromString("hello").indexOf(UTF8String.EMPTY_UTF8, 0));
    assertEquals(2, UTF8String.fromString("hello").indexOf(UTF8String.fromString("l"), 0));
    assertEquals(3, UTF8String.fromString("hello").indexOf(UTF8String.fromString("l"), 3));
    assertEquals(-1, UTF8String.fromString("hello").indexOf(UTF8String.fromString("a"), 0));
    assertEquals(2, UTF8String.fromString("hello").indexOf(UTF8String.fromString("ll"), 0));
    assertEquals(-1, UTF8String.fromString("hello").indexOf(UTF8String.fromString("ll"), 4));
    assertEquals(1, UTF8String.fromString("数据砖头").indexOf(UTF8String.fromString("据砖"), 0));
    assertEquals(-1, UTF8String.fromString("数据砖头").indexOf(UTF8String.fromString("数"), 3));
    assertEquals(0, UTF8String.fromString("数据砖头").indexOf(UTF8String.fromString("数"), 0));
    assertEquals(3, UTF8String.fromString("数据砖头").indexOf(UTF8String.fromString("头"), 0));
  }

  @Test
  public void substring_index() {
    assertEquals(UTF8String.fromString("www.apache.org"),
      UTF8String.fromString("www.apache.org").subStringIndex(UTF8String.fromString("."), 3));
    assertEquals(UTF8String.fromString("www.apache"),
      UTF8String.fromString("www.apache.org").subStringIndex(UTF8String.fromString("."), 2));
    assertEquals(UTF8String.fromString("www"),
      UTF8String.fromString("www.apache.org").subStringIndex(UTF8String.fromString("."), 1));
    assertEquals(UTF8String.fromString(""),
      UTF8String.fromString("www.apache.org").subStringIndex(UTF8String.fromString("."), 0));
    assertEquals(UTF8String.fromString("org"),
      UTF8String.fromString("www.apache.org").subStringIndex(UTF8String.fromString("."), -1));
    assertEquals(UTF8String.fromString("apache.org"),
      UTF8String.fromString("www.apache.org").subStringIndex(UTF8String.fromString("."), -2));
    assertEquals(UTF8String.fromString("www.apache.org"),
      UTF8String.fromString("www.apache.org").subStringIndex(UTF8String.fromString("."), -3));
    // str is empty string
    assertEquals(UTF8String.fromString(""),
      UTF8String.fromString("").subStringIndex(UTF8String.fromString("."), 1));
    // empty string delim
    assertEquals(UTF8String.fromString(""),
      UTF8String.fromString("www.apache.org").subStringIndex(UTF8String.fromString(""), 1));
    // delim does not exist in str
    assertEquals(UTF8String.fromString("www.apache.org"),
      UTF8String.fromString("www.apache.org").subStringIndex(UTF8String.fromString("#"), 2));
    // delim is 2 chars
    assertEquals(UTF8String.fromString("www||apache"),
      UTF8String.fromString("www||apache||org").subStringIndex(UTF8String.fromString("||"), 2));
    assertEquals(UTF8String.fromString("apache||org"),
      UTF8String.fromString("www||apache||org").subStringIndex(UTF8String.fromString("||"), -2));
    // non ascii chars
    assertEquals(UTF8String.fromString("大千世界大"),
      UTF8String.fromString("大千世界大千世界").subStringIndex(UTF8String.fromString("千"), 2));
    // overlapped delim
    assertEquals(UTF8String.fromString("||"), UTF8String.fromString("||||||").subStringIndex(UTF8String.fromString("|||"), 3));
    assertEquals(UTF8String.fromString("|||"), UTF8String.fromString("||||||").subStringIndex(UTF8String.fromString("|||"), -4));
  }

  @Test
  public void reverse() {
    assertEquals(UTF8String.fromString("olleh"), UTF8String.fromString("hello").reverse());
    assertEquals(UTF8String.EMPTY_UTF8, UTF8String.EMPTY_UTF8.reverse());
    assertEquals(UTF8String.fromString("者行孙"), UTF8String.fromString("孙行者").reverse());
    assertEquals(UTF8String.fromString("者行孙 olleh"), UTF8String.fromString("hello 孙行者").reverse());
  }

  @Test
  public void repeat() {
    assertEquals(UTF8String.fromString("数d数d数d数d数d"), UTF8String.fromString("数d").repeat(5));
    assertEquals(UTF8String.fromString("数d"), UTF8String.fromString("数d").repeat(1));
    assertEquals(UTF8String.EMPTY_UTF8, UTF8String.fromString("数d").repeat(-1));
  }

  @Test
  public void pad() {
    assertEquals(UTF8String.fromString("hel"), UTF8String.fromString("hello").lpad(3, UTF8String.fromString("????")));
    assertEquals(UTF8String.fromString("hello"), UTF8String.fromString("hello").lpad(5, UTF8String.fromString("????")));
    assertEquals(UTF8String.fromString("?hello"), UTF8String.fromString("hello").lpad(6, UTF8String.fromString("????")));
    assertEquals(UTF8String.fromString("???????hello"), UTF8String.fromString("hello").lpad(12, UTF8String.fromString("????")));
    assertEquals(UTF8String.fromString("?????hello"), UTF8String.fromString("hello").lpad(10, UTF8String.fromString("?????")));
    assertEquals(UTF8String.fromString("???????"), UTF8String.EMPTY_UTF8.lpad(7, UTF8String.fromString("?????")));

    assertEquals(UTF8String.fromString("hel"), UTF8String.fromString("hello").rpad(3, UTF8String.fromString("????")));
    assertEquals(UTF8String.fromString("hello"), UTF8String.fromString("hello").rpad(5, UTF8String.fromString("????")));
    assertEquals(UTF8String.fromString("hello?"), UTF8String.fromString("hello").rpad(6, UTF8String.fromString("????")));
    assertEquals(UTF8String.fromString("hello???????"), UTF8String.fromString("hello").rpad(12, UTF8String.fromString("????")));
    assertEquals(UTF8String.fromString("hello?????"), UTF8String.fromString("hello").rpad(10, UTF8String.fromString("?????")));
    assertEquals(UTF8String.fromString("???????"), UTF8String.EMPTY_UTF8.rpad(7, UTF8String.fromString("?????")));

    assertEquals(UTF8String.fromString("数据砖"), UTF8String.fromString("数据砖头").lpad(3, UTF8String.fromString("????")));
    assertEquals(UTF8String.fromString("?数据砖头"), UTF8String.fromString("数据砖头").lpad(5, UTF8String.fromString("????")));
    assertEquals(UTF8String.fromString("??数据砖头"), UTF8String.fromString("数据砖头").lpad(6, UTF8String.fromString("????")));
    assertEquals(UTF8String.fromString("孙行数据砖头"), UTF8String.fromString("数据砖头").lpad(6, UTF8String.fromString("孙行者")));
    assertEquals(UTF8String.fromString("孙行者数据砖头"), UTF8String.fromString("数据砖头").lpad(7, UTF8String.fromString("孙行者")));
    assertEquals(
      UTF8String.fromString("孙行者孙行者孙行数据砖头"),
      UTF8String.fromString("数据砖头").lpad(12, UTF8String.fromString("孙行者")));

    assertEquals(UTF8String.fromString("数据砖"), UTF8String.fromString("数据砖头").rpad(3, UTF8String.fromString("????")));
    assertEquals(UTF8String.fromString("数据砖头?"), UTF8String.fromString("数据砖头").rpad(5, UTF8String.fromString("????")));
    assertEquals(UTF8String.fromString("数据砖头??"), UTF8String.fromString("数据砖头").rpad(6, UTF8String.fromString("????")));
    assertEquals(UTF8String.fromString("数据砖头孙行"), UTF8String.fromString("数据砖头").rpad(6, UTF8String.fromString("孙行者")));
    assertEquals(UTF8String.fromString("数据砖头孙行者"), UTF8String.fromString("数据砖头").rpad(7, UTF8String.fromString("孙行者")));
    assertEquals(
      UTF8String.fromString("数据砖头孙行者孙行者孙行"),
      UTF8String.fromString("数据砖头").rpad(12, UTF8String.fromString("孙行者")));

    assertEquals(UTF8String.EMPTY_UTF8, UTF8String.fromString("数据砖头").lpad(-10, UTF8String.fromString("孙行者")));
    assertEquals(UTF8String.EMPTY_UTF8, UTF8String.fromString("数据砖头").lpad(-10, UTF8String.EMPTY_UTF8));
    assertEquals(UTF8String.fromString("数据砖头"), UTF8String.fromString("数据砖头").lpad(5, UTF8String.EMPTY_UTF8));
    assertEquals(UTF8String.fromString("数据砖"), UTF8String.fromString("数据砖头").lpad(3, UTF8String.EMPTY_UTF8));
    assertEquals(UTF8String.EMPTY_UTF8, UTF8String.EMPTY_UTF8.lpad(3, UTF8String.EMPTY_UTF8));

    assertEquals(UTF8String.EMPTY_UTF8, UTF8String.fromString("数据砖头").rpad(-10, UTF8String.fromString("孙行者")));
    assertEquals(UTF8String.EMPTY_UTF8, UTF8String.fromString("数据砖头").rpad(-10, UTF8String.EMPTY_UTF8));
    assertEquals(UTF8String.fromString("数据砖头"), UTF8String.fromString("数据砖头").rpad(5, UTF8String.EMPTY_UTF8));
    assertEquals(UTF8String.fromString("数据砖"), UTF8String.fromString("数据砖头").rpad(3, UTF8String.EMPTY_UTF8));
    assertEquals(UTF8String.EMPTY_UTF8, UTF8String.EMPTY_UTF8.rpad(3, UTF8String.EMPTY_UTF8));
  }

  @Test
  public void substringSQL() {
    UTF8String e = UTF8String.fromString("example");
    assertEquals(e.substringSQL(0, 2), UTF8String.fromString("ex"));
    assertEquals(e.substringSQL(1, 2), UTF8String.fromString("ex"));
    assertEquals(e.substringSQL(0, 7), UTF8String.fromString("example"));
    assertEquals(e.substringSQL(1, 2), UTF8String.fromString("ex"));
    assertEquals(e.substringSQL(0, 100), UTF8String.fromString("example"));
    assertEquals(e.substringSQL(1, 100), UTF8String.fromString("example"));
    assertEquals(e.substringSQL(2, 2), UTF8String.fromString("xa"));
    assertEquals(e.substringSQL(1, 6), UTF8String.fromString("exampl"));
    assertEquals(e.substringSQL(2, 100), UTF8String.fromString("xample"));
    assertEquals(e.substringSQL(0, 0), UTF8String.fromString(""));
    assertEquals(e.substringSQL(100, 4), UTF8String.EMPTY_UTF8);
    assertEquals(e.substringSQL(0, Integer.MAX_VALUE), UTF8String.fromString("example"));
    assertEquals(e.substringSQL(1, Integer.MAX_VALUE), UTF8String.fromString("example"));
    assertEquals(e.substringSQL(2, Integer.MAX_VALUE), UTF8String.fromString("xample"));
  }

  @Test
  public void split() {
    assertTrue(Arrays.equals(UTF8String.fromString("ab,def,ghi").split(UTF8String.fromString(","), -1),
      new UTF8String[]{UTF8String.fromString("ab"), UTF8String.fromString("def"), UTF8String.fromString("ghi")}));
    assertTrue(Arrays.equals(UTF8String.fromString("ab,def,ghi").split(UTF8String.fromString(","), 2),
      new UTF8String[]{UTF8String.fromString("ab"), UTF8String.fromString("def,ghi")}));
    assertTrue(Arrays.equals(UTF8String.fromString("ab,def,ghi").split(UTF8String.fromString(","), 2),
      new UTF8String[]{UTF8String.fromString("ab"), UTF8String.fromString("def,ghi")}));
  }
  
  @Test
  public void levenshteinDistance() {
    assertEquals(0, UTF8String.EMPTY_UTF8.levenshteinDistance(UTF8String.EMPTY_UTF8));
    assertEquals(1, UTF8String.EMPTY_UTF8.levenshteinDistance(UTF8String.fromString("a")));
    assertEquals(7, UTF8String.fromString("aaapppp").levenshteinDistance(UTF8String.EMPTY_UTF8));
    assertEquals(1, UTF8String.fromString("frog").levenshteinDistance(UTF8String.fromString("fog")));
    assertEquals(3, UTF8String.fromString("fly").levenshteinDistance(UTF8String.fromString("ant")));
    assertEquals(7, UTF8String.fromString("elephant").levenshteinDistance(UTF8String.fromString("hippo")));
    assertEquals(7, UTF8String.fromString("hippo").levenshteinDistance(UTF8String.fromString("elephant")));
    assertEquals(8, UTF8String.fromString("hippo").levenshteinDistance(UTF8String.fromString("zzzzzzzz")));
    assertEquals(1, UTF8String.fromString("hello").levenshteinDistance(UTF8String.fromString("hallo")));
    assertEquals(4, UTF8String.fromString("世界千世").levenshteinDistance(UTF8String.fromString("千a世b")));
  }

  @Test
  public void translate() {
    assertEquals(
      UTF8String.fromString("1a2s3ae"),
      UTF8String.fromString("translate").translate(ImmutableMap.of(
        'r', '1',
        'n', '2',
        'l', '3',
        't', '\0'
      )));
    assertEquals(
      UTF8String.fromString("translate"),
      UTF8String.fromString("translate").translate(new HashMap<Character, Character>()));
    assertEquals(
      UTF8String.fromString("asae"),
      UTF8String.fromString("translate").translate(ImmutableMap.of(
        'r', '\0',
        'n', '\0',
        'l', '\0',
        't', '\0'
      )));
    assertEquals(
      UTF8String.fromString("aa世b"),
      UTF8String.fromString("花花世界").translate(ImmutableMap.of(
        '花', 'a',
        '界', 'b'
      )));
  }

  @Test
  public void createBlankString() {
    assertEquals(UTF8String.fromString(" "), UTF8String.blankString(1));
    assertEquals(UTF8String.fromString("  "), UTF8String.blankString(2));
    assertEquals(UTF8String.fromString("   "), UTF8String.blankString(3));
    assertEquals(UTF8String.fromString(""), UTF8String.blankString(0));
  }

  @Test
  public void findInSet() {
    assertEquals(1, UTF8String.fromString("ab").findInSet(UTF8String.fromString("ab")));
    assertEquals(2, UTF8String.fromString("a,b").findInSet(UTF8String.fromString("b")));
    assertEquals(3, UTF8String.fromString("abc,b,ab,c,def").findInSet(UTF8String.fromString("ab")));
    assertEquals(1, UTF8String.fromString("ab,abc,b,ab,c,def").findInSet(UTF8String.fromString("ab")));
    assertEquals(4, UTF8String.fromString(",,,ab,abc,b,ab,c,def").findInSet(UTF8String.fromString("ab")));
    assertEquals(1, UTF8String.fromString(",ab,abc,b,ab,c,def").findInSet(UTF8String.fromString("")));
    assertEquals(4, UTF8String.fromString("数据砖头,abc,b,ab,c,def").findInSet(UTF8String.fromString("ab")));
    assertEquals(6, UTF8String.fromString("数据砖头,abc,b,ab,c,def").findInSet(UTF8String.fromString("def")));
  }

  @Test
  public void soundex() {
    assertEquals(UTF8String.fromString("Robert").soundex(), UTF8String.fromString("R163"));
    assertEquals(UTF8String.fromString("Rupert").soundex(), UTF8String.fromString("R163"));
    assertEquals(UTF8String.fromString("Rubin").soundex(), UTF8String.fromString("R150"));
    assertEquals(UTF8String.fromString("Ashcraft").soundex(), UTF8String.fromString("A261"));
    assertEquals(UTF8String.fromString("Ashcroft").soundex(), UTF8String.fromString("A261"));
    assertEquals(UTF8String.fromString("Burroughs").soundex(), UTF8String.fromString("B620"));
    assertEquals(UTF8String.fromString("Burrows").soundex(), UTF8String.fromString("B620"));
    assertEquals(UTF8String.fromString("Ekzampul").soundex(), UTF8String.fromString("E251"));
    assertEquals(UTF8String.fromString("Example").soundex(), UTF8String.fromString("E251"));
    assertEquals(UTF8String.fromString("Ellery").soundex(), UTF8String.fromString("E460"));
    assertEquals(UTF8String.fromString("Euler").soundex(), UTF8String.fromString("E460"));
    assertEquals(UTF8String.fromString("Ghosh").soundex(), UTF8String.fromString("G200"));
    assertEquals(UTF8String.fromString("Gauss").soundex(), UTF8String.fromString("G200"));
    assertEquals(UTF8String.fromString("Gutierrez").soundex(), UTF8String.fromString("G362"));
    assertEquals(UTF8String.fromString("Heilbronn").soundex(), UTF8String.fromString("H416"));
    assertEquals(UTF8String.fromString("Hilbert").soundex(), UTF8String.fromString("H416"));
    assertEquals(UTF8String.fromString("Jackson").soundex(), UTF8String.fromString("J250"));
    assertEquals(UTF8String.fromString("Kant").soundex(), UTF8String.fromString("K530"));
    assertEquals(UTF8String.fromString("Knuth").soundex(), UTF8String.fromString("K530"));
    assertEquals(UTF8String.fromString("Lee").soundex(), UTF8String.fromString("L000"));
    assertEquals(UTF8String.fromString("Lukasiewicz").soundex(), UTF8String.fromString("L222"));
    assertEquals(UTF8String.fromString("Lissajous").soundex(), UTF8String.fromString("L222"));
    assertEquals(UTF8String.fromString("Ladd").soundex(), UTF8String.fromString("L300"));
    assertEquals(UTF8String.fromString("Lloyd").soundex(), UTF8String.fromString("L300"));
    assertEquals(UTF8String.fromString("Moses").soundex(), UTF8String.fromString("M220"));
    assertEquals(UTF8String.fromString("O'Hara").soundex(), UTF8String.fromString("O600"));
    assertEquals(UTF8String.fromString("Pfister").soundex(), UTF8String.fromString("P236"));
    assertEquals(UTF8String.fromString("Rubin").soundex(), UTF8String.fromString("R150"));
    assertEquals(UTF8String.fromString("Robert").soundex(), UTF8String.fromString("R163"));
    assertEquals(UTF8String.fromString("Rupert").soundex(), UTF8String.fromString("R163"));
    assertEquals(UTF8String.fromString("Soundex").soundex(), UTF8String.fromString("S532"));
    assertEquals(UTF8String.fromString("Sownteks").soundex(), UTF8String.fromString("S532"));
    assertEquals(UTF8String.fromString("Tymczak").soundex(), UTF8String.fromString("T522"));
    assertEquals(UTF8String.fromString("VanDeusen").soundex(), UTF8String.fromString("V532"));
    assertEquals(UTF8String.fromString("Washington").soundex(), UTF8String.fromString("W252"));
    assertEquals(UTF8String.fromString("Wheaton").soundex(), UTF8String.fromString("W350"));

    assertEquals(UTF8String.fromString("a").soundex(), UTF8String.fromString("A000"));
    assertEquals(UTF8String.fromString("ab").soundex(), UTF8String.fromString("A100"));
    assertEquals(UTF8String.fromString("abc").soundex(), UTF8String.fromString("A120"));
    assertEquals(UTF8String.fromString("abcd").soundex(), UTF8String.fromString("A123"));
    assertEquals(UTF8String.fromString("").soundex(), UTF8String.fromString(""));
    assertEquals(UTF8String.fromString("123").soundex(), UTF8String.fromString("123"));
    assertEquals(UTF8String.fromString("世界千世").soundex(), UTF8String.fromString("世界千世"));
  }
}
