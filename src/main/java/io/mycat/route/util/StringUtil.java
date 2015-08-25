
package io.mycat.route.util;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.regex.Pattern;
import java.util.zip.CRC32;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

/**
 * 
 * @author yan.yan@huawei.com
 * 
 */
public class StringUtil extends StringUtils {

    /**  判断字符串是否为16进制数字 */
    public static final Pattern HAX_PATTERN             = Pattern.compile("^[0-9a-fA-F]+$");

    /**  判断字符串是否为数字 */
    public static final Pattern NUMBER_PATTERN          = Pattern.compile("^[0-9]+$");

    /** 默认中文编码字符 */
    public static final String  DEFAULT_CHINESE_CHARSET = "GBK";

    private static final String LINE_END                = System.getProperty("line.separator");

    /**
     * 扩展并左对齐字符串，用指定字符串填充右边
     * 新增对中文字符串的支持，注意方法名称<code>alignLeft<b>s</b></code>
     * 
     * <pre>
     * StringUtil.alignLeft(null, *, *)      = null
     * StringUtil.alignLeft("", 3, "z")      = "zzz"
     * StringUtil.alignLeft("bat", 3, "yz")  = "bat"
     * StringUtil.alignLeft("bat", 5, "yz")  = "batyz"
     * StringUtil.alignLeft("bat", 8, "yz")  = "batyzyzy"
     * StringUtil.alignLeft("bat", 1, "yz")  = "bat"
     * StringUtil.alignLeft("bat", -1, "yz") = "bat"
     * StringUtil.alignLeft("bat", 5, null)  = "bat  "
     * StringUtil.alignLeft("bat", 5, "")    = "bat  "
     * StringUtil.alignLeft("中文", 5, "")    = "中文 "
     * </pre>
     *
     * @param str 要对齐的字符串
     * @param size 扩展字符串到指定宽度
     * @param padStr 填充字符串
     *
     * @return 扩展后的字符串，如果字符串为<code>null</code>，则返回<code>null</code>
     */
    public static String alignLefts(String str, int size, String padStr) {
        if (str == null) {
            return null;
        }

        String padStringFinal = (isEmpty(padStr)) ? EMPTY : padStr;
        int padLen = padStringFinal.length();
        int strLen = str.getBytes().length;
        int pads = size - strLen;

        if (pads <= 0) {
            return str;
        }

        if (pads == padLen) {
            return str.concat(padStringFinal);
        } else if (pads < padLen) {
            return str.concat(padStringFinal.substring(0, pads));
        } else {
            char[] padding = new char[pads];
            char[] padChars = padStringFinal.toCharArray();

            for (int i = 0; i < pads; i++) {
                padding[i] = padChars[i % padLen];
            }

            return str.concat(new String(padding));
        }
    }

    /**
     * 扩展并右对齐字符串，用指定字符串填充左边
     * 新增对中文字符串的支持，注意方法名称<code>alignRight<b>s</b></code>
     * 
     * <pre>
     * StringUtil.alignRight(null, *, *)      = null
     * StringUtil.alignRight("", 3, "z")      = "zzz"
     * StringUtil.alignRight("bat", 3, "yz")  = "bat"
     * StringUtil.alignRight("bat", 5, "yz")  = "yzbat"
     * StringUtil.alignRight("bat", 8, "yz")  = "yzyzybat"
     * StringUtil.alignRight("bat", 1, "yz")  = "bat"
     * StringUtil.alignRight("bat", -1, "yz") = "bat"
     * StringUtil.alignRight("bat", 5, null)  = "  bat"
     * StringUtil.alignRight("bat", 5, "")    = "  bat"
     * StringUtil.alignRight("中文", 5, "")    = " 中文"
     * </pre>
     *
     * @param str 要对齐的字符串
     * @param size 扩展字符串到指定宽度
     * @param padStr 填充字符串
     *
     * @return 扩展后的字符串，如果字符串为<code>null</code>，则返回<code>null</code>
     */
    public static String alignRights(String str, int size, String padStr) {
        if (str == null) {
            return null;
        }

        String padStringFinal = (isEmpty(padStr)) ? EMPTY : padStr;
        int padLen = padStringFinal.length();
        int strLen = str.getBytes().length;
        int pads = size - strLen;
        if (pads <= 0) {
            return str;
        }

        if (pads == padLen) {
            return padStringFinal.concat(str);
        } else if (pads < padLen) {
            return padStringFinal.substring(0, pads).concat(str);
        } else {
            char[] padding = new char[pads];
            char[] padChars = padStringFinal.toCharArray();

            for (int i = 0; i < pads; i++) {
                padding[i] = padChars[i % padLen];
            }

            return new String(padding).concat(str);
        }
    }

    /**
     * 取指定字符串的子串，新增对中文字符串的支持
     * 注意方法名称<code>substring<b>s</b></code>   
     *
     * @param str 字符串
     * @param start 起始索引，如果为负数，表示从尾部计算
     * @param end 结束索引（不含），如果为负数，表示从尾部计算
     *
     * @return 子串，如果原始串<code>null</code>，则返回<code>null</code>
     */
    public static String substrings(String str, int start, int end) {
        if (str == null) {
            return null;
        }

        int length = end - start;
        byte[] dest = new byte[length];
        System.arraycopy(str.getBytes(), start, dest, 0, length);

        return new String(dest);
    }

    /**
     * 根据长度<code>length</code>把字符串切成两段，保存数据<br>
     * 确保中文不要被切成两段
     * 
     * @param message
     * @param length
     * @return
     */
    public static String[] cutString(String message, int length) {
        String normal = StringUtil.substrings(message, 0, length);
        if (isContainChinese(message, length)) {
            normal = StringUtil.substrings(message, 0, length - 1);
        }

        return new String[] { normal, StringUtil.substringAfter(message, normal) };
    }

    /**
     * 字符串是否包含中文
     * 
     * @param message
     * @param length
     * @return
     */
    public static boolean isContainChinese(String message) {
        char[] chars = message.toCharArray();
        byte[] bytes;
        try {
            bytes = message.getBytes("GBK");
        } catch (UnsupportedEncodingException e) {
            bytes = message.getBytes();
        }
        return (chars.length != bytes.length);
    }

    /**
     * 字符串起始长度<code>length</code>的字符串是否包含中文
     * 
     * @param message
     * @param length
     * @return
     */
    public static boolean isContainChinese(String message, int length) {
        char[] chars = StringUtil.substrings(message, 0, length).toCharArray();
        char[] charsPlus = StringUtil.substrings(message, 0, length + 1).toCharArray();
        return ArrayUtils.isSameLength(chars, charsPlus);
    }

    /**
     * 在字符串中查找指定字符集合中的字符，并返回匹配的起始索引 如果字符串为<code>null</code>，则返回<code>-1</code>
     * 如果字符集合<code>null</code>或空，也返回<code>-1</code>
     * <pre>
     * StringUtil.indexOfAny(null, *,0)                = -1
     * StringUtil.indexOfAny("", *,0)                  = -1
     * StringUtil.indexOfAny(*, null,0)                = -1
     * StringUtil.indexOfAny(*, [],0)                  = -1
     * StringUtil.indexOfAny("zzabyycdxx",['z','a'],0) = 0
     * StringUtil.indexOfAny("zzabyycdxx",['b','y'],0) = 3
     * StringUtil.indexOfAny("aba", ['z'],0)           = -1
     * </pre>
     *
     * @param str 要扫描的字符串
     * @param searchChars 要搜索的字符集合
     * @param startPos 起始搜索的索引值，如果小于0，则看作0
     *
     * @return 第一个匹配的索引值 如果字符串<code>null</code>或未找到，则返回<code>-1</code>
     */
    public static int indexOfAny(String str, char[] searchChars, int startPos) {
        if ((str == null) || (str.length() == 0) || (searchChars == null)
            || (searchChars.length == 0)) {
            return -1;
        }

        for (int i = startPos; i < str.length(); i++) {
            char ch = str.charAt(i);

            for (int j = 0; j < searchChars.length; j++) {
                if (searchChars[j] == ch) {
                    return i;
                }
            }
        }

        return -1;
    }

    /**
     * 过滤要输出到json的字符串，进行转义输出
     * @param input
     * @return
     */
    public static String filterJsonString(String input) {
        if (input == null) {
            return EMPTY;
        }
        int length = input.length();
        StringBuilder result = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            char c = input.charAt(i);
            switch (c) {
                case '\'': {
                    result.append("\\'");
                    break;
                }
                case '\"': {
                    result.append("\\\"");
                    break;
                }
                default: {
                    result.append(c);
                }
            }
        }
        return result.toString();
    }

    /**
     * 过滤要输出到xml的字符串，将<,>,&,"进行转义输出
     * @param string
     * @return
     */
    public static String filterXMLString(String input) {
        if (input == null) {
            return EMPTY;
        }
        int length = input.length();
        StringBuilder result = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            char c = input.charAt(i);
            switch (c) {
                case '<': {
                    result.append("&lt;");
                    break;
                }
                case '>': {
                    result.append("&gt;");
                    break;
                }
                case '\"': {
                    result.append("&uot;");
                    break;
                }
                case '&': {
                    result.append("&amp;");
                    break;
                }
                default: {
                    result.append(c);
                }
            }
        }
        return result.toString();
    }

    /**
     * 根据url获取系统名称
     * 如果url里面包括系统名就返回，否则直接返回域名
     * 如http://bops.alipay.com 返回bops
     * http://www.alipay.com  返回  alipay.com
     * @param url
     * @return
     * @throws MalformedURLException
     */
    public static String getSystemNameByURL(String url) throws MalformedURLException {
        URL netURL = new URL(url);
        String domain = netURL.getHost();
        if (domain.startsWith("www.")) {
            domain = domain.substring(5);
        }
        int offset = domain.indexOf("alipay");
        if (offset > 0) {
            return domain.substring(0, offset - 1);
        } else {
            return domain;
        }
    }

    /**
     * 获取指定字符串按GBK编码转换成的byte长度
     * 由于String.getByte方法依赖于操作系统编码，处理中文字符串时建议用此方法
     * 
     * @param data
     * @return
     */
    public static byte[] getGBKByte(String data) {
        if (data == null) {
            return new byte[0];
        }
        try {
            return data.getBytes("GBK");
        } catch (UnsupportedEncodingException e) {
            return data.getBytes();
        }
    }

    /**
     * 获取指定字符串按GBK编码转换成byte的长�?
     * 由于String.getByte方法依赖于操作系统编码，处理中文字符串时建议用此方法
     * 
     * @param data
     * @return
     */
    public static int getGBKByteLength(String data) {
        return getGBKByte(data).length;
    }

    /**
     * 生成�?定长度的序列�?
     * 
     * @param length
     * @param padding
     * @return
     */
    public static String genSerialNo(int length, String padding) {
        String nanoTime = System.nanoTime() + "";
        if (nanoTime.length() >= length) {
            nanoTime = nanoTime.substring(0, length);
        } else {
            nanoTime = nanoTime + repeat(padding, length - nanoTime.length());
        }
        return nanoTime;
    }

    /**
     * 将数字格式化到固定长�?
     * 
     * @param input
     * @param fixLength
     * @return
     */
    public static String formatNumberToFixedLength(String input, int fixLength) {
        if (input.length() <= fixLength) {
            // 未到指定长度，左�?0
            return StringUtils.leftPad(input, fixLength, '0');
        } else {
            // 超过长度，砍掉左边超长部�?
            return input.substring(input.length() - fixLength);
        }
    }

    /**
     * 判断字符串是否为16进制数字
     * @param input
     * @return
     */
    public static boolean isHexString(String input) {
        if (input == null) {
            return false;
        }
        if (NUMBER_PATTERN.matcher(input).matches()) {
            // 过滤掉纯数字
            return false;
        }
        return HAX_PATTERN.matcher(input).matches();
    }

    /**
     * 获取换行符（\n），供vm中使用，屏蔽了操作系统的差异
     * 
     * @return
     */
    public static String getNewLine() {
        return LINE_END;
    }

    /**
     * 将byte[]按指定编码转换为字符串，供velocity中使�?
     * @param bytes
     * @param charsetName
     * @return
     */
    public static String getNewString(byte[] bytes, String charsetName) {
        try {
            return new String(bytes, charsetName);
        } catch (UnsupportedEncodingException e) {
            return EMPTY;
        }
    }

    public static String crc32(String str) {
        CRC32 crc32 = new CRC32();
        crc32.update(str.getBytes());
        return crc32.getValue() + "";
    }

    public static void main(String[] args) {
        System.out.println(substring(crc32("123123123123123"), -3, -1));

    }

}
