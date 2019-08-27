package io.mycat.route.parser.util;

public class WildcardUtil {
  
  public static String wildcard(String name) {
    if (name.startsWith("`")) {
      name = name.replaceAll("`", "");
    } else if (name.startsWith("\"")) {
      name = name.replaceAll("\"", "");
    } else if (name.startsWith("'")) {
      name = name.replaceAll("'", "");
    }
    return name;
  }

  public static void wildcards(String[] names) {
    for (int i = 0; i < names.length; i++) {
      names[i] = wildcard(names[i]);
    }
  }
}
