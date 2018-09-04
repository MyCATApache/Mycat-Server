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

package io.mycat.memory.unsafe.utils.sort;

/**
 * 前缀比较器，用于排序
 * 比较前缀排序中的8字节键前缀。 子类可以实现特定于类型的比较，例如字符串的字典比较。
 *
 * Compares 8-byte key prefixes in prefix sort. Subclasses may implement type-specific
 * comparisons, such as lexicographic comparison for strings.
 */

public abstract class PrefixComparator {
  public abstract int compare(long prefix1, long prefix2);
}
