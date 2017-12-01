package io.mycat.route.parser.druid;

import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import io.mycat.config.model.SchemaConfig;

/**
 * 为防止SchemaStatVisitor被污染，采用factory创建
 *
 * Date：2017年12月1日
 * 
 * @author SvenAugustus
 * @version 1.0
 * @since JDK 1.7
 */
public class SchemaStatVisitorFactory {

  /**
   * 创建
   * 
   * @return
   */
  public static SchemaStatVisitor create(SchemaConfig schema) {
    SchemaStatVisitor visitor = new MycatSchemaStatVisitor();
    return visitor;
  }
}
