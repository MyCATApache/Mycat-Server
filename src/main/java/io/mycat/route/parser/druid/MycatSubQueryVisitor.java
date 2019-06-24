package io.mycat.route.parser.druid;

import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;

/**
 * 子查询访问器
 */
public class MycatSubQueryVisitor extends MySqlSchemaStatVisitor{
	
	private boolean relationOr;
	
	@Override
	public boolean visit(SQLBinaryOpExpr x) {

		switch (x.getOperator()) {
	            case Equality:
	            case LessThanOrEqualOrGreaterThan:
	            case GreaterThan:
	            case GreaterThanOrEqual:
	            case LessThan:
	            case LessThanOrEqual:
	            case NotLessThan:
	            case LessThanOrGreater:
   			 	case NotEqual:
   			 	case NotGreaterThan:	            	
	                break;
	            case BooleanOr:
	            	relationOr = true;
	            	break;
	            case Like:
	            case NotLike:
	            default:
	                break;
	        }
	        return true;
	}

	public boolean isRelationOr() {
		return relationOr;
	}
	
	
}
