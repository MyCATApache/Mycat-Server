package nl.anchormen.sql4es.parse.sql;

import org.elasticsearch.search.sort.SortOrder;

import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.WhenClause;

import nl.anchormen.sql4es.QueryState;
import nl.anchormen.sql4es.model.Column;
import nl.anchormen.sql4es.model.Heading;
import nl.anchormen.sql4es.model.OrderBy;
import nl.anchormen.sql4es.model.QuerySource;

/**
 * Parses the ORDER BY clause (still a work in progress)
 * @author cversloot
 *
 */
public class OrderByParser extends AstVisitor<OrderBy, QueryState>{
	
	@Override
	protected OrderBy visitSortItem(SortItem si, QueryState state){
		String orderKey = null;
		if(si.getSortKey() instanceof DereferenceExpression){
			orderKey = SelectParser.visitDereferenceExpression((DereferenceExpression)si.getSortKey());
		}else if (si.getSortKey() instanceof FunctionCall){
			orderKey = si.getSortKey().toString().replaceAll("\"","");
		}else if(si.getSortKey() instanceof SearchedCaseExpression){
			//... order by CASE WHEN field IS NULL THEN 1 ELSE 0 END
			// TODO: improve this quick and dirty implementation
			SearchedCaseExpression sce = (SearchedCaseExpression)si.getSortKey();
			for(WhenClause when : sce.getWhenClauses()){
				orderKey = SelectParser.visitDereferenceExpression(
						(DereferenceExpression)((IsNullPredicate)when.getOperand()).getValue());
			}
		}else if(si.getSortKey() instanceof QualifiedNameReference){
			orderKey = ((QualifiedNameReference)si.getSortKey()).getName().toString();
		}else {
			state.addException("Order statement with type '"+si.getSortKey().getClass().getName()+"' is not supported");
			return null;
		}
		// fix case
		orderKey = Heading.findOriginal(state.originalSql()+";", orderKey, "order by.+", "\\W");
		// remove any table reference or alias
		if(orderKey.contains(".")){
			String prefix = orderKey.split("\\.")[0];
			for(QuerySource tr : state.getSources()){
				if(tr.getAlias() != null){
					if(prefix.equals(tr.getAlias())) orderKey = orderKey.substring(orderKey.indexOf('.')+1);
				}else if (tr.getSource() != null && prefix.equals(tr.getSource())) orderKey = orderKey.substring(orderKey.indexOf('.')+1);
			}
		}
		// select column to order on
		Column column = state.getHeading().getColumnByLabel(orderKey);
		if(column != null){
			if(si.getOrdering().toString().startsWith("ASC")){
				return new OrderBy(column.getColumn(), SortOrder.ASC, column.getIndex());
			}else{
				return new OrderBy(column.getColumn(), SortOrder.DESC, column.getIndex());
			}
		}else{
			state.addException("Order key '"+orderKey+"' is not specified in SELECT clause");
			return null;
		}
	}

}
