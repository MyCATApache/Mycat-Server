package nl.anchormen.sql4es.parse.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.QueryBody;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.SampledRelation;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableSubquery;

import nl.anchormen.sql4es.QueryState;
import nl.anchormen.sql4es.model.QuerySource;

/**
 * A Presto {@link AstVisitor} implementation that parses FROM clauses
 * @author cversloot
 *
 */
public class RelationParser extends AstVisitor<List<QuerySource> , QueryState>{
	
	@Override
	protected List<QuerySource> visitRelation(Relation node, QueryState state){
		if(node instanceof Join){
			return node.accept(this, state);
		}else if( node instanceof SampledRelation){
			state.addException("Sampled relations are not supported");
			return null;
		}else if( node instanceof AliasedRelation){
			AliasedRelation ar = (AliasedRelation)node;
			state.setKeyValue("table_alias", ar.getAlias());
			List<QuerySource> relations = ar.getRelation().accept(this, state);
			for(QuerySource rr : relations) rr.setAlias(ar.getAlias());
			return relations;
		}else if( node instanceof QueryBody){
			return node.accept(this, state);
		}else{
			state.addException("Unable to parse node because it has an unknown type :"+node.getClass());
			return null;
		}
	}
	
	@Override
	protected List<QuerySource>  visitJoin(Join node, QueryState state){
		// possible to parse multiple tables but it is not supported
		List<QuerySource> relations = node.getLeft().accept(this,state);
		relations.addAll( node.getRight().accept(this, state) );
		return relations;
	}
	
	@Override
	protected List<QuerySource> visitQueryBody(QueryBody node, QueryState state){
		ArrayList<QuerySource> relations = new ArrayList<QuerySource>();
		if(node instanceof Table){
			String table = ((Table)node).getName().toString();
			// resolve relations provided in dot notation (schema.index.type) and just get the type for now
			String[] catIndexType = table.split("\\.");
			if(catIndexType.length == 1) {
				relations.add(new QuerySource(table));
			}else{
				relations.add(new QuerySource(catIndexType[catIndexType.length-1]));
			}
		}else if (node instanceof TableSubquery){
			TableSubquery ts = (TableSubquery)node;
			Object alias = state.getValue("table_alias");
			Pattern queryRegex = Pattern.compile("from\\s*\\((.+)\\)\\s*(where|as|having|limit|$"+(alias==null ? "":"|"+alias)+")", Pattern.CASE_INSENSITIVE);
			Matcher m = queryRegex.matcher(state.originalSql());
			if(m.find()) {
				relations.add(new QuerySource(m.group(1), ts.getQuery().getQueryBody()));
			}else state.addException("Unable to parse provided subquery in FROM clause");
		}else state.addException("Unable to parse FROM clause, "+node.getClass().getName()+" is not supported");
		return relations;
	}

}
