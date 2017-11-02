package nl.anchormen.sql4es.parse.sql;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.TimeLiteral;
import com.facebook.presto.sql.tree.TimestampLiteral;
import com.facebook.presto.sql.tree.Values;

import nl.anchormen.sql4es.QueryState;
import nl.anchormen.sql4es.model.Column;

/**
 * A Presto {@link AstVisitor} implementation that parses INSERT clauses
 * 
 * @author cversloot
 *
 */
public class UpdateParser extends AstVisitor<List<Object>, QueryState>{

	
	/**
	 * Parses the Insert statement and returns the values as one list (even if multiple value sets were found). Columns
	 * to be inserted are added to the provided heading
	 * @param insert
	 * @param updateState
	 * @param heading
	 * @return
	 * @throws SQLException
	 */
	public List<Object> parse(Insert insert, QueryState state) throws SQLException{
		if(!insert.getColumns().isPresent()) throw new SQLException("Unable to insert data without column references");
		if(insert.getQuery().getQueryBody() == null) throw new SQLException("Unable to insert data without any values");
		if(!(insert.getQuery().getQueryBody() instanceof Values)) throw new SQLException("Unable to insert data from a query, use ... VALUES (...)");
		
		List<String> fields = insert.getColumns().get();
		List<Object> values = insert.getQuery().getQueryBody().accept(this,  state);
		if(state.hasException()) throw state.getException();
		for(String field : fields) state.getHeading().add(new Column(field));
		
		return values;
	}

	/**
	 * Parses the list with values to insert and returns them as Objects
	 */
	@Override
	public List<Object> visitValues(Values values, QueryState state){
		List<Object> result = new ArrayList<Object>();
		
		for(Expression rowExpression : values.getRows()){
			if(rowExpression instanceof Row) {
				Row row = (Row)rowExpression;
				for(Expression rowValue : row.getItems()){
					if(!(rowValue instanceof Literal)) {
						state.addException("Unable to parse non-literal value : "+rowValue);
						return result;
					}
					result.add(getObject((Literal)rowValue));
				}
			}else if (rowExpression instanceof Literal){
				result.add(getObject((Literal)rowExpression));
			}else {
				state.addException("Unknown VALUES type "+rowExpression.getClass()+" encountered");
				return null;
			}
		}
		return result;
	}
	
	private Object getObject(Literal literal){
		Object value = null;
		if(literal instanceof LongLiteral) value = ((LongLiteral)literal).getValue();
		else if(literal instanceof BooleanLiteral) value = ((BooleanLiteral)literal).getValue();
		else if(literal instanceof DoubleLiteral) value = ((DoubleLiteral)literal).getValue();
		else if(literal instanceof StringLiteral) value = ((StringLiteral)literal).getValue();
		else if(literal instanceof TimeLiteral) value = ((TimeLiteral)literal).getValue();
		else if(literal instanceof TimestampLiteral) value = ((TimestampLiteral)literal).getValue();
		return value;
	}
	
}
