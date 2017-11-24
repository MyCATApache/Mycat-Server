package nl.anchormen.sql4es.model.expression;

import java.sql.SQLException;
import java.util.List;

public class BooleanComparison implements IComparison{
	
	private IComparison left;
	private IComparison right;
	private boolean must;

	public BooleanComparison(IComparison left, IComparison right, boolean must){
		this.left = left;
		this.right = right;
		this.must = must;
	}

	@Override
	public boolean evaluate(List<Object> row) throws SQLException {
		if(must) return left.evaluate(row) && right.evaluate(row);
		else return left.evaluate(row) || right.evaluate(row);
	}
	
	public String toString(){
		return (must?"AND ": "OR  ")+"Left: "+left+"\tRight: "+right;
	}

}
