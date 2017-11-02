package nl.anchormen.sql4es.model.expression;

import com.facebook.presto.sql.tree.ArithmeticUnaryExpression.Sign;

import nl.anchormen.sql4es.ESResultSet;

public interface ICalculation {

	public Number evaluate(ESResultSet result, int rowNr);
	
	public ICalculation setSign(Sign sign);
	
}
