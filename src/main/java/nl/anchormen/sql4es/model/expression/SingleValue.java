package nl.anchormen.sql4es.model.expression;

import com.facebook.presto.sql.tree.ArithmeticUnaryExpression.Sign;

import nl.anchormen.sql4es.ESResultSet;

public class SingleValue implements ICalculation{

	private Number value;
	
	public SingleValue(Number value) {
		this.value = value;
	}
	
	public void invertSign(){
		this.value = value.doubleValue() * -1;
	}
	
	@Override
	public SingleValue setSign(Sign sign) {
		if(sign == Sign.MINUS) this.value = value.doubleValue() * -1;
		return this;
	}

	@Override
	public Number evaluate(ESResultSet result, int rowNr){
		return this.value;
	}

	public String toString(){
		return ""+value;
	}

}
