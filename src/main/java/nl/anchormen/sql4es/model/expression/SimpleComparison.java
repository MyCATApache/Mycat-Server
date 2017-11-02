package nl.anchormen.sql4es.model.expression;

import java.sql.SQLException;
import java.util.List;

import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.ComparisonExpression.Type;

import nl.anchormen.sql4es.model.Column;

public class SimpleComparison implements IComparison{

	private Column leftColumn;
	private ComparisonExpression.Type comparisonType;
	private Number rightValue;
	private Column rightColumn;
	
	public SimpleComparison(Column column, Type comparisonType, Number value) {
		super();
		this.leftColumn = column;
		this.comparisonType = comparisonType;
		this.rightValue = value;
	}
	
	public SimpleComparison(Column leftColumn, Type comparisonType, Column rightColumn) {
		super();
		this.leftColumn = leftColumn;
		this.comparisonType = comparisonType;
		this.rightColumn = rightColumn;
	}

	public String toString(){
		return leftColumn.getFullName()+" "+comparisonType+" "+rightValue +" ("+rightValue.getClass().getSimpleName()+")";
	}

	@Override
	public boolean evaluate(List<Object> row) throws SQLException {
		if(leftColumn.getIndex() >= row.size()) throw new SQLException("Unable to filter row, index "+leftColumn.getIndex()+" is out of bounds");
		try{
			Double leftValue = null;
			Double rightValue = null;
			Object leftObject = row.get(leftColumn.getIndex());
			if(!(leftObject instanceof Number)) throw new SQLException("Unable to filter row because value '"+leftObject+"' has unknown type "+leftObject.getClass().getSimpleName());
			leftValue = ((Number)leftObject).doubleValue();
			
			if(this.rightValue != null){
				rightValue = this.rightValue.doubleValue();
			}else{
				Object colValue = row.get(rightColumn.getIndex());
				if(!(colValue instanceof Number)) throw new SQLException("Unable to filter row because value '"+colValue+"' has unknown type "+colValue.getClass().getSimpleName());
				this.rightValue = (Number)colValue;
				rightValue = this.rightValue.doubleValue();
			}

			if(this.comparisonType == Type.EQUAL) return leftValue == rightValue;
			if(this.comparisonType == Type.GREATER_THAN) return leftValue > rightValue;
			if(this.comparisonType == Type.GREATER_THAN_OR_EQUAL) return leftValue >= rightValue;
			if(this.comparisonType == Type.LESS_THAN) return leftValue < rightValue;
			if(this.comparisonType == Type.LESS_THAN_OR_EQUAL) return leftValue <= rightValue;
		}catch(Exception e){
			throw new SQLException("Unable to filter row because: "+e.getMessage(), e);
		}
		return false;
	}

}
