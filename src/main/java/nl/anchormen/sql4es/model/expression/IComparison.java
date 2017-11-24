package nl.anchormen.sql4es.model.expression;

import java.sql.SQLException;
import java.util.List;

public interface IComparison {

	public boolean evaluate(List<Object> row) throws SQLException;
	
}
