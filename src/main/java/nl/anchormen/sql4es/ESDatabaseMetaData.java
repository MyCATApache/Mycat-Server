package nl.anchormen.sql4es;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.regex.Pattern;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;

import com.carrotsearch.hppc.cursors.ObjectCursor;

import nl.anchormen.sql4es.jdbc.ESDriver;
import nl.anchormen.sql4es.model.Column;
import nl.anchormen.sql4es.model.Heading;
import nl.anchormen.sql4es.model.Utils;

/**
 * Implementation of the {@link DatabaseMetaData} interface describing an Elasticsearch cluster. Conceptually a 
 * database is mapped to an index and tables to types. Listing all tables will thus list all types present within
 * an index. Aliases are presented like databases and the indexes they expose as views.
 * @author cversloot
 *
 */
public class ESDatabaseMetaData implements DatabaseMetaData{
	
	private String host;
	private int port;
	private Client client;
	private Properties clientInfo;
	private Connection conn;

	public ESDatabaseMetaData(String host, int port, Client client, Properties clientInfo, Connection conn) {
		this.host = host;
		this.port = port;
		this.client = client;
		this.clientInfo = clientInfo;
		this.conn = conn;
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

	@Override
	public boolean allProceduresAreCallable() throws SQLException {
		return false;
	}

	@Override
	public boolean allTablesAreSelectable() throws SQLException {
		return true;
	}

	@Override
	public String getURL() throws SQLException {
		return Utils.PREFIX+"//"+host+":"+port;
	}

	@Override
	public String getUserName() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return true;
	}

	@Override
	public boolean nullsAreSortedHigh() throws SQLException {
		return false;
	}

	@Override
	public boolean nullsAreSortedLow() throws SQLException {
		return true;
	}

	@Override
	public boolean nullsAreSortedAtStart() throws SQLException {
		return false;
	}

	@Override
	public boolean nullsAreSortedAtEnd() throws SQLException {
		return true;
	}

	@Override
	public String getDatabaseProductName() throws SQLException {
		return Utils.ELASTICSEARCH_NAME;
	}

	@Override
	public String getDatabaseProductVersion() throws SQLException {
		return Utils.ELASTICSEARCH_VERSION;
	}

	@Override
	public String getDriverName() throws SQLException {
		return new ESDriver().getClass().getName();
	}

	@Override
	public String getDriverVersion() throws SQLException {
		return getDatabaseProductVersion();
	}

	@Override
	public int getDriverMajorVersion() {
		return Utils.DRIVER_MAJOR_VERSION;
	}

	@Override
	public int getDriverMinorVersion() {
		return Utils.DRIVER_MINOR_VERSION;
	}

	@Override
	public boolean usesLocalFiles() throws SQLException {
		return false;
	}

	@Override
	public boolean usesLocalFilePerTable() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMixedCaseIdentifiers() throws SQLException {
		return true;
	}

	@Override
	public boolean storesUpperCaseIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesLowerCaseIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesMixedCaseIdentifiers() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
		return true;
	}

	@Override
	public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
		return true;
	}

	@Override
	public String getIdentifierQuoteString() throws SQLException {
		return "\"";
	}

	@Override
	public String getSQLKeywords() throws SQLException {
		return "";
	}

	@Override
	public String getNumericFunctions() throws SQLException {
		return "";
	}

	@Override
	public String getStringFunctions() throws SQLException {
		return "";
	}

	@Override
	public String getSystemFunctions() throws SQLException {
		return "";
	}

	@Override
	public String getTimeDateFunctions() throws SQLException {
		return "";
	}

	@Override
	public String getSearchStringEscape() throws SQLException {
		return "\\";
	}

	@Override
	public String getExtraNameCharacters() throws SQLException {
		return "";
	}

	@Override
	public boolean supportsAlterTableWithAddColumn() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsAlterTableWithDropColumn() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsColumnAliasing() throws SQLException {
		return true;
	}

	@Override
	public boolean nullPlusNonNullIsNull() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsConvert() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsConvert(int fromType, int toType) throws SQLException {
		return false;
	}

	@Override
	public boolean supportsTableCorrelationNames() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsDifferentTableCorrelationNames() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsExpressionsInOrderBy() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOrderByUnrelated() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsGroupBy() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsGroupByUnrelated() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsGroupByBeyondSelect() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsLikeEscapeClause() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsMultipleResultSets() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMultipleTransactions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsNonNullableColumns() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMinimumSQLGrammar() throws SQLException {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public boolean supportsCoreSQLGrammar() throws SQLException {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public boolean supportsExtendedSQLGrammar() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsANSI92EntryLevelSQL() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsANSI92IntermediateSQL() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsANSI92FullSQL() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsIntegrityEnhancementFacility() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOuterJoins() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsFullOuterJoins() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsLimitedOuterJoins() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String getSchemaTerm() throws SQLException {
		// TODO Auto-generated method stub
		return "schema";
	}

	@Override
	public String getProcedureTerm() throws SQLException {
		// TODO Auto-generated method stub
		return "procedure";
	}

	@Override
	public String getCatalogTerm() throws SQLException {
		// TODO Auto-generated method stub
		return "catalog";
	}

	@Override
	public boolean isCatalogAtStart() throws SQLException {
		return true;
	}

	@Override
	public String getCatalogSeparator() throws SQLException {
		return Utils.CATALOG_SEPARATOR;
	}

	@Override
	public boolean supportsSchemasInDataManipulation() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInProcedureCalls() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInTableDefinitions() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsSchemasInIndexDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInDataManipulation() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInProcedureCalls() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInTableDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsPositionedDelete() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsPositionedUpdate() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSelectForUpdate() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsStoredProcedures() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSubqueriesInComparisons() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsSubqueriesInExists() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsSubqueriesInIns() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsSubqueriesInQuantifieds() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsCorrelatedSubqueries() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsUnion() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsUnionAll() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
		return false;
	}

	@Override
	public int getMaxBinaryLiteralLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxCharLiteralLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnNameLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInGroupBy() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInIndex() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInOrderBy() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInSelect() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInTable() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxConnections() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxCursorNameLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxIndexLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxSchemaNameLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxProcedureNameLength() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxCatalogNameLength() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxRowSize() throws SQLException {
		Object o = clientInfo.get(Utils.PROP_DEFAULT_ROW_LENGTH);
		if(o != null && o instanceof Integer) return (int)o;
		else return 1000;
	}

	@Override
	public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int getMaxStatementLength() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxStatements() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxTableNameLength() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxTablesInSelect() throws SQLException {
		// TODO Auto-generated method stub
		return 1;
	}

	@Override
	public int getMaxUserNameLength() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getDefaultTransactionIsolation() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean supportsTransactions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
		return false;
	}

	@Override
	public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
		return false;
	}

	@Override
	public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
		return false;
	}

	@Override
	public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	private static String cleanPattern(String original){
		if(original == null) return ".*";
		return original.replaceAll("%",".*");
	}
	
	@Override
	public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
			throws SQLException {
		
		Heading heading = new Heading();
		heading.add(new Column("PROCEDURE_CAT"));
		heading.add(new Column("PROCEDURE_SCHEM"));
		heading.add(new Column("PROCEDURE_NAME"));
		heading.add(new Column("reserved_1"));
		heading.add(new Column("reserved_2"));
		heading.add(new Column("reserved_3"));
		heading.add(new Column("REMARKS"));
		heading.add(new Column("PROCEDURE_TYPE"));
		heading.add(new Column("SPECIFIC_NAME"));
		return new ESResultSet(heading, 0, heading.getColumnCount());
	}

	@Override
	public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
			String columnNamePattern) throws SQLException {
		
		Heading heading = new Heading();
		heading.add(new Column("PROCEDURE_CAT"));
		heading.add(new Column("PROCEDURE_SCHEM"));
		heading.add(new Column("PROCEDURE_NAME"));
		heading.add(new Column("COLUMN_NAME"));
		heading.add(new Column("COLUMN_TYPE"));
		heading.add(new Column("DATA_TYPE"));
		heading.add(new Column("TYPE_NAME"));
		heading.add(new Column("PRECISION"));
		heading.add(new Column("LENGTH"));
		heading.add(new Column("SCALE"));
		heading.add(new Column("RADIX"));
		heading.add(new Column("NULLABLE"));
		heading.add(new Column("REMARKS"));
		heading.add(new Column("COLUMN_DEF"));
		heading.add(new Column("SQL_DATA_TYPE"));
		heading.add(new Column("SQL_DATETIME_SUB"));
		heading.add(new Column("CHAR_OCTET_LENGTH"));
		heading.add(new Column("ORDINAL_POSITION"));
		heading.add(new Column("IS_NULLABLE"));
		heading.add(new Column("SPECIFIC_NAME"));
		return new ESResultSet(heading, 0, heading.getColumnCount());
	}

	@Override
	public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
			throws SQLException {
		//schemaPattern = index;
		//System.out.println("TABLES: cat="+catalog+", schemas="+schemaPattern+", tables="+tableNamePattern+", types="+Arrays.toString(types));
		Heading heading = new Heading();
		heading.add(new Column("TABLE_CAT"));
		heading.add(new Column("TABLE_SCHEM"));
		heading.add(new Column("TABLE_NAME"));
		heading.add(new Column("TABLE_TYPE"));
		heading.add(new Column("REMARKS"));
		heading.add(new Column("TYPE_CAT"));
		heading.add(new Column("TYPE_SCHEM"));
		heading.add(new Column("TYPE_NAME"));
		heading.add(new Column("SELF_REFERENCING_COL_NAME"));
		heading.add(new Column("REF_GENERATION"));
		ESResultSet result = new ESResultSet(heading, 0, heading.getColumnCount());
		//if(catalog != null && !catalog.equals(Utils.CATALOG)) return result;
		boolean lookForTables = types == null;
		boolean lookForViews = types == null;
		if(types != null) for(String type : types) {
			if(type.equals("TABLE")) lookForTables = true;
			if(type.equals("VIEW")) lookForViews = true;
		}
		if(!lookForTables && !lookForViews) return result;
		// add query cache as a view to the list. It is not an actual table but can be used as such in FROM clause
		
		schemaPattern = cleanPattern(schemaPattern);
		tableNamePattern = cleanPattern(tableNamePattern);
		
		if(lookForTables){
			if(Pattern.matches(tableNamePattern, (String)clientInfo.get(Utils.PROP_QUERY_CACHE_TABLE))){
				List<Object> row = result.getNewRow();
				row.set(2, clientInfo.get(Utils.PROP_QUERY_CACHE_TABLE));
				row.set(3, "GLOBAL TEMPORARY");
				result.add(row);
			}
			
			ImmutableOpenMap<String, IndexMetaData> indices = client.admin().cluster()
				    .prepareState().get().getState()
				    .getMetaData().getIndices();
			for(ObjectCursor<String> index : indices.keys()){
				if(!Pattern.matches(schemaPattern, index.value)) continue;
				for(ObjectCursor<String> type : indices.get(index.value).getMappings().keys()){
					if(!Pattern.matches(tableNamePattern, type.value)) continue;
					List<Object> row = result.getNewRow();
					row.set(2, type.value);
					row.set(3, "TABLE");
					result.add(row);
				}
			}
		}
		
		// add aliases as VIEW on this index
		if(lookForViews){
			ImmutableOpenMap<String, List<AliasMetaData>> aliasMd = client.admin().indices().prepareGetAliases().get().getAliases();
			for(ObjectCursor<String> key : aliasMd.keys()){
				if(schemaPattern != null && schemaPattern.length() > 0 && !Pattern.matches(schemaPattern, key.value)) continue;
				for(AliasMetaData amd : aliasMd.get(key.value)){
					List<Object> row = result.getNewRow();
					row.set(1, amd.alias());
					row.set(3, "VIEW");
					row.set(4, "Filter ("+amd.filter() == null ? "NONE" : amd.filter()+")");
					result.add(row);
				}
			}
			
			// OR get indexes part of the Alias provided as schema
			if(result.getNrRows() <= 1){
				for(ObjectCursor<String> key : aliasMd.keys()){
					for(AliasMetaData amd : aliasMd.get(key.value)){
						if(schemaPattern != null && schemaPattern.length() > 0 && !Pattern.matches(schemaPattern, amd.alias())) continue;
						List<Object> row = result.getNewRow();
						row.set(1, key.value);
						row.set(3, "VIEW");
						row.set(4, "Filter ("+amd.filter() == null ? "NONE" : amd.filter()+")");
						result.add(row);
					}
				}
			}
		}
		
		result.setTotal(result.getNrRows());
		//System.out.println(result);
		return result;
	}

	@Override
	public ResultSet getSchemas() throws SQLException {
		ImmutableOpenMap<String, IndexMetaData> indices = client.admin().cluster()
			    .prepareState().get().getState()
			    .getMetaData().getIndices();
		
		SortedMap<String, AliasOrIndex> aliasAndIndices = client.admin().cluster().prepareState().get().getState()
    		.getMetaData().getAliasAndIndexLookup();
		
		Heading heading = new Heading();
		heading.add(new Column("TABLE_SCHEM"));
		heading.add(new Column("TABLE_CATALOG"));
		ESResultSet result = new ESResultSet(heading, indices.size(), heading.getColumnCount());
		for(String key : aliasAndIndices.keySet()){
			List<Object> row = result.getNewRow();
			row.set(0, key);
			row.set(1,  aliasAndIndices.get(key).isAlias() ? "alias" : "index");
			result.add(row);
		}
		return result;
	}

	@Override
	public ResultSet getCatalogs() throws SQLException {
		Heading heading = new Heading();
		heading.add(new Column("TABLE_CAT"));
		ESResultSet result = new ESResultSet(heading, 1, heading.getColumnCount());
		List<Object> row = result.getNewRow();
		row.set(0, "elasticsearch");
		result.add(row);
		return result;
	}

	@Override
	public ResultSet getTableTypes() throws SQLException {
		//System.out.println("TABLE TYPES");
		Heading heading = new Heading();
		heading.add(new Column("TABLE_TYPE"));
		ESResultSet result = new ESResultSet(heading, 1, heading.getColumnCount());
		List<Object> row = result.getNewRow();
		row.set(0, "TABLE");
		result.add(row);
		row = result.getNewRow();
		row.set(0,  "VIEW");
		result.add(row);
		return result;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
			throws SQLException {
		//schemaPattern = index;
		//System.out.println("COLUMNS: cat="+catalog+", schemas="+schemaPattern+", tables="+tableNamePattern+", columns="+columnNamePattern);
		schemaPattern = cleanPattern(schemaPattern);
		tableNamePattern = cleanPattern(tableNamePattern);
		columnNamePattern = cleanPattern(columnNamePattern);
		boolean lateral = Utils.getBooleanProp(clientInfo, Utils.PROP_RESULT_NESTED_LATERAL, true);
		
		Heading heading = new Heading();
		heading.add(new Column("TABLE_CAT"));
		heading.add(new Column("TABLE_SCHEM"));
		heading.add(new Column("TABLE_NAME"));
		heading.add(new Column("COLUMN_NAME"));
		heading.add(new Column("DATA_TYPE"));
		heading.add(new Column("TYPE_NAME"));
		heading.add(new Column("COLUMN_SIZE"));
		heading.add(new Column("BUFFER_LENGTH"));
		heading.add(new Column("DECIMAL_DIGITS"));
		heading.add(new Column("NUM_PREC_RADIX"));
		heading.add(new Column("NULLABLE"));
		heading.add(new Column("REMARKS"));
		heading.add(new Column("COLUMN_DEF"));
		heading.add(new Column("SQL_DATA_TYPE"));
		heading.add(new Column("SQL_DATETIME_SUB"));
		heading.add(new Column("CHAR_OCTET_LENGTH"));
		heading.add(new Column("ORDINAL_POSITION"));
		heading.add(new Column("IS_NULLABLE"));
		heading.add(new Column("SCOPE_CATALOG"));
		heading.add(new Column("SCOPE_SCHEMA"));
		heading.add(new Column("SCOPE_TABLE"));
		heading.add(new Column("SOURCE_DATA_TYPE"));
		heading.add(new Column("IS_AUTOINCREMENT"));
		heading.add(new Column("IS_GENERATEDCOLUMN"));
		ESResultSet result = new ESResultSet(heading, 0, heading.getColumnCount());
		//if(catalog != null && !catalog.equals(Utils.CATALOG)) return result;
		
		try{
			ImmutableOpenMap<String, IndexMetaData> indices = client.admin().cluster()
				    .prepareState().get().getState()
				    .getMetaData().getIndices();
			for(ObjectCursor<String> index : indices.keys()){
				if(schemaPattern != null && !Pattern.matches(schemaPattern, index.value)) continue;
				for(ObjectCursor<String> type : indices.get(index.value).getMappings().keys()){
					if(tableNamePattern != null && !Pattern.matches(tableNamePattern, type.value)) continue;
					
					// add _id, _type and _index fields
					List<Object> row = result.getNewRow();
					row.set(0, null);
					row.set(1, null);
					row.set(2, type.value);
					row.set(3, "_id");
					row.set(4, Heading.getTypeIdForObject(new String())); 
					row.set(5, "string"); 
					row.set(11, "The document _id used by elasticsearch");
					row.set(22, "YES");
					row.set(23, "YES");
					result.add(row);
					
					row = result.getNewRow();				
					row.set(0, null);
					row.set(1, null);
					row.set(2, type.value);
					row.set(3, "_type");
					row.set(4, Heading.getTypeIdForObject(new String())); 
					row.set(5, "string");
					row.set(11, "The type a record is part of");
					row.set(22, "NO");
					row.set(23, "YES");
					result.add(row);
					
					row = result.getNewRow();				
					row.set(0, null);
					row.set(1, null);
					row.set(2, type.value);
					row.set(3, "_index");
					row.set(4, Heading.getTypeIdForObject(new String())); 
					row.set(5, "string"); 
					row.set(11, "The index a record is part of");
					row.set(22, "NO");
					row.set(23, "YES");
					result.add(row);
					
					MappingMetaData typeMd = indices.get(index.value).getMappings().get(type.value);
					if(typeMd != null && (Map)typeMd.getSourceAsMap().get("properties") != null){
						addColumnInfo((Map)typeMd.getSourceAsMap().get("properties"), null, 0, type.value, result, columnNamePattern, lateral);
					}
				}
			}
		}catch(Exception e){
			throw new SQLException("Unable to retrieve table data", e);
		}
		result.setTotal(result.getNrRows());
		//System.out.println(result);
		return result;
	}

	@SuppressWarnings("unchecked")
	private int addColumnInfo(Map<String, Object> info, String parent, int nextIndex, String esType, ESResultSet result, 
			String columnNamePattern, boolean lateral){
		List<Object> row = result.getNewRow();				
		for(String key : info.keySet()){
			String colName = parent != null ? parent+"."+key : key;
			//if(columnNamePattern != null && !Pattern.matches(columnNamePattern, colName)) continue;
			Map<String, Object> properties = (Map<String, Object>)info.get(key);
			//if(properties.containsKey("properties") && lateral){
			//	System.out.println(key+"\t"+properties);
			//	nextIndex = addColumnInfo((Map<String, Object>)properties.get("properties"), colName, nextIndex, esType, result, columnNamePattern, lateral);
			//}else 
				if(properties.containsKey("properties")){
				row = result.getNewRow();				
				row.set(0, null);
				row.set(1, null);
				row.set(2, esType);
				row.set(3, colName);
				if("nested".equals( properties.get("type")) ){
					row.set(4, Types.REF); 
					row.set(5, properties.get("type"));					
				}else{
					row.set(4, Types.JAVA_OBJECT); 
					row.set(5, Object.class.getName()); 
				}
				row.set(6, 1);
				row.set(11, properties.toString());
				nextIndex++;
				result.add(row);
				nextIndex = addColumnInfo((Map<String, Object>)properties.get("properties"), colName, nextIndex, esType, result, columnNamePattern, lateral);
			}else {
				row = result.getNewRow();				
				String type = (String)properties.get("type");
				row.set(0, null);
				row.set(1, null);
				row.set(2, esType);
				row.set(3, colName);
				row.set(6, 1);
				row.set(11, properties.toString());
				row.set(5, type); 
				switch (type){
					case "string" : // field type before version 5.0
						row.set(4, Heading.getTypeIdForObject(new String())); 
						break;
					case "text" : // field type after version 5.0
						row.set(4, Heading.getTypeIdForObject(new String())); 
						break;
					case "keyword" : // field type after version 5.0
						row.set(4, Heading.getTypeIdForObject(new String())); 
						break;
					case "long" :
						row.set(4, Heading.getTypeIdForObject(new Long(1))); 
						break;
					case "integer" :
						row.set(4, Heading.getTypeIdForObject(new Integer(1))); 
						break;
					case "double" :
						row.set(4, Heading.getTypeIdForObject(new Double(1))); 
						break;
					case "float" :
						row.set(4, Heading.getTypeIdForObject(new Float(1))); 
						break;
					case "date" :
						row.set(4, Heading.getTypeIdForObject(new Date())); 
						break;
					case "short" :
						row.set(4, Heading.getTypeIdForObject(new Short((short)1))); 
						break;
					case "byte" :
						row.set(4, Heading.getTypeIdForObject(new Byte((byte)1))); 
						break;
					case "boolean" :
						row.set(4, Types.BOOLEAN); 
						break;
					case "nested" :
						row.set(4, Types.REF); 
						break;
					case "object" :
						row.set(4, Types.JAVA_OBJECT); 
						break;
					default :
						row.set(4, Types.OTHER); 
						row.set(5, "unknown"); 
						break;
				}
				nextIndex++;
				result.add(row);
			}
		}
		return nextIndex;
	}
	
	@Override
	public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
		Heading heading = new Heading();
		heading.add(new Column("TABLE_CAT"));
		heading.add(new Column("TABLE_SCHEM"));
		heading.add(new Column("TABLE_NAME"));
		heading.add(new Column("COLUMN_NAME"));
		heading.add(new Column("KEY_SEQ"));
		heading.add(new Column("PK_NAME"));
		ESResultSet result = new ESResultSet(heading, 1, heading.getColumnCount());
		List<Object> row = result.getNewRow();
		row.set(0, catalog);
		row.set(1, schema);
		row.set(2, table);
		row.set(3, "_id");
		row.set(4, 1);
		result.add(row);
		return result;
	}

	@Override
	public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
			String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getTypeInfo() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean supportsResultSetType(int type) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean ownUpdatesAreVisible(int type) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean ownDeletesAreVisible(int type) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean ownInsertsAreVisible(int type) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean othersUpdatesAreVisible(int type) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean othersDeletesAreVisible(int type) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean othersInsertsAreVisible(int type) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean updatesAreDetected(int type) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean deletesAreDetected(int type) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean insertsAreDetected(int type) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsBatchUpdates() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Connection getConnection() throws SQLException {
		return conn;
	}

	@Override
	public boolean supportsSavepoints() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsNamedParameters() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsMultipleOpenResults() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsGetGeneratedKeys() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
			String attributeNamePattern) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean supportsResultSetHoldability(int holdability) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getDatabaseMajorVersion() throws SQLException {
		return Utils.ES_MAJOR_VERSION;
	}

	@Override
	public int getDatabaseMinorVersion() throws SQLException {
		return Utils.ES_MINOR_VERSION;
	}

	@Override
	public int getJDBCMajorVersion() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getJDBCMinorVersion() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getSQLStateType() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean locatorsUpdateCopy() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsStatementPooling() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public RowIdLifetime getRowIdLifetime() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public ResultSet getClientInfoProperties() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
			String columnNamePattern) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
			String columnNamePattern) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean generatedKeyAlwaysReturned() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

}
