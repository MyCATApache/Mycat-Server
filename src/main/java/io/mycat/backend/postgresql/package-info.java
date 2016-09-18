/**
 * @author Coollf
 *
 */
package io.mycat.backend.postgresql;
/*

postgresql mycat 相关支持

config demo
============================================================================================================


    <schema-config>
        <schema name="mycat" checkSQLschema="true" sqlMaxLimit="100" dataNode="dn1" />        	
		<dataNode name="dn1" dataHost="pghost" database="mycat" />      
	    <dataHost name="pghost" maxCon="100" minCon="5" balance="1" 
	       writeType="0" dbType="PostgreSQL" dbDriver="native" switchType="1">
			<heartbeat>select 1</heartbeat>
			<writeHost host="host_a" url="localhost:5432" user="postgres"
				password="coollf"/>
		</dataHost>		
    </schema-config>

=============================================================================================================


*/