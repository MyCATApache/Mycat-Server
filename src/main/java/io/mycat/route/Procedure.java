package io.mycat.route;

import com.google.common.base.*;

import java.io.Serializable;
import java.sql.Types;
import java.util.*;

/**
 * Created by magicdoom on 2016/3/24.
 *
 *
 * 1.no return

 ok


 2.simple

 ok
 row
 eof


 3.list


 row
 row
 row
 row
 eof
 ok

 */
public class Procedure implements Serializable
{
    private String originSql;
    private String name;
    private String callSql;
    private String setSql ;
    private String selectSql;
    private Set<String> selectColumns=new LinkedHashSet<>();
    private Set<String> listFields=new LinkedHashSet<>();
    private boolean isResultList=false;

    public boolean isResultList()
    {
        return isResultList;
    }
    public boolean isResultSimpleValue()
    {
        return selectSql!=null&&!isResultList;
    }
    public boolean isResultNothing()
    {
        return selectSql==null&&!isResultList;
    }
    public void setResultList(boolean resultList)
    {
        isResultList = resultList;
    }

    public String toPreCallSql(String dbType)
    {
        StringBuilder sb=new StringBuilder();
        sb.append("{ call ")  ;
        sb.append(this.getName()).append("(") ;
        Collection<ProcedureParameter> paramters=    this.getParamterMap().values();
        int j=0;
        for (ProcedureParameter paramter : paramters)
        {

            String name="?";
            String joinStr=  j==this.getParamterMap().size()-1?name:name+"," ;
            sb.append(joinStr);
            j++;
        }
        sb.append(")}")  ;
        return sb.toString();
    }

    public String toChangeCallSql(String dbType)
    {
        StringBuilder sb=new StringBuilder();
        sb.append("call ")  ;
        sb.append(this.getName()).append("(") ;
        Collection<ProcedureParameter> paramters=    this.getParamterMap().values();
        int j=0;
        for (ProcedureParameter paramter : paramters)
        {
            Object value=paramter.getValue()!=null&& Types.VARCHAR==paramter.getJdbcType() ?"'"+paramter.getValue()+"'":paramter.getValue();
             String name=paramter.getValue()==null?paramter.getName():String.valueOf(value);
            String joinStr=  j==this.getParamterMap().size()-1?name:name+"," ;
            sb.append(joinStr);
            j++;
        }
        sb.append(")")  ;
        if(isResultSimpleValue())
        {
            sb.append(";select ");
          sb.append(  Joiner.on(",").join(selectColumns)  );
        }
        return sb.toString();
    }

    public Set<String> getListFields()
    {
        return listFields;
    }

    public void setListFields(Set<String> listFields)
    {
        this.listFields = listFields;
    }

    public Set<String> getSelectColumns()
    {
        return selectColumns;
    }

    public String getSetSql()
    {
        return setSql;
    }

    public void setSetSql(String setSql)
    {
        this.setSql = setSql;
    }

    public String getSelectSql()
    {
        return selectSql;
    }

    public void setSelectSql(String selectSql)
    {
        this.selectSql = selectSql;
    }

    private Map<String,ProcedureParameter> paramterMap=new LinkedHashMap<>();

    public String getOriginSql()
    {
        return originSql;
    }

    public void setOriginSql(String originSql)
    {
        this.originSql = originSql;
    }

    public Map<String, ProcedureParameter> getParamterMap()
    {
        return paramterMap;
    }

    public void setParamterMap(Map<String, ProcedureParameter> paramterMap)
    {
        this.paramterMap = paramterMap;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String getCallSql()
    {
        return callSql;
    }

    public void setCallSql(String callSql)
    {
        this.callSql = callSql;
    }
}
