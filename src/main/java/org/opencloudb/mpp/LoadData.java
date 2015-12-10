package org.opencloudb.mpp;

import java.util.List;

/**
 * Created by magicdoom on 2015/3/30.
 */
public class LoadData
{
    public static final String loadDataHint="/*loaddata*/";
    private boolean isLocal;
    private List<String> data;
    private String fileName;
    private  String charset;
    private  String lineTerminatedBy;
    private String fieldTerminatedBy;
    private  String enclose;
    private  String escape;

    public String getEscape()
    {
        return escape;
    }

    public void setEscape(String escape)
    {
        this.escape = escape;
    }

    public boolean isLocal()
    {
        return isLocal;
    }

    public void setLocal(boolean isLocal)
    {
        this.isLocal = isLocal;
    }

    public List<String> getData()
    {
        return data;
    }

    public void setData(List<String> data)
    {
        this.data = data;
    }

    public String getFileName()
    {
        return fileName;
    }

    public void setFileName(String fileName)
    {
        this.fileName = fileName;
    }

    public String getCharset()
    {
        return charset;
    }

    public void setCharset(String charset)
    {
        this.charset = charset;
    }

    public String getLineTerminatedBy()
    {
        return lineTerminatedBy;
    }

    public void setLineTerminatedBy(String lineTerminatedBy)
    {
        this.lineTerminatedBy = lineTerminatedBy;
    }

    public String getFieldTerminatedBy()
    {
        return fieldTerminatedBy;
    }

    public void setFieldTerminatedBy(String fieldTerminatedBy)
    {
        this.fieldTerminatedBy = fieldTerminatedBy;
    }

    public String getEnclose()
    {
        return enclose;
    }

    public void setEnclose(String enclose)
    {
        this.enclose = enclose;
    }
}
