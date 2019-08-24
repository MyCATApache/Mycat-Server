package io.mycat.parser.primitive;

import io.mycat.route.parser.primitive.FunctionParser;
import io.mycat.route.parser.primitive.Model.Function;
import junit.framework.Assert;
import org.junit.Test;

import java.sql.SQLNonTransientException;

/**
 * @author Hash Zhang
 * @version 1.0.0
 * @date 2016/7/26
 */
public class TestFunctionParser {

    @Test
    public void testMultiFunctions() throws SQLNonTransientException {
        Assert.assertEquals("[arg1, a.t]",testFunctionParse("function1(arg1,a.t)"));
        Assert.assertEquals("[arg1, a.t]",testFunctionParse("function1(arg1,a.t,\"ast(,)\")"));
        Assert.assertEquals("[arg1, a.t, c.t, x]",testFunctionParse("function1(arg1,a.t,\"ast(,)\",\",\",function2(c.t,function3(x)))"));
        Assert.assertEquals("[arg1, a.t, c.t, x]",testFunctionParse("function1(arg1,a.t,\"ast(,)\",\",\",function2(c.t,\"(,)\",function3(function4(x))))"));
    }

    public String testFunctionParse(String function) throws SQLNonTransientException {
        Function function1 = FunctionParser.parseFunction(function);
        return FunctionParser.getFields(function1).toString();
    }
}
