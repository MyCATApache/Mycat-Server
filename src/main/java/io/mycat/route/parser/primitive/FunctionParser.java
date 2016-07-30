package io.mycat.route.parser.primitive;

import io.mycat.route.parser.primitive.Model.Commons;
import io.mycat.route.parser.primitive.Model.Field;
import io.mycat.route.parser.primitive.Model.Function;
import io.mycat.route.parser.primitive.Model.Identifier;
import io.mycat.util.StringUtil;

import java.sql.SQLNonTransientException;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

/**
 * @author Hash Zhang
 * @version 1.0.0
 * @date 2016/7/26
 */
public class FunctionParser {
    public static Function parseFunction(String function) throws SQLNonTransientException {
        StringBuilder buffer = new StringBuilder();
        Stack<Function> functions = new Stack<>();

        int flag = 0;
        for (int i = 0; i < function.length(); i++) {
            char current = function.charAt(i);
            switch (current) {
                case Commons.LEFT_BRACKET:
                    if (flag == 0) {
                        String currentIdentifier = buffer.toString().trim();
                        buffer = new StringBuilder();
                        if (!StringUtil.isEmpty(currentIdentifier)) {
                            Function function1 = new Function(currentIdentifier);
                            if (!functions.empty() && functions.peek() != null) {
                                functions.peek().getArguments().add(function1);
                            }
                            functions.push(function1);
                        }
                        break;
                    }
                    buffer.append(current);
                    break;

                case Commons.ARGUMENT_SEPARATOR:
                    if (flag == 0 || flag == 3) {
                        String currentIdentifier = buffer.toString().trim();
                        buffer = new StringBuilder();
                        if (!StringUtil.isEmpty(currentIdentifier)) {
                            if (flag == 3) {
                                flag = 0;
                                Identifier identifier = new Identifier(currentIdentifier);
                                functions.peek().getArguments().add(identifier);
                            } else {
                                Field field = new Field(currentIdentifier);
                                functions.peek().getArguments().add(field);
                            }
                        }
                        break;
                    }
                    buffer.append(current);
                    break;
                case Commons.RIGHT_BRACKET:
                    if (flag != 1 && flag != 2) {
                        String currentIdentifier = buffer.toString().trim();
                        buffer = new StringBuilder();
                        if (!StringUtil.isEmpty(currentIdentifier)) {
                            if (flag == 3) {
                                flag = 0;
                                Identifier identifier = new Identifier(currentIdentifier);
                                functions.peek().getArguments().add(identifier);
                            } else {
                                Field field = new Field(currentIdentifier);
                                functions.peek().getArguments().add(field);
                            }
                        }
                        if (flag == 0) {
                            if (functions.size() == 1) {
                                return functions.pop();
                            } else {
                                functions.pop();
                            }
                        }
                        break;
                    }
                    buffer.append(current);
                    break;
                case Commons.QUOTE:
                    if (flag == 0) {
                        flag = 1;
                    } else if (flag == 1) {
                        flag = 3;
                    }
                case Commons.DOUBLE_QUOTE:
                    if (flag == 0) {
                        flag = 2;
                    } else if (flag == 2) {
                        flag = 3;
                    }
                default:
                    buffer.append(current);
            }
        }
        throw new SQLNonTransientException("Function is not in right format!");
    }

    public static List<String> getFields(Function function){
        List<String> fields = new LinkedList<>();
        for(Identifier identifier : function.getArguments()){
            if(identifier instanceof Field){
                fields.add(identifier.getName());
            } else if (identifier instanceof Function){
                fields.addAll(getFields((Function) identifier));
            }
        }
        return fields;
    }
    public static void main(String[] args) throws SQLNonTransientException {
        Function function = FunctionParser.parseFunction("function1(arg1,a.t,\"ast()\",function2(c.t,function3(x)))");
        System.out.println(getFields(function));
    }
}
