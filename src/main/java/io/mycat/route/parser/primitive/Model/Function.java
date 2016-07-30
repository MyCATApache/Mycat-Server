package io.mycat.route.parser.primitive.Model;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Hash Zhang
 * @version 1.0.0
 * @date 2016/7/26
 */
public class Function extends Identifier {
    private final List<Identifier> arguments;

    public Function(String name) {
        super(name);
        this.arguments = new LinkedList<>();
    }

    public List<Identifier> getArguments() {
        return arguments;
    }
}
