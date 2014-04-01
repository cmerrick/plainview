package plainview;

import clojure.lang.ISeq;
import clojure.lang.Var;
import clojure.lang.RT;

public class Consumer {

    private static final Var SYMBOL  = RT.var("clojure.core", "symbol");
    private static final Var REQUIRE = RT.var("clojure.core", "require");
    private static final Var MAIN    = RT.var("plainview.consumer", "-main");

    public static void main(String... args) {
        REQUIRE.invoke(SYMBOL.invoke("plainview.consumer"));
        MAIN.invoke(args);
    }
}
