import clojure.lang.ISeq;
import clojure.lang.Var;
import clojure.lang.RT;

public class Cascalog {

    private static final Var SYMBOL  = RT.var("clojure.core", "symbol");
    private static final Var REQUIRE = RT.var("clojure.core", "require");
    private static final Var MAIN    = RT.var("plainview.cascalog", "-main");

    public static void main(String... args) {
        REQUIRE.invoke(SYMBOL.invoke("plainview.cascalog"));
        MAIN.invoke(args);
    }
}
