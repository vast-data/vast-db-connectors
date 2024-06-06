/*
 *  Copyright (C) Vast Data Ltd.
 */

package spark.sql.catalog.ndb;

public enum NDBFunction
{
    CREATE_TX("create_tx"), ROLLBACK_TX("rollback_tx"), COMMIT_TX("commit_tx");

    private final String funcName;

    NDBFunction(String funcName) {
        this.funcName = funcName;
    }

    public String getFuncName()
    {
        return funcName;
    }
}
