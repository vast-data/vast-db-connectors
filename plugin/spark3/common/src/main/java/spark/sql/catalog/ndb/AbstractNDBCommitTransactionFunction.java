/*
 *  Copyright (C) Vast Data Ltd.
 */

package spark.sql.catalog.ndb;

public abstract class AbstractNDBCommitTransactionFunction
        extends AbstractNDBFunction
{
    protected String getDescription()
    {
        return "Commit a Vast transaction";
    }

    protected String getName()
    {
        return NDBFunction.COMMIT_TX.getFuncName();
    }
}
