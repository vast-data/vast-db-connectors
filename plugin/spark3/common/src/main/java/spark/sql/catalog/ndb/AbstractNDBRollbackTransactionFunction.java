/*
 *  Copyright (C) Vast Data Ltd.
 */

package spark.sql.catalog.ndb;

public abstract class AbstractNDBRollbackTransactionFunction extends AbstractNDBFunction
{

    @Override
    String getDescription()
    {
        return "Rolling back a Vast transaction";
    }

    @Override
    String getName()
    {
        return NDBFunction.ROLLBACK_TX.getFuncName();
    }
}
