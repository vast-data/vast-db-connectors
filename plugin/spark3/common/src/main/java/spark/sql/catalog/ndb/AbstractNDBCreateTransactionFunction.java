/*
 *  Copyright (C) Vast Data Ltd.
 */

package spark.sql.catalog.ndb;

public abstract class AbstractNDBCreateTransactionFunction extends AbstractNDBFunction
{
    @Override
    String getDescription()
    {
        return "Creating Vast transaction";
    }

    @Override
    String getName()
    {
        return NDBFunction.CREATE_TX.getFuncName();
    }
}
