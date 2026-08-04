/*
 *  Copyright (C) Vast Data Ltd.
 */

package spark.sql.catalog.ndb;

import java.util.Objects;

public class InitializedVastCatalog
{
    private static VastCatalog initializedVastCatalog = null;

    private InitializedVastCatalog()
    {
    }

    public static synchronized VastCatalog getVastCatalog()
    {
        return Objects.requireNonNull(initializedVastCatalog,
                "Trying to access an uninitialized VastCatalog object");
    }

    public static synchronized void setVastCatalog(VastCatalog vastCatalog)
    {
        initializedVastCatalog = vastCatalog;
    }
}
