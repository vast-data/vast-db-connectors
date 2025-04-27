/*
 *  Copyright (C) Vast Data Ltd.
 */

package spark.sql.catalog.ndb;

import java.util.Objects;

public class InitializedVastCatalog
{
    private InitializedVastCatalog() {}

    private static VastCatalog initializedVastCatalog = null;

    public static synchronized void setVastCatalog(VastCatalog vastCatalog)
    {
        if (initializedVastCatalog == null) {
            initializedVastCatalog = vastCatalog;
        }
    }

    public static synchronized VastCatalog getVastCatalog()
    {
        return Objects.requireNonNull(initializedVastCatalog);
    }
}
