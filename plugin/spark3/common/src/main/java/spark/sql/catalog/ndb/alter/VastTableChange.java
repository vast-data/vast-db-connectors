/*
 *  Copyright (C) Vast Data Ltd.
 */

package spark.sql.catalog.ndb.alter;

import com.vastdata.client.VastClient;
import com.vastdata.client.tx.VastTransaction;

import java.util.function.BiConsumer;

public interface VastTableChange
        extends BiConsumer<VastClient, VastTransaction> {}
