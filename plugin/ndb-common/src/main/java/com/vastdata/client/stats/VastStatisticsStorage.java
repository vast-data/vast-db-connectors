/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.stats;

import java.util.Optional;

public interface VastStatisticsStorage<U, T>
{
    Optional<T> getTableStatistics(U tableUrl);

    void setTableStatistics(U tableUrl, T tableStatistics);

    void deleteTableStatistics(U tableUrl);
}
