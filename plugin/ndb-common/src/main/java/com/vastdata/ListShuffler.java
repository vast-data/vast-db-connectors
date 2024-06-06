
/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;

public class ListShuffler<T>
{
    private final Optional<Long> seed;

    public ListShuffler(Optional<Long> seed)
    {
        this.seed = seed;
    }

    public List<T>  randomizeList(List<T> list)
    {
        Random random = new Random();
        seed.ifPresent(random::setSeed);
        List<T> newList = new ArrayList<>(list);
        Collections.shuffle(newList, random);
        return Collections.unmodifiableList(newList);
    }
}
