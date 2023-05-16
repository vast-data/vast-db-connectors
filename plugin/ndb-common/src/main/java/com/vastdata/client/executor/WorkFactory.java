/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.client.executor;

import java.net.URI;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public final class WorkFactory
{
    private WorkFactory() {}

    static class Work<T, R>
            implements Function<URI, R>
    {
        private final T subject;
        private final BiFunction<T, URI, R> action;

        Work(T subject, BiFunction<T, URI, R> action)
        {
            this.subject = subject;
            this.action = action;
        }

        @Override
        public R apply(URI uri)
        {
            return action.apply(subject, uri);
        }
    }

    public static <T, R> Supplier<Function<URI, R>> fromCollection(Collection<T> collection, BiFunction<T, URI, R> action)
    {
        final Iterator<T> iterator = collection.iterator();
        return () -> {
            if (iterator.hasNext()) {
                T next = iterator.next();
                return new Work<>(next, action);
            }
            else {
                return null;
            }
        };
    }
}
