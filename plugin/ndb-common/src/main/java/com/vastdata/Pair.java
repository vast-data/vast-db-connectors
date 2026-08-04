/*
 *  Copyright (C) Vast Data Ltd.
 */
package com.vastdata;

import java.util.Objects;

public class Pair<L, R>
{
    private final L left;
    private final R right;

    private Pair(L left, R right)
    {
        this.left = left;
        this.right = right;
    }

    public static <L, R> Pair<L, R> of(L left, R right)
    {
        return new Pair<>(left, right);
    }

    public L getLeft()
    {
        return left;
    }

    public R getRight()
    {
        return right;
    }

    public L getKey()
    {
        return left;
    }

    public R getValue()
    {
        return right;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Pair<?, ?> pair = (Pair<?, ?>) o;
        return Objects.equals(left, pair.left) && Objects.equals(right,
                pair.right);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(left, right);
    }

    @Override
    public String toString()
    {
        return "Pair{" + "left=" + left + ", right=" + right + '}';
    }
}
