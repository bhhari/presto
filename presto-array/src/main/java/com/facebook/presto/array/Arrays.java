/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.array;

import static com.facebook.presto.array.Arrays.ExpansionFactor.LARGE;
import static com.facebook.presto.array.Arrays.ExpansionFactor.MEDIUM;
import static com.facebook.presto.array.Arrays.ExpansionFactor.SMALL;

public class Arrays
{
    private static ExpansionFactor defaultExpansionFactor = SMALL;

    private Arrays() {}

    public static int[] ensureCapacity(int[] buffer, int capacity)
    {
        return ensureCapacity(buffer, capacity, defaultExpansionFactor, false, false);
    }

    public static int[] ensureCapacity(int[] buffer, int capacity, ExpansionFactor expansionFactor, boolean preserveValues, boolean initializeValues)
    {
        checkValidExpansionFactor(expansionFactor);

        int newCapacity = (int) (capacity * expansionFactor.expansionFactor);

        if (buffer == null) {
            buffer = new int[newCapacity];
        }
        else if (buffer.length < capacity) {
            if (preserveValues) {
                buffer = java.util.Arrays.copyOf(buffer, newCapacity);
            }
            else {
                buffer = new int[newCapacity];
            }
        }
        else if (initializeValues) {
            java.util.Arrays.fill(buffer, 0);
        }

        return buffer;
    }

    public static long[] ensureCapacity(long[] buffer, int capacity)
    {
        if (buffer == null || buffer.length < capacity) {
            return new long[(int) (capacity * defaultExpansionFactor.expansionFactor)];
        }

        return buffer;
    }

    public static boolean[] ensureCapacity(boolean[] buffer, int capacity)
    {
        if (buffer == null || buffer.length < capacity) {
            return new boolean[(int) (capacity * defaultExpansionFactor.expansionFactor)];
        }

        return buffer;
    }

    public static byte[] ensureCapacity(byte[] buffer, int capacity)
    {
        if (buffer == null || buffer.length < capacity) {
            return new byte[(int) (capacity * defaultExpansionFactor.expansionFactor)];
        }

        return buffer;
    }

    public static int[][] ensureCapacity(int[][] buffer, int capacity)
    {
        if (buffer == null || buffer.length < capacity) {
            return new int[capacity][];
        }

        return buffer;
    }

    public static boolean[][] ensureCapacity(boolean[][] buffer, int capacity)
    {
        if (buffer == null || buffer.length < capacity) {
            return new boolean[capacity][];
        }

        return buffer;
    }

    public static byte[] ensureCapacity(byte[] buffer, int capacity, ExpansionFactor expansionFactor, boolean preserveValues)
    {
        checkValidExpansionFactor(expansionFactor);

        int newCapacity = (int) (capacity * expansionFactor.expansionFactor);

        if (buffer == null) {
            buffer = new byte[newCapacity];
        }
        else if (buffer.length < capacity) {
            if (preserveValues) {
                buffer = java.util.Arrays.copyOf(buffer, newCapacity);
            }
            else {
                buffer = new byte[newCapacity];
            }
        }

        return buffer;
    }

    private static void checkValidExpansionFactor(ExpansionFactor expansionFactor)
    {
        if (!expansionFactor.equals(SMALL) && !expansionFactor.equals(MEDIUM) && !expansionFactor.equals(LARGE)) {
            throw new IllegalArgumentException("expansionFactor must be 1.0, 1.5 or 2.0");
        }
    }

    public enum ExpansionFactor
    {
        SMALL(1.0),
        MEDIUM(1.5),
        LARGE(2.0);

        private final double expansionFactor;

        ExpansionFactor(double expansionFactor)
        {
            this.expansionFactor = expansionFactor;
        }
    }
}
