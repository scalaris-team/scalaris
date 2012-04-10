package de.zib.scalaris.examples.wikipedia;

import java.util.Arrays;

import de.zib.scalaris.RequestList;

/**
 * Implements a list append operation using the append operation of
 * Scalaris.
 *
 * @param <T> the type of objects in the list
 *
 * @author Nico Kruber, kruber@zib.de
 */
public class ScalarisListAppendOp2<T> extends ScalarisListOp2<T> {
    final protected T toAdd;

    /**
     * Creates a new append operation.
     *
     * @param key       the key to append the value to
     * @param toAdd     the value to add
     * @param countKey  the key for the counter of the entries in the list
     *                  (may be <tt>null</tt>)
     */
    public ScalarisListAppendOp2(final String key, final T toAdd, final String countKey) {
        super(key, countKey);
        this.toAdd = toAdd;
    }

    /**
     * Adds {@link #toAdd} to the list at {@link ScalarisListOp2#key}.
     *
     * @param requests
     *            the request list
     *
     * @return <tt>0</tt> (number of processed operations)
     */
    @SuppressWarnings("unchecked")
    @Override
    protected int changeList(final RequestList requests) {
        requests.addAddDelOnList(key, Arrays.asList(toAdd), null);
        if (countKey != null) {
            requests.addAddOnNr(countKey, 1);
        }
        return 0;
    }

    @Override
    public String toString() {
        return "Scalaris.append(" + key + ", " + toAdd + ", " + countKey + ")";
    }
}
