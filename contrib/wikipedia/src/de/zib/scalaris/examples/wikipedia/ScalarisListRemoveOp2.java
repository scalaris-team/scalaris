package de.zib.scalaris.examples.wikipedia;

import java.util.Arrays;

import de.zib.scalaris.RequestList;

/**
 * Implements a list remove operation using the append operation of
 * Scalaris.
 *
 * @param <T> the type of objects in the list
 *
 * @author Nico Kruber, kruber@zib.de
 */
public class ScalarisListRemoveOp2<T> extends ScalarisListOp2<T> {
    final protected T toRemove;

    /**
     * Creates a new append operation.
     *
     * @param key       the key to remove the value from
     * @param toRemove  the value to remove
     * @param countKey  the key for the counter of the entries in the list
     *                  (may be <tt>null</tt>)
     */
    public ScalarisListRemoveOp2(final String key, final T toRemove, final String countKey) {
        super(key, countKey);
        this.toRemove = toRemove;
    }

    /**
     * Removes {@link #toRemove} from the list at {@link ScalarisListOp2#key}.
     *
     * @param requests
     *            the request list
     *
     * @return <tt>0</tt> (number of processed operations)
     */
    @SuppressWarnings("unchecked")
    @Override
    protected int changeList(final RequestList requests) {
        requests.addAddDelOnList(key, null, Arrays.asList(toRemove));
        if (countKey != null) {
            requests.addAddOnNr(countKey, -1);
        }
        return 0;
    }

    @Override
    public String toString() {
        return "Scalaris.remove(" + key + ", " + toRemove.toString() + ", " + countKey + ")";
    }
}
