package de.zib.scalaris.executor;

import java.util.List;

import de.zib.scalaris.RequestList;
import de.zib.scalaris.operations.AddDelOnListOp;
import de.zib.scalaris.operations.AddOnNrOp;

/**
 * Implements a list append operation using the append operation of Scalaris.
 * Supports an (optional) list counter key which is updated accordingly.
 *
 * For a correct counter in the <tt>countKey</tt>, this class assumes that every
 * element from the <tt>toRemove</tt> list existed in the list (at least after
 * adding the elements from <tt>toAdd</tt>).
 *
 * @param <T>
 *            the type of objects in the list
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.18
 * @since 3.18
 */
public class ScalarisListAppendRemoveOp2<T> extends ScalarisChangeListOp2 {
    /**
     * Elements to add to the stored list.
     */
    final protected List<T> toAdd;
    /**
     * Elements to remove from the stored list (after adding {@link #toAdd}).
     */
    final protected List<T> toRemove;

    /**
     * Creates a new append+remove operation.
     *
     * @param key       the key to append/remove the values to/from
     * @param toAdd     the values to add
     * @param toRemove  the values to remove
     * @param countKey  the key for the counter of the entries in the list
     *                  (may be <tt>null</tt>)
     */
    public ScalarisListAppendRemoveOp2(final String key, final List<T> toAdd,
            final List<T> toRemove, final String countKey) {
        super(key, countKey);
        this.toAdd = toAdd;
        this.toRemove = toRemove;
    }

    /**
     * Adds {@link #toAdd} to the list at {@link ScalarisListOp2#key},
     * removes {@link #toRemove} and updates the list counter (if present).
     *
     * @param requests
     *            the request list
     *
     * @return <tt>0</tt> (number of processed operations)
     */
    @Override
    protected int changeList(final RequestList requests) {
        requests.addOp(new AddDelOnListOp(key, toAdd, toRemove));
        if (countKey != null) {
            requests.addOp(new AddOnNrOp(countKey, toAdd.size() - toRemove.size()));
        }
        return 0;
    }

    @Override
    public String toString() {
        return "Scalaris.append_remove(" + key + ", " + toAdd.toString() + ", "
                + toRemove.toString() + ", " + countKey + ")";
    }
}
