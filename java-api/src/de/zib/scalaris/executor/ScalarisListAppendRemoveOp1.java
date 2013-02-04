package de.zib.scalaris.executor;

import java.util.List;

import de.zib.scalaris.ErlangValue;

/**
 * Implements a list append and remove operation using the read and write
 * operations of Scalaris. Supports an (optional) list counter key which is
 * updated accordingly.
 *
 * First adds a set of elements, then removes another set of elements.
 *
 * @param <T>
 *            the type of objects in the list
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.18
 * @since 3.18
 */
public class ScalarisListAppendRemoveOp1<T> extends ScalarisChangeListOp1 {
    /**
     * Elements to add to the stored list.
     */
    final protected List<ErlangValue> toAdd;
    /**
     * Elements to remove from the stored list (after adding {@link #toAdd}).
     */
    final protected List<ErlangValue> toRemove;

    /**
     * Creates a new append+remove operation.
     *
     * @param key       the key to append/remove the values to/from
     * @param toAdd     the values to add
     * @param toRemove  the values to remove
     * @param countKey  the key for the counter of the entries in the list
     *                  (may be <tt>null</tt>)
     */
    public ScalarisListAppendRemoveOp1(final String key, final List<T> toAdd,
            final List<T> toRemove, final String countKey) {
        super(key, countKey);
        this.toAdd = toErlangValueList(toAdd);
        this.toRemove = toErlangValueList(toRemove);
    }

    /**
     * Adds {@link #toAdd} to the given page list.
     *
     * @param pageList
     *            the original page list (will be changed in-place)
     *
     * @return the changed list (a reference to the changed <tt>pageList</tt>)
     */
    @Override
    protected List<ErlangValue> changeList(final List<ErlangValue> pageList) {
        final int oldCount = pageList.size();
        int count = oldCount;
        pageList.addAll(toAdd);
        if (pageList.size() != count) {
            count = pageList.size();
            listChanged = true;
        }
        pageList.removeAll(toRemove);
        if (pageList.size() != count) {
            count = pageList.size();
            listChanged = true;
        }
        if (count != oldCount) {
            listCountChanged = true;
        }
        return pageList;
    }

    @Override
    public String toString() {
        return "Scalaris.append_remove(" + key + ", " + toAdd.toString() + ", "
                + toRemove.toString() + ", " + countKey + ")";
    }
}
