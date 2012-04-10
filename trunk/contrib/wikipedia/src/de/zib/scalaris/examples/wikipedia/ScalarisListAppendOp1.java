package de.zib.scalaris.examples.wikipedia;

import java.util.List;

import de.zib.scalaris.ErlangValue;

/**
 * Implements a list append operation using the read and write operations of
 * Scalaris.
 * 
 * @param <T> the type of objects in the list
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class ScalarisListAppendOp1<T> extends ScalarisListOp1<T> {
    final protected ErlangValue toAdd;

    /**
     * Creates a new append operation.
     * 
     * @param key       the key to append the value to
     * @param toAdd     the value to add
     * @param countKey  the key for the counter of the entries in the list
     *                  (may be <tt>null</tt>)
     */
    public ScalarisListAppendOp1(String key, T toAdd, String countKey) {
        super(key, countKey);
        this.toAdd = new ErlangValue(toAdd);
    }

    /**
     * Adds {@link #toAdd} to the given page list.
     * 
     * @param pageList
     *            the original page list
     */
    @Override
    protected void changeList(List<ErlangValue> pageList) {
        pageList.add(toAdd);
    }

    @Override
    public String toString() {
        return "Scalaris.append(" + key + ", " + toAdd + ", " + countKey + ")";
    }
}
