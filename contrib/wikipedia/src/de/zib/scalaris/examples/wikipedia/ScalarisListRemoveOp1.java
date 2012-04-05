package de.zib.scalaris.examples.wikipedia;

import java.util.List;

import de.zib.scalaris.ErlangValue;

/**
 * Implements a list remove operation using the read and write operations of
 * Scalaris.
 * 
 * @param <T> the type of objects in the list
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class ScalarisListRemoveOp1<T> extends ScalarisListOp1<T> {
    final protected ErlangValue toRemove;

    /**
     * Creates a new remove operation.
     * 
     * @param key       the key to remove the value from
     * @param toRemove  the value to remove
     * @param countKey  the key for the counter of the entries in the list
     *                  (may be <tt>null</tt>)
     */
    public ScalarisListRemoveOp1(String key, T toRemove, String countKey) {
        super(key, countKey);
        this.toRemove = new ErlangValue(toRemove);
    }

    /**
     * Removes {@link #toRemove} from the given page list.
     * 
     * @param pageList
     *            the original page list
     */
    @Override
    protected void changeList(List<ErlangValue> pageList) {
        pageList.remove(toRemove);
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.ScalarisOp#toString()
     */
    @Override
    public String toString() {
        return "Scalaris.remove(" + key + ", " + toRemove + ", " + countKey + ")";
    }
}
