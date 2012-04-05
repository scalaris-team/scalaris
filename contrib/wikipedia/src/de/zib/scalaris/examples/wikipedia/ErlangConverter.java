package de.zib.scalaris.examples.wikipedia;

import de.zib.scalaris.ErlangValue;

/**
 * Interface converting an {@link ErlangValue} to a custom type.
 * 
 * @author Nico Kruber, kruber@zib.de
 *
 * @param <T> the type to convert to
 */
public interface ErlangConverter<T> {
    /**
     * Converts the given {@link ErlangValue} to the desired type
     * 
     * @param v  the value to convert
     * 
     * @return the converted value
     * 
     * @throws ClassCastException if the value can not be converted
     */
    public T convert(ErlangValue v) throws ClassCastException;
}