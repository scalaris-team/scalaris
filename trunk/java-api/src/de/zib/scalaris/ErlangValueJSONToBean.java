/**
 *  Copyright 2011 Zuse Institute Berlin
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package de.zib.scalaris;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.ericsson.otp.erlang.OtpErlangAtom;
import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangString;
import com.ericsson.otp.erlang.OtpErlangTuple;

/**
 * Converts Scalaris-JSON to Beans with setters and getters for each JSON key
 * and such Beans to Scalaris-JSON.
 * 
 * Supported member types of the Bean:
 * <ul>
 *  <li>boolean, {@link Boolean}</li>
 *  <li>int, {@link Integer}</li>
 *  <li>long, {@link Long}</li>
 *  <li>{@link BigInteger}</li>
 *  <li>double, {@link Double}</li>
 *  <li>{@link String}</li>
 *  <li>{@link List} of primitive types (any other element will be converted to {@link Map}&lt;String, Object&gt;)</li>
 *  <li>{@link Map} with string keys and primitive-typed values (any other value will be converted to {@link Map}&lt;String, Object&gt;)</li>
 *  <li>such a Bean
 * </ul>
 * 
 * Note: regarding lists and maps: it is not possible to get type information
 * from their elements (type erasure).
 * 
 * @param <T> the Bean to convert to/from
 * 
 * @author Nico Kruber, kruber@zib.de
 */
class ErlangValueJSONToBean<T> extends ErlangValueJSONBase<T> {
    /**
     * Current key of the JSON object.
     */
    String currentKey = null;
    
    /**
     * The class to convert the object to.
     */
    private Class<T> c;
    
    /**
     * Creates a new object converting to the given class.
     * 
     * @param c
     *            the class to convert JSON to
     */
    public ErlangValueJSONToBean(Class<T> c) {
        this.c = c;
    }
    
    /**
     * Gets an {@link ErlangValueJSONToBean} instance using the given
     * {@link Class}.
     * 
     * @param <U>
     *            type of the {@link Class}
     * 
     * @param c
     *            the class
     * 
     * @return an {@link ErlangValueJSONToBean}<U>
     */
    public static <U> ErlangValueJSONToBean<U> getInstance(Class<U> c) {
        return new ErlangValueJSONToBean<U>(c);
    }

    
    /**
     * Uses introspection to get the setter method for the given key of class
     * {@link #c}.
     * Setter methods must be of the form setKey(xxx).
     * 
     * @param key
     *            the key to get the setter for
     * 
     * @return the setter method
     * 
     * @throws ClassCastException
     *             if there is no public setter method for <tt>key</tt>
     */
    private Method getSetterFor(String key) {
        String key1 = capFirst(key);
        String getMethod = "get" + key1;
        String setMethod = "set" + key1;
        Class<?> type = getTypeOf2(getMethod);
        try {
            return c.getMethod(setMethod, type);
        } catch (Exception e) {
            throw new ClassCastException("no setter " + setMethod + "(" + type.getSimpleName() + "): " + e.getMessage());
        }
    }
    
    /**
     * Capitalize the first letter of the given string.
     * 
     * @param key
     *            the string
     * 
     * @return a string with the first character being upper case
     */
    private static String capFirst(String key) {
        if (key.length() > 0) {
            String keyCap = key.substring(0, 1).toUpperCase() + key.substring(1);
            return keyCap;
        }
        return "";
    }
    
    /**
     * De-capitalize the first letter of the given string.
     * 
     * @param key
     *            the string
     * 
     * @return a string with the first character being lower case
     */
    private static String decapFirst(String key) {
        if (key.length() > 0) {
            String keyCap = key.substring(0, 1).toLowerCase() + key.substring(1);
            return keyCap;
        }
        return "";
    }
    
    /**
     * Uses introspection to get the type of the given key of class {@link #c}.
     * Assumes there is a getter of the form getKey().
     * 
     * @param key
     *            the key to get the type for
     * 
     * @return the {@link Class} of the type.
     * 
     * @throws ClassCastException
     *             if there is no public getter method for <tt>key</tt>
     */
    private Class<?> getTypeOf(String key) throws ClassCastException {
        String getMethod = "get" + capFirst(key);
        return getTypeOf2(getMethod);
    }

    /**
     * Uses introspection to get the return type of the given method of class
     * {@link #c}.
     * 
     * @param getMethod
     *            the method
     * 
     * @return the {@link Class} of the method's return type.
     * 
     * @throws ClassCastException
     *             if there is no public getter method for <tt>key</tt>
     */
    private Class<?> getTypeOf2(String getMethod) throws ClassCastException {
        try {
            return c.getMethod(getMethod).getReturnType();
        } catch (Exception e) {
            throw new ClassCastException("no getter " + getMethod + ": " + e.getMessage());
        }
    }
    
    /**
     * Converts a JSON object value (a list of key/value pairs) to a Java Map.
     * 
     * @param value
     *            a list of key/value pairs with JSON values and string keys as
     *            stored by Scalaris' JSON API
     * 
     * @return a Java object representing the value
     * 
     * @throws ClassCastException
     *                if thrown if a conversion is not possible, i.e. the type
     *                is not supported
     */
    @Override
    protected Object convertScalarisJSONtoJava_object(
            OtpErlangList value) throws ClassCastException {
        if (currentKey != null) {
            Class<?> keyType = getTypeOf(currentKey);
            if (keyType.equals(Map.class)) {
                ErlangValueJSONToMap json_converter = new ErlangValueJSONToMap();
                return json_converter.convertScalarisJSONtoJava_object(value);
            } else {
                ErlangValueJSONToBean<?> json_converter = getInstance(keyType);
                return json_converter.convertScalarisJSONtoJava_object(value);
            }
        } else {
            T result;
            try {
                result = c.getConstructor().newInstance();
            } catch (Exception e) {
                throw new ClassCastException("Cannot store value to JSON object (value: " + value.toString() + "): " + e.getMessage());
            }
            for (OtpErlangObject iter : value) {
                OtpErlangTuple iter_tpl = (OtpErlangTuple) iter;
                if (iter_tpl.arity() == 2) {
                    OtpErlangObject key_erl = iter_tpl.elementAt(0);
                    String key;
                    if (key_erl instanceof OtpErlangAtom) {
                        key = ((OtpErlangAtom) key_erl).atomValue();
                    } else {
                        try {
                            key = ErlangValue.otpObjectToString(key_erl);
                        } catch (ClassCastException e) {
                            throw new ClassCastException("Unsupported JSON type (value: " + value.toString() + ")");
                        }
                    }
                    currentKey = key;
                    Method setter = getSetterFor(key);
                    Object myValue = convertScalarisJSONtoJava_value(iter_tpl.elementAt(1));
                    try {
                        setter.invoke(result, myValue);
                    } catch (Exception e) {
                        throw new ClassCastException("Cannot store value to JSON object (key: " + key + ", value: "  + myValue + ", complete object: "+ value.toString() + "): " + e.getMessage());
                    }
                } else {
                    throw new ClassCastException("Unsupported JSON type (value: " + value.toString() + ")");
                }
            }
            return result;
        }
    }
    
    private Pattern getMatcher = java.util.regex.Pattern.compile("get");

    /**
     * Converts a Java Map to a JSON object as expected by Scalaris.
     * 
     * @param value_
     *            a Java Map with String-keys and supported JSON types as values
     * 
     * @return a JSON object representing the value
     * 
     * @throws ClassCastException
     *             if thrown if a conversion is not possible, i.e. the type is
     *             not supported
     */
    @Override
    protected OtpErlangTuple convertJavaToScalarisJSON_object(
            Object value_) throws ClassCastException {
        if (currentKey != null) {
            Class<?> keyType = getTypeOf(currentKey);
            if (keyType.equals(Map.class)) {
                ErlangValueJSONToMap json_converter = new ErlangValueJSONToMap();
                return json_converter.convertJavaToScalarisJSON_object(value_);
            } else {
                ErlangValueJSONToBean<?> json_converter = getInstance(keyType);
                return json_converter.convertJavaToScalarisJSON_object(value_);
            }
        } else {
            try {
                @SuppressWarnings("unchecked")
                T value = (T) value_;

                // get all getters:
                Method[] methods = c.getDeclaredMethods();
                List<OtpErlangObject> resultList = new LinkedList<OtpErlangObject>();

                for (int j = 0; j < methods.length; ++j) {
                    String methodName = methods[j].getName();
                    if (methodName.startsWith("get")) {
                        String key_j = decapFirst(getMatcher.matcher(methodName).replaceFirst(""));
                        currentKey = key_j;
                        try {
                            OtpErlangObject value_j = convertJavaToScalarisJSON_value(methods[j].invoke(value));
                            resultList.add(new OtpErlangTuple(new OtpErlangObject[] {new OtpErlangString(key_j), value_j}));
                        } catch (IllegalArgumentException e) {
                            throw new ClassCastException("cannot access getter " + methodName + "on class " + c.getSimpleName() + e.getMessage());
                        } catch (IllegalAccessException e) {
                            throw new ClassCastException("cannot access getter " + methodName + "on class " + c.getSimpleName() + e.getMessage());
                        } catch (InvocationTargetException e) {
                            throw new ClassCastException("cannot access getter " + methodName + "on class " + c.getSimpleName() + e.getMessage());
                        }
                    }
                }
                OtpErlangTuple resultTpl = new OtpErlangTuple(new OtpErlangObject[] {
                        CommonErlangObjects.structAtom,
                        new OtpErlangList(resultList.toArray(new OtpErlangObject[0])) });
                return resultTpl;
            } catch (ClassCastException e) {
                throw new ClassCastException("Unsupported JSON type (value: " + value_.toString() + ")");
            }
        }
    }
}
