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
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
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
 * Setter methods must be of the form setKey(xxx),
 * getter methods of the form getKey() or isKey().
 * 
 * @param <T> the Bean to convert to/from
 * 
 * @author Nico Kruber, kruber@zib.de
 */
class ErlangValueJSONToBean<T> extends ErlangValueJSONBase implements ErlangValueJSONInterface<T> {
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
     * Gets an {@link ErlangValueJSONToBean} instance using the given
     * {@link Type}.
     * 
     * @param t
     *            the type
     * 
     * @return an {@link ErlangValueJSONToBean}<U>
     */
    public static ErlangValueJSONToBean<?> getInstance(Type t) {
        ErlangValueJSONToBean<?> json_converter = getInstance(getRawType(t));
        return json_converter;
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.ErlangValueJSON#toScalarisJSON(T)
     */
    public OtpErlangTuple toScalarisJSON(T value) throws ClassCastException {
        return convertJavaToScalarisJSON_object2(value);
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.ErlangValueJSON#toJava(com.ericsson.otp.erlang.OtpErlangList)
     */
    @SuppressWarnings("unchecked")
    public T toJava(OtpErlangList value) throws ClassCastException {
        return (T) convertScalarisJSONtoJava_object2(value, null);
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
     * Gets the class of the raw type of the given type.
     * 
     * @param type the type object
     * 
     * @return the class behind the type
     */
    private static Class<?> getRawType(Type type) {
        if (type instanceof ParameterizedType) {
            return (Class<?>) ((ParameterizedType) type).getRawType();
        } else {
            return (Class<?>) type;
        }
    }
    
    /**
     * Uses introspection to get the setter method for the given key of class
     * {@link #c}.
     * Setter methods must be of the form setKey(xxx),
     * getter methods of the form getKey() or isKey().
     * 
     * @param key
     *            the key to get the setter for
     * 
     * @return the setter method
     * 
     * @throws ClassCastException
     *             if there is no public setter method for <tt>key</tt>
     */
    private Method getSetterFor(String key, Type type) {
        String keyCap1st = capFirst(key);
        String setMethod = "set" + keyCap1st;
        Class<?> class_ = getRawType(type);
        try {
            return c.getMethod(setMethod, class_);
        } catch (Exception e) {
            throw new ClassCastException("no setter " + setMethod + "(" + class_.getSimpleName() + "): " + e.getMessage());
        }
    }
    
    /**
     * Uses introspection to get the type of the given key of class {@link #c}.
     * Assumes there is a getter of the form getKey() or isKey().
     * 
     * @param key
     *            the key to get the type for
     * 
     * @return the {@link Class} of the type.
     * 
     * @throws ClassCastException
     *             if there is no public getter method for <tt>key</tt>
     */
    private Type getTypeOf(String key) throws ClassCastException {
        String keyCap1st = capFirst(key);
        try {
            try {
                return c.getMethod("get" + keyCap1st).getGenericReturnType();
            } catch (NoSuchMethodException e) {
                return c.getMethod("is" + keyCap1st).getGenericReturnType();
            }
        } catch (Exception e) {
            throw new ClassCastException("no getter [get|is]" + keyCap1st + ": " + e.getMessage());
        }
    }
    
    private Pattern getMatcher = java.util.regex.Pattern.compile("^get|is");

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
            Object value) throws ClassCastException {
        ErlangValueJSONToBean<?> json_converter = getInstance(value.getClass());
        return json_converter.convertJavaToScalarisJSON_object2(value);
    }

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
    protected OtpErlangTuple convertJavaToScalarisJSON_object2(
            Object value_) throws ClassCastException {
        try {
            if (value_ instanceof Map<?, ?>) {
                @SuppressWarnings("unchecked")
                Map<String, Object> value = (Map<String, Object>) value_;
                OtpErlangTuple[] resultList = new OtpErlangTuple[value.size()];
                int i = 0;

                for (Map.Entry<String, Object> entry : value.entrySet()) {
                    resultList[i] = new OtpErlangTuple(new OtpErlangObject[] {
                            new OtpErlangString(entry.getKey()),
                            convertJavaToScalarisJSON_value(entry.getValue()) });
                    ++i;
                }
                OtpErlangTuple resultTpl = new OtpErlangTuple(new OtpErlangObject[] {
                        CommonErlangObjects.structAtom, new OtpErlangList(resultList) });
                return resultTpl;
            } else {
                @SuppressWarnings("unchecked")
                T value = (T) value_;

                // get all getters:
                Method[] methods = c.getDeclaredMethods();
                List<OtpErlangObject> resultList = new LinkedList<OtpErlangObject>();

                for (int j = 0; j < methods.length; ++j) {
                    String methodName = methods[j].getName();
                    if (getMatcher.matcher(methodName).lookingAt()) {
                        String key_j = decapFirst(getMatcher.matcher(methodName).replaceFirst(""));
                        try {
                            OtpErlangObject value_j = convertJavaToScalarisJSON_value(methods[j].invoke(value));
                            resultList.add(new OtpErlangTuple(new OtpErlangObject[] {new OtpErlangString(key_j), value_j}));
                        } catch (IllegalArgumentException e) {
                            e.printStackTrace();
                            throw new ClassCastException("cannot access getter " + methodName + "() of class " + c.getSimpleName() + ": " + e.getMessage());
                        } catch (IllegalAccessException e) {
                            throw new ClassCastException("cannot access getter " + methodName + "() of class " + c.getSimpleName() + ": " + e.getMessage());
                        } catch (InvocationTargetException e) {
                            throw new ClassCastException("cannot access getter " + methodName + "() of class " + c.getSimpleName() + ": " + e.getMessage());
                        }
                    }
                }
                OtpErlangTuple resultTpl = new OtpErlangTuple(new OtpErlangObject[] {
                        CommonErlangObjects.structAtom,
                        new OtpErlangList(resultList.toArray(new OtpErlangObject[0])) });
                return resultTpl;
            }
        } catch (ClassCastException e) {
            e.printStackTrace();
            throw new ClassCastException("Unsupported JSON type (value: " + value_ + ")");
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
    protected Object convertScalarisJSONtoJava_object2(
            OtpErlangList value, Type type_) throws ClassCastException {
        if (c.equals(Map.class) || Arrays.asList(c.getInterfaces()).contains(Map.class)) {
            // target type is a map:
            Type elementType = Object.class;
            
            // we might have some more details about its value's type:
            if (type_ != null && type_ instanceof ParameterizedType) {
                ParameterizedType type = (ParameterizedType) type_;
                Type[] typeArguments = type.getActualTypeArguments();
                if (typeArguments.length == 2) {
                    elementType = typeArguments[1];
                }
            }
            
            Map<String, Object> result = new LinkedHashMap<String, Object>(
                    value.arity());
            for (OtpErlangObject iter : value) {
                OtpErlangTuple iter_tpl = (OtpErlangTuple) iter;
                if (iter_tpl.arity() == 2) {
                    String key = convertScalarisJSONtoJava_key(iter_tpl.elementAt(0));
                    result.put(key,
                            convertScalarisJSONtoJava_value2(iter_tpl.elementAt(1), elementType));
                } else {
                    throw new ClassCastException("Unsupported JSON type (value: " + value.toString() + ")");
                }
            }
            return result;
        } else {
            // target type is a bean:
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
                    Type elementType = getTypeOf(key);
                    Method setter = getSetterFor(key, elementType);
                    Object myValue = convertScalarisJSONtoJava_value2(iter_tpl.elementAt(1), elementType);
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

    /**
     * Converts a JSON array value (a list of values) to a Java List.
     * 
     * @param value
     *            a list of JSON values as stored by Scalaris' JSON API
     * @param type_
     *            the (complete) type of the list, including parameters
     * 
     * @return a Java object representing the value
     * 
     * @throws ClassCastException
     *                if thrown if a conversion is not possible, i.e. the type
     *                is not supported
     */
    protected List<Object> convertScalarisJSONtoJava_array2(OtpErlangList value, Type type_)
            throws ClassCastException {
        List<Object> result = new ArrayList<Object>(value.arity());
        for (OtpErlangObject iter : value) {
            if (type_ instanceof ParameterizedType) {
                ParameterizedType type = (ParameterizedType) type_;
                Type[] typeArguments = type.getActualTypeArguments();
                if (typeArguments.length == 1) {
                    result.add(convertScalarisJSONtoJava_value2(iter, typeArguments[0]));
                    continue;
                }
            } else {
                // note: list type could be Object if encapsulated in another type
                result.add(convertScalarisJSONtoJava_value2(iter, Object.class));
            }
        }
        return result;
    }
    
    /**
     * Converts an unknown JSON value to a Java object.
     * 
     * @param value
     *            a JSON value as stored by Scalaris' JSON API
     * @param type
     *            the (supposed) type of the returned value
     *            (hint for container types)
     * 
     * @return a Java object representing the value
     * 
     * @throws ClassCastException
     *                if thrown if a conversion is not possible, i.e. the type
     *                is not supported
     */
    protected Object convertScalarisJSONtoJava_value2(OtpErlangObject value, Type type)
            throws ClassCastException {
        if (value instanceof OtpErlangTuple) {
            OtpErlangTuple value_tpl = (OtpErlangTuple) value;
            if (value_tpl.arity() == 2) {
                OtpErlangObject tag = value_tpl.elementAt(0);
                if (tag.equals(CommonErlangObjects.structAtom)) {
                    // converting an object
                    OtpErlangList value_obj = (OtpErlangList) value_tpl.elementAt(1);
                    Type type1 = type.equals(Object.class) ? Map.class : type;
                    ErlangValueJSONToBean<?> json_converter = getInstance(type1);
                    return json_converter.convertScalarisJSONtoJava_object2(value_obj, type1);
                } else if (tag.equals(CommonErlangObjects.arrayAtom)) {
                    // converting a list
                    OtpErlangList value_list = ErlangValue.otpObjectToOtpList(value_tpl.elementAt(1));
                    return convertScalarisJSONtoJava_array2(value_list, type);
                } else {
                    throw new ClassCastException("unknown JSON tag");
                }
            } else {
                throw new ClassCastException("wrong tuple arity");
            }
        } else {
            return super.convertScalarisJSONtoJava_value_simple(value);
        }
    }
}
