/**********************************************************************
Copyright (c) 2008 Erik Bengtson and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors:
2013 Orange - port to Scalaris key/value store
    ...
 **********************************************************************/
package org.datanucleus.store.scalaris;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.fieldmanager.AbstractFieldManager;
import org.datanucleus.store.schema.naming.ColumnType;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.store.types.TypeManager;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.store.types.converters.TypeConverterHelper;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.TypeConversionHelper;

import com.orange.org.json.JSONArray;
import com.orange.org.json.JSONException;
import com.orange.org.json.JSONObject;

/**
 * FieldManager for fetching from JSON.
 */
@SuppressWarnings("rawtypes")
public class FetchFieldManager extends AbstractFieldManager {
    protected final ObjectProvider op;
    protected final AbstractClassMetaData acmd;
    protected final ExecutionContext ec;
    protected final JSONObject result;
    protected StoreManager storeMgr;

    /**
     * FetchCache contains all object references of objects currently fetched
     * from the data store. This is necessary to prevent StackOverFlowErrors 
     * in case of circular references of the stored object (e.g. Bidirectional
     * relationships) which are all in the same fetch group, since 
     * the PersistenceHandlers fetchObject method is called (indirectly) whenever 
     * the FetchFieldManager must populate a member referencing to another object
     * in the store. 
     * Whenever the (most outer) fetch operation is completed the cache is cleared. 
     * This is caused by activeCount reaching 0 (see fetchObjectField(int)). 
     * 
     * TODO: DataNucleas manages already several caches in which (theoretically) 
     * each fetched object is inserted into, to handle exactly the same problem
     * than the ObjectFetchCache. Unfortunately this is not working. This could be
     * caused by the PersistenceHandlers method to generate new IDs for keys using
     * IdGeneratorStrategy.Identity.(?)
    **/
    private static final ObjectFetchCache fetchCache = new ObjectFetchCache();
    private static class ObjectFetchCache {
        private HashMap<String, Object> cache;
        private int activeCount;
        
        private ObjectFetchCache(){
            cache = new HashMap<String, Object>();
            activeCount = 0;
        };
        
        void increaseActive() {
            activeCount++;
        }
        void decreaseActive() {
            activeCount = Math.max(0, activeCount-1);
            if (activeCount <= 0) {
                // clear 
                cache = new HashMap<String, Object>();
            }
        }
        boolean exists(String key) {
            return cache.containsKey(key);
        }
        Object lookUp(String key) {
            return cache.get(key);
        }
        void add(String key, Object value) {
            cache.put(key, value);
        }
    }
    
    public FetchFieldManager(ExecutionContext ec, AbstractClassMetaData acmd,
            JSONObject result) {
        this.acmd = acmd;
        this.ec = ec;
        this.result = result;
        this.op = null;
        this.storeMgr = ec.getStoreManager();
    }

    public FetchFieldManager(ObjectProvider op, JSONObject result) {
        this.acmd = op.getClassMetaData();
        this.ec = op.getExecutionContext();
        this.result = result;
        this.op = op;
        this.storeMgr = ec.getStoreManager();
    }

    public boolean fetchBooleanField(int fieldNumber) {
        String memberName = storeMgr
                .getNamingFactory()
                .getColumnName(
                        acmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber),
                        ColumnType.COLUMN);
        if (result.isNull(memberName)) {
            return false;
        }
        try {
            return result.getBoolean(memberName);
        } catch (JSONException e) {
            // ignore
            return false;
        }
    }

    public byte fetchByteField(int fieldNumber) {
        String memberName = storeMgr
                .getNamingFactory()
                .getColumnName(
                        acmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber),
                        ColumnType.COLUMN);
        if (result.isNull(memberName)) {
            return 0;
        }
        try {
            String str = result.getString(memberName);
            return Byte.valueOf(str).byteValue();
        } catch (JSONException e) {
            // ignore
            return 0;
        }
    }

    public char fetchCharField(int fieldNumber) {
        String memberName = storeMgr
                .getNamingFactory()
                .getColumnName(
                        acmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber),
                        ColumnType.COLUMN);
        if (result.isNull(memberName)) {
            return 0;
        }
        try {
            return result.getString(memberName).charAt(0);
        } catch (JSONException e) {
            // ignore
            return 0;
        }
    }

    public double fetchDoubleField(int fieldNumber) {
        String memberName = storeMgr
                .getNamingFactory()
                .getColumnName(
                        acmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber),
                        ColumnType.COLUMN);
        if (result.isNull(memberName)) {
            return 0;
        }
        try {
            return result.getDouble(memberName);
        } catch (JSONException e) {
            // ignore
            return 0;
        }
    }

    public float fetchFloatField(int fieldNumber) {
        String memberName = storeMgr
                .getNamingFactory()
                .getColumnName(
                        acmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber),
                        ColumnType.COLUMN);
        if (result.isNull(memberName)) {
            return 0;
        }
        try {
            return (float) result.getDouble(memberName);
        } catch (JSONException e) {
            // ignore
            return 0;
        }
    }

    public int fetchIntField(int fieldNumber) {
        String memberName = storeMgr
                .getNamingFactory()
                .getColumnName(
                        acmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber),
                        ColumnType.COLUMN);
        if (result.isNull(memberName)) {
            return 0;
        }
        try {
            return result.getInt(memberName);
        } catch (JSONException e) {
            // ignore
            return 0;
        }
    }

    public long fetchLongField(int fieldNumber) {
        String memberName = storeMgr
                .getNamingFactory()
                .getColumnName(
                        acmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber),
                        ColumnType.COLUMN);
        if (result.isNull(memberName)) {
            return 0;
        }
        try {
            return result.getLong(memberName);
        } catch (JSONException e) {
            // ignore
            return 0;
        }
    }

    public short fetchShortField(int fieldNumber) {
        String memberName = storeMgr
                .getNamingFactory()
                .getColumnName(
                        acmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber),
                        ColumnType.COLUMN);
        if (result.isNull(memberName)) {
            return 0;
        }
        try {
            return (short) result.getInt(memberName);
        } catch (JSONException e) {
            // ignore
            return 0;
        }
    }

    public String fetchStringField(int fieldNumber) {
        String memberName = storeMgr
                .getNamingFactory()
                .getColumnName(
                        acmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber),
                        ColumnType.COLUMN);
        if (result.isNull(memberName)) {
            return null;
        }
        try {
            return result.getString(memberName);
        } catch (JSONException e) {
            // ignore
            return null;
        }
    }

    public Object fetchObjectField(int fieldNumber) {
        fetchCache.increaseActive();
        try {
            fetchCache.add(IdentityUtils.getPersistableIdentityForId(
                    op.getExternalObjectId()), 
                    op.getObject());
            
            AbstractMemberMetaData mmd = acmd
                    .getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
            String memberName = storeMgr.getNamingFactory().getColumnName(mmd,
                    ColumnType.COLUMN);
            System.out.println("looking for field " + memberName + " "
                    + acmd.getEntityName());
            
            if (result.isNull(memberName)) {
                return null;
            }
    
            // Special cases
            ClassLoaderResolver clr = ec.getClassLoaderResolver();
            RelationType relationType = mmd.getRelationType(clr);
            if (RelationType.isRelationSingleValued(relationType)
                    && mmd.isEmbedded()) {
                // Persistable object embedded into table of this object TODO
                // Support this
                throw new NucleusException(
                        "Don't currently support embedded fields");
            }
    
            try {
                return fetchObjectFieldInternal(mmd, memberName, clr);
            } catch (JSONException e) {
                throw new NucleusException(e.getMessage(), e);
            }
        } finally {
            fetchCache.decreaseActive();
        }
    }

    protected Object fetchObjectFieldInternal_RelationTypeNone(
            AbstractMemberMetaData mmd, String memberName,
            ClassLoaderResolver clr) throws JSONException {
        final Object returnValue;
        if (mmd.getTypeConverterName() != null) {
            // User-defined converter
            TypeConverter conv = ec.getNucleusContext().getTypeManager()
                    .getTypeConverterForName(mmd.getTypeConverterName());
            Class datastoreType = TypeConverterHelper
                    .getDatastoreTypeForTypeConverter(conv, mmd.getType());
            if (datastoreType == String.class) {
                returnValue = conv.toMemberType(result.getString(memberName));
            } else if (datastoreType == Boolean.class) {
                returnValue = conv.toMemberType(result.getBoolean(memberName));
            } else if (datastoreType == Double.class) {
                returnValue = conv.toMemberType(result.getDouble(memberName));
            } else if (datastoreType == Float.class) {
                returnValue = conv.toMemberType(result.getDouble(memberName));
            } else if (datastoreType == Integer.class) {
                returnValue = conv.toMemberType(result.getInt(memberName));
            } else if (datastoreType == Long.class) {
                returnValue = conv.toMemberType(result.getLong(memberName));
            } else {
                returnValue = null;
            }
            if (op != null) {
                op.wrapSCOField(mmd.getAbsoluteFieldNumber(), returnValue,
                        false, false, true);
            }
        } else if (Boolean.class.isAssignableFrom(mmd.getType())) {
            return result.getBoolean(memberName);
        } else if (Integer.class.isAssignableFrom(mmd.getType())) {
            return result.getInt(memberName);
        } else if (Long.class.isAssignableFrom(mmd.getType())) {
            return result.getLong(memberName);
        } else if (Double.class.isAssignableFrom(mmd.getType())) {
            return result.getDouble(memberName);
        } else if (Enum.class.isAssignableFrom(mmd.getType())) {
            ColumnMetaData[] colmds = mmd.getColumnMetaData();
            // boolean useNumeric = MetaDataUtils
            // .persistColumnAsNumeric(colmds != null && colmds.length>0?
            // colmds[0] : null);
            boolean useNumeric = true;
            if (useNumeric) {
                return mmd.getType().getEnumConstants()[result
                        .getInt(memberName)];
            } else {
                return Enum.valueOf(mmd.getType(),
                        (String) result.get(memberName));
            }
        } else if (BigDecimal.class.isAssignableFrom(mmd.getType())
                || BigInteger.class.isAssignableFrom(mmd.getType())) {
            return TypeConversionHelper.convertTo(result.get(memberName),
                    mmd.getType());
        } else if (Collection.class.isAssignableFrom(mmd.getType())) {
            // Collection<Non-PC>
            Collection<Object> coll;
            try {
                Class instanceType = SCOUtils.getContainerInstanceType(
                        mmd.getType(), mmd.getOrderMetaData() != null);
                coll = (Collection<Object>) instanceType.newInstance();
            } catch (Exception e) {
                throw new NucleusDataStoreException(e.getMessage(), e);
            }

            JSONArray array = result.getJSONArray(memberName);
            Class elementCls = null;
            if (mmd.getCollection() != null
                    && mmd.getCollection().getElementType() != null) {
                elementCls = clr.classForName(mmd.getCollection()
                        .getElementType());
            }
            for (int i = 0; i < array.length(); i++) {
                if (array.isNull(i)) {
                    coll.add(null);
                } else {
                    Object value = array.get(i);
                    if (value instanceof JSONObject) {
                        Class cls = clr.classForName(
                                ((JSONObject) value).getString("class"), true);
                        coll.add(getNonpersistableObjectFromJSON(
                                (JSONObject) value, cls, clr));
                    } else {
                        if (elementCls != null) {
                            coll.add(TypeConversionHelper.convertTo(value,
                                    elementCls));
                        } else {
                            coll.add(value);
                        }
                    }
                }
            }

            if (op != null) {
                op.wrapSCOField(mmd.getAbsoluteFieldNumber(), coll, false,
                        false, true);
            }
            return coll;
        } else if (Map.class.isAssignableFrom(mmd.getType())) {
            // Map<Non-PC, Non-PC>
            Map map;
            try {
                Class instanceType = SCOUtils.getContainerInstanceType(
                        mmd.getType(), false);
                map = (Map) instanceType.newInstance();
            } catch (Exception e) {
                throw new NucleusDataStoreException(e.getMessage(), e);
            }

            JSONObject mapValue = result.getJSONObject(memberName);
            Iterator keyIter = mapValue.keys();
            Class keyCls = null;
            if (mmd.getMap() != null && mmd.getMap().getKeyType() != null) {
                keyCls = clr.classForName(mmd.getMap().getKeyType());
            }
            Class valCls = null;
            if (mmd.getMap() != null && mmd.getMap().getValueType() != null) {
                valCls = clr.classForName(mmd.getMap().getValueType());
            }

            while (keyIter.hasNext()) {
                Object jsonKey = keyIter.next();

                Object key = jsonKey;
                if (keyCls != null) {
                    key = TypeConversionHelper.convertTo(jsonKey, keyCls);
                }

                Object jsonVal = mapValue.get((String) key);
                Object val = jsonVal;
                if (jsonVal instanceof JSONObject) {
                    Class cls = clr.classForName(
                            ((JSONObject) jsonVal).getString("class"), true);
                    val = getNonpersistableObjectFromJSON((JSONObject) jsonVal,
                            cls, clr);
                } else {
                    if (valCls != null) {
                        val = TypeConversionHelper.convertTo(jsonVal, valCls);
                    }
                }
                map.put(key, val);
            }

            if (op != null) {
                op.wrapSCOField(mmd.getAbsoluteFieldNumber(), map, false,
                        false, true);
            }
            return map;
        } else if (mmd.getType().isArray()) {
            // Non-PC[]
            JSONArray arrayJson = result.getJSONArray(memberName);
            Object array = Array.newInstance(mmd.getType().getComponentType(),
                    arrayJson.length());
            for (int i = 0; i < arrayJson.length(); i++) {
                if (arrayJson.isNull(i)) {
                    Array.set(array, i, null);
                } else {
                    Object value = arrayJson.get(i);
                    if (value instanceof JSONObject) {
                        JSONObject valueJson = (JSONObject) value;
                        Class valueCls = clr.classForName(valueJson
                                .getString("class"));
                        System.out.println("TYPE="
                                + valueJson.getString("class"));
                        Array.set(
                                array,
                                i,
                                getNonpersistableObjectFromJSON(
                                        (JSONObject) value, valueCls, clr));
                    } else {
                        Array.set(array, i, TypeConversionHelper.convertTo(
                                value, mmd.getType().getComponentType()));
                    }
                }
            }
            return array;
        } else {
            System.out.println("FALLBACK");
            // Fallback to built-in type converters
            boolean useLong = false;
            ColumnMetaData[] colmds = mmd.getColumnMetaData();
            if (colmds != null && colmds.length == 1) {
                String jdbc = colmds[0].getJdbcType().name();
                if (jdbc != null
                        && (jdbc.equalsIgnoreCase("INTEGER") || jdbc
                                .equalsIgnoreCase("NUMERIC"))) {
                    useLong = true;
                }
            }
            TypeConverter strConv = ec.getNucleusContext().getTypeManager()
                    .getTypeConverterForType(mmd.getType(), String.class);
            TypeConverter longConv = ec.getNucleusContext().getTypeManager()
                    .getTypeConverterForType(mmd.getType(), Long.class);

            if (useLong && longConv != null) {
                returnValue = longConv.toMemberType(result.getLong(memberName));
            } else if (!useLong && strConv != null) {
                returnValue = strConv.toMemberType((String) result
                        .get(memberName));
            } else if (!useLong && longConv != null) {
                returnValue = longConv.toMemberType(result.getLong(memberName));
            } else {
                Object value = result.get(memberName);
                if (value instanceof JSONObject) {
                    Class cls = clr.classForName(
                            ((JSONObject) value).getString("class"), true);
                    returnValue = getNonpersistableObjectFromJSON(
                            (JSONObject) value, cls, clr);
                } else {
                    returnValue = TypeConversionHelper.convertTo(
                            result.get(memberName), mmd.getType());
                }
            }

            if (op != null) {
                op.wrapSCOField(mmd.getAbsoluteFieldNumber(), returnValue,
                        false, false, true);
            }
        }
        return returnValue;
    }

    protected Object fetchObjectFieldInternal(AbstractMemberMetaData mmd,
            String memberName, ClassLoaderResolver clr) throws JSONException {

        RelationType relationType = mmd.getRelationType(clr);
        if (relationType == RelationType.NONE) {
            return fetchObjectFieldInternal_RelationTypeNone(mmd, memberName,
                    clr);
        } else if (RelationType.isRelationSingleValued(relationType)) {
            // Persistable object - retrieve the string form of the identity,
            // and find the object
            String idStr = (String) result.get(memberName);
            if (idStr == null) {
                return null;
            }
            
            return getNestedObjectById(idStr,
                    mmd.getAbstractClassMetaData(), ec);
            
        } else if (RelationType.isRelationMultiValued(relationType)) {
            if (mmd.hasCollection()) {
                // Collection<PC>
                JSONArray array = (JSONArray) result.get(memberName);
                Collection<Object> coll;
                try {
                    Class instanceType = SCOUtils.getContainerInstanceType(
                            mmd.getType(), mmd.getOrderMetaData() != null);
                    coll = (Collection<Object>) instanceType.newInstance();
                } catch (Exception e) {
                    throw new NucleusDataStoreException(e.getMessage(), e);
                }

                AbstractClassMetaData elementCmd = mmd.getCollection()
                        .getElementClassMetaData(ec.getClassLoaderResolver(),
                                ec.getMetaDataManager());
                for (int i = 0; i < array.length(); i++) {
                    String idStr = (String) array.get(i);
                    Object element = getNestedObjectById(idStr, elementCmd, ec);
                    coll.add(element);
                }

                if (op != null) {
                    return op.wrapSCOField(mmd.getAbsoluteFieldNumber(), coll,
                            false, false, true);
                }
                return coll;
            } else if (mmd.hasArray()) {
                // PC[]
                JSONArray array = (JSONArray) result.get(memberName);
                Object arrayField = Array.newInstance(mmd.getType()
                        .getComponentType(), array.length());

                AbstractClassMetaData elementCmd = mmd.getCollection()
                        .getElementClassMetaData(ec.getClassLoaderResolver(),
                                ec.getMetaDataManager());
                for (int i = 0; i < array.length(); i++) {
                    String idStr = (String) array.get(i);
                    Object element = getNestedObjectById(idStr, elementCmd, ec);
                    Array.set(arrayField, i, element);
                }

                if (op != null) {
                    return op.wrapSCOField(mmd.getAbsoluteFieldNumber(),
                            arrayField, false, false, true);
                }
                return arrayField;
            } else if (mmd.hasMap()) {
                // Map<Non-PC, PC>, Map<PC, PC>, Map<PC, Non-PC>
                JSONObject mapVal = (JSONObject) result.get(memberName);
                Map map;
                try {
                    Class instanceType = SCOUtils.getContainerInstanceType(
                            mmd.getType(), false);
                    map = (Map) instanceType.newInstance();
                } catch (Exception e) {
                    throw new NucleusDataStoreException(e.getMessage(), e);
                }

                AbstractClassMetaData keyCmd = mmd.getMap()
                        .getKeyClassMetaData(clr, ec.getMetaDataManager());
                AbstractClassMetaData valCmd = mmd.getMap()
                        .getValueClassMetaData(clr, ec.getMetaDataManager());

                Iterator keyIter = mapVal.keys();
                while (keyIter.hasNext()) {
                    Object jsonKey = keyIter.next();
                    Object key = null;
                    if (keyCmd != null) {
                        // The jsonKey is the string form of the identity
                        String idStr = (String) jsonKey;
                        key = getNestedObjectById(idStr, keyCmd, ec);
                    } else {
                        Class keyCls = ec.getClassLoaderResolver()
                                .classForName(mmd.getMap().getKeyType());
                        key = TypeConversionHelper.convertTo(jsonKey, keyCls);
                    }

                    Object jsonVal = mapVal.get((String) key);
                    Object val = null;
                    if (valCmd != null) {
                        // The jsonVal is the string form of the identity
                        String idStr = (String) jsonVal;
                        val = getNestedObjectById(idStr, valCmd, ec);
                    } else {
                        Class valCls = ec.getClassLoaderResolver()
                                .classForName(mmd.getMap().getValueType());
                        val = TypeConversionHelper.convertTo(jsonVal, valCls);
                    }

                    map.put(key, val);
                }

                if (op != null) {
                    return op.wrapSCOField(mmd.getAbsoluteFieldNumber(), map,
                            false, false, true);
                }
                return map;
            }
        }

        throw new NucleusException("Dont currently support field "
                + mmd.getFullFieldName() + " of type " + mmd.getTypeName());
    }

    private Object getNestedObjectById(String persistableId, AbstractClassMetaData acmd, ExecutionContext ec) {
        if (fetchCache.exists(persistableId)) {
            return fetchCache.lookUp(persistableId);
        }
        return IdentityUtils.getObjectFromPersistableIdentity(persistableId, acmd, ec);
    }
    
    
    /**
     * Deserialise from JSON to a non-persistable object.
     * 
     * @param jsonobj
     *            JSONObject
     * @param cls
     *            The class of the object required
     * @param clr
     *            ClassLoader resolver
     * @return The object
     */
    private Object getNonpersistableObjectFromJSON(final JSONObject jsonobj,
            final Class cls, final ClassLoaderResolver clr) {
        if (cls.getName().equals("com.google.appengine.api.users.User")) {
            return getComGoogleAppengineApiUsersUserFromJSON(jsonobj, cls, clr);
        } else if (cls.getName().equals(
                "com.google.appengine.api.datastore.Key")) {
            return getComGoogleAppengineApiDatastoreKeyFromJSON(jsonobj, cls,
                    clr);
        } else {
            // Try to reconstruct the object as a Java bean
            try {
                return AccessController.doPrivileged(new PrivilegedAction() {
                    public Object run() {
                        try {
                            Constructor c = ClassUtils
                                    .getConstructorWithArguments(cls,
                                            new Class[] {});
                            c.setAccessible(true);
                            Object obj = c.newInstance(new Object[] {});
                            String[] fieldNames = JSONObject.getNames(jsonobj);
                            for (int i = 0; i < jsonobj.length(); i++) {
                                // ignore class field
                                if (!fieldNames[i].equals("class")) {
                                    Field field = cls.getField(fieldNames[i]);
                                    field.setAccessible(true);
                                    field.set(obj, jsonobj.get(fieldNames[i]));
                                }
                            }
                            return obj;
                        } catch (Exception e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                        return null;
                    }
                });
            } catch (SecurityException ex) {
                ex.printStackTrace();
            }

        }
        return null;
    }

    /**
     * Convenience method to create an AppEngine User from a JSON object. TODO
     * Move this out somewhere else
     * 
     * @param jsonobj
     *            The JSONObject
     * @param cls
     *            Class being represented (User)
     * @param clr
     *            ClassLoader resolver
     * @return The Key
     */
    protected Object getComGoogleAppengineApiUsersUserFromJSON(
            JSONObject jsonobj, Class cls, ClassLoaderResolver clr) {
        String email = null;
        String authDomain = null;
        try {
            email = jsonobj.getString("email");
        } catch (JSONException e) {
            // should not happen if the field exists
        }
        try {
            authDomain = jsonobj.getString("authDomain");
        } catch (JSONException e) {
            // should not happen if the field exists
        }
        return ClassUtils.newInstance(cls, new Class[] { String.class,
                String.class }, new String[] { email, authDomain });
    }

    /**
     * Convenience method to create an AppEngine Key from a JSON object. TODO
     * Move this out somewhere else
     * 
     * @param jsonobj
     *            The JSONObject
     * @param cls
     *            Class being represented (Key)
     * @param clr
     *            ClassLoader resolver
     * @return The Key
     */
    protected Object getComGoogleAppengineApiDatastoreKeyFromJSON(
            JSONObject jsonobj, Class cls, ClassLoaderResolver clr) {
        try {
            Object parent = null;
            if (jsonobj.has("parent") && !jsonobj.isNull("parent")) {
                // if it's a JSONObject
                JSONObject parentobj = jsonobj.getJSONObject("parent");
                parent = getNonpersistableObjectFromJSON(parentobj,
                        clr.classForName(jsonobj.getString("class")), clr);
            }

            if (jsonobj.has("appId")) {
                String appId = jsonobj.getString("appId");
                String kind = jsonobj.getString("kind");
                Class keyFactory = clr.classForName(
                        "com.google.appengine.api.datastore.KeyFactory",
                        cls.getClassLoader(), false);
                if (parent != null) {
                    return ClassUtils.getMethodForClass(keyFactory,
                            "createKey",
                            new Class[] { cls, String.class, String.class })
                            .invoke(null, new Object[] { parent, kind, appId });
                } else {
                    return ClassUtils.getMethodForClass(keyFactory,
                            "createKey",
                            new Class[] { String.class, String.class }).invoke(
                            null, new Object[] { kind, appId });
                }
            } else {
                long id = jsonobj.getLong("id");
                String kind = jsonobj.getString("kind");
                Class keyFactory = clr.classForName(
                        "com.google.appengine.api.datastore.KeyFactory",
                        cls.getClassLoader(), false);
                if (parent != null) {
                    return ClassUtils.getMethodForClass(keyFactory,
                            "createKey",
                            new Class[] { cls, String.class, long.class })
                            .invoke(null,
                                    new Object[] { parent, kind,
                                            Long.valueOf(id) });
                } else {
                    return ClassUtils.getMethodForClass(keyFactory,
                            "createKey",
                            new Class[] { String.class, long.class }).invoke(
                            null, new Object[] { kind, Long.valueOf(id) });
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
