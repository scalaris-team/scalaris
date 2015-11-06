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
package org.datanucleus.store.scalaris.fieldmanager;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.JdbcType;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.fieldmanager.AbstractFieldManager;
import org.datanucleus.store.schema.naming.ColumnType;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.store.types.converters.TypeConverterHelper;
import org.datanucleus.util.TypeConversionHelper;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * FieldManager for fetching from JSON.
 */
public class FetchFieldManager extends AbstractFieldManager {
    protected final ObjectProvider<?> op;
    protected final AbstractClassMetaData acmd;
    protected final ExecutionContext ec;
    protected final JSONObject result;
    protected StoreManager storeMgr;

    public FetchFieldManager(ExecutionContext ec, AbstractClassMetaData acmd,
            JSONObject result) {
        this.acmd = acmd;
        this.ec = ec;
        this.result = result;
        this.op = null;
        this.storeMgr = ec.getStoreManager();
    }

    public FetchFieldManager(ObjectProvider<?> op, JSONObject result) {
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
        AbstractMemberMetaData mmd = acmd
                .getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        String memberName = storeMgr.getNamingFactory().getColumnName(mmd,
                ColumnType.COLUMN);

        if (result.isNull(memberName)) {
            return null;
        }

        // Special cases
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        RelationType relationType = mmd.getRelationType(clr);
        if (RelationType.isRelationSingleValued(relationType)
                && mmd.isEmbedded()) {
            throw new NucleusException(
                    "Don't currently support embedded fields");
        }

        try {
            return fetchObjectFieldInternal(mmd, memberName, clr);
        } catch (JSONException e) {
            throw new NucleusException(e.getMessage(), e);
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected Object fetchObjectFieldInternal_RelationTypeNone(
            AbstractMemberMetaData mmd, String memberName,
            ClassLoaderResolver clr) throws JSONException {
        final Object returnValue;
        if (mmd.getTypeConverterName() != null) {
            // User-defined converter
            TypeConverter<Object,Object> conv = ec.getNucleusContext().getTypeManager()
                    .getTypeConverterForName(mmd.getTypeConverterName());
            Class<?> datastoreType = TypeConverterHelper
                    .getDatastoreTypeForTypeConverter(conv, mmd.getType());
            if (datastoreType == String.class) {
                returnValue = (Object)conv.toMemberType(result.getString(memberName));
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
                return SCOUtils.wrapSCOField(op, mmd.getAbsoluteFieldNumber(), returnValue, true);
            }
        } else if (Boolean.class.isAssignableFrom(mmd.getType())) {
            return result.getBoolean(memberName);
        } else if (Integer.class.isAssignableFrom(mmd.getType())) {
            return result.getInt(memberName);
        } else if (Long.class.isAssignableFrom(mmd.getType())) {
            return result.getLong(memberName);
        } else if (Double.class.isAssignableFrom(mmd.getType())) {
            return result.getDouble(memberName);
        } else if (Date.class.isAssignableFrom(mmd.getType())) {
            Long dateValue = result.getLong(memberName);
            return new Date(dateValue);
        } else if (Enum.class.isAssignableFrom(mmd.getType())) {
            if (mmd.getType().getEnumConstants() != null) {
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
                Class<?> instanceType = SCOUtils.getContainerInstanceType(
                        mmd.getType(), mmd.getOrderMetaData() != null);
                coll = (Collection<Object>) instanceType.newInstance();
            } catch (Exception e) {
                throw new NucleusDataStoreException(e.getMessage(), e);
            }

            JSONArray array = result.getJSONArray(memberName);
            Class<?> elementCls = null;
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
                    if (elementCls != null) {
                        coll.add(TypeConversionHelper.convertTo(value,
                                elementCls));
                    } else {
                        coll.add(value);
                    }
                }
            }

            if (op != null) {
                return SCOUtils.wrapSCOField(op, mmd.getAbsoluteFieldNumber(), coll, true);
            }
            return coll;
        } else if (Map.class.isAssignableFrom(mmd.getType())) {
            // Map<Non-PC, Non-PC>
            Map map;
            try {
                Class<?> instanceType = SCOUtils.getContainerInstanceType(
                        mmd.getType(), false);
                map = (Map) instanceType.newInstance();
            } catch (Exception e) {
                throw new NucleusDataStoreException(e.getMessage(), e);
            }

            JSONObject mapValue = result.getJSONObject(memberName);
            Iterator<?> keyIter = mapValue.keys();
            Class<?> keyCls = null;
            if (mmd.getMap() != null && mmd.getMap().getKeyType() != null) {
                keyCls = clr.classForName(mmd.getMap().getKeyType());
            }
            Class<?> valCls = null;
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
                if (valCls != null) {
                    val = TypeConversionHelper.convertTo(jsonVal, valCls);
                }
                map.put(key, val);
            }

            if (op != null) {
                return SCOUtils.wrapSCOField(op, mmd.getAbsoluteFieldNumber(), map, true);
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
                    Array.set(array, i, TypeConversionHelper.convertTo(
                            value, mmd.getType().getComponentType()));
                }
            }
            return array;
        } else {
            // Fallback to built-in type converters
            boolean useLong = false;
            ColumnMetaData[] colmds = mmd.getColumnMetaData();
            if (colmds != null && colmds.length == 1) {
                JdbcType jdbcType = colmds[0].getJdbcType();
                if (jdbcType != null) {
                    String jdbc = jdbcType.name();
                    if (jdbc != null
                            && (jdbc.equalsIgnoreCase("INTEGER") || jdbc
                                    .equalsIgnoreCase("NUMERIC"))) {
                        useLong = true;
                    }
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
                returnValue = TypeConversionHelper.convertTo(
                        result.get(memberName), mmd.getType());
            }

            if (op != null) {
                return SCOUtils.wrapSCOField(op, mmd.getAbsoluteFieldNumber(), returnValue, true);
            }
        }
        return returnValue;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
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
            AbstractClassMetaData acmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
            return getNestedObjectById(idStr,acmd, ec);
            
        } else if (RelationType.isRelationMultiValued(relationType)) {
            if (mmd.hasCollection()) {
                // Collection<PC>
                JSONArray array = (JSONArray) result.get(memberName);
                Collection<Object> coll;
                try {
                    Class<?> instanceType = SCOUtils.getContainerInstanceType(
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
                    if (idStr == null) {
                        coll.add(idStr);
                    } else {
                        Object element = getNestedObjectById(idStr, elementCmd, ec);
                        if (element != null) {
                            coll.add(element);
                        }
                    }
                }

                if (op != null) {
                    return SCOUtils.wrapSCOField(op, mmd.getAbsoluteFieldNumber(), coll, true);
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
                    if (idStr == null) {
                        Array.set(arrayField, i, idStr);
                    } else {
                        Object element = getNestedObjectById(idStr, elementCmd, ec);
                        Array.set(arrayField, i, element);
                    }
                }

                if (op != null) {
                    return SCOUtils.wrapSCOField(op, mmd.getAbsoluteFieldNumber(), arrayField, true);
                }
                return arrayField;
            } else if (mmd.hasMap()) {
                // Map<Non-PC, PC>, Map<PC, PC>, Map<PC, Non-PC>
                JSONObject mapVal = (JSONObject) result.get(memberName);
                Map map;
                try {
                    Class<?> instanceType = SCOUtils.getContainerInstanceType(
                            mmd.getType(), false);
                    map = (Map) instanceType.newInstance();
                } catch (Exception e) {
                    throw new NucleusDataStoreException(e.getMessage(), e);
                }

                AbstractClassMetaData keyCmd = mmd.getMap()
                        .getKeyClassMetaData(clr, ec.getMetaDataManager());
                AbstractClassMetaData valCmd = mmd.getMap()
                        .getValueClassMetaData(clr, ec.getMetaDataManager());

                Iterator<?> keyIter = mapVal.keys();
                while (keyIter.hasNext()) {
                    Object jsonKey = keyIter.next();
                    Object key = null;
                    if (keyCmd != null) {
                        // The jsonKey is the string form of the identity
                        String idStr = (String) jsonKey;
                        key = getNestedObjectById(idStr, keyCmd, ec);
                    } else {
                        Class<?> keyCls = ec.getClassLoaderResolver()
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
                    return SCOUtils.wrapSCOField(op, mmd.getAbsoluteFieldNumber(), map, true);
                }
                return map;
            }
        }

        throw new NucleusException("Dont currently support field "
                + mmd.getFullFieldName() + " of type " + mmd.getTypeName());
    }

    private Object getNestedObjectById(String persistableId, AbstractClassMetaData acmd, ExecutionContext ec) {
        try {
            return IdentityUtils.getObjectFromPersistableIdentity(persistableId, acmd, ec);
        } catch (NucleusObjectNotFoundException e) {
            // TODO This can happen when an object currently in use in the external
            // application has references to other objects, which in its lifetime are deleted but the
            // reference stored is not updated yet
            // This happening should be prevented.
            return null;
        }
    }
}
