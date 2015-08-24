package org.datanucleus.store.scalaris;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ForeignKeyAction;
import org.datanucleus.metadata.ForeignKeyMetaData;
import org.datanucleus.metadata.UniqueMetaData;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.transaction.NucleusTransactionException;

import com.ericsson.otp.erlang.OtpErlangLong;
import com.orange.org.json.JSONArray;
import com.orange.org.json.JSONException;
import com.orange.org.json.JSONObject;

import de.zib.scalaris.AbortException;
import de.zib.scalaris.Connection;
import de.zib.scalaris.ConnectionException;
import de.zib.scalaris.ErlangValue;
import de.zib.scalaris.NotAListException;
import de.zib.scalaris.NotANumberException;
import de.zib.scalaris.NotFoundException;
import de.zib.scalaris.Transaction;
import de.zib.scalaris.TransactionSingleOp;
import de.zib.scalaris.UnknownException;

/**
 * This class contains convenience methods to provide functionalities that are not natively supported by 
 * Scalaris. For example generating identities, managing all primary keys of a class to "iterate" over all stored 
 * instances, or ensuring uniqueness. 
 */
public class ScalarisUtils {

    /**
     * Object which will be used for synchronization when performing
     * write operations.
     */
    public static final Object WRITE_LOCK = new Object();

    /**
     * Value which will be used to signal a deleted key.
     */
    public static final String DELETED_RECORD_VALUE = new JSONObject().toString();

    /* **********************************************************************
     *                  ID GENERATION
     * **********************************************************************/
    
    /**
     * Generate a new ID which can be used to store a value at an unique key.
     * Every time this function is called the value stored at key ID_GEN_KEY is
     * incremented by one. The value stored there is the value which is returned by
     * this function. All object classes share the same ID generator key.
     * 
     * @param op
     *            ObjectProvider of the object this ID is generated for.
     * @return A new ID.
     */
    private synchronized static long generateNextIdentity(ObjectProvider<?> op) {
        StoreManager storeMgr = op.getExecutionContext().getStoreManager();
        
        ExecutionContext ec = op.getExecutionContext();
        String keyName = ScalarisSchemaHandler.getIDGeneratorKeyName(op.getClassMetaData().getFullClassName());
        ManagedConnection mConn = storeMgr.getConnection(ec);
        de.zib.scalaris.Connection conn = (de.zib.scalaris.Connection) mConn
                .getConnection();

        long newID = 0l;
        Transaction t = new Transaction(conn);
        try {
            try {
                ErlangValue storedVal = t.read(keyName);
                newID = storedVal.longValue() + 1l;
                t.addOnNr(keyName, new OtpErlangLong(1l));
            } catch (NotFoundException e) {
                // No ID was generated yet
                newID = 1l;
                t.write(keyName, newID);
            }

            t.commit();
        } catch (ConnectionException e) {
            throw new NucleusTransactionException(
                    "Could not generate a new ID because of transaction failure",
                    e);
        } catch (AbortException e) {
            throw new NucleusTransactionException(
                    "Could not generate a new ID becasue of transaction failure",
                    e);
        } catch (ClassCastException e) {
            // This happens if the key does not exist
            // which means no ID was generated yet.
            throw new NucleusTransactionException(
                    "The value of the ID generator key was altered to an invalid value",
                    e);
        } catch (NotANumberException e) {
            // this should never ever happen since the ClassCastException
            // is thrown before we can try to increment the number
            throw new NucleusTransactionException(
                    "The value of the ID generator key was altered to an invalid value",
                    e);
        } finally {
            mConn.release();
        }

        return newID;
    }

    /**
     * Returns the object identity as string which can be used of the key-value
     * store as key to uniquely identify the object. ATTENTION: If the data
     * store is (partially) responsible to generate an ID (e.g. because of
     * IdGeneratorStrategy.IDENTITY). This method may change primary key
     * attribute values. This method should be used only to insert a new object
     * in the data store, otherwise consider using getPersitableIdentity.
     * 
     * @param op
     *            data source.
     * @return Identity of object provided by op or null if at least one primary
     *         key field is not loaded.
     */
    static String generatePersistableIdentity(ObjectProvider<?> op) {
        StoreManager storeMgr = op.getExecutionContext().getStoreManager();
        AbstractClassMetaData cmd = op.getClassMetaData();

        if (cmd.pkIsDatastoreAttributed(storeMgr)) {
            // The primary key must be (partially) calculated by the data store.
            // There is no distinction between APPLICATION and DATASTORE
            // IdentityType (yet)

            int[] pkFieldNumbers = cmd.getPKMemberPositions();
            long idKey = 0;
            for (int i = 0; i < pkFieldNumbers.length; i++) {
                AbstractMemberMetaData mmd = cmd
                        .getMetaDataForManagedMemberAtAbsolutePosition(pkFieldNumbers[i]);

                if (storeMgr.isStrategyDatastoreAttributed(cmd,
                        pkFieldNumbers[i])) {
                    Class<?> mType = mmd.getType();
                    if (!(mType.equals(Long.class) || mType.equals(long.class)
                            || mType.equals(Integer.class) || mType
                                .equals(int.class))) {
                        // Field type must be long/Long or int/Integer since
                        // this is the only IDENTITY value that is currently
                        // supported
                        throw new NucleusUserException(
                                "Any field using IDENTITY value generation with Scalaris should be of type long/Long or int/Integer");
                    }
                    idKey = generateNextIdentity(op);
                    if (mType.equals(Integer.class) || mType.equals(int.class)) {
                        if (idKey > Integer.MAX_VALUE) {
                            throw new NucleusException("We ran out of integer IDs!");
                        }
                        op.replaceField(mmd.getAbsoluteFieldNumber(), (int) idKey);
                    } else {
                        op.replaceField(mmd.getAbsoluteFieldNumber(), idKey);
                    }
                    System.out.println("GENERATED KEY: " + idKey);
                }
            }
        }
        return getPersistableIdentity(op);
    }

    /**
     * Returns the object identity as string which can be used of the key-value
     * store as key to uniquely identify the object.
     * 
     * @param op
     * @return Identity of object provided by op or null if at least one primary
     *         key field is not loaded.
     */
    public static String getPersistableIdentity(ObjectProvider<?> op) {
        AbstractClassMetaData cmd = op.getClassMetaData();
        String keySeparator = ":";

        if (op.getExternalObjectId() == null) {
            // The primary key must be (partially) calculated by the data store.
            // There is no distinction between APPLICATION and DATASTORE
            // IdentityType (yet)
            // ID structure is: <pk1>:<pk2>

            StringBuilder keyBuilder = new StringBuilder();

            int[] pkFieldNumbers = cmd.getPKMemberPositions();
            Object firstKey = null;
            for (int i = 0; i < pkFieldNumbers.length; i++) {
                AbstractMemberMetaData mmd = cmd
                        .getMetaDataForManagedMemberAtAbsolutePosition(pkFieldNumbers[i]);

                Object keyVal = op.provideField(mmd.getAbsoluteFieldNumber());
                if (i == 0) {
                    firstKey = keyVal;
                } else {
                    keyBuilder.append(keySeparator);    
                }
                keyBuilder.append(keyVal);
            }
            String identity = keyBuilder.toString();
            if (op.getExternalObjectId() == null) {
                // DataNucleus expects as internal object id an integer value if there is only one
                // primary key member which is an integer. Otherwise it can be an arbitrary
                // object.
                if (pkFieldNumbers.length == 1) {
                    op.setPostStoreNewObjectId(firstKey);
                } else {
                    op.setPostStoreNewObjectId(identity);
                }
            }
        }

        String id = IdentityUtils.getPersistableIdentityForId(op.getExternalObjectId());
        System.out.println(id);
        return id;
    }

    /* **********************************************************************
     *                  HOOKS FOR ScalarisStoreManager
     * **********************************************************************/
    static JSONObject performScalarisObjectFetch(ObjectProvider<?> op, Connection conn)
            throws ConnectionException, NotFoundException, UnknownException, JSONException {

        final String objId = ScalarisUtils.getPersistableIdentity(op);
        final String objClassName = op.getClassMetaData().getFullClassName();
        final String objKey = ScalarisSchemaHandler.getObjectStorageKey(objClassName, objId);

        TransactionSingleOp t = new TransactionSingleOp(conn);
        return new JSONObject(t.read(objKey).stringValue());
    }

    static void performScalarisObjectInsert(ObjectProvider<?> op, String objectId, JSONObject json, Connection conn)
            throws ConnectionException, ClassCastException, UnknownException, NotAListException, AbortException {
        synchronized(WRITE_LOCK) {
            Transaction t = new Transaction(conn);

            insertObjectToIDIndex(op, t);
            updateUniqueMemberKey(op, json, null, t);
            insertToForeignKeyAction(op, json, t);

            String className = op.getClassMetaData().getFullClassName();
            String storageKey = ScalarisSchemaHandler.getObjectStorageKey(className, objectId);
            t.write(storageKey, json.toString());
            t.commit();
        }
    }

    static void performScalarisObjectUpdate(ObjectProvider<?> op, String objectId, JSONObject changedVals, Connection conn)
            throws ConnectionException, ClassCastException, UnknownException, NotAListException, 
            NotFoundException, JSONException, AbortException {

        synchronized(WRITE_LOCK) {
            // get old value
            String className = op.getClassMetaData().getFullClassName();
            String objectKey = ScalarisSchemaHandler.getObjectStorageKey(className, objectId);

            Transaction t = new Transaction(conn);
            JSONObject stored = new JSONObject(t.read(objectKey).stringValue());

            // update stored object values
            JSONObject changedValsOld = new JSONObject();
            Iterator<?> keyIter = changedVals.keys();
            while (keyIter.hasNext()) {
                String key = (String) keyIter.next();
                if (stored.has(key)) {
                    changedValsOld.put(key, stored.get(key));
                }
                stored.put(key, changedVals.get(key));
            }

            updateUniqueMemberKey(op, changedVals, changedValsOld, t);
            updateForeignKeyAction(op, changedVals, changedValsOld, t);

            t.write(objectKey, stored.toString());
            t.commit();
        }
    }

    static void performScalarisObjectDelete(ObjectProvider<?> op, String objectId, Connection conn)
            throws ConnectionException, ClassCastException, UnknownException, NotAListException,
            NotFoundException, JSONException, AbortException {

        synchronized(WRITE_LOCK) {
            String className = op.getClassMetaData().getFullClassName();
            String objectKey = ScalarisSchemaHandler.getObjectStorageKey(className, objectId);

            Transaction t = new Transaction(conn);
            JSONObject oldJson = new JSONObject(t.read(objectKey).stringValue());

            removeObjectFromIDIndex(op, t);
            removeObjectFromUniqueMemberKey(op, oldJson, t);
            performForeignKeyActionDelete(op, t);

            t.write(objectKey,  DELETED_RECORD_VALUE);
            t.commit();
        }
    }

    /* **********************************************************************
     *               INDICIES OF ALL OBJECTS OF ONE TYPE (for queries)
     * **********************************************************************/
    
    /**
     * To support queries it is necessary to have the possibility to iterate
     * over all stored objects of a specific type. Since Scalaris stores only
     * key-value pairs without structured tables, this is not "natively"
     * supported. Therefore an extra key is added to the store containing all
     * keys of available objects of a type. This key has the structure
     * <full-class-name><ALL_ID_PREFIX>. The value is an JSON-array containing
     * all keys of <full-class-name> instances.
     * 
     * This methods adds another entry to such a key based on the passed
     * ObjectProvider. If no such key-value pair exists, it is created.
     * 
     * @param op
     *            The data source
     * @throws JSONException 
     * @throws UnknownException 
     * @throws ClassCastException 
     * @throws ConnectionException 
     * @throws NotAListException 
     */
    private static void insertObjectToIDIndex(ObjectProvider<?> op, Transaction t)
            throws ConnectionException, ClassCastException, UnknownException, NotAListException {
        AbstractClassMetaData cmd = op.getClassMetaData();
        String key = ScalarisSchemaHandler.getIDIndexKeyName(cmd.getFullClassName());
        String objectStringIdentity = getPersistableIdentity(op);

        List<ErlangValue> toAdd = new ArrayList<ErlangValue>();
        toAdd.add(new ErlangValue(objectStringIdentity));
        t.addDelOnList(key, toAdd, new ArrayList<ErlangValue>());
    }
    /**
     * To support queries it is necessary to have the possibility to iterate
     * over all stored objects of a specific type. Since Scalaris stores only
     * key-value pairs without structured tables, this is not "natively"
     * supported. Therefore an extra key is added to the store containing all
     * keys of available objects of a type. This key has the structure
     * <full-class-name><ALL_KEY_PREFIX>. The value is an JSON-array containing
     * all keys of <full-class-name> instances.
     * 
     * This methods removes an entry of such a key based on the passed
     * ObjectProvider. If no such key-value pair exists, nothing happens.
     * 
     * @param op
     *            The data source
     * @throws JSONException 
     * @throws UnknownException 
     * @throws ClassCastException 
     * @throws ConnectionException 
     */
    private static void removeObjectFromIDIndex(ObjectProvider<?> op, Transaction t)
            throws ConnectionException, ClassCastException, UnknownException, NotAListException {
        
        AbstractClassMetaData cmd = op.getClassMetaData();
        String key = ScalarisSchemaHandler.getIDIndexKeyName(cmd.getFullClassName());
        String objectStringIdentity = getPersistableIdentity(op);

        List<ErlangValue> toRemove = new ArrayList<ErlangValue>();
        toRemove.add(new ErlangValue(objectStringIdentity));
        t.addDelOnList(key, new ArrayList<ErlangValue>(), toRemove);
    }
    
    /* **********************************************************************
     *                  ACTIONS TO GUARANTEE UNIQUENESS
     * **********************************************************************/
    
    private static void updateUniqueMemberKey(ObjectProvider<?> op, JSONObject newJson, JSONObject oldJson, Transaction t) 
            throws ConnectionException, ClassCastException, UnknownException {
        AbstractClassMetaData cmd = op.getClassMetaData();
        String objectStringIdentity = getPersistableIdentity(op);
        String className = cmd.getFullClassName();
        
        for (int field : cmd.getAllMemberPositions()) {
            AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(field);
            UniqueMetaData umd = mmd.getUniqueMetaData();
            if (umd != null) {
                // this member has @Unique annotation -> lookup all stored values for this member
                String fieldName = mmd.getName();
                String oldFieldValue = null, newFieldValue = null;
                try {
                    newFieldValue = (newJson != null && newJson.has(fieldName)) ? newJson.getString(fieldName) : null;
                    oldFieldValue = (oldJson != null && oldJson.has(fieldName)) ? oldJson.getString(fieldName) : null;
                } catch (JSONException e) {
                    // unique members can be null which means they are not found in the JSON
                }
                if (newFieldValue != null && newFieldValue.equals(oldFieldValue)) {
                    // this field has not changed -> skip update
                    continue;
                }
                
                if (oldFieldValue != null) {
                    // mark the old key as removed
                    String oldValueKey = ScalarisSchemaHandler.getUniqueMemberKey(className, fieldName, oldFieldValue);
                    t.write(oldValueKey, DELETED_RECORD_VALUE);
                }
                
                if (newFieldValue != null) {
                    // check if this value already exists
                    // if it does -> exception; if not -> store
                    String newValueKey = ScalarisSchemaHandler.getUniqueMemberKey(className, fieldName, newFieldValue);
                    String idStoringThisValue = null;
                    try {
                        idStoringThisValue = t.read(newValueKey).stringValue();
                    } catch (NotFoundException e) {} // this value does not exist yet, therefore there is no conflict
                    
                    if (idStoringThisValue != null && !isDeletedRecord(idStoringThisValue) 
                            && !idStoringThisValue.equals(objectStringIdentity)) {
                        // another object has stored this value -> violation of uniqueness
                        throw new NucleusDataStoreException("The value '" + newFieldValue + "' of unique member '" + 
                                fieldName + "' of class '" + className + "' already exists");
                    } else {
                        t.write(newValueKey, objectStringIdentity);
                    }
                }
            }
        }
    }
    
    private static void removeObjectFromUniqueMemberKey(ObjectProvider<?> op, JSONObject oldJson, Transaction t) 
            throws ConnectionException, ClassCastException, UnknownException {
        AbstractClassMetaData cmd = op.getClassMetaData();
        String className = cmd.getFullClassName();
        
        for (int field : cmd.getAllMemberPositions()) {
            AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(field);
            UniqueMetaData umd = mmd.getUniqueMetaData();
            if (umd != null) {
                // this member has @Unique annotation -> lookup all stored values for this member
                String fieldName = mmd.getName();

                String oldFieldValue = null;
                try {
                    oldFieldValue = oldJson.has(fieldName) ? oldJson.getString(fieldName) : null;
                } catch (JSONException e) {} // can not happen since it is checked before 
                
                if (oldFieldValue != null) {
                    String oldValueKey = ScalarisSchemaHandler.getUniqueMemberKey(className, fieldName, oldFieldValue);
                    t.write(oldValueKey, DELETED_RECORD_VALUE);
                }
            }
        }
    }

    /* **********************************************************************
     *                  FOREIGN KEY ACTIONS
     * **********************************************************************/

    private static void insertToForeignKeyAction(ObjectProvider<?> op, JSONObject objToInsert, Transaction t)
            throws ConnectionException, ClassCastException, UnknownException, NotAListException {
        updateForeignKeyAction(op, objToInsert, null, t);
    }

    @SuppressWarnings("unchecked")
    private static void updateForeignKeyAction(ObjectProvider<?> op, JSONObject changedFieldsNewVal, 
            JSONObject changedFieldsOldVal, Transaction t)
            throws ConnectionException, ClassCastException, UnknownException, NotAListException {
        AbstractClassMetaData cmd = op.getClassMetaData();
        String objectStringIdentity = getPersistableIdentity(op);

        // the map will store all elements which must be added/removed from which key
        HashMap<String, List<ErlangValue>> toAddToKey = new HashMap<String, List<ErlangValue>>();
        HashMap<String, List<ErlangValue>> toRemoveFromKey = new HashMap<String, List<ErlangValue>>();

        for (int field : cmd.getAllMemberPositions()) {
            AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(field);
            // do nothing if this field has not changed
            if (mmd == null || !changedFieldsNewVal.has(mmd.getName())) continue;

            ForeignKeyMetaData fmd = mmd.getForeignKeyMetaData();
            boolean isJoin = false;
            if (mmd.getJoinMetaData() != null) {
                // The member is a collection with an ForeignKeyAction attached
                fmd = mmd.getJoinMetaData().getForeignKeyMetaData();
                isJoin = true;
            }
            // add to actions keys if it is a cascading delete
            if (fmd != null && fmd.getDeleteAction() == ForeignKeyAction.CASCADE) {
                String fieldName = mmd.getName();
                // parse JSON entries 
                ArrayList<String> foreignObjectIdsNew = new ArrayList<String>();
                ArrayList<String> foreignObjectIdsOld = new ArrayList<String>();
                try {
                    if (isJoin) {
                        String elementClassName = mmd.getCollection().getElementType();
                        JSONArray arrNew = changedFieldsNewVal.getJSONArray(fieldName);
                        JSONArray arrOld = new JSONArray();
                        if (changedFieldsOldVal != null) {
                            arrOld = changedFieldsOldVal.getJSONArray(fieldName);
                        }
                        for (int i = 0; i < arrNew.length(); i++) {
                            foreignObjectIdsNew.add(
                                    ScalarisSchemaHandler.getForeignKeyActionKey(
                                            elementClassName, arrNew.getString(i)));
                        }
                        for (int i = 0; i < arrOld.length(); i++) {
                            foreignObjectIdsOld.add(
                                    ScalarisSchemaHandler.getForeignKeyActionKey(
                                            elementClassName, arrOld.getString(i)));
                        }
                    } else {
                        String className = mmd.getType().getCanonicalName();
                        foreignObjectIdsNew.add(
                                ScalarisSchemaHandler.getForeignKeyActionKey(
                                        className, changedFieldsNewVal.getString(fieldName)));
                    }
                } catch (JSONException e) {
                    // not found -> this action will be skipped
                }
                // ignore the objects in both the new and old list
                for (int i = foreignObjectIdsNew.size() - 1; i >= 0; i--) {
                    String s = foreignObjectIdsNew.get(i);
                    if (foreignObjectIdsOld.remove(s)) {
                        foreignObjectIdsNew.remove(s);
                    }
                }

                // construct the FKA key for every foreign object id
                ArrayList<String>[] bothNewAndOld = new ArrayList[]{foreignObjectIdsOld, foreignObjectIdsNew};
                for (ArrayList<String> changeList : bothNewAndOld) {
                    for (String fkaKey : changeList) {
                        if (fkaKey == null) continue;

                        List<String> newFka = new ArrayList<String>(2);
                        newFka.add(objectStringIdentity);
                        newFka.add(cmd.getFullClassName());
                        if (isJoin) {
                            newFka.add(fieldName);
                        } else {
                            newFka.add(ScalarisSchemaHandler.FKA_DELETE_OBJ);
                        }

                        // check in which list we are currently in to choose the
                        // HashMap to which the action must be added to
                        HashMap<String, List<ErlangValue>> toChangeMap = (foreignObjectIdsNew == changeList) ?
                                toAddToKey : toRemoveFromKey;

                        List<ErlangValue> toChange = toChangeMap.get(fkaKey);
                        if (toChange == null) {
                            toChange = new ArrayList<ErlangValue>();
                        }
                        toChange.add(new ErlangValue(newFka));
                        toChangeMap.put(fkaKey, toChange);
                    }
                }
            }
        }

        // update all keys where new entries are added
        for (String key : toAddToKey.keySet()) {
            List<ErlangValue> toAdd = toAddToKey.get(key);
            List<ErlangValue> toRemove = new ArrayList<ErlangValue>(0);
            if (toRemoveFromKey.containsKey(key)) {
                toRemove = toRemoveFromKey.get(key);
            }
            t.addDelOnList(key, toAdd, toRemove);
        }
        // update the remaining keys (only deletions)
        for (String key : toRemoveFromKey.keySet()) {
            if (toAddToKey.containsKey(key)) {
                List<ErlangValue> toRemove = toRemoveFromKey.get(key);
                t.addDelOnList(key, new ArrayList<ErlangValue>(0), toRemove);
            }
        }
    }
    
    private static void performForeignKeyActionDelete(ObjectProvider<?> op, Transaction t)
            throws ConnectionException, ClassCastException, UnknownException, NotAListException {
        AbstractClassMetaData cmd = op.getClassMetaData();
        String objClassName = cmd.getFullClassName();
        String objectStringIdentity = getPersistableIdentity(op);
        
        ExecutionContext ec = op.getExecutionContext();
        StoreManager storeMgr = ec.getStoreManager();
        
        String fkaKey = ScalarisSchemaHandler.getForeignKeyActionKey(objClassName, objectStringIdentity);
        List<ErlangValue> attachedActions;
        try {
            attachedActions = t.read(fkaKey).listValue();
        } catch (NotFoundException e) {
            // there is no FKA key for this object --> there are no actions to perform
            return;
        }
        // now search in every found action entries with op's id and start a delete as sub transaction
        for (ErlangValue action : attachedActions) {
            try {
                List<String> actionAsList = action.stringListValue();
                
                String objId = actionAsList.get(0);
                String toDeleteClassName = actionAsList.get(1);
                String memberToDelete = actionAsList.get(2);
                
                AbstractClassMetaData obCmd = storeMgr.getMetaDataManager()
                        .getMetaDataForClass(toDeleteClassName, ec.getClassLoaderResolver());
                Object obj = IdentityUtils.getObjectFromPersistableIdentity(objId, obCmd, ec);
                
                if (ScalarisSchemaHandler.FKA_DELETE_OBJ.equals(memberToDelete)) {
                    // delete the complete object
                    ec.deleteObject(obj);
                } else {
                    // remove the object reference of the deleted object from the collection
    
                    ObjectProvider<?> toDelOp = ec.findObjectProvider(obj);
    
                    int memberId = toDelOp.getClassMetaData().getAbsolutePositionOfMember(memberToDelete);
    
                    if (toDelOp.isFieldLoaded(memberId)) {
                        // the field is loaded which means that the object could be used by the overlying 
                        // application
                        Object objColl = toDelOp.provideField(memberId);
    
                        if (objColl instanceof Collection) {
                            Collection<?> collection = (Collection<?>) objColl;
    
                            ArrayList<Object> toRemove = new ArrayList<Object>();
                            Iterator<?> iter = collection.iterator();
                            while (iter.hasNext()) {
                                Object item = iter.next();
                                ObjectProvider<?> itemOp = ec.findObjectProvider(item);
                                String itemId = getPersistableIdentity(itemOp);
    
                                if (itemId.equals(objectStringIdentity)) {
                                    toRemove.add(item);
                                }
                            }
                            for (Object o : toRemove) {
                                collection.remove(o);
                            }
                            storeMgr.getPersistenceHandler().updateObject(toDelOp, new int[]{memberId});
                            // updated field must be marked as dirty to ensure that instances of this object used
                            // somewhere else will fetch the updated value
                            toDelOp.makeDirty(memberId);
                        }
                    } else {
                        // if the member is not loaded it is way faster to directly alter the
                        // stored JSON object
                        String objKey = ScalarisSchemaHandler.getObjectStorageKey(toDeleteClassName, objId);
                        JSONObject objAsJson = new JSONObject(t.read(objKey).stringValue());
                        if (isDeletedRecord(objAsJson)) {
                            continue;
                        }
                        JSONArray memberArr = objAsJson.getJSONArray(memberToDelete);
                        JSONArray newMemberArr = new JSONArray();
                        for (int j = 0; j < memberArr.length(); j++) {
                            if (!memberArr.get(j).equals(objectStringIdentity)) {
                                newMemberArr.put(memberArr.get(j));
                            }
                        }
                        // only write new value if something changed
                        if (newMemberArr.length() != memberArr.length()) {
                            objAsJson.put(memberToDelete, newMemberArr);
                            t.write(objId, objAsJson.toString());
                            // if the object is not removed from cache after this update
                            // it is possible that the cached value returned is out dated
                            ec.removeObjectFromLevel1Cache(toDelOp.getInternalObjectId());
                            ec.removeObjectFromLevel2Cache(toDelOp.getInternalObjectId());
                        }
                    }
                }
            } catch (NucleusObjectNotFoundException e) {
                // the object we want to delete is already deleted 
                // nothing must be done
            } catch (NotFoundException e) {
                throw new NucleusException(e.getMessage(), e);
            } catch (JSONException e) {
                throw new NucleusException(e.getMessage(), e);
                // the member containing the current object does not exist any more
                // that means we don't have to remove it any more
            }
        }
        // all objects are gone
        t.write(fkaKey, DELETED_RECORD_VALUE);
    }

    /* **********************************************************************
     *                     MISC
     * **********************************************************************/
    
    /**
     * Scalaris does not support deletion (in a usable way). Therefore, deletion
     * is simulated by overwriting an object with a "deleted" value.
     * 
     * This method returns true if json is a json of a deleted record.
     * 
     * @param record
     * @return
     */
    public static boolean isDeletedRecord(final JSONObject record) {
        return record == null || isDeletedRecord(record.toString());
    }
    
    public static boolean isDeletedRecord(final String record) {
        return record == null || record.isEmpty() || record.equals(DELETED_RECORD_VALUE);
    }
}
