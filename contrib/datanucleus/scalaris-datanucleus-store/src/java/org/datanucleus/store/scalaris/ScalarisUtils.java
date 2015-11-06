package org.datanucleus.store.scalaris;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

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
import org.datanucleus.store.scalaris.fieldmanager.StoreFieldManager;
import org.datanucleus.transaction.NucleusTransactionException;
import org.datanucleus.util.NucleusLogger;

import com.ericsson.otp.erlang.OtpErlangLong;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

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
     * Placeholder to signal a foreign key action deleting the whole object,
     * instead of an element of a collection.
     */
    private static final String FKA_DELETE_OBJ = "_DEL_OBJECT";

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
     * this function. Every object class has its own ID generator key
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
        } catch (NotANumberException e) {
            throw new NucleusTransactionException(
                    "The value of the ID generator key was altered to an invalid value",
                    e);
        } finally {
            mConn.release();
        }

        return newID;
    }

    /**
     * Generates the ID of an object which will be persisted, but is not persisted yet, 
     * in the data store.  
     * ATTENTION: If the data store is (partially) responsible to generate an ID (e.g. 
     * because of IdGeneratorStrategy.IDENTITY). This method may change primary key
     * attribute values. This method should be used only when inserting a new object
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
                    if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled()) {
                        NucleusLogger.DATASTORE_PERSIST.debug("Generated ID " + idKey + " for object " +
                        		"of class " + cmd.getFullClassName());
                    }
                }
            }
        }
        return getPersistableIdentity(op);
    }

    /**
     * Returns the object identity as string. If this ObjectProvider has not yet
     * set its external object id, it is set to the returned ID.
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

            StringBuilder keyBuilder = new StringBuilder();

            int[] pkFieldNumbers = cmd.getPKMemberPositions();
            for (int i = 0; i < pkFieldNumbers.length; i++) {
                AbstractMemberMetaData mmd = cmd
                        .getMetaDataForManagedMemberAtAbsolutePosition(pkFieldNumbers[i]);

                Object keyVal = op.provideField(mmd.getAbsoluteFieldNumber());
                if (i > 0) {
                    keyBuilder.append(keySeparator);
                }
                keyBuilder.append(keyVal);
            }

            op.setPostStoreNewObjectId(keyBuilder.toString());
        }

        String id = IdentityUtils.getPersistableIdentityForId(op.getExternalObjectId());
        return id;
    }

    /* **********************************************************************
     *                  HOOKS FOR ScalarisStoreManager
     * **********************************************************************/
 
    /**
     * Retrieves an object from Scalaris based on the ObjectProviders ID and class.
     * @param op 
     *      ObjectProvider representing the stored object in DataNucleus
     * @param conn 
     *      Connection used for the operation
     * @return The stored object
     */
    static JSONObject performScalarisObjectFetch(ObjectProvider<?> op, Transaction scalarisTransaction)
            throws ConnectionException, NotFoundException {

        final String objId = ScalarisUtils.getPersistableIdentity(op);
        final String objClassName = op.getClassMetaData().getFullClassName();
        final String objKey = ScalarisSchemaHandler.getObjectStorageKey(objClassName, objId);
        
        return new JSONObject(scalarisTransaction.read(objKey).stringValue());
    }
    
    static JSONObject performScalarisObjectFetch(ObjectProvider<?> op, Connection scalarisConnection)
            throws ConnectionException, NotFoundException {

        final String objId = ScalarisUtils.getPersistableIdentity(op);
        final String objClassName = op.getClassMetaData().getFullClassName();
        final String objKey = ScalarisSchemaHandler.getObjectStorageKey(objClassName, objId);

        TransactionSingleOp scalarisTransaction = new TransactionSingleOp(scalarisConnection);
        return new JSONObject(scalarisTransaction.read(objKey).stringValue());
    }
    /**
     * Inserts an object into Scalaris and performs all necessary management operations (updating indices etc.)
     * ScalarisUtils.WRITE_LOCK is used to ensure thread safety.
     * @param op
     *      ObjectPrevider representing the object to be inserted 
     * @param json
     *      The JSON representation of the object
     * @param conn
     *      Connection used for the operation
     */
    static void performScalarisObjectInsert(ObjectProvider<?> op, JSONObject json, Transaction scalarisTransaction)
            throws ConnectionException {

        try {
            String objectId = ScalarisUtils.getPersistableIdentity(op);

            insertObjectToIDIndex(op, scalarisTransaction);
            updateUniqueMemberKey(op, json, null, scalarisTransaction);
            insertToForeignKeyAction(op, json, scalarisTransaction);

            String className = op.getClassMetaData().getFullClassName();
            String storageKey = ScalarisSchemaHandler.getObjectStorageKey(className, objectId);

            scalarisTransaction.write(storageKey, json.toString());
        } catch (NotAListException e) {
            throw new NucleusDataStoreException("Keys used internally have values of unexpected structure", e);
        }
    }

    /**
     * Updates a stored object and performs all necessary management operations (updating indices etc.)
     * ScalarisUtils.WRITE_LOCK is used to ensure thread safety.
     * @param op
     *      ObjectProvider representing the updated state of the object 
     * @param updatedFieldNumbers
     *      The ObjectProviders fields which shall be updated 
     * @param conn
     *      Connection used for the operation
     */
    static void performScalarisObjectUpdate(ObjectProvider<?> op, int[] updatedFieldNumbers, Transaction scalarisTransaction)
            throws ConnectionException, NotFoundException {

        String objectId = ScalarisUtils.getPersistableIdentity(op);

        // get old value
        String className = op.getClassMetaData().getFullClassName();
        String objectKey = ScalarisSchemaHandler.getObjectStorageKey(className, objectId);

        JSONObject stored = new JSONObject(scalarisTransaction.read(objectKey).stringValue());

        JSONObject changedVals = new JSONObject();
        op.provideFields(updatedFieldNumbers, new StoreFieldManager(op,
                changedVals, true));

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
        try {
            updateUniqueMemberKey(op, changedVals, changedValsOld, scalarisTransaction);
            updateForeignKeyAction(op, changedVals, changedValsOld, scalarisTransaction);

            scalarisTransaction.write(objectKey, stored.toString());
        } catch (NotAListException e) {
            throw new NucleusDataStoreException("Keys used internally have values of unexpected structure", e);
        }
    }

    /**
     * Deletes a stored object and performs all necessary management operations (updating indices etc.)
     * ScalarisUtils.WRITE_LOCK is used to ensure thread safety.
     * @param op
     *      ObjectProvider representing the object to be deleted 
     * @param conn
     *      Connection used for the operation
     */
    static void performScalarisObjectDelete(ObjectProvider<?> op, Transaction scalarisTransaction)
            throws ConnectionException, NotFoundException {

        String objectId = ScalarisUtils.getPersistableIdentity(op);
        String className = op.getClassMetaData().getFullClassName();
        String objectKey = ScalarisSchemaHandler.getObjectStorageKey(className, objectId);

        JSONObject oldJson = new JSONObject(scalarisTransaction.read(objectKey).stringValue());

        try {
            removeObjectFromIDIndex(op, scalarisTransaction);
            removeObjectFromUniqueMemberKey(op, oldJson, scalarisTransaction);
            performForeignKeyActionDelete(op, scalarisTransaction);

            scalarisTransaction.write(objectKey,  DELETED_RECORD_VALUE);
        } catch (NotAListException e) {
            throw new NucleusDataStoreException("Keys used internally have values of unexpected structure", e);
        }
    }

    /* **********************************************************************
     *          INDICIES OF ALL OBJECTS OF ONE TYPE (used for queries)
     * **********************************************************************/

    /**
     * To support queries it is necessary to have the possibility to iterate
     * over all stored objects of a specific type. Since Scalaris stores only
     * key-value pairs without structured tables, this is not "natively"
     * supported. Therefore an extra key is added to the store containing all
     * keys of available objects of a type.
     * 
     * This methods adds another entry to such a key based on the passed
     * ObjectProvider. If no such key-value pair exists, it is created.
     * 
     * @param op
     *            The data source
     * @throws ConnectionException
     * @throws NotAListExceptionn
     */
    private static void insertObjectToIDIndex(ObjectProvider<?> op, Transaction t)
            throws ConnectionException, NotAListException {
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
     * keys of available objects of a type.
     * 
     * This methods removes an entry of such a key based on the passed
     * ObjectProvider. If no such key-value pair exists, nothing happens.
     * 
     * @param op
     *            The data source
     * @throws NotAListException 
     * @throws ConnectionException 
     */
    private static void removeObjectFromIDIndex(ObjectProvider<?> op, Transaction t)
            throws ConnectionException, NotAListException {

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

    /**
     * To support unique member values (@Unique annotation) an extra key is
     * inserted to signal the object ID whose object has stored the given value.
     * Removed the keys belonging to the old state of the object and inserts new
     * keys according to the new state.
     * If this method is called and it turns out that the updated member value is
     * stored by a different object a NucleusDataStoreException is thrown.
     * @param op
     *      The data source representing the object
     * @param newJson
     *      The new state of the object in JSON form
     * @param oldJson
     *      The old state of the object in JSON form. If this is null it is assumed
     *      that this object is inserted for the first time and thus no old value will
     *      be deleted
     * @param t
     *      The transaction used for the performed operations. 
     * @throws ConnectionException
     * @throws UnknownException
     */
    private static void updateUniqueMemberKey(ObjectProvider<?> op, JSONObject newJson, JSONObject oldJson, Transaction t) 
            throws ConnectionException, UnknownException {
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

    /**
     * To support unique member values (@Unique annotation) an extra key is
     * inserted to signal the object ID whose object has stored the given value.
     * Deletes the keys belonging to the object represented by the ObjectProvider
     * @param op
     *      The data source representing the object
     * @param oldJson
     *      The current state of the object in JSON form.
     * @param t
     *      The transaction used for the performed operations. 
     * @throws ConnectionException
     * @throws UnknownException
     */
    private static void removeObjectFromUniqueMemberKey(ObjectProvider<?> op, JSONObject oldJson, Transaction t) 
            throws ConnectionException, UnknownException {
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

    /**
     * All foreign key action of an object are stored at a separate key.
     * If (persisted) object A has a ForeignKeyAction.CASCADE annotation at a member
     * whose value is (persisted) object B, An entry is added to B's FKA-index.
     * If an persisted object is deleted, all entries of its FKA-index will be processed.
     *
     * Checks a newly inserted object for foreign key actions.
     *
     * @param op
     *      The data source of the object
     * @param objToInsert
     *      The JSON representation of the object.
     * @param t
     *      The transaction used to perform the operations
     * @throws ConnectionException
     * @throws NotAListException
     */
    private static void insertToForeignKeyAction(ObjectProvider<?> op, JSONObject objToInsert, Transaction t)
            throws ConnectionException, NotAListException {
        updateForeignKeyAction(op, objToInsert, null, t);
    }

    /**
     * All foreign key action of an object are stored at a separate key.
     * If (persisted) object A has a ForeignKeyAction.CASCADE annotation at a member
     * whose value is (persisted) object B, An entry is added to B's FKA-index.
     * If an persisted object is deleted, all entries of its FKA-index will be processed.
     *
     * Deletes the FKAs of the old objects state and adds new FKAs according to its new state.
     *
     * @param op
     *      The data source of the object
     * @param changedFiledsNewVal
     *      The JSON representation of the objects new state. It is sufficient if the JSON
     *      only contains the changed fields value.
     * @param changedFiledsOldVal
     *      The JSON representation of the objects old state. It is sufficient if the JSON
     *      only contains the changed fields value. If this is null this operation is treated
     *      as an insert of new object, thus not deleting any old FKAs
     * @param t
     *      The transaction used to perform the operations
     * @throws ConnectionException
     * @throws NotAListException
     */
    @SuppressWarnings("unchecked")
    private static void updateForeignKeyAction(ObjectProvider<?> op, JSONObject changedFieldsNewVal, 
            JSONObject changedFieldsOldVal, Transaction t)
            throws ConnectionException, NotAListException {
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
                        if (className == null) {
                             className = mmd.getType().getName();
                        }
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
                            newFka.add(FKA_DELETE_OBJ);
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
        for (Entry<String, List<ErlangValue>> entry : toAddToKey.entrySet()) {
            List<ErlangValue> toAdd = entry.getValue();
            List<ErlangValue> toRemove = new ArrayList<ErlangValue>(0);
            if (toRemoveFromKey.containsKey(entry.getKey())) {
                toRemove = toRemoveFromKey.get(entry.getKey());
            }
            t.addDelOnList(entry.getKey(), toAdd, toRemove);
        }
        // update the remaining keys (only deletions)
        for (Entry<String, List<ErlangValue>> entry : toRemoveFromKey.entrySet()) {
            if (toAddToKey.containsKey(entry.getKey())) {
                List<ErlangValue> toRemove = entry.getValue();
                t.addDelOnList(entry.getKey(), new ArrayList<ErlangValue>(0), toRemove);
            }
        }
    }

    /**
     * All foreign key action of an object are stored at a separate key.
     * If (persisted) object A has a ForeignKeyAction.CASCADE annotation at a member
     * whose value is (persisted) object B, An entry is added to B's FKA-index.
     * If an persisted object is deleted, all entries of its FKA-index will be processed.
     *
     * This method will perform all foreign key actions of the object represented by the
     * passed ObjectProvider.
     *
     * @param op
     *      The data source of the object
     * @param t
     *      The transaction used to perform the operations
     * @throws ConnectionException
     * @throws NotAListException
     */
    private static void performForeignKeyActionDelete(ObjectProvider<?> op, Transaction t)
            throws ConnectionException, NotAListException {
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
                
                if (FKA_DELETE_OBJ.equals(memberToDelete)) {
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
                throw new NucleusObjectNotFoundException(e.getMessage(), e);
            } catch (JSONException e) {
                throw new NucleusDataStoreException(e.getMessage(), e);
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
     * This method returns true if record is a JSON of a deleted record.
     * 
     * @param record
     *      The JSON to check.
     * @return true if record is JSON of a deleted record, false otherwise.
     */
    public static boolean isDeletedRecord(final JSONObject record) {
        return record == null || isDeletedRecord(record.toString());
    }
    
    public static boolean isDeletedRecord(final String record) {
        return record == null || record.isEmpty() || record.equals(DELETED_RECORD_VALUE);
    }
}
