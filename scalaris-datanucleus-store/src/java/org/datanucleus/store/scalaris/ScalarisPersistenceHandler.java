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

import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.identity.SCOID;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.metadata.VersionStrategy;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.AbstractPersistenceHandler;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.VersionHelper;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.schema.naming.ColumnType;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;


import com.orange.org.json.JSONException;
import com.orange.org.json.JSONObject;

import de.zib.scalaris.AbortException;
import de.zib.scalaris.ConnectionException;
import de.zib.scalaris.NotFoundException;
import de.zib.scalaris.Transaction;
import de.zib.scalaris.UnknownException;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class ScalarisPersistenceHandler extends AbstractPersistenceHandler {

    /** Setup localiser for messages. */
    static {
        Localiser.registerBundle("org.datanucleus.store.scalaris.Localisation",
                ScalarisStoreManager.class.getClassLoader());
    }

    ScalarisPersistenceHandler(StoreManager storeMgr) {
        super(storeMgr);
    }

    public void close() {
        // nothing to do
    }

    /**
     * Populates JSONObject with information of the object provider
     * 
     * @param jsonobj
     *            updated with data from op
     * @param op
     *            data source
     * @return primary key as string
     */
    private void populateJsonObj(JSONObject jsonobj, ObjectProvider op) {
        AbstractClassMetaData acmd = op.getClassMetaData();
        int[] fieldNumbers = acmd.getAllMemberPositions();
        op.provideFields(fieldNumbers, new StoreFieldManager(op, jsonobj, true));
    }


    public void insertObject(ObjectProvider op) {
        System.out.println("INSERT");
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(op);

        Map<String, String> options = new HashMap<String, String>();
        // options.put("Content-Type", "application/json");
        ExecutionContext ec = op.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnection(ec, options);

        de.zib.scalaris.Connection conn = (de.zib.scalaris.Connection) mconn
                .getConnection();

        try {
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled()) {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg(
                        "Scalaris.Insert.Start", op.getObjectAsPrintable(),
                        op.getInternalObjectId()));
            }

            JSONObject jsonobj = new JSONObject();

            AbstractClassMetaData cmd = op.getClassMetaData();
            System.out.println("insert looking for key=" + "key"
                    + " discriminator=" + cmd.getDiscriminatorColumnName()
                    + " table=" + cmd.getTable());

            if (cmd.getIdentityType() == IdentityType.DATASTORE) {
                String memberName = storeMgr.getNamingFactory().getColumnName(
                        cmd, ColumnType.DATASTOREID_COLUMN);
                Object idKey = ((SCOID) op.getInternalObjectId());
                System.out.println("idKey=" + idKey);
                try {
                    jsonobj.put(memberName, idKey);
                } catch (JSONException e) {
                    throw new NucleusException(
                            "Exception setting datastore identity in JSON object",
                            e);
                }
            }

            if (cmd.isVersioned()) {
                VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                String memberName = storeMgr.getNamingFactory().getColumnName(
                        cmd, ColumnType.VERSION_COLUMN);
                if (vermd.getVersionStrategy() == VersionStrategy.VERSION_NUMBER) {
                    long versionNumber = 1;
                    op.setTransactionalVersion(new Long(versionNumber));
                    if (NucleusLogger.DATASTORE.isDebugEnabled()) {
                        NucleusLogger.DATASTORE.debug(Localiser.msg(
                                "Scalaris.Insert.ObjectPersistedWithVersion",
                                StringUtils.toJVMIDString(op.getObject()),
                                op.getInternalObjectId(), "" + versionNumber));
                    }
                    try {
                        jsonobj.put(memberName, versionNumber);
                    } catch (JSONException e) {
                        throw new NucleusException(
                                "Exception setting version in JSON object", e);
                    }

                    if (vermd.getFieldName() != null) {
                        // Version is stored in a field, so set it there too
                        AbstractMemberMetaData verfmd = cmd
                                .getMetaDataForMember(vermd.getFieldName());
                        if (verfmd.getType() == Integer.class) {
                            op.replaceField(verfmd.getAbsoluteFieldNumber(),
                                    new Integer((int) versionNumber));
                        } else {
                            op.replaceField(verfmd.getAbsoluteFieldNumber(),
                                    new Long(versionNumber));
                        }
                    }
                } else if (vermd.getVersionStrategy() == VersionStrategy.DATE_TIME) {
                    Date date = new Date();
                    Timestamp ts = new Timestamp(date.getTime());
                    op.setTransactionalVersion(ts);
                    if (NucleusLogger.DATASTORE.isDebugEnabled()) {
                        NucleusLogger.DATASTORE.debug(Localiser.msg(
                                "Scalaris.Insert.ObjectPersistedWithVersion",
                                StringUtils.toJVMIDString(op.getObject()),
                                op.getInternalObjectId(), "" + ts));
                    }
                    try {
                        jsonobj.put(memberName, ts.getTime());
                    } catch (JSONException e) {
                        throw new NucleusException(
                                "Exception setting version in JSON object", e);
                    }
                }
            }

            final String id = ScalarisUtils.generatePersistableIdentity(op);
            populateJsonObj(jsonobj, op);

            if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled()) {
                NucleusLogger.DATASTORE_NATIVE.debug("POST "
                        + jsonobj.toString());
            }
            // write("POST",conn.getURL().toExternalForm(), conn, jsonobj,
            // getHeaders("POST",options));

            System.out.println("id=" + id + " json=" + jsonobj.toString());

            { // TRANSACTION
                Transaction t1 = new Transaction(conn); // Transaction()

                try {
                    t1.write(id, jsonobj.toString());
                    ScalarisUtils.performScalarisManagementForInsert(op, jsonobj, t1);
                    t1.commit();
                } catch (ConnectionException e) {
                    e.printStackTrace();
                } catch (UnknownException e) {
                    e.printStackTrace();
                } catch (ClassCastException e) {
                    e.printStackTrace();
                } catch (JSONException e) {
                    e.printStackTrace();
                }

            }

            if (ec.getStatistics() != null) {
                // Add to statistics
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementInsertCount();
            }

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled()) {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg(
                        "Scalaris.ExecutionTime",
                        (System.currentTimeMillis() - startTime)));
            }
        } catch (AbortException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (UnknownException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            mconn.release();
        }
    }

    public void updateObject(ObjectProvider op, int[] fieldNumbers) {
        System.out.println("UPDATE " + ScalarisUtils.getPersistableIdentity(op));
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(op);

        
        Map<String, String> options = new HashMap<String, String>();
        // options.put("Content-Type", "application/json");
        ExecutionContext ec = op.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnection(ec, options);

        de.zib.scalaris.Connection conn = (de.zib.scalaris.Connection) mconn
                .getConnection();

        try {

            AbstractClassMetaData cmd = op.getClassMetaData();
            int[] updatedFieldNums = fieldNumbers;
            Object currentVersion = op.getTransactionalVersion();
            Object nextVersion = null;
            if (cmd.isVersioned()) {
                // Version object so calculate version to store with
                VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                if (vermd.getFieldName() != null) {
                    // Version field
                    AbstractMemberMetaData verMmd = cmd
                            .getMetaDataForMember(vermd.getFieldName());
                    if (currentVersion instanceof Integer) {
                        // Cater for Integer-based versions TODO Generalise this
                        currentVersion = new Long(
                                ((Integer) currentVersion).longValue());
                    }

                    nextVersion = VersionHelper.getNextVersion(
                            vermd.getVersionStrategy(), currentVersion);
                    if (verMmd.getType() == Integer.class
                            || verMmd.getType() == int.class) {
                        // Cater for Integer-based versions TODO Generalise this
                        nextVersion = new Integer(
                                ((Long) nextVersion).intValue());
                    }
                    op.replaceField(verMmd.getAbsoluteFieldNumber(),
                            nextVersion);

                    boolean updatingVerField = false;
                    for (int i = 0; i < fieldNumbers.length; i++) {
                        if (fieldNumbers[i] == verMmd.getAbsoluteFieldNumber()) {
                            updatingVerField = true;
                        }
                    }
                    if (!updatingVerField) {
                        // Add the version field to the fields to be updated
                        updatedFieldNums = new int[fieldNumbers.length + 1];
                        System.arraycopy(fieldNumbers, 0, updatedFieldNums, 0,
                                fieldNumbers.length);
                        updatedFieldNums[fieldNumbers.length] = verMmd
                                .getAbsoluteFieldNumber();
                    }
                } else {
                    // Surrogate version column
                    nextVersion = VersionHelper.getNextVersion(
                            vermd.getVersionStrategy(), currentVersion);
                }
                op.setTransactionalVersion(nextVersion);
            }

            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled()) {
                StringBuffer fieldStr = new StringBuffer();
                for (int i = 0; i < fieldNumbers.length; i++) {
                    if (i > 0) {
                        fieldStr.append(",");
                    }
                    fieldStr.append(cmd
                            .getMetaDataForManagedMemberAtAbsolutePosition(
                                    fieldNumbers[i]).getName());
                }
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg(
                        "Scalaris.Update.Start", op.getObjectAsPrintable(),
                        op.getInternalObjectId(), fieldStr.toString()));
            }

            JSONObject jsonobj = new JSONObject();
            if (cmd.isVersioned()) {
                VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                String memberName = storeMgr.getNamingFactory().getColumnName(
                        cmd, ColumnType.VERSION_COLUMN);
                if (vermd.getVersionStrategy() == VersionStrategy.VERSION_NUMBER) {
                    if (NucleusLogger.DATASTORE.isDebugEnabled()) {
                        NucleusLogger.DATASTORE.debug(Localiser.msg(
                                "Scalaris.Insert.ObjectPersistedWithVersion",
                                StringUtils.toJVMIDString(op.getObject()),
                                op.getInternalObjectId(), "" + nextVersion));
                    }
                    try {
                        jsonobj.put(memberName, nextVersion);
                    } catch (JSONException e) {
                        throw new NucleusException(e.getMessage(), e);
                    }
                } else if (vermd.getVersionStrategy() == VersionStrategy.DATE_TIME) {
                    op.setTransactionalVersion(nextVersion);
                    if (NucleusLogger.DATASTORE.isDebugEnabled()) {
                        NucleusLogger.DATASTORE.debug(Localiser.msg(
                                "Scalaris.Insert.ObjectPersistedWithVersion",
                                StringUtils.toJVMIDString(op.getObject()),
                                op.getInternalObjectId(), "" + nextVersion));
                    }
                    Timestamp ts = (Timestamp) nextVersion;
                    Date date = new Date();
                    date.setTime(ts.getTime());
                    try {
                        jsonobj.put(memberName, ts.getTime());
                    } catch (JSONException e) {
                        throw new NucleusException(e.getMessage(), e);
                    }
                }
            }

            final String id;
            {
                op.provideFields(updatedFieldNums, new StoreFieldManager(op,
                        jsonobj, false));
                op.provideFields(op.getClassMetaData().getPKMemberPositions(),
                        new StoreFieldManager(op, jsonobj, false));
                id = ScalarisUtils.getPersistableIdentity(op);
                System.out.println("update id=" + id);
            }

            if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled()) {
                NucleusLogger.DATASTORE_NATIVE.debug("PUT "
                        + jsonobj.toString());
            }


            Transaction t1 = new Transaction(conn);
            try {
                JSONObject stored = new JSONObject(t1.read(id).stringValue());

                // update stored object
                Iterator<String> keyIter = jsonobj.keys();
                while (keyIter.hasNext()) {
                    String key = keyIter.next();
                    stored.put(key, jsonobj.get(key));
                }

                t1.write(id, stored.toString());
                System.out.println("json!!!!" + stored.toString());
                ScalarisUtils.performScalarisManagementForUpdate(op, stored, t1);
                t1.commit();
            } catch (ConnectionException e) {
                throw new NucleusException(e.getMessage(), e);
            } catch (AbortException e) {
                throw new NucleusException(e.getMessage(), e);
            }catch (UnknownException e) {
                throw new NucleusException(e.getMessage(), e);
            }catch (NotFoundException e) {
                // if we have an update we should already have this object stored
                throw new NucleusException("Could not update object since its original value was not found", e);
            } catch (ClassCastException e) {
                throw new NucleusException("The stored object has a broken structure", e);
            } catch (JSONException e) {
                throw new NucleusException("The stored object has a broken structure", e);
            }

            if (ec.getStatistics() != null) {
                // Add to statistics
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementUpdateCount();
            }

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled()) {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg(
                        "Scalaris.ExecutionTime",
                        (System.currentTimeMillis() - startTime)));
            }
        } finally {
            mconn.release();
        }
    }

    /**
     * Deletes a persistent object from the database. The delete can take place
     * in several steps, one delete per table that it is stored in. e.g When
     * deleting an object that uses "new-table" inheritance for each level of
     * the inheritance tree then will get an DELETE for each table. When
     * deleting an object that uses "complete-table" inheritance then will get a
     * single DELETE for its table.
     * 
     * @param op
     *            The ObjectProvider of the object to be deleted.
     * 
     * @throws NucleusDataStoreException
     *             when an error occurs in the datastore communication
     */
    public void deleteObject(ObjectProvider op) {
        System.out.println("DELETE");
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(op);

        Map<String, String> options = new HashMap<String, String>();
        // options.put(ConnectionFactoryImpl.STORE_JSON_URL, getURLPath(op));
        options.put("Content-Type", "application/json");
        ExecutionContext ec = op.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnection(ec, options);
        de.zib.scalaris.Connection conn = (de.zib.scalaris.Connection) mconn
                .getConnection();

        try {
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled()) {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg(
                        "Scalaris.Delete.Start", op.getObjectAsPrintable(),
                        op.getInternalObjectId()));
            }

            final String id = ScalarisUtils.getPersistableIdentity(op);
            System.out.println("deleting object with key=" + id);

            final JSONObject jsonobj = new JSONObject();

            { // TRANSACTION
                Transaction t1 = new Transaction(conn); // Transaction()

                try {
                    t1.write(id, jsonobj.toString());
                    ScalarisUtils.performScalarisManagementForDelete(op, t1);
                    t1.commit();
                    System.out.println("deleted id=" + id);
                } catch (ConnectionException e) {
                    throw new NucleusDataStoreException(e.getMessage(), e);
                } catch (UnknownException e) {
                    throw new NucleusDataStoreException(e.getMessage(), e);
                } catch (AbortException e) {
                    throw new NucleusDataStoreException(e.getMessage(), e);
                } catch (ClassCastException e) {
                    throw new NucleusDataStoreException(e.getMessage(), e);
                } catch (JSONException e) {
                    throw new NucleusDataStoreException(e.getMessage(), e);
                }
            }

            if (ec.getStatistics() != null) {
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementDeleteCount();
            }

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled()) {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg(
                        "Scalaris.ExecutionTime",
                        (System.currentTimeMillis() - startTime)));
            }
        } finally {
            mconn.release();
        }
    }

    /**
     * Scalaris does not support deletion (in a usable way). Therefore, deletion
     * is simulated by overwriting an object with a "deleted" value.
     * 
     * This method returns true if json is a json of a deleted record.
     * 
     * @param record
     * @return
     */
    public static boolean isADeletedRecord(final JSONObject record) {
        return record == null || record.length() == 0;
    }

    /**
     * Fetches (fields of) a persistent object from the database. This does a
     * single SELECT on the candidate of the class in question. Will join to
     * inherited tables as appropriate to get values persisted into other
     * tables. Can also join to the tables of related objects (1-1, N-1) as
     * neccessary to retrieve those objects.
     * 
     * @param op
     *            Object Provider of the object to be fetched.
     * @param memberNumbers
     *            The numbers of the members to be fetched.
     * @throws NucleusObjectNotFoundException
     *             if the object doesn't exist
     * @throws NucleusDataStoreException
     *             when an error occurs in the datastore communication
     */
    public void fetchObject(ObjectProvider op, int[] fieldNumbers) {
        System.out.println("FETCH " + op.getObject().getClass().getName());

        Map<String, String> options = new HashMap<String, String>();
        // options.put("Content-Type", "application/json");
        ExecutionContext ec = op.getExecutionContext();

        ManagedConnection mconn = storeMgr.getConnection(ec, options);

        de.zib.scalaris.Connection conn = (de.zib.scalaris.Connection) mconn
                .getConnection();

        try {
            final long startTime = System.currentTimeMillis();

            final String key = ScalarisUtils.getPersistableIdentity(op);
            System.out.println("FETCH KEY: " + key);
            final JSONObject result;
            {
                try {
                    Transaction t1 = new Transaction(conn);

                    result = new JSONObject(t1.read(key).stringValue());
                    if (isADeletedRecord(result)) {
                        throw new NucleusObjectNotFoundException(
                                "Record has been deleted");
                    }
                    final String declaredClassQName = result.getString("class");
                    final Class declaredClass = op.getExecutionContext()
                            .getClassLoaderResolver()
                            .classForName(declaredClassQName);
                    final Class objectClass = op.getObject().getClass();

                    if (!objectClass.isAssignableFrom(declaredClass)) {
                        System.out
                                .println("Type found in db not compatible with requested type");
                        throw new NucleusObjectNotFoundException(
                                "Type found in db not compatible with requested type");
                    }

                    op.replaceFields(fieldNumbers, new FetchFieldManager(op, result));

                    t1.commit();
                } catch (NotFoundException e) {
                    throw new NucleusObjectNotFoundException(e.getMessage(), e);
                } catch (ConnectionException e) {
                    throw new NucleusDataStoreException(e.getMessage(), e);
                } catch (UnknownException e) {
                    throw new NucleusDataStoreException(e.getMessage(), e);
                } catch (AbortException e) {
                    throw new NucleusDataStoreException(e.getMessage(), e);
                } catch (JSONException e) {
                    throw new NucleusDataStoreException(e.getMessage(), e);
                }
            }

            if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled()) {
                NucleusLogger.DATASTORE_NATIVE
                        .debug("GET " + result.toString());
            }
            if (ec.getStatistics() != null) {
                // Add to statistics
                ec.getStatistics().incrementNumReads();
                ec.getStatistics().incrementFetchCount();
            }

            if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled()) {
                NucleusLogger.DATASTORE_RETRIEVE.debug(Localiser.msg(
                        "Scalaris.ExecutionTime",
                        (System.currentTimeMillis() - startTime)));
            }
        } finally {
            mconn.release();
        }
    }

    /**
     * Method to return a persistable object with the specified id. Optional
     * operation for StoreManagers. Should return a (at least) hollow
     * PersistenceCapable object if the store manager supports the operation. If
     * the StoreManager is managing the in-memory object instantiation (as part
     * of co-managing the object lifecycle in general), then the StoreManager
     * has to create the object during this call (if it is not already created).
     * Most relational databases leave the in-memory object instantion to Core,
     * but some object databases may manage the in-memory object instantion,
     * effectively preventing Core of doing this.
     * <p>
     * StoreManager implementations may simply return null, indicating that they
     * leave the object instantiate to us. Other implementations may instantiate
     * the object in question (whether the implementation may trust that the
     * object is not already instantiated has still to be determined). If an
     * implementation believes that an object with the given ID should exist,
     * but in fact does not exist, then the implementation should throw a
     * RuntimeException. It should not silently return null in this case.
     * </p>
     * 
     * @param ec
     *            execution context
     * @param id
     *            the id of the object in question.
     * @return a persistable object with a valid object state (for example:
     *         hollow) or null, indicating that the implementation leaves the
     *         instantiation work to us.
     */
    public Object findObject(ExecutionContext ec, Object id) {
        System.out.println("FIND id=" + id.getClass());

        return null;
    }

    /**
     * Locates this object in the datastore.
     * 
     * @param op
     *            ObjectProvider for the object to be found
     * @throws NucleusObjectNotFoundException
     *             if the object doesnt exist
     * @throws NucleusDataStoreException
     *             when an error occurs in the datastore communication
     */
    public void locateObject(ObjectProvider op) {
        System.out.println("LOCATE");

        final String key;
        {
            final JSONObject jsonobj = new JSONObject();
            populateJsonObj(jsonobj, op);
            key = ScalarisUtils.getPersistableIdentity(op);
        }

        System.out.println("############# locateObject(class="
                + op.getObject().getClass().getName() + ",key=" + key);

        final ExecutionContext ec = op.getExecutionContext();
        final de.zib.scalaris.Connection conn;
        {
            Map<String, String> options = new HashMap<String, String>();
            ManagedConnection mconn = storeMgr.getConnection(ec, options);
            conn = (de.zib.scalaris.Connection) mconn.getConnection();
        }

        try {
            Transaction t1 = new Transaction(conn);
            final String indb = t1.read(key).stringValue();

            System.out.println("locate : " + indb);
            t1.commit();
        } catch (NotFoundException e) {
            throw new NucleusObjectNotFoundException(e.getMessage(), e);
        } catch (ConnectionException e) {
            throw new NucleusDataStoreException(e.getMessage(), e);
        } catch (UnknownException e) {
            throw new NucleusDataStoreException(e.getMessage(), e);
        } catch (AbortException e) {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }

        if (ec.getStatistics() != null) {
            // Add to statistics
            ec.getStatistics().incrementNumReads();
        }
    }
}
