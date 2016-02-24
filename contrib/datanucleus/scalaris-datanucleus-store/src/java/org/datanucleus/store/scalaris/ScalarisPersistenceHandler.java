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

import java.util.ArrayList;
import java.util.List;

import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.AbstractPersistenceHandler;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.scalaris.fieldmanager.FetchFieldManager;
import org.datanucleus.store.scalaris.fieldmanager.StoreFieldManager;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

import org.json.JSONObject;

import de.zib.scalaris.AbortException;
import de.zib.scalaris.ConnectionException;
import de.zib.scalaris.NotFoundException;
import de.zib.scalaris.Transaction;
import de.zib.scalaris.TransactionSingleOp;
import de.zib.scalaris.UnknownException;

@SuppressWarnings("rawtypes")
public class ScalarisPersistenceHandler extends AbstractPersistenceHandler {

    /** Setup localizer for messages. */
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
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(op);
        ExecutionContext ec = op.getExecutionContext();
        Transaction scalarisTransaction = ((ScalarisStoreManager) storeMgr)
                .getScalarisTransaction(ec);
        boolean dnTransactionStarted = scalarisTransaction != null;

        ManagedConnection mConn = null;
        try {
            if (!dnTransactionStarted) {
                mConn = storeMgr.getConnection(ec);
                de.zib.scalaris.Connection scalarisConnection =
                        (de.zib.scalaris.Connection) mConn.getConnection();
                scalarisTransaction = new Transaction(scalarisConnection);
            }

            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled()) {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg(
                        "Scalaris.Insert.Start", op.getObjectAsPrintable(),
                        op.getInternalObjectId()));
            }

            // prepare object
            ScalarisUtils.generatePersistableIdentity(op);
            JSONObject jsonobj = new JSONObject();
            populateJsonObj(jsonobj, op);

            if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled()) {
                NucleusLogger.DATASTORE_NATIVE.debug("POST "
                        + jsonobj.toString());
            }

            // insert object
            ScalarisUtils.performScalarisObjectInsert(op, jsonobj, scalarisTransaction);
            if (!dnTransactionStarted) {
                scalarisTransaction.commit();
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
        } catch (UnknownException e) {
            throw new NucleusDataStoreException(e.getMessage(), e);
        } catch (ConnectionException e) {
            throw new NucleusDataStoreException(e.getMessage(), e);
        } catch (AbortException e) {
            throw new NucleusDataStoreException(e.getMessage(), e);
        } finally {
            if (mConn != null) {
                mConn.release();
            }
        }
    }

    public void updateObject(ObjectProvider op, int[] updatedFieldNumbers) {
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(op);

        ExecutionContext ec = op.getExecutionContext();
        Transaction scalarisTransaction = ((ScalarisStoreManager) storeMgr)
                .getScalarisTransaction(ec);
        boolean dnTransactionStarted = scalarisTransaction != null;

        ManagedConnection mConn = null;
        try {
            if (!dnTransactionStarted) {
                mConn = storeMgr.getConnection(ec);
                de.zib.scalaris.Connection scalarisConnection =
                        (de.zib.scalaris.Connection) mConn.getConnection();
                scalarisTransaction = new Transaction(scalarisConnection);
            }
            
            AbstractClassMetaData cmd = op.getClassMetaData();

            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled()) {
                StringBuffer fieldStr = new StringBuffer();
                for (int i = 0; i < updatedFieldNumbers.length; i++) {
                    if (i > 0) {
                        fieldStr.append(",");
                    }
                    fieldStr.append(cmd
                            .getMetaDataForManagedMemberAtAbsolutePosition(
                                    updatedFieldNumbers[i]).getName());
                }
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg(
                        "Scalaris.Update.Start", op.getObjectAsPrintable(),
                        op.getInternalObjectId(), fieldStr.toString()));
            }

            ScalarisUtils.performScalarisObjectUpdate(op, updatedFieldNumbers, scalarisTransaction);
            if (!dnTransactionStarted) {
                scalarisTransaction.commit();
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
        } catch (ConnectionException e) {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }catch (NotFoundException e) {
            // if we have an update we should already have this object stored
            throw new NucleusObjectNotFoundException("Could not update object since its original value was not found", e);
        } catch (AbortException e) {
            throw new NucleusDataStoreException(e.getMessage(), e);
        } finally {
            if (mConn != null) {
                mConn.release();
            }
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
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(op);

        ExecutionContext ec = op.getExecutionContext();
        Transaction scalarisTransaction = ((ScalarisStoreManager) storeMgr)
                .getScalarisTransaction(ec);
        boolean dnTransactionStarted = scalarisTransaction != null;

        ManagedConnection mConn = null;
        try {
            if (!dnTransactionStarted) {
                mConn = storeMgr.getConnection(ec);
                de.zib.scalaris.Connection scalarisConnection =
                        (de.zib.scalaris.Connection) mConn.getConnection();
                scalarisTransaction = new Transaction(scalarisConnection);
            }

            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled()) {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg(
                        "Scalaris.Delete.Start", op.getObjectAsPrintable(),
                        op.getInternalObjectId()));
            }

            ScalarisUtils.performScalarisObjectDelete(op, scalarisTransaction);
            if (!dnTransactionStarted) {
                scalarisTransaction.commit();
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
        } catch (ConnectionException e) {
            throw new NucleusDataStoreException(e.getMessage(), e);
        } catch (NotFoundException e) {
            throw new NucleusObjectNotFoundException(e.getMessage(), e);
        } catch (AbortException e) {
            throw new NucleusObjectNotFoundException(e.getMessage(), e);
        } finally {
            if (mConn != null) {
                mConn.release();
            }
        }
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
        ExecutionContext ec = op.getExecutionContext();
        Transaction scalarisTransaction = ((ScalarisStoreManager) storeMgr)
                .getScalarisTransaction(ec);

        final long startTime = System.currentTimeMillis();

        try {
            JSONObject result;
            if (scalarisTransaction != null) {
                result = ScalarisUtils.performScalarisObjectFetch(op, scalarisTransaction);
            } else {
                // non transactional read
                ManagedConnection mConn = storeMgr.getConnection(ec);
                try {
                    de.zib.scalaris.Connection scalarisConnection =
                            (de.zib.scalaris.Connection) mConn.getConnection();
                    result = ScalarisUtils.performScalarisObjectFetch(op, scalarisConnection);
                } finally {
                    mConn.release();
                }
            }

            if (ScalarisUtils.isDeletedRecord(result)) {
                throw new NucleusObjectNotFoundException(
                        "Record has been deleted");
            }

            final String declaredClassQName = result.getString("class");
            final Class declaredClass = op.getExecutionContext()
                    .getClassLoaderResolver()
                    .classForName(declaredClassQName);
            final Class<?> objectClass = op.getObject().getClass();

            if (!objectClass.isAssignableFrom(declaredClass)) {
                    System.out.println("Type found in db not compatible with requested type");
                throw new NucleusObjectNotFoundException(
                        "Type found in db not compatible with requested type");
            }

            op.replaceFields(fieldNumbers, new FetchFieldManager(op, result));

            if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled()) {
                NucleusLogger.DATASTORE_NATIVE
                        .debug("GET " + result.toString());
            }
        } catch (NotFoundException e) {
            // throwing the NeucleusExpection directly would cause the StateManagerImpl
            // to print a warning. This is annoying because all that happened was that
            // we couldn't find an object in the store
            NucleusException ne = new NucleusObjectNotFoundException(e.getMessage(), e);
            throw storeMgr.getApiAdapter().getApiExceptionForNucleusException(ne);
        } catch (ConnectionException e) {
            throw new NucleusDataStoreException(e.getMessage(), e);
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
        // The implementation of this method did not server an apparent purpose,
        // since removing all code here did not result in a test failure.
        // But removing it greatly improved (read: doubled in some cases) the speed 
        // of read operations. 
        // TODO: What does this method do?
    }

    /**
     * Convenience method to get all objects of the candidate type from the
     * specified connection. Objects of subclasses are ignored.
     * 
     * @param ec
     * @param mconn
     * @param candidateClass
     */
    public List<Object> getObjectsOfCandidateType(ExecutionContext ec,
            Class<?> candidateClass,AbstractClassMetaData cmd) {
        List<Object> results = new ArrayList<Object>();
        String idIndexKey = ScalarisSchemaHandler.getIDIndexKeyName(candidateClass);

        ManagedConnection mconn = ec.getStoreManager().getConnection(ec);
        de.zib.scalaris.Connection conn = (de.zib.scalaris.Connection) mconn
                .getConnection();
        Transaction scalarisTransaction = ((ScalarisStoreManager) storeMgr)
                .getScalarisTransaction(ec);
        boolean dnTransactionStarted = scalarisTransaction != null;

        try {
            // read the management key
            List<String> idIndex;
            if (dnTransactionStarted) {
                idIndex = scalarisTransaction.read(idIndexKey).stringListValue();
            } else {
                TransactionSingleOp t = new TransactionSingleOp(conn);
                idIndex = t.read(idIndexKey).stringListValue();
            }

            // retrieve all values from the management key
            for (String id : idIndex) {
                results.add(IdentityUtils.getObjectFromPersistableIdentity(id, cmd, ec));
            }

        } catch (NotFoundException e) {
            // the management key does not exist which means there
            // are no instances of this class stored.
        } catch (ConnectionException e) {
            throw new NucleusException(e.getMessage(), e);
        } catch (UnknownException e) {
            throw new NucleusException(e.getMessage(), e);
        } finally {
            if (mconn != null) {
                mconn.release();
            }
        }

        return results;
    }

    /**
     * Returns an object persisted in the store by an unique member value.
     * If there is no such object, null is returned
     * @param ec
     * @param mconn Connection used to connect to the store.
     * @param objectClass Class of the object
     * @param memberName The (simple) name of the unique member which is used to retrieve
     *      the object. The object is only correctly returned if the member has an 
     *      '@Unique' annotation.
     * @param memberValue Value of the unique member
     * @return The persisted object, or null if there is no such object.
     */
    public Object getObjectByUniqueMember(ExecutionContext ec,
                Class<?> objectClass, String memberName, String memberValue) {
        AbstractClassMetaData cmd = ec.getMetaDataManager()
                .getMetaDataForClass(objectClass,
                        ec.getClassLoaderResolver());
        ManagedConnection mconn = ec.getStoreManager().getConnection(ec);
        de.zib.scalaris.Connection conn = (de.zib.scalaris.Connection) mconn
                .getConnection();
        Transaction scalarisTransaction = ((ScalarisStoreManager) storeMgr)
                .getScalarisTransaction(ec);
        boolean dnTransactionStarted = scalarisTransaction != null;

        String uniqueMemberValueKey = ScalarisSchemaHandler.getUniqueMemberKey(
                objectClass.getCanonicalName(), memberName, memberValue);
        try {
            String uniqueObjectId;
            if (dnTransactionStarted) {
                uniqueObjectId = scalarisTransaction.read(uniqueMemberValueKey).stringValue();
            } else {
                TransactionSingleOp t = new TransactionSingleOp(conn);
                uniqueObjectId = t.read(uniqueMemberValueKey).stringValue();
            }

            if (!ScalarisUtils.isDeletedRecord(uniqueObjectId)) {
               return IdentityUtils.getObjectFromPersistableIdentity(uniqueObjectId, cmd, ec);
            }
        } catch (ConnectionException e) {
            throw new NucleusException(e.getMessage(), e);
        } catch (NotFoundException e) {
            // there does not exist an object with the unique member value
        } catch (UnknownException e) {
            throw new NucleusException(e.getMessage(), e);
        } finally {
            if (mconn != null) {
                mconn.release();
            }
        }
        return null;
    }
}
