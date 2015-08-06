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
2008 Andy Jefferson - abstracted methods up to AbstractStoreManager
2013 Orange - port to Scalaris key/value store
    ...
 **********************************************************************/
package org.datanucleus.store.scalaris;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.PersistenceNucleusContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.ClassMetaData;
import org.datanucleus.metadata.ClassPersistenceModifier;
import org.datanucleus.store.AbstractStoreManager;
import org.datanucleus.store.NucleusConnection;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.schema.table.CompleteClassTable;

import com.orange.org.json.JSONException;
import com.orange.org.json.JSONObject;

import de.zib.scalaris.AbortException;
import de.zib.scalaris.Connection;
import de.zib.scalaris.ConnectionException;
import de.zib.scalaris.NotFoundException;
import de.zib.scalaris.Transaction;
import de.zib.scalaris.UnknownException;

public class ScalarisStoreManager extends AbstractStoreManager {
    public ScalarisStoreManager(ClassLoaderResolver clr,
            PersistenceNucleusContext ctx, Map<String, Object> props) {
        super("scalaris", clr, ctx, props);

        // Handler for persistence process
        persistenceHandler = new ScalarisPersistenceHandler(this);
        schemaHandler = new ScalarisSchemaHandler(this);
        connectionMgr.disableConnectionPool();

        logConfiguration();
    }

    public NucleusConnection getNucleusConnection(ExecutionContext om) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getClassNameForObjectID(Object id, ClassLoaderResolver clr,
            ExecutionContext ec) {

        Map<String, String> options = new HashMap<String, String>();
        ManagedConnection mconn = this.getConnection(ec, options);
        de.zib.scalaris.Connection conn = (de.zib.scalaris.Connection) mconn
                .getConnection();

        String myType = null;

        try {
            Transaction t1 = new Transaction(conn);
            JSONObject result = new JSONObject(t1.read(id.toString())
                    .stringValue());

            if (ScalarisUtils.isDeletedRecord(result)) {
                throw new NucleusObjectNotFoundException(
                        "Record has been deleted");
            }
            myType = result.getString("class");

            System.out.println("CLASS " + myType + " for " + id.toString());

            t1.commit();
            return myType;
        } catch (ConnectionException e) {
            throw new NucleusException(e.getMessage(), e);
        } catch (UnknownException e) {
            throw new NucleusException(e.getMessage(), e);
        } catch (AbortException e) {
            throw new NucleusException(e.getMessage(), e);
        }

        catch (JSONException e) {

            throw new NucleusException(e.getMessage(), e);
        } catch (NotFoundException e) {

        } catch (ClassCastException e) {
            throw new NucleusException(e.getMessage(), e);
        }

        System.out.println("!!! -> connection");
        if (myType == null) {
            // revert to default implementation for special cases
            myType = super.getClassNameForObjectID(id, clr, ec);
        }

        System.out.println("!!!!!!!!!" + myType);

        return myType;
    }

    @Override
    public void manageClasses(ClassLoaderResolver clr, String... classNames) {
        if (classNames == null) {
            return;
        }

        ManagedConnection mconn = getConnection(-1);
        try{
           Connection conn = (Connection) mconn.getConnection();
           manageClasses(classNames, clr, conn);
        } finally {
            mconn.release();
        }
    }

    /*
     * Checks if the classes and all references classes are already managed by this store,
     * for all which are not managed yet, create the their "schema".
     * Schema in context of Scalaris means all management keys needed for ForaignkeyActions, or queries 
     */
    public void manageClasses(String[] classNames, ClassLoaderResolver clr, Connection conn) {
        if (classNames == null) {
            return;
        }

        // Filter out any "simple" type classes
        String[] filteredClassNames = getNucleusContext().getTypeManager().filterOutSupportedSecondClassNames(classNames);

        // Find the ClassMetaData for these classes and all referenced by these classes
        Set<String> clsNameSet = new HashSet<String>();
        Iterator<AbstractClassMetaData> iter = getMetaDataManager().getReferencedClasses(filteredClassNames, clr).iterator();
        while (iter.hasNext()) {
            ClassMetaData cmd = (ClassMetaData) iter.next();
            if (cmd.getPersistenceModifier() == ClassPersistenceModifier.PERSISTENCE_CAPABLE && !cmd.isAbstract()) {
                if (!storeDataMgr.managesClass(cmd.getFullClassName())) {
                    StoreData sd = storeDataMgr.get(cmd.getFullClassName());
                    if (sd == null) {
                        CompleteClassTable table = new CompleteClassTable(this, cmd, null);
                        sd = newStoreData(cmd, clr);
                        sd.setTable(table);
                        registerStoreData(sd);
                    }
                    clsNameSet.add(cmd.getFullClassName());
                }
            }
        }

        // Create schema for classes
        schemaHandler.createSchemaForClasses(clsNameSet, null, conn);
    }    
}
