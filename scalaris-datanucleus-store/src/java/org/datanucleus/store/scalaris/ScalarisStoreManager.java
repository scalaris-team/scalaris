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
import java.util.Map;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.PersistenceNucleusContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.store.AbstractStoreManager;
import org.datanucleus.store.NucleusConnection;
import org.datanucleus.store.connection.ManagedConnection;

import com.orange.org.json.JSONException;
import com.orange.org.json.JSONObject;

import de.zib.scalaris.AbortException;
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

            if (ScalarisPersistenceHandler.isADeletedRecord(result)) {
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
}
