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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.PersistenceNucleusContext;
import org.datanucleus.TransactionEventListener;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.store.AbstractStoreManager;
import org.datanucleus.store.NucleusConnection;
import org.datanucleus.store.connection.ManagedConnection;

import de.zib.scalaris.AbortException;
import de.zib.scalaris.ConnectionException;
import de.zib.scalaris.Transaction;

public class ScalarisStoreManager extends AbstractStoreManager {

    private Map<org.datanucleus.Transaction, de.zib.scalaris.Transaction> transactionMap;

    public ScalarisStoreManager(ClassLoaderResolver clr,
            PersistenceNucleusContext ctx, Map<String, Object> props) {
        super("scalaris", clr, ctx, props);

        transactionMap = Collections.synchronizedMap(
                new HashMap<org.datanucleus.Transaction, de.zib.scalaris.Transaction>());

        // Handler for persistence process
        persistenceHandler = new ScalarisPersistenceHandler(this);
        schemaHandler = new ScalarisSchemaHandler(this);

        logConfiguration();
    }

    public NucleusConnection getNucleusConnection(ExecutionContext om) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void transactionStarted(final ExecutionContext ec) {
        final org.datanucleus.Transaction dnTransaction = ec.getTransaction();
        final ManagedConnection mConn = getConnection(ec);
        de.zib.scalaris.Connection conn = (de.zib.scalaris.Connection) mConn
                .getConnection();
        final Transaction scalarisTransaction = new Transaction(conn);

        if (transactionMap.containsKey(dnTransaction)) {
            throw new NucleusDataStoreException("Cannot start the same transaction multiple times");
        }
        transactionMap.put(dnTransaction, scalarisTransaction);
        dnTransaction.addTransactionEventListener(new TransactionEventListener() {
            /**
             * Signal if managed connection was released in pre-commit, preventing
             * multiple releases which can lead to undefined behavior.
             */
            private boolean connectionIsReleased = false;

            public void transactionPreCommit() {
                try {
                    // ensure that all updates operations are executed before commit
                    ec.flushInternal(true);
                    scalarisTransaction.commit();
                } catch (ConnectionException e) {
                    throw new NucleusDataStoreException(e.getMessage(), e);
                } catch (AbortException e) {
                    throw new NucleusDataStoreException(e.getMessage(), e);
                } finally {
                    transactionMap.remove(dnTransaction);
                    if (mConn != null && !connectionIsReleased) {
                        connectionIsReleased = true;
                        mConn.release();
                    }
                }
            }

            public void transactionPreRollBack() {
                scalarisTransaction.abort();
                transactionMap.remove(dnTransaction);
                if (mConn != null && !connectionIsReleased) {
                    connectionIsReleased = true;
                    mConn.release();
                }
            }

            // Not implemented because not needed
            public void transactionStarted() {}
            public void transactionEnded() {}
            public void transactionPreFlush() {}
            public void transactionFlushed() {}
            public void transactionCommitted() {}
            public void transactionRolledBack() {}
            public void transactionSetSavepoint(String name) {}
            public void transactionReleaseSavepoint(String name) {}
            public void transactionRollbackToSavepoint(String name) {}
        });
    }

    /**
     * Returns the Scalaris transaction which belongs to the
     * transaction of the passed ExecutionContext. If its transaction
     * is not active, no Scalaris transaction exists, thus returning null
     * 
     * @param ec
     *      ExecutionContext
     * @return The Scalaris transaction or null if ec has no active
     *      transaction.
     */
    public Transaction getScalarisTransaction(ExecutionContext ec) {
        return transactionMap.get(ec.getTransaction());
    }

    /**
     * Method defining which value strategy to use when the user specified native strategy 
     * or no strategy.
     */
    @Override
    public String getStrategyForNative(AbstractClassMetaData cmd, int absFiledNumber) {
        return "uuid-hex";
    }
}
