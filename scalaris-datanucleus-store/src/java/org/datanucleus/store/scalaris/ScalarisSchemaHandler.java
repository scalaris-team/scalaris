package org.datanucleus.store.scalaris;

import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ClassMetaData;
import org.datanucleus.metadata.ClassPersistenceModifier;
import org.datanucleus.metadata.ForeignKeyAction;
import org.datanucleus.metadata.ForeignKeyMetaData;
import org.datanucleus.metadata.JoinMetaData;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.schema.AbstractStoreSchemaHandler;

import com.orange.org.json.JSONArray;
import com.orange.org.json.JSONException;

import de.zib.scalaris.AbortException;
import de.zib.scalaris.Connection;
import de.zib.scalaris.ConnectionException;
import de.zib.scalaris.NotFoundException;
import de.zib.scalaris.Transaction;
import de.zib.scalaris.UnknownException;

public class ScalarisSchemaHandler extends AbstractStoreSchemaHandler {
    
    /**
     * TODO Doc
     */
    private static String FKA_INDEX_KEY = "FKA_INDEX";
    
    private static String FKA_KEY_PREFIX = "FKA";
    
    public ScalarisSchemaHandler(StoreManager storeMgr) {
        super(storeMgr);
    }

    public static String getForeignKeyActionIndexKey() {
        return FKA_INDEX_KEY;
    }
    public static String getForeignKeyActionKey(String foreignClassName, String thisClassName) {
        return String.format("%s_%s_%s", foreignClassName, thisClassName, FKA_KEY_PREFIX);
    }
    
    @Override
    public void createSchemaForClasses(Set<String> classNames, Properties props, Object connection) {
        if (classNames == null || classNames.isEmpty() || connection == null) {
            return;
        }
        Connection conn = (Connection) connection;
        
        Iterator<String> classIter = classNames.iterator();
        ClassLoaderResolver clr = storeMgr.getNucleusContext().getClassLoaderResolver(null);
        
        while (classIter.hasNext()) {
            String className = classIter.next();
            AbstractClassMetaData cmd = storeMgr.getMetaDataManager().getMetaDataForClass(className, clr);
            if (cmd != null) {
                createSchemaForClass(cmd, conn);
            }
        }
    }
    
    private void createSchemaForClass(AbstractClassMetaData cmd, Connection conn) {
        if (cmd.isEmbeddedOnly() || cmd.getPersistenceModifier() != ClassPersistenceModifier.PERSISTENCE_CAPABLE) {
            // No action  required here
            return;
        }
        if (cmd instanceof ClassMetaData && ((ClassMetaData) cmd).isAbstract()) {
            // No action required here
            return;
        }
        
        String className = cmd.getFullClassName();
        
        for (int field : cmd.getAllMemberPositions()) {
            AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(field);
            if (mmd == null) continue;
            
            // TODO: handle ForeignKeyAction in Join, element etc.
            ForeignKeyMetaData fmd = mmd.getForeignKeyMetaData();

            // TODO: support for other ForeignKeyActions
            if (fmd != null && fmd.getDeleteAction() == ForeignKeyAction.CASCADE) {
                String memberClassName = mmd.getType().getCanonicalName();
            
                try {
                    // append the new ForeignKeyAction-Relation to the index key
                    
                    String indexKey = getForeignKeyActionIndexKey();
                    Transaction t = new Transaction(conn);
                    
                    JSONArray arr = null;
                    try {
                        arr = new JSONArray(t.read(indexKey).stringValue());
                    } catch (NotFoundException e) {
                        // no index key created yet
                        arr = new JSONArray();
                    }
                    JSONArray row = new JSONArray();
                    row.put(memberClassName);
                    row.put(className);
                    
                    // check if this entry already exists
                    boolean exists = false;
                    for (int i = 0; i < arr.length(); i++) {
                        JSONArray tmpRow = (JSONArray) arr.get(i);
                        if (tmpRow.getString(0).equals(row.getString(0)) &&
                                tmpRow.getString(1).equals(row.getString(1))) {
                            exists = true;
                            break;
                        }
                    }
                    if (!exists) {
                        arr.put(row);
                        t.write(indexKey, arr.toString());
                    }
                    t.commit();
                // TODO: proper exception handling
                } catch (JSONException e) {
                    e.printStackTrace();
                } catch (ConnectionException e) {
                    e.printStackTrace();
                } catch (UnknownException e) {
                    e.printStackTrace();
                } catch (AbortException e) {
                    e.printStackTrace();
                }
            }
        }
        return;
    }
}
