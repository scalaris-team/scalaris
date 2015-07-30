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
    
    
    static final String FKA_DELETE_OBJ = "_DEL_OBJECT";
    
    /**
     * This key stores all foreign key actions.
     */
    private static final String FKA_INDEX_KEY = "FKA_INDEX";
    
    /**
     * Used to signal a key in which all instances of a single foreign key action is stored.
     */
    private static final String FKA_KEY_PREFIX = "FKA";
    
    /**
     * Key prefix used to signal a key in which a collection of all key IDs of the same 
     * type is stored. This is necessary for queries which need access to all stored
     * instances of the same type.
     */
    private static final String ALL_ID_PREFIX = "ALL_IDS";
    
    /**
     * Key prefix used to signal a key which is used for identity generation. Its value
     * is an integer which is incremented every time an ID is generated.
     */
    private static final String ID_GEN_PREXIF = "ID_GEN";
    
    /**
     * Key prefix used to store values of members which are marked as "@Unique".
     * For each stored value two of these keys are needed.
     * 1. classname:member:value -> id-of-instance-storing-this-value 
     *      When an object with an Unique member is inserted into the data store,
     *      this key is used to check if this value already exists
     * 2. id-of-instance-storing-this-value:member -> value 
     *      Needed to find the first key when updating/deleting the value,
     *      since the old value might not be accessible anymore
     */
    private static final String UNIQUE_MEMBER_PREFIX = "UNIQUE";
    
    
    public ScalarisSchemaHandler(StoreManager storeMgr) {
        super(storeMgr);
    }
    /**
     * The following methods can be used to generate keys with special meaning which are needed
     * to provide functionality not natively supported by Scalaris. See doc of the constants for meaning of 
     * these keys.
     **/
    
    static String getManagementKeyName(Class<?> clazz) {
        return getManagementKeyName(clazz.getCanonicalName());
    }

    static String getManagementKeyName(String className) {
        return String.format("%s_%s", className, ALL_ID_PREFIX);
    }

    static String getIDGeneratorKeyName(String className) {
        return String.format("%s_%s", className, ID_GEN_PREXIF);
    }
    
    static String getUniqueMemberValueToIdKeyName(String className, String memberName, String memberValue) {
        return String.format("%s_%s_%s_%s", className, memberName, memberValue, UNIQUE_MEMBER_PREFIX);
    }
    
    static String geIdToUniqueMemberValueKeyName(String objectId, String memberName) {
        return String.format("%s_%s_%s", objectId, memberName, UNIQUE_MEMBER_PREFIX);
    }
    
    public static String getForeignKeyActionIndexKey() {
        return FKA_INDEX_KEY;
    }
    public static String getForeignKeyActionKey(String foreignClassName, String thisClassName, String inMember) {
        return String.format("%s_%s_%s_%s", foreignClassName, thisClassName, inMember, FKA_KEY_PREFIX);
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
    
    /**
     * This method should only be called once for each class containing foreign key actions.
     * It will search the passed ClassMetaData for ForeignKeyAction annotations and updates the 
     * FKA_INDEX_KEY accordingly.
     * Currently only deleteAction = ForeignKeyACtion.CASCADE is supported (not inside a join) 
     * @param cmd MetaData of the class used to update the index.
     * @param conn Scalaris Connection used for the necessary transaction.
     */
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
            
            boolean isJoin = false;
            if (mmd.getJoinMetaData() != null) {
                // The member is a collection with an ForeignKeyAction attached
                fmd = mmd.getJoinMetaData().getForeignKeyMetaData();
                isJoin = true;
            }
            
            // TODO: support for other ForeignKeyActions
            if (fmd != null && fmd.getDeleteAction() == ForeignKeyAction.CASCADE) {
                String memberClassName;
                if (isJoin) {
                    // the member in the collection is important 
                    memberClassName = mmd.getCollection().getElementType();
                } else {
                    memberClassName = mmd.getType().getCanonicalName();
                }
            
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
                    
                    if (isJoin) {
                        row.put(mmd.getName());
                    } else {
                        row.put(FKA_DELETE_OBJ);
                    }
                    
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
