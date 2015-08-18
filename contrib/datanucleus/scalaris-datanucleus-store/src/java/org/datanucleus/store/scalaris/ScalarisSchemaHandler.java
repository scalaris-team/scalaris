package org.datanucleus.store.scalaris;

import org.datanucleus.store.StoreManager;
import org.datanucleus.store.schema.AbstractStoreSchemaHandler;

public class ScalarisSchemaHandler extends AbstractStoreSchemaHandler {
    
    
    static final String FKA_DELETE_OBJ = "_DEL_OBJECT";
    
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
    
    static String getObjectStorageKey(String objClassName, String objectId) {
        if (objectId.startsWith(objClassName)) {
            return objectId;
        }
        return String.format("%s:%s", objClassName, objectId);
    }
    
    static String getIDIndexKeyName(Class<?> clazz) {
        return getIDIndexKeyName(clazz.getCanonicalName());
    }

    static String getIDIndexKeyName(String className) {
        return String.format("%s_%s", className, ALL_ID_PREFIX);
    }

    static String getIDGeneratorKeyName(String className) {
        return String.format("%s_%s", className, ID_GEN_PREXIF);
    }
    
    static String getUniqueMemberKey(String className, String memberName, String memberValue) {
        return String.format("%s_%s_%s_%s", className, memberName, memberValue, UNIQUE_MEMBER_PREFIX);
    }
    
    public static String getForeignKeyActionKey(String foreignObjectClass, String foreignObjectId) {
        String storageKey = getObjectStorageKey(foreignObjectClass, foreignObjectId);
        return String.format("%s_%s", storageKey, FKA_KEY_PREFIX);
    }
}
