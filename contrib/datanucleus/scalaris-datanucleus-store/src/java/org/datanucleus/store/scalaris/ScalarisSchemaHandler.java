package org.datanucleus.store.scalaris;

import org.datanucleus.store.StoreManager;
import org.datanucleus.store.schema.AbstractStoreSchemaHandler;

public class ScalarisSchemaHandler extends AbstractStoreSchemaHandler {

    /**
     * Used to signal a key at which all foreign key actions for an object are stored.
     */
    private static final String FKA_KEY_SUFFIX = "FKA";

    /**
     * Used to signal a key at which the IDs of all persisted objects of the same
     * class are stored.
     */
    private static final String ALL_ID_SUFFIX = "ALL_IDS";

    /**
     * Key prefix used to signal a key which is used for identity generation.
     */
    private static final String ID_GEN_SUFFIX = "ID_GEN";

    /**
     * Used to signal a key at which the persisted object ID is stored which
     * belongs to a given unique member value. 
     */
    private static final String UNIQUE_MEMBER_SUFFIX = "UNIQUE";


    public ScalarisSchemaHandler(StoreManager storeMgr) {
        super(storeMgr);
    }

    /**
     * Returns the Scalaris key used to store object with full class name 
     * objClassName and ID objectId.
     * @param objClassName 
     *      The full class name of the object
     * @param objectId 
     *      The ID of the object
     * @return The Scalaris key
     */
    public static String getObjectStorageKey(String objClassName, String objectId) {
        if (objectId.startsWith(objClassName)) {
            return objectId;
        }
        return String.format("%s:%s", objClassName, objectId);
    }

    /**
     * Returns the Scalaris key at which the IDs of all persisted 
     * objects of a given class are stored.
     * @param clazz 
     *      The objects class
     * @return The Scalaris key
     */
    static String getIDIndexKeyName(Class<?> clazz) {
        return getIDIndexKeyName(clazz.getCanonicalName());
    }

    /**
     * Returns the Scalaris key at which the IDs of all persisted 
     * objects of a given class are stored.
     * @param clazz 
     *      The objects class
     * @return The Scalaris key
     */
    static String getIDIndexKeyName(String className) {
        return String.format("%s_%s", className, ALL_ID_SUFFIX);
    }
 
    /**
     * Returns the Scalaris key used for generating IDs for new object instances
     * persisted in the data store.
     * @param className 
     *      The full class name of the object the ID will be generated for.
     * @return The Scalaris key
     */
    static String getIDGeneratorKeyName(String className) {
        return String.format("%s_%s", className, ID_GEN_SUFFIX);
    }

    /**
     * Returns the Scalaris key used to store which persisted object
     * has stored the given unique value.
     * @param className
     *      Class of the object with the unique member annotation
     * @param memberName
     *      Simple member name which has an 'unique' annotation
     * @param memberValue
     *      The unique value of the member
     * @return The Scalaris key
     */
    static String getUniqueMemberKey(String className, String memberName, String memberValue) {
        return String.format("%s_%s_%s_%s", className, memberName, memberValue, UNIQUE_MEMBER_SUFFIX);
    }

    /**
     * Returns the Scalaris key used to store all foreign key actions
     * of the given object.
     * @param foreignObjectClass
     *      Class of the object
     * @param foreignObjectId
     *      ID of the object
     * @return The Scalaris key
     */
    static String getForeignKeyActionKey(String foreignObjectClass, String foreignObjectId) {
        String storageKey = getObjectStorageKey(foreignObjectClass, foreignObjectId);
        return String.format("%s_%s", storageKey, FKA_KEY_SUFFIX);
    }
}
