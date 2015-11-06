package de.zib.scalaris.datanucleus.store.test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;
import javax.jdo.Transaction;

public class StoreUtils {

    public static final String PERSISTENCE_UNIT_NAME = "Scalaris_Test";

    // For some reason Datanucleus complains sometimes that this property is
    // not set and fails.
    static {
        Properties props = System.getProperties();
        props.setProperty("javax.jdo.option.PersistenecUnitName", PERSISTENCE_UNIT_NAME);
    }

    /**
     * Returns a new PersistenceManager of persistence unit
     * PERSISTANCE_UNIT_NAME
     * 
     * @return a new PersistenceManager
     */
    public static PersistenceManager getNewPersistenceManager() {
        Map<String, String> propOverrides = new HashMap<>();
        String scalarisNodeOverride = System.getProperty("scalaris.node");
        if (scalarisNodeOverride != null) {
            propOverrides.put("scalaris.node", scalarisNodeOverride);
        }

        PersistenceManagerFactory pmf = JDOHelper
                .getPersistenceManagerFactory(propOverrides, PERSISTENCE_UNIT_NAME);
        return pmf.getPersistenceManager();
    }

    /**
     * Delete all instances stored in the data store of the passed candidate
     * classes
     * 
     * @param clazzes
     *            Candidate classes used for deletion.
     * @return Number of deleted elements.
     */
    public static long deleteAllInstances(Class<?>... clazzes) {
        long deleted = 0;

        for (Class<?> clazz : clazzes) {
            PersistenceManager pm = getNewPersistenceManager();
            Query q = pm.newQuery(clazz);
            deleted += q.deletePersistentAll();
        }
        return deleted;
    }

    /*
     * Utility to store multiple objects. The order in which objects are stored
     * is not guaranteed.
     */
    public static void storeObjects(Object... o) {
        for (Object obj : o) {
            storeObject(obj);
        }
    }

    /**
     * Store an object in the data store. Returns the identity of the stored
     * object.
     */
    public static Object storeObject(Object o) {
        PersistenceManager pm = getNewPersistenceManager();
        Transaction tx = pm.currentTransaction();

        Object objectID;
        try {
            tx.begin();
            pm.makePersistent(o);
            objectID = pm.getObjectId(o);
            tx.commit();
        } finally {
            if (tx.isActive()) {
                tx.rollback();
            }
            pm.close();
        }
        return objectID;
    }

    /**
     * Retrieve an object stored in the data store by its ID. Can throw
     * exceptions if unsuccessful (e.g. JDOObjectNotFoundException).
     */
    public static Object retrieveObjectById(Object objectId) {
        PersistenceManager pm = getNewPersistenceManager();
        Transaction tx = pm.currentTransaction();

        Object retrieved;
        try {
            tx.begin();
            retrieved = pm.getObjectById(objectId);
            tx.commit();
        } finally {
            if (tx.isActive()) {
                tx.rollback();
            }
            pm.close();
        }
        return retrieved;
    }

    /**
     * This method can be used to retrieve an object by its primary key if it
     * consists of only one attribute. Can throw exceptions if unsuccessful
     * (e.g. JDOObjectNotFoundException).
     */
    public static Object retrieveObjectBySingleKey(Class<?> objectClass,
            Object keyValue) {
        PersistenceManager pm = getNewPersistenceManager();
        Transaction tx = pm.currentTransaction();

        Object retrieved;
        try {
            tx.begin();
            retrieved = pm.getObjectById(objectClass, keyValue);
            tx.commit();
        } finally {
            if (tx.isActive()) {
                tx.rollback();
            }
            pm.close();
        }
        return retrieved;
    }

    /**
     * Delete an object stored in the data store by its ID. Can throw exceptions
     * if unsuccessful (e.g. JDOObjectNotFoundException).
     */
    public static void deleteObjectById(Object objectId) {
        PersistenceManager pm = getNewPersistenceManager();
        Transaction tx = pm.currentTransaction();
        try {
            tx.begin();
            Object retrieved = pm.getObjectById(objectId);
            pm.deletePersistent(retrieved);
            tx.commit();
        } finally {
            if (tx.isActive()) {
                tx.rollback();
            }
            pm.close();
        }
    }
}
