package de.zib.scalaris.datanucleus.store.test;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;
import javax.jdo.Transaction;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

public class ScalarisTestBase {

	
	protected static final String PERSISTENCE_UNIT_NAME = "Scalaris_Test";
	
	@Rule
	public TestName testName = new TestName();
	
	@Before
	public void before() {
		System.out.println("\n################");
		System.out.printf("Starting test: %s\n\n", testName.getMethodName());
	}
	
	@After
	public void after() {
		deleteAllInstances(Author.class, Product.class, Book.class, Inventory.class);
	}
	
	/**
	 * Returns a new PersistenceManager of persistence unit PERSISTANCE_UNIT_NAME
	 * @return a new PersistenceManager
	 */
	public PersistenceManager getNewPersistenceManager() {
		PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(PERSISTENCE_UNIT_NAME);
		return pmf.getPersistenceManager();
	}
	
	/**
	 * Delete all instances stored in the data store of the passed candidate classes
	 * @param clazzes
	 * 		Candidate classes used for deletion.
	 * @return 
	 * 		Number of deleted elements.
	 */
	public long deleteAllInstances(Class<?>... clazzes) {
		long deleted = 0;
		
		for (Class<?> clazz : clazzes) {
			PersistenceManager pm = getNewPersistenceManager();
			Query q = pm.newQuery(clazz);
			deleted += q.deletePersistentAll();	
		}
		return deleted;
	}
	
	/**
	 * Store an object in the data store. 
	 * Returns the identity of the stored object.
	 */
	public Object storeObject(Object o) {
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
	 * Retrieve an object stored in the data store by its ID.
	 * Can throw exceptions if unsuccessful (e.g. JDOObjectNotFoundException).
	 */
	public Object retrieveObjectById(Object objectId) {
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
	 * This method can be used to retrieve an object by its primary key if it consists of only
	 * one attribute. Can throw exceptions if unsuccessful (e.g. JDOObjectNotFoundException).
	 */
	public Object retrieveObjectBySingleKey(Class<?> objectClass, Object keyValue) {
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
	 * Delete an object stored in the data store by its ID.
	 * Can throw exceptions if unsuccessful (e.g. JDOObjectNotFoundException).
	 */
	public void deleteObjectById(Object objectId) {
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
