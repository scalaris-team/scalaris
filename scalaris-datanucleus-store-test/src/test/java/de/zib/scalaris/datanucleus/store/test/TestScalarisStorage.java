package de.zib.scalaris.datanucleus.store.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import javax.jdo.JDOHelper;
import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Transaction;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runners.Suite.SuiteClasses;

import de.zib.scalaris.datanucleus.store.test.Product;


@SuiteClasses({
    TestScalarisStorage.class
})


public class TestScalarisStorage {
	
	private static final String PERSISTENCE_UNIT_NAME = "Scalaris_Test";
	
	@Rule
	public TestName testName = new TestName();
	
	@Before
	public void before() {
		System.out.println("\n################");
		System.out.printf("Starting test: %s\n\n", testName.getMethodName());
	}
	
	@After
	public void after() {
		// TODO: Clear datastore
	}
	
	/**
	 * Store a simple object (without relationships).
	 */
	@Test
	public void test_01_scalaris_store() {
		Product product = new Product(1, "Sony Discman", "A standard discman from Sony", 1.99);
		storeObject(product);
	}
	
	/**
	 * Store a object with a relationship to another persistable object. 
	 */
	@Test
	public void test_02_scalaris_store() {
		Author author = new Author("Jrr");
		Book book = new Book(2, "Lord of the Rings by Tolkien","The classic story",49.99, author, "12345678", "MyBooks Factory");
		storeObject(book);
	}
	
	/**
	 * Store an object with 1-N relationships.
	 */
	@Test
	public void test_03_scalaris_store() {
		Author author = new Author("Tolkien");
        Inventory inv = new Inventory("My Inventory");
        Product product = new Product(20, "Sony Discman","A standard discman from Sony",200.00);
        Book book = new Book(21, "Lord of the Rings by Tolkien","The classic story",49.99, author, "12345678", "MyBooks Factory");
        inv.add(product);
        inv.add(book);
        
		storeObject(inv);
	}
	
	/**
	 * Retrieve a simple object (without relationships) by its ID.
	 */
	@Test
	public void test_01_scalaris_retrieve_by_id() {
		Product product = new Product(100, "Sony Discman", "A standard discman from Sony", 1.99);
		Object productId = storeObject(product);
		
		Product retrieved = (Product) retrieveObjectById(productId);
		assertEquals(product, retrieved);
	}
	
	/**
	 * Retrieve a simple object which inherits from another persistence class and has relationships 
	 * by its ID.
	 */
	@Test
	public void test_02_scalaris_retrieve_by_id() {
		Author author = new Author("JRRR Tolkien");
		Book book = new Book(42, "Lord of the Rings by Tolkien","The classic story",49.99, author, "12345678", "MyBooks Factory");
		Object bookId = storeObject(book);
		
		Book retrieved = (Book) retrieveObjectById(bookId);
		assertEquals(book, retrieved);
	}
	
	/**
	 * Retrieve an object with 1-N relationships by its ID.
	 */
	@Test
	public void test_03_scalaris_retrieve_by_id() {
		int prodKey = 301;
		int bookKey = 302;
		
        Inventory inv = new Inventory("Retrieval_Inventory");
        Product product = new Product(prodKey, "Sony Discman","A standard discman from Sony",200.00);
        Author author = new Author("JRR Tolkien");
        Book book = new Book(bookKey, "Lord of the Rings by Tolkien","The classic story",49.99, author , "12345678", "MyBooks Factory");
        inv.add(product);
        inv.add(book);
        Object inventoryId = storeObject(inv);
        
        
        // check child objects
        Product retrievedProduct = (Product) retrieveObjectBySingleKey(Product.class, prodKey);
        assertEquals(product, retrievedProduct);
        Book retrievedBook = (Book) retrieveObjectBySingleKey(Book.class, bookKey);
        assertEquals(book, retrievedBook);
        
        // check parent
        Inventory retrieved = (Inventory) retrieveObjectById(inventoryId);      
        assertEquals(inv, retrieved);
	}
	
	/**
	 * Retrieve an object by its primary key which consists of only one attribute.
	 */
	@Test
	public void test_01_scalaris_retrieve_by_single_key() {
		int keyValue = 501;
		Product product = new Product(keyValue, "Sony Discman", "A standard discman from Sony", 1.99);
		storeObject(product);
		
		Product retrieved = (Product) retrieveObjectBySingleKey(product.getClass(), keyValue);
		assertEquals(product, retrieved);
	}
	
	/**
	 * Try to retrieve an object by its primary key which does not exist.
	 */
	@Test
	public void test_02_scalaris_retrieve_by_single_key() {
		int keyValue = 502;
		// it is not stored
		Product product = new Product(keyValue, "Sony Discman", "A standard discman from Sony", 1.99);

		try {
			retrieveObjectBySingleKey(product.getClass(), keyValue);
			fail("Expected Expcetion because the stored object does not exist"); 
		} catch (JDOObjectNotFoundException e) {
			// good
		}
	}
	
	/**
	 * Delete an object by its ID
	 */
	@Test
	public void test_01_scalaris_delete_by_id() {
		Author author = new Author("JRR");
		Book book = new Book(200, "Lord","The ",49.99, author, "1234", "MyBooks");
		Object bookId = storeObject(book);
		
		deleteObjectById(bookId);
		
		try {
			retrieveObjectById(bookId);
			fail("Excepted JDOObjectNotFoundException");
		} catch (JDOObjectNotFoundException e) {
			// if we are here everything worked fine
		}
	}

	/**
	 * Update one field of a stored object
	 */
	@Test
	public void test_01_scalaris_single_field_update() {
		Author author = new Author("JRR2");
		Book book = new Book(600, "Lord","The ",49.99, author, "1234", "MyBooks");
		Object bookId = storeObject(book);
		
		Book retrieved  = (Book) retrieveObjectById(bookId);
		assertEquals(book, retrieved);
		
		retrieved.setPrice(100.99);
		Object updatedId = storeObject(retrieved);
		
		Book updated = (Book) retrieveObjectById(updatedId);
		assertEquals(retrieved, updated);
	}
	
	/*
	 * Store an object in the data store. 
	 * Returns the identity of the stored object.
	 */
	private Object storeObject(Object o) {
		PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(PERSISTENCE_UNIT_NAME);
		PersistenceManager pm = pmf.getPersistenceManager();
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
	
	/*
	 * Retrieve an object stored in the data store by its ID.
	 * Can throw exceptions if unsuccessful (e.g. JDOObjectNotFoundException).
	 */
	private Object retrieveObjectById(Object objectId) {
		PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(PERSISTENCE_UNIT_NAME);
		PersistenceManager pm = pmf.getPersistenceManager();
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
	
	/*
	 * This method can be used to retrieve an object by its primary key if it consists of only
	 * one attribute. Can throw exceptions if unsuccessful (e.g. JDOObjectNotFoundException).
	 */
	private Object retrieveObjectBySingleKey(Class<?> objectClass, Object keyValue) {
		PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(PERSISTENCE_UNIT_NAME);
		PersistenceManager pm = pmf.getPersistenceManager();
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
	
	/*
	 * Delete an object stored in the data store by its ID.
	 * Can throw exceptions if unsuccessful (e.g. JDOObjectNotFoundException).
	 */
	private void deleteObjectById(Object objectId) {
		PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(PERSISTENCE_UNIT_NAME);
		PersistenceManager pm = pmf.getPersistenceManager();
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