package de.zib.scalaris.datanucleus.store.test;

import static org.junit.Assert.*;
import static de.zib.scalaris.datanucleus.store.test.StoreUtils.*;

import java.util.List;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressWarnings("rawtypes")
public class TestScalarisQuery extends ScalarisTestBase {
	
	private static Product discman = new Product(1, "Sony Discman", "A standard discman from Sony", 1.99);
	
	@BeforeClass
	public static void setup() {
		Product[] products = {
			discman,
			new Product(2, "Sony Xperia Z1", "A fancy smartphone", 200.3),
			new Product(3, "Sony Xperia Z2", "Another smartphone", 300.1),
			new Product(4, "Sony Xperia Z3", "Yet another smartphone", 400.2),
			new Product(5, "Sony Xperia Z1 Compact", "A compact smartphone", 250.7),
		};
		Inventory invSony = new Inventory("Sony");
		invSony.addAll(products);
		storeObject(invSony);
		
		Author tolkien = new Author("JRR Tolkien");
		Author lovecraft = new Author("H. P. LoveCraft");
		Object[] books = {
				new Book(1, "Lord of the rings 1", "Stuff happens", 56.00, tolkien, "111111", "PublisherA"),
				new Book(2, "Lord of the rings 2", "More Stuff happens", 23.00, tolkien, "22222", "PublisherB"),
				new Book(3, "Cthulhu", "horror stuff", 19.99, lovecraft, "43433", "PublisherC")
		};
		storeObjects(books);
	}
	
	@AfterClass
	public static void tearDown() {
		deleteAllInstances(Author.class, Product.class, Book.class, Inventory.class);
	}
	
	@Test
	public void testQueryProductByName() {
		PersistenceManager pm = getNewPersistenceManager();
		Query q = pm.newQuery(Product.class, "name == 'Sony Discman'");
		List result = (List) q.execute();
		assertEquals(1, result.size());
		
		Product p = (Product) result.get(0);
		assertEquals("Sony Discman", p.name);
		
		assertEquals(discman, p);
	}
	
	@Test
	public void testQueryOrderByPrice() {
		PersistenceManager pm = getNewPersistenceManager();
		Query q = pm.newQuery(Product.class);
		q.setOrdering("price ascending");
		List result = (List) q.execute();
		assertEquals(5, result.size());
		
		// check if order is correct
		double lastSeenPrice = -1;
		for (int i = 0; i < result.size(); i++) {
			Product p = (Product) result.get(i);
			assertTrue(lastSeenPrice <= p.getPrice());
			lastSeenPrice = p.getPrice();
		}
	}
}
