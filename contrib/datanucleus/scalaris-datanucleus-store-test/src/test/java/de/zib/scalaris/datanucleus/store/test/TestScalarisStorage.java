package de.zib.scalaris.datanucleus.store.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import javax.jdo.JDOObjectNotFoundException;

import org.junit.After;
import org.junit.Test;

import static de.zib.scalaris.datanucleus.store.test.StoreUtils.*;

public class TestScalarisStorage {

    @After
    public void after() {
        deleteAllInstances(Author.class, Product.class, Book.class,
                Inventory.class);
    }

    /**
     * Store a simple object (without relationships).
     */
    @Test
    public void test01Store() {
        Product product = new Product("Sony Discman",
                "A standard discman from Sony", 1.99);
        storeObject(product);
    }

    /**
     * Store a object with a relationship to another persistable object.
     */
    @Test
    public void test02Store() {
        Author author = new Author("Jrr");
        Book book = new Book("Lord of the Rings by Tolkien",
                "The classic story", 49.99, author, "12345678",
                "MyBooks Factory");
        storeObject(book);
    }

    /**
     * Store an object with 1-N relationships.
     */
    @Test
    public void test03Store() {
        Author author = new Author("Tolkien");
        Inventory inv = new Inventory("My Inventory");
        Product product = new Product("Sony Discman",
                "A standard discman from Sony", 200.00);
        Book book = new Book("Lord of the Rings by Tolkien",
                "The classic story", 49.99, author, "12345678",
                "MyBooks Factory");
        inv.add(product);
        inv.add(book);

        storeObject(inv);
    }

    /**
     * Retrieve a simple object (without relationships) by its ID.
     */
    @Test
    public void test01RetrieveById() {
        Product product = new Product("Sony Discman",
                "A standard discman from Sony", 1.99);
        Object productId = storeObject(product);

        Product retrieved = (Product) retrieveObjectById(productId);
        assertEquals(product, retrieved);
    }

    /**
     * Retrieve a simple object which inherits from another persistence class
     * and has relationships by its ID.
     */
    @Test
    public void test02RetrieveById() {
        Author author = new Author("JRRR Tolkien");
        Book book = new Book("Lord of the Rings by Tolkien",
                "The classic story", 49.99, author, "12345678",
                "MyBooks Factory");
        Object bookId = storeObject(book);

        Book retrieved = (Book) retrieveObjectById(bookId);
        assertEquals(book, retrieved);
    }

    /**
     * Retrieve an object with 1-N relationships by its ID.
     */
    @Test
    public void test03RetrieveById() {
        Inventory inv = new Inventory("Retrieval_Inventory");
        Product product = new Product("Sony Discman",
                "A standard discman from Sony", 200.00);
        Author author = new Author("JRR Tolkien");
        Book book = new Book("Lord of the Rings by Tolkien",
                "The classic story", 49.99, author, "12345678",
                "MyBooks Factory");
        inv.add(product);
        inv.add(book);
        Object inventoryId = storeObject(inv);

        // check child objects
        Product retrievedProduct = (Product) retrieveObjectBySingleKey(
                Product.class, product.getId());
        assertEquals(product, retrievedProduct);
        Book retrievedBook = (Book) retrieveObjectBySingleKey(Book.class,
                book.getId());
        assertEquals(book, retrievedBook);

        // check parent
        Inventory retrieved = (Inventory) retrieveObjectById(inventoryId);
        assertEquals(inv, retrieved);
    }

    /**
     * Retrieve an object by its primary key which consists of only one
     * attribute.
     */
    @Test
    public void test01RetrieveBySingleKey() {
        Product product = new Product("Sony Discman",
                "A standard discman from Sony", 1.99);
        storeObject(product);

        Product retrieved = (Product) retrieveObjectBySingleKey(
                product.getClass(), product.getId());
        assertEquals(product, retrieved);
    }

    /**
     * Try to retrieve an object by its primary key which does not exist.
     */
    @Test
    public void test02RetrieveBySingleKey() {
        Product product = new Product("Sony Discman",
                "A standard discman from Sony", 1.99);

        try {
            retrieveObjectBySingleKey(product.getClass(), product.getId());
            fail("Expected an expcetion to be thrown because the stored object does not exist");
        } catch (JDOObjectNotFoundException e) {
            // good
        }
    }

    /**
     * Delete an object by its ID
     */
    @Test
    public void test01DeleteById() {
        Author author = new Author("JRR");
        Book book = new Book("Lord", "The ", 49.99, author, "1234", "MyBooks");
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
    public void test01SingleFieldUpdate() {
        Author author = new Author("JRR2");
        Book book = new Book("Lord", "The ", 49.99, author, "1234", "MyBooks");
        Object bookId = storeObject(book);
        Book retrieved = (Book) retrieveObjectById(bookId);
        assertEquals(book, retrieved);

        retrieved.setPrice(100.99);
        Object updatedId = storeObject(retrieved);

        Book updated = (Book) retrieveObjectById(updatedId);
        assertEquals(retrieved, updated);
    }
}