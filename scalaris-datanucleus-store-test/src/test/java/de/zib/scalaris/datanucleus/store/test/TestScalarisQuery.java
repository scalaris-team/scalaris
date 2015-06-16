package de.zib.scalaris.datanucleus.store.test;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

public class TestScalarisQuery {
	
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
		// TODO: Clear datastore
	}	
}
