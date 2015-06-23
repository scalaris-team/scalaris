package de.zib.scalaris.datanucleus.store.test;

import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

public class ScalarisTestBase {

    @Rule
    public TestName testName = new TestName();

    @Before
    public void before() {
        System.out.println("\n################");
        System.out.printf("Starting test: %s\n\n", testName.getMethodName());
    }
}
