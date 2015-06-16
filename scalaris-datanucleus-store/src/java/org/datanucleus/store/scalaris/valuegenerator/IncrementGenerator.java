/**********************************************************************
Copyright (c) 2011 Andy Jefferson and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors :
    ...
***********************************************************************/
package org.datanucleus.store.scalaris.valuegenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.valuegenerator.AbstractDatastoreGenerator;
import org.datanucleus.store.valuegenerator.ValueGenerationBlock;
import org.datanucleus.util.Localiser;

/**
 * Generator to store and allocate identity values.
 */
public class IncrementGenerator extends AbstractDatastoreGenerator<Long>
{
    static final String INCREMENT_KEY_SUFFIX = "increment";

    /** Key used in the Table to access the increment count */
    private String key;

    private String collectionName = null;

    /**
     * Constructor. Will receive the following properties (as a minimum) through this constructor.
     * <ul>
     * <li>class-name : Name of the class whose object is being inserted.</li>
     * <li>root-class-name : Name of the root class in this inheritance tree</li>
     * <li>field-name : Name of the field with the strategy (unless datastore identity field)</li>
     * <li>catalog-name : Catalog of the table (if specified)</li>
     * <li>schema-name : Schema of the table (if specified)</li>
     * <li>table-name : Name of the root table for this inheritance tree (containing the field).</li>
     * <li>column-name : Name of the column in the table (for the field)</li>
     * <li>sequence-name : Name of the sequence (if specified in MetaData as "sequence)</li>
     * </ul>
     * @param name Symbolic name for this generator
     * @param props Properties controlling the behaviour of the generator (or null if not required).
     */
    public IncrementGenerator(String name, Properties props)
    {
        super(name, props);
        this.key = properties.getProperty("field-name", name);
        this.collectionName = properties.getProperty("sequence-table-name");
        if (this.collectionName == null)
        {
            this.collectionName = "IncrementTable";
        }
        if (properties.containsKey("key-cache-size"))
        {
            allocationSize = Integer.valueOf(properties.getProperty("key-cache-size"));
        }
        else
        {
            allocationSize = 1;
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.valuegenerator.AbstractGenerator#reserveBlock(long)
     */
    protected ValueGenerationBlock<Long> reserveBlock(long size)
    {
        if (size < 1)
        {
            return null;
        }

        List<Long> oids = new ArrayList<Long>();
        oids.add(12321l);
        return new ValueGenerationBlock<Long>(oids);
    }
}