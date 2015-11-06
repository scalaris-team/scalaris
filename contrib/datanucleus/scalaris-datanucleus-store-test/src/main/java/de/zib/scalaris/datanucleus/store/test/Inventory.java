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

Contributors:
    ...
 **********************************************************************/
package de.zib.scalaris.datanucleus.store.test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

/**
 * Definition of an Inventory of products.
 */
@PersistenceCapable
public class Inventory {

    @PrimaryKey
    protected String name = null;

    @Persistent(defaultFetchGroup = "true")
    protected Set<Product> products = new HashSet<Product>();

    public Inventory(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public Set<Product> getProducts() {
        return products;
    }

    public void addAll(Product... p) {
        for (Product prod : p) {
            add(prod);
        }
    }

    public void add(Product p) {
        products.add(p);
    }

    public String toString() {
        StringBuilder prodString = new StringBuilder("null");
        if (products != null) {
            Product[] prod = products.toArray(new Product[0]);
            prodString = new StringBuilder("[");
            for (Product p : prod) {
                prodString.append("{").append(p.toString()).append("},");
            }
            prodString.append("]");
        }
        return "Inventory: " + name + "; Products: " + prodString;
    }

    public boolean equals(Object o) {
        if (o instanceof Inventory) {
            Inventory other = (Inventory) o;
            if (!name.equals(other.name))
                return false;

            // compare their products
            if (products == null && other.products == null)
                return true;
            if (products == null || other.products == null)
                return false;

            Product[] prods1 = products.toArray(new Product[0]);
            Product[] prods2 = other.products.toArray(new Product[0]);

            return Arrays.asList(prods1).containsAll(other.products)
                    && Arrays.asList(prods2).containsAll(products);
        }
        return false;
    }

    @Override
    public int hashCode() {
        int hash = name.hashCode();
        if (products == null) return hash;

        for (Product p : products) {
            hash += p.hashCode();
        }
        return hash;
    }
}