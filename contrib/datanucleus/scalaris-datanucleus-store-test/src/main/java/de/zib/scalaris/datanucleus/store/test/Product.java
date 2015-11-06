/**********************************************************************
Copyright (c) 2003 Andy Jefferson and others. All rights reserved.
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

import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

/**
 * Definition of a Product Represents a product, and contains the key aspects of
 * the item.
 */
@PersistenceCapable
public class Product {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private long id;

    protected String name = null;

    protected String description = null;

    protected double price = 0.0;

    public Product(String name, String description, double price) {
        this.name = name;
        this.description = description;
        this.price = price;
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public double getPrice() {
        return price;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public String toString() {
        return "Product : " + id + " name=" + name + " [" + description + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Product) {
            Product other = (Product) o;
            return other.price == price
                    && other.description.equals(description)
                    && name.equals(other.name);
        }
        return false;
    }

    @Override
    public int hashCode() {
        int descHash = description != null? description.hashCode() : 0;
        int nameHash = name != null? name.hashCode() : 0;

        return descHash + nameHash + ((int) price);
    }
}