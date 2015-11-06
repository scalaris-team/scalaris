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

import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;

/**
 * Definition of a Book. Extends basic Product class.
 */
@PersistenceCapable
public class Book extends Product {

    @Persistent(defaultFetchGroup = "true")
    protected Author author = null;

    protected String isbn = null;

    protected String publisher = null;

    public Book(String name, String description, double price, Author author,
            String isbn, String publisher) {
        super(name, description, price);
        this.author = author;
        this.isbn = isbn;
        this.publisher = publisher;
    }

    public Author getAuthor() {
        return author;
    }

    public String getIsbn() {
        return isbn;
    }

    public String getPublisher() {
        return publisher;
    }

    public void setAuthor(Author author) {
        this.author = author;
    }

    public void setIsbn(String isbn) {
        this.isbn = isbn;
    }

    public void setPublisher(String publisher) {
        this.publisher = publisher;
    }

    public String toString() {
        return "Book : " + author + " - " + name;
    }

    public boolean equals(Object o) {
        if (o instanceof Book && super.equals(o)) {
            Book other = (Book) o;

            if (other.isbn.equals(isbn) && other.publisher.equals(publisher)) {

                if (author == null && other.author == null)
                    return true;
                if (author == null || other.author == null)
                    return false;

                return other.author.equals(author);
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        int authorHash = author != null? author.hashCode() : 0;
        int isbnHash = isbn != null ? isbn.hashCode() : 0;
        int publisherHash = publisher != null? publisher.hashCode() : 0;

        return super.hashCode() + authorHash + isbnHash + publisherHash;
    }
}