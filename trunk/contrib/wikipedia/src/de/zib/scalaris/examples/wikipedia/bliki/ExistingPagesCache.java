/**
 *  Copyright 2013 Zuse Institute Berlin
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package de.zib.scalaris.examples.wikipedia.bliki;

import java.security.InvalidParameterException;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Set;

import com.skjegstad.utils.BloomFilter;

import de.zib.scalaris.examples.wikipedia.Options;
import de.zib.scalaris.examples.wikipedia.bliki.MyNamespace.NamespaceEnum;

/**
 * Base class for a cache for set of the existing pages. This class provides no
 * cached list - use {@link #createCache(Collection)} to get an actual cache
 * implementation.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class ExistingPagesCache {
    /**
     * Empty cache implementation which does not add elements when
     * {@link #add(NormalisedTitle)} or {@link #addAll(Collection)} are called.
     */
    public static ExistingPagesCache NULL_CACHE = new ExistingPagesCache();
    
    /**
     * Constructor
     */
    private ExistingPagesCache() {
    }

    /**
     * Gets an empty cache implementation.
     * 
     * @param size
     *            the size of the cache to create
     * 
     * @return a pages cache of the given size
     */
    public static ExistingPagesCache createCache(int size) {
        Class<? extends ExistingPagesCache> clazz = Options.getInstance().WIKI_PAGES_CACHE_IMPL;
        // note: cannot use reflection as the constructors are not public (and should not be)
        if (clazz.equals(ExistingPagesCacheBloom.class)) {
            return new ExistingPagesCacheBloom(size);
        } else if (clazz.equals(ExistingPagesCacheFull.class)) {
            return new ExistingPagesCacheFull(size);
        } else {
            throw new InvalidParameterException("unknown pages cache class: " + clazz.getCanonicalName());
        }
    }

    /**
     * Gets an cache implementation with the given elements.
     * 
     * @param elements
     *            the elements to add to the cache
     * 
     * @return a pages cache
     */
    public static ExistingPagesCache createCache(
            Collection<? extends NormalisedTitle> elements) {
        Class<? extends ExistingPagesCache> clazz = Options.getInstance().WIKI_PAGES_CACHE_IMPL;
        // note: cannot use reflection as the constructors are not public (and should not be)
        if (clazz.equals(ExistingPagesCacheBloom.class)) {
            return new ExistingPagesCacheBloom(elements);
        } else if (clazz.equals(ExistingPagesCacheFull.class)) {
            return new ExistingPagesCacheFull(elements);
        } else {
            throw new InvalidParameterException("unknown pages cache class: " + clazz.getCanonicalName());
        }
    }

    /**
     * Add the given page title to the pages cache.
     * 
     * @param element
     *            page title to add
     */
    public void add(NormalisedTitle element) {
    }

    /**
     * Adds all elements from a Collection to the pages cache.
     * 
     * @param elements
     *            collection of elements
     */
    public void addAll(Collection<? extends NormalisedTitle> elements) {
    }

    /**
     * Gets whether the pages cache implementation supports
     * {@link #contains(NormalisedTitle)}.
     * 
     * @return support for {@link #contains(NormalisedTitle)}
     */
    public boolean hasContains() {
        return false;
    }
    
    /**
     * Checks whether the given title is contained in the cache.
     * 
     * Be sure to check for this capability with {@link #hasContains()}!
     * 
     * @param element
     *            the page title to check
     * 
     * @return whether the title is in the cache or not
     * 
     * @see #hasContains()
     */
    public boolean contains(NormalisedTitle element) {
        return false;
    }

    /**
     * Gets whether the pages cache implementation supports
     * {@link #getList(NamespaceEnum)}.
     * 
     * @return support for {@link #getList(NamespaceEnum)}
     */
    public boolean hasFullList() {
        return false;
    }
    
    /**
     * Gets the page titles in the given namespace.
     * 
     * Be sure to check for this capability with {@link #hasFullList()}!
     * 
     * @param ns
     *            the namespace to get page titles for
     * 
     * @return a set of page titles
     * 
     * @see #hasFullList()
     */
    public Set<NormalisedTitle> getList(NamespaceEnum ns) {
        return new HashSet<NormalisedTitle>(0);
    }

    /**
     * Existing pages cache using bloom filters.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    public static class ExistingPagesCacheBloom extends ExistingPagesCache {
        /**
         * False positive rate of the bloom filter for the existing pages checks.
         */
        protected static final double existingPagesFPR = 0.1;

        protected final BloomFilter<NormalisedTitle> bloom;

        protected ExistingPagesCacheBloom(int size) {
            this.bloom = new BloomFilter<NormalisedTitle>(existingPagesFPR, size);
        }

        protected ExistingPagesCacheBloom(
                Collection<? extends NormalisedTitle> elements) {
            this(Math.max(100, elements.size() + Math.max(10, elements.size() / 10)));
            bloom.addAll(elements);
        }

        @Override
        public void add(NormalisedTitle element) {
            bloom.add(element);
        }

        @Override
        public void addAll(Collection<? extends NormalisedTitle> elements) {
            bloom.addAll(elements);
        }

        @Override
        public boolean hasContains() {
            return true;
        }
        
        @Override
        public boolean contains(NormalisedTitle element) {
            return bloom.contains(element);
        }
    }

    /**
     * Existing pages cache using a hash set to cache the full list.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    public static class ExistingPagesCacheFull extends ExistingPagesCache {
        protected final EnumMap<NamespaceEnum, Set<NormalisedTitle>> cache = new EnumMap<NamespaceEnum, Set<NormalisedTitle>>(
                NamespaceEnum.class);

        protected ExistingPagesCacheFull(int size) {
            for (NamespaceEnum ns : NamespaceEnum.values()) {
                cache.put(ns, new HashSet<NormalisedTitle>());
            }
        }

        protected ExistingPagesCacheFull(
                Collection<? extends NormalisedTitle> elements) {
            this(elements.size());
            addAll(elements);
        }

        @Override
        public void add(NormalisedTitle element) {
            cache.get(NamespaceEnum.fromId(element.namespace)).add(element);
        }

        @Override
        public void addAll(Collection<? extends NormalisedTitle> elements) {
            for (NormalisedTitle element : elements) {
                add(element);
            }
        }

        @Override
        public boolean hasContains() {
            return true;
        }
        
        @Override
        public boolean contains(NormalisedTitle element) {
            return cache.get(NamespaceEnum.fromId(element.namespace)).contains(element);
        }

        @Override
        public boolean hasFullList() {
            return true;
        }

        @Override
        public Set<NormalisedTitle> getList(NamespaceEnum ns) {
            return cache.get(ns);
        }
    }
}
