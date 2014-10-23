/**
 *  Copyright 2012-2013 Zuse Institute Berlin
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
package de.zib.scalaris.examples.wikipedia;

import java.util.Collection;
import java.util.List;



/**
 * POD object for involved keys.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class InvolvedKey {
    
    /**
     * Type of operation for the key.
     */
    public static enum OP {
        /**
         * The key was read.
         */
        NOOP(""),
        /**
         * The key was read.
         */
        READ("r:"),
        /**
         * The key was written.
         */
        WRITE("w:");

        private final String text;

        OP(String text) {
            this.text = text;
        }

        /**
         * Converts the enum to text.
         */
        public String toString() {
            return this.text;
        }

        /**
         * Tries to convert a text to the according enum value.
         * 
         * @param text the text to convert
         * 
         * @return the according enum value
         */
        public static OP fromString(String text) {
            if (text != null) {
                for (OP b : OP.values()) {
                    if (text.equalsIgnoreCase(b.text)) {
                        return b;
                    }
                }
            }
            throw new IllegalArgumentException("No constant with text " + text
                    + " found");
        }
    }
    
    /**
     * The operation performed at the key.
     */
    final public OP op;
    /**
     * The involved key.
     */
    final public String key;
    
    /**
     * Constructor of a no-op key.
     */
    public InvolvedKey() {
        this.op = OP.NOOP;
        this.key = "";
    }
    
    /**
     * Constructor.
     * 
     * @param op
     *            the operation performed at the key
     * @param key
     *            the involved key
     */
    public InvolvedKey(final OP op, final String key) {
        this.op = op;
        this.key = key;
    }
    
    @Override
    public String toString() {
        if (op == OP.NOOP && key.isEmpty()) {
            return "";
        }
        return op.toString() + key.toString();
    }
    
    /**
     * Tries to parse an involved key from the given string.
     * 
     * @param text
     *            the string to parse from
     * 
     * @return an {@link InvolvedKey} object
     * 
     * @throws IllegalArgumentException
     *             if the string is incorrect
     */
    public static InvolvedKey fromString(String text) throws IllegalArgumentException {
        assert text != null;
        if (text.isEmpty()) {
            return new InvolvedKey();
        } else if (text.length() >= 2) {
            final OP op = OP.fromString(text.substring(0, 2));
            final String key = text.substring(2);
            return new InvolvedKey(op, key);
        } else {
            throw new IllegalArgumentException("No valid involvedKey: " + text);
        }
    }
    
    /**
     * Tries to parse multiple involved keys from the given collection and adds
     * them the the list of involved keys.
     * 
     * @param involvedKeys
     *            list of involved keys
     * @param keysStr
     *            the collection to parse strings from
     * @param includeNoops
     *            whether to include noop keys
     * 
     * @throws IllegalArgumentException
     *             if the string is incorrect
     */
    public static void addInvolvedKeys(List<InvolvedKey> involvedKeys,
            Collection<? extends String> keysStr, boolean includeNoops)
            throws IllegalArgumentException {
        assert involvedKeys != null;
        assert keysStr != null;
        
        for (String text : keysStr) {
            final InvolvedKey involvedKey = InvolvedKey.fromString(text);
            if (includeNoops || !(involvedKey.op == OP.NOOP && involvedKey.key.isEmpty())) {
                involvedKeys.add(involvedKey);
            }
        }
    }
}
