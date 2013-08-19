/**
 *  Copyright 2011-2013 Zuse Institute Berlin
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
package de.zib.scalaris.examples.wikipedia.data.xml;

import org.xml.sax.SAXException;

/**
 * Exception that is thrown if a {@link WikiDumpHandler} instance is told to
 * stop parsing while parsing a file.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class SAXParsingInterruptedException extends SAXException {

    /**
     * Generated version for serialisation.
     */
    private static final long serialVersionUID = -4208711186203540999L;

    /**
     * Create a new SAXException.
     */
    public SAXParsingInterruptedException() {
        super();
    }

    /**
     * Create a new SAXException.
     * 
     * @param message
     *            the error or warning message
     */
    public SAXParsingInterruptedException(String message) {
        super(message);
    }

    /**
     * Create a new SAXException wrapping an existing exception.
     * 
     * The existing exception will be embedded in the new one, and its message
     * will become the default message for the SAXException.
     * 
     * @param e
     *            the exception to be wrapped in a SAXException
     */
    public SAXParsingInterruptedException(Exception e) {
        super(e);
    }

    /**
     * Create a new SAXException from an existing exception.
     * 
     * The existing exception will be embedded in the new one, but the new
     * exception will have its own message.
     * 
     * @param message
     *            the detail message
     * @param e
     *            the exception to be wrapped in a SAXException
     */
    public SAXParsingInterruptedException(String message, Exception e) {
        super(message, e);
    }

}
