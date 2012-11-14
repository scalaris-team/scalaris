package de.zib.scalaris.examples.wikipedia.bliki;

/**
 * Represents a normalised title, split into its two components: namespace
 * and page title.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class NormalisedTitle {
    /**
     * The namespace number.
     */
    public final Integer namespace;
    /**
     * The page title without the namespace.
     */
    public final String title;
    
    /**
     * Constructor.
     * 
     * @param namespace
     *            namespace number
     * @param title
     *            page title without the namespace
     */
    public NormalisedTitle(Integer namespace, String title) {
        assert (namespace != null);
        assert (title != null);
        this.namespace = namespace;
        this.title = title;
    }
    
    /**
     * Creates the full page name with namespace and title.
     * 
     * @return <tt>namespace:title</tt>
     */
    @Override
    public String toString() {
        return namespace + ":" + title;
    }
    
    /**
     * Creates the full page name with namespace and title.
     * 
     * @param normTitleStr
     *            a normalised title of the form <tt>namespace:title</tt>
     * 
     * @return a {@link NormalisedTitle} object
     * 
     * @throws IllegalArgumentException
     *             if the parameter string was not a normalised title
     */
    public static NormalisedTitle fromNormalised(String normTitleStr) throws IllegalArgumentException {
        int colonIndex = normTitleStr.indexOf(':');
        if (colonIndex == (-1)) {
            throw new IllegalArgumentException(
                    "no normalised title string: " + normTitleStr);
        }

        try {
            final Integer ns = Integer.parseInt(normTitleStr.substring(0, colonIndex));
            final String title = normTitleStr.substring(colonIndex + 1);
            return new NormalisedTitle(ns, title);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "no normalised title string: " + normTitleStr);
        }
    }
    
    /**
     * Normalises the given page title by capitalising its first letter after
     * the namespace.
     * 
     * @param title
     *            a unnormalised title
     * @param nsObject
     *            the namespace for determining how to split the title
     * 
     * @return a {@link NormalisedTitle} object
     */
    public static NormalisedTitle fromUnnormalised(String title, final MyNamespace nsObject) {
        String[] parts = nsObject.splitNsTitle(title);
        return new NormalisedTitle(nsObject.getNumberByName(parts[0]), parts[1]);
    }
    
    /**
     * Normalises the given page title by capitalising its first letter after
     * the namespace.
     * 
     * @param maybeNs
     *            the namespace of the page
     * @param articleName
     *            the (unnormalised) page's name without the namespace
     * @param nsObject
     *            the namespace for determining how to split the title
     * 
     * @return the normalised page title
     */
    public static NormalisedTitle fromUnnormalised(final String maybeNs, final String articleName, final MyNamespace nsObject) {
        Integer nsNumber = nsObject.getNumberByName(maybeNs);
        if (nsNumber == null) {
            nsNumber = MyNamespace.MAIN_NAMESPACE_KEY;
        }
        return new NormalisedTitle(nsNumber, MyWikiModel.normaliseName(articleName));
    }
    
    /**
     * Gets a de-normalised version of the page title, including the
     * namespace.
     * 
     * @param nsObject
     *            the namespace object for determining the string of the
     *            namespace id
     * 
     * @return de-normalised <tt>namespace:title</tt> or <tt>title</tt>
     */
    public String denormalise(final MyNamespace nsObject) {
        return MyWikiModel.createFullPageName(nsObject.getNamespaceByNumber(namespace),
                title);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof NormalisedTitle)) {
            return false;
        }
        
        NormalisedTitle obj2 = (NormalisedTitle) obj;
        return this.namespace.equals(obj2.namespace) &&
                this.title.equals(obj2.title);
    }
    
    @Override
    public int hashCode() {
        return toString().hashCode();
    }
}