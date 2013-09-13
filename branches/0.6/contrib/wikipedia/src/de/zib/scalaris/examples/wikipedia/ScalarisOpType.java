package de.zib.scalaris.examples.wikipedia;

/**
 * Different types of DB operations.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public enum ScalarisOpType {
    /**
     * Operation involving a (central) page list.
     */
    PAGE_LIST("PAGE_LIST"),
    /**
     * Operation involving a counter of a (central) page list.
     */
    PAGE_COUNT("PAGE_COUNT"),
    /**
     * Operation involving a category page list.
     */
    CATEGORY_PAGE_LIST("CATEGORY_PAGE_LIST"),
    /**
     * Operation involving a counter of a category page list.
     */
    CATEGORY_PAGE_COUNT("CATEGORY_PAGE_COUNT"),
    /**
     * Operation involving a template page list.
     */
    TEMPLATE_PAGE_LIST("TEMPLATE_PAGE_LIST"),
    /**
     * Operation involving a backlink page list.
     */
    BACKLINK_PAGE_LIST("BACKLINK_PAGE_LIST"),
    /**
     * Operation involving a list of (short) revisions.
     */
    SHORTREV_LIST("SHORTREV_LIST"),
    /**
     * Operation involving the article counter.
     */
    ARTICLE_COUNT("ARTICLE_COUNT"),
    /**
     * Operation involving a wiki page.
     */
    PAGE("PAGE"),
    /**
     * Operation involving a wiki page revision.
     */
    REVISION("REVISION"),
    /**
     * Operation involving a contribution.
     */
    CONTRIBUTION("CONTRIBUTION"),
    /**
     * Operation involving the edit stats.
     */
    EDIT_STAT("EDIT_STAT");

    private final String text;

    ScalarisOpType(String text) {
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
    public static ScalarisOpType fromString(String text) {
        if (text != null) {
            for (ScalarisOpType b : ScalarisOpType.values()) {
                if (text.equalsIgnoreCase(b.text)) {
                    return b;
                }
            }
        }
        throw new IllegalArgumentException("No constant with text " + text
                + " found");
    }
}