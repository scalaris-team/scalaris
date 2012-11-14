// adapted from info.bliki.wiki.template.Fullurl which is licensed under
// Eclipse Public License 1.0
package de.zib.scalaris.examples.wikipedia.bliki;

import info.bliki.wiki.model.IWikiModel;
import info.bliki.wiki.template.Fullurl;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * A template parser function for <code>{{fullurl: ... }}</code> syntax.
 * Note: Falls back to {@link Fullurl} if the model is not a {@link MyWikiModel}
 * model.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class MyFullurl extends Fullurl {
    /**
     * Single static instance of a {@link MyFullurl} object.
     */
    public final static MyFullurl CONST = new MyFullurl();
    
    /**
     * Constructor
     */
    public MyFullurl() {
        super();
    }

    @Override
    public String parseFunction(List<String> list, IWikiModel model,
            char[] src, int beginIndex, int endIndex, boolean isSubst)
            throws UnsupportedEncodingException {
        final String server = MyMagicWord.processMagicWord("{{SERVER}}", "", model, false);
        final String localurl = MyLocalurl.CONST.parseFunction(list, model,
                src, beginIndex, endIndex, isSubst);
        return server + localurl;
    }
}
