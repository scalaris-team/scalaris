// adapted from info.bliki.wiki.template.Fullurl which is licensed under
// Eclipse Public License 1.0
package de.zib.scalaris.examples.wikipedia.bliki;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;

import info.bliki.api.Connector;
import info.bliki.wiki.model.IWikiModel;
import info.bliki.wiki.template.Fullurl;
import info.bliki.wiki.template.ITemplateFunction;

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
    public final static ITemplateFunction CONST = new MyFullurl();
    
    /**
     * Constructor
     */
    public MyFullurl() {
        super();
    }

    @Override
    public String parseFunction(List<String> list, IWikiModel model, char[] src, int beginIndex, int endIndex) throws UnsupportedEncodingException {
        if (model instanceof MyWikiModel) {
            MyWikiModel myModel = (MyWikiModel) model;
            if (list.size() > 0) {
                String arg0 = parse(list.get(0), myModel);
                if (arg0.length() > 0 && list.size() == 1) {
                    String result = myModel.getLinkBaseFullURL().replace(
                            "${title}",
                            URLEncoder.encode(arg0, Connector.UTF8_CHARSET));
                    return result;
                }
                StringBuilder builder = new StringBuilder(arg0.length() + 64);
                builder.append(myModel.getLinkBaseFullURL().replace("${title}",
                        URLEncoder.encode(arg0, Connector.UTF8_CHARSET)));
                for (int i = 1; i < list.size(); i++) {
                    builder.append("&");
                    builder.append(parse(list.get(i), myModel));
                }
                return builder.toString();
            }
            return null;
        } else {
            return super.parseFunction(list, model, src, beginIndex, endIndex);
        }
    }
}
