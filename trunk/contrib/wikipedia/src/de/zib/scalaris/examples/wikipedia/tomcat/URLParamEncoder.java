package de.zib.scalaris.examples.wikipedia.tomcat;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

public class URLParamEncoder {

    public static String encode(String s) throws UnsupportedEncodingException {
        StringBuilder b = new StringBuilder(URLEncoder.encode(s, "UTF-8"));
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
            case '?':
                b.append("%3F");
                break;
            case '&':
                b.append("%26");
                break;
            default:
                b.append(c);
            }
        }
        return b.toString();
    }
}
