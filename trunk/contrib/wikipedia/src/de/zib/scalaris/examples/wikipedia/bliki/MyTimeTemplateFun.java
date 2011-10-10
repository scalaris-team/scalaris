package de.zib.scalaris.examples.wikipedia.bliki;

import info.bliki.wiki.model.IWikiModel;
import info.bliki.wiki.template.AbstractTemplateFunction;
import info.bliki.wiki.template.ITemplateFunction;
import info.bliki.wiki.template.Time;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * A template parser function for <code>{{ #time: ... }}</code> syntax.
 * 
 * Based on {@link Time} but supporting more flags (still not complete though).
 * 
 * See <a
 * href="http://www.mediawiki.org/wiki/Help:Extension:ParserFunctions#.23time">
 * Mediwiki's Help:Extension:ParserFunctions - #time</a>
 */
public class MyTimeTemplateFun extends AbstractTemplateFunction {
	/**
	 * Static instance of this template function parser.
	 */
	public final static ITemplateFunction CONST = new MyTimeTemplateFun();

	protected static Map<String, String> FORMAT_WIKI_TO_JAVA = new HashMap<String, String>();
	protected static SimpleDateFormat RFC822DATEFORMAT
    = new SimpleDateFormat("EEE', 'dd' 'MMM' 'yyyy' 'HH:mm:ss' '+0000", Locale.US);
	
	static {
	    // year
	    FORMAT_WIKI_TO_JAVA.put("Y", "yyyy"); // 4-digit year, e.g. "2011"
	    FORMAT_WIKI_TO_JAVA.put("y", "yy"); // 2-digit year, e.g. "11"
        // FORMAT_WIKI_TO_JAVA.put("L", "??"); // 1 or 0 whether it's a leap year or not, e.g. "0"
        FORMAT_WIKI_TO_JAVA.put("o", "yyyy"); // ISO-8601 year number, e.g. "2011"
        
        // month
        FORMAT_WIKI_TO_JAVA.put("n", "M"); // month index, not zero-padded, e.g. "10"
        FORMAT_WIKI_TO_JAVA.put("m", "MM"); // month index, zero-padded, e.g. "10"
        FORMAT_WIKI_TO_JAVA.put("M", "MMM"); // an abbreviation of the month name, in the site language, e.g. "Oct"
        FORMAT_WIKI_TO_JAVA.put("F", "MMMM"); // the full month name in the site language, e.g. "October"
        // FORMAT_WIKI_TO_JAVA.put("xg", "??"); // Output the full month name in the genitive form for site languages that distinguish between genitive and nominative forms.
        
        // week
        FORMAT_WIKI_TO_JAVA.put("y", "yy"); // W    ISO 8601 week number, zero-padded, e.g. "40"
        
        // day
        FORMAT_WIKI_TO_JAVA.put("j", "d"); // j    Day of the month, not zero-padded, e.g. "6"
        FORMAT_WIKI_TO_JAVA.put("d", "dd"); // d    Day of the month, zero-padded, e.g. "06"
        // FORMAT_WIKI_TO_JAVA.put("z", "??"); // z    Day of the year (January 1 = 0). Note: To get the ISO day of the year add 1, e.g. "278"
        FORMAT_WIKI_TO_JAVA.put("D", "EEE"); // D    An abbreviation for the day of the week. Rarely internationalised, e.g. "Thu"
        FORMAT_WIKI_TO_JAVA.put("l", "EEEE"); // l    The full weekday name. Rarely internationalised, e.g. "Thursday"
        // FORMAT_WIKI_TO_JAVA.put("N", "??"); // N    ISO 8601 day of the week (Monday = 1, Sunday = 7), e.g. "4"
        // FORMAT_WIKI_TO_JAVA.put("w", "??"); // w    Number of the day of the week (Sunday = 0, Saturday = 6), e.g. "4"

        // hour
        // FORMAT_WIKI_TO_JAVA.put("a", "??"); // "am" during the morning (00:00:00 -> 11:59:59), "pm" otherwise (12:00:00 -> 23:59:59), e.g. "pm"
        FORMAT_WIKI_TO_JAVA.put("A", "a"); // Uppercase version of a above, e.g. "PM"
        FORMAT_WIKI_TO_JAVA.put("g", "h"); // Hour in 12-hour format, not zero-padded, e.g. "3"
        FORMAT_WIKI_TO_JAVA.put("h", "hh"); // Hour in 12-hour format, zero-padded, e.g. "03"
        FORMAT_WIKI_TO_JAVA.put("G", "H"); // Hour in 24-hour format, not zero-padded, e.g. "15"
        FORMAT_WIKI_TO_JAVA.put("H", "HH"); // Hour in 24-hour format, zero-padded, e.g. "15"

        // minutes, seconds
        FORMAT_WIKI_TO_JAVA.put("i", "mm"); // Minutes past the hour, zero-padded, e.g. "07"
        FORMAT_WIKI_TO_JAVA.put("s", "ss"); // Seconds past the minute, zero-padded, e.g. "30"
        // FORMAT_WIKI_TO_JAVA.put("U", "??"); // Seconds since January 1 1970 00:00:00 GMT, e.g. "1317913650"

        // misc.
        // FORMAT_WIKI_TO_JAVA.put("t", "??"); // Number of days in the current month, e.g. "31"
        // note: assume date is in GMT+00:00:
        FORMAT_WIKI_TO_JAVA.put("c", "yyyy-MM-dd'T'HH:mm:ss+00:00"); // ISO 8601 formatted date, equivalent to Y-m-dTH:i:s+00:00, e.g. "2011-10-06T15:07:30+00:00"
        // FORMAT_WIKI_TO_JAVA.put("r", "??"); // RFC 5322 formatted date, equivalent to D, j M Y H:i:s +0000, with weekday name and month name not internationalised, e.g. "Thu, 06 Oct 2011 15:07:30 +0000"

        // non-gregorian
        // Iranian:
        // FORMAT_WIKI_TO_JAVA.put("xij", "??"); // Day of the month, e.g. "14"
        // FORMAT_WIKI_TO_JAVA.put("xiF", "??"); // Full month name, e.g. "Mehr"
        // FORMAT_WIKI_TO_JAVA.put("xin", "??"); // Month index, e.g. "7"
        // FORMAT_WIKI_TO_JAVA.put("xiY", "??"); // Full year, e.g. "1390"
        // Hebrew:
        // FORMAT_WIKI_TO_JAVA.put("xjj", "??"); // Day of the month, e.g. "8"
        // FORMAT_WIKI_TO_JAVA.put("xjF", "??"); // Full month name, e.g. "Tishrei"
        // FORMAT_WIKI_TO_JAVA.put("xjx", "??"); // Genitive form of the month name, e.g. "Tishrei"
        // FORMAT_WIKI_TO_JAVA.put("xjn", "??"); // Month number, e.g. "1"
        // FORMAT_WIKI_TO_JAVA.put("xjY", "??"); // Full year, e.g. "5772"
        // Thai solar
        // FORMAT_WIKI_TO_JAVA.put("xkY", "??"); // Full year, e.g. "2554"

        // Flags
        // FORMAT_WIKI_TO_JAVA.put("xn", "??"); // Format the next numeric code as a raw ASCII number
        // FORMAT_WIKI_TO_JAVA.put("xN", "??"); // Like xn, but as a toggled flag, which endures until the end of the string or until the next appearance of xN in the string.
        // FORMAT_WIKI_TO_JAVA.put("Xr", "??"); // Format the next number as a roman numeral. Only works for numbers up to 3000
	}
	
	/**
	 * Creates a new parser for the #time template function.
	 */
	public MyTimeTemplateFun() {
	}

	public String parseFunction(List<String> list, IWikiModel model, char[] src, int beginIndex, int endIndex) {
		if (list.size() > 0) {
			Date date;
			DateFormat df = DateFormat.getDateInstance(DateFormat.MEDIUM, model.getLocale());
			if (list.size() > 1) {
				String dateTimeParameter = list.get(1);

				try {
					date = df.parse(dateTimeParameter);
				} catch (ParseException e) {
					return "<span class=\"error\">Error: invalid time</span>";
				}
			} else {
				date = model.getCurrentTimeStamp();
			}

			String condition = parse(list.get(0), model);
            StringBuilder result = new StringBuilder(condition.length() * 2);
            boolean inDoubleQuotes = false;
            GregorianCalendar cal = new GregorianCalendar(model.getLocale());
            cal.setTime(date);
            for (int curPos = 0; curPos < condition.length(); ++curPos) {
                char curCh = condition.charAt(curPos);
                if (curPos == 0 || !inDoubleQuotes || condition.charAt(curPos - 1) == '\\') {
                    String formatCh = String.valueOf(curCh);
                    if (FORMAT_WIKI_TO_JAVA.containsKey(formatCh)) {
                        SimpleDateFormat sdf = new SimpleDateFormat(FORMAT_WIKI_TO_JAVA.get(formatCh), model.getLocale());
                        result.append(sdf.format(date));
                        continue;
                    }
                    // TODO:
                    // FORMAT_WIKI_TO_JAVA.put("xg", "??"); // Output the full month name in the genitive form for site languages that distinguish between genitive and nominative forms.
                    // Iranian:
                    // FORMAT_WIKI_TO_JAVA.put("xij", "??"); // Day of the month, e.g. "14"
                    // FORMAT_WIKI_TO_JAVA.put("xiF", "??"); // Full month name, e.g. "Mehr"
                    // FORMAT_WIKI_TO_JAVA.put("xin", "??"); // Month index, e.g. "7"
                    // FORMAT_WIKI_TO_JAVA.put("xiY", "??"); // Full year, e.g. "1390"
                    // Hebrew:
                    // FORMAT_WIKI_TO_JAVA.put("xjj", "??"); // Day of the month, e.g. "8"
                    // FORMAT_WIKI_TO_JAVA.put("xjF", "??"); // Full month name, e.g. "Tishrei"
                    // FORMAT_WIKI_TO_JAVA.put("xjx", "??"); // Genitive form of the month name, e.g. "Tishrei"
                    // FORMAT_WIKI_TO_JAVA.put("xjn", "??"); // Month number, e.g. "1"
                    // FORMAT_WIKI_TO_JAVA.put("xjY", "??"); // Full year, e.g. "5772"
                    // Thai solar
                    // FORMAT_WIKI_TO_JAVA.put("xkY", "??"); // Full year, e.g. "2554"

                    // Flags
                    // FORMAT_WIKI_TO_JAVA.put("xn", "??"); // Format the next numeric code as a raw ASCII number
                    // FORMAT_WIKI_TO_JAVA.put("xN", "??"); // Like xn, but as a toggled flag, which endures until the end of the string or until the next appearance of xN in the string.
                    // FORMAT_WIKI_TO_JAVA.put("Xr", "??"); // Format the next number as a roman numeral. Only works for numbers up to 3000
                    switch (curCh) {
                        case 'L': // 1 or 0 whether it's a leap year or not
                            if (cal.isLeapYear(cal.get(Calendar.YEAR))) {
                                result.append(1);
                            } else {
                                result.append(0);
                            }
                            continue;
                        case 'z': // Day of the year (January 1 = 0). Note: To get the ISO day of the year add 1
                            result.append(cal.get(Calendar.DAY_OF_YEAR) - 1);
                            continue;
                        case 'N': // ISO 8601 day of the week (Monday = 1, Sunday = 7)
                            int dayOfWeek_N = cal.get(Calendar.DAY_OF_WEEK) - Calendar.MONDAY + 1;
                            if (dayOfWeek_N <= 0) {
                                result.append(dayOfWeek_N + 7);
                            } else {
                                result.append(dayOfWeek_N);
                            }
                            continue;
                        case 'w': // Number of the day of the week (Sunday = 0, Saturday = 6)
                            int dayOfWeek_w = cal.get(Calendar.DAY_OF_WEEK) - Calendar.SUNDAY;
                            if (dayOfWeek_w < 0) {
                                result.append(dayOfWeek_w + 7);
                            } else {
                                result.append(dayOfWeek_w);
                            }
                            continue;
                        case 'a': // "am" during the morning (00:00:00 -> 11:59:59), "pm" otherwise (12:00:00 -> 23:59:59)
                            result.append((new SimpleDateFormat("a", model.getLocale())).format(date).toLowerCase());
                            continue; 
                        case 'U': // Seconds since January 1 1970 00:00:00 GMT
                            result.append(date.getTime() / 1000);
                            continue;
                        case 't': // Number of days in the current month
                            result.append(cal.getActualMaximum(Calendar.DAY_OF_MONTH));
                            continue;
                        case 'r': // // RFC 5322 formatted date, equivalent to D, j M Y H:i:s +0000, with weekday name and month name not internationalised
                            result.append(RFC822DATEFORMAT.format(date));
                            continue;
                        default:
                            break;
                    }
                }
                if (curCh == '"') {
                    inDoubleQuotes = !inDoubleQuotes;
                }
                result.append(curCh);
            }
			return result.toString();
		}
		return null;
	}
}
