package edu.umass.ciir;

import java.util.ArrayList;
import java.util.List;

public class GalagoTokenizer {
    enum StringStatus {
        Clean,
        NeedsSimpleFix,
        NeedsComplexFix,
        NeedsAcronymProcessing
    }

    static final char[] splitChars = {
            ' ', '\t', '\n', '\r', // spaces
            ';', '\"', '&', '/', ':', '!', // '#',
            '?', '$', '%', '(', ')', '@', '^',
            '*', '+', '-', ',', '=', '>', '<', '[',
            ']', '{', '}', '|', '`', '~', '_'
    };
    // GLB: I removed '#' from the above so the ## tokens will come through for Negin's purposes

    public static final boolean[] splits;
    static {
        splits = buildSplits();
    }

    static boolean[] buildSplits() {
        boolean[] localSplits = new boolean[257];

        for (int i = 0; i < localSplits.length; i++) {
            localSplits[i] = false;
        }

        localSplits[160] = true;  // NBSP

        for (char c : splitChars) {
            localSplits[(byte) c] = true;
        }

        for (byte c = 0; c <= 32; c++) {
            localSplits[c] = true;
        }

        return localSplits;
    }

    List<String> finalTokens = new ArrayList<>();
    public String text;
    public int position;
    int lastSplit;

    public GalagoTokenizer() {
    }

    public String[] parseLine(String line) {
        // main parsing loop.
//        System.out.println("Line: " + line);
        finalTokens.clear();
        text = line;
        position = 0;
        lastSplit = -1;
        for (; position >= 0 && position < text.length(); position++) {
            char c = text.charAt(position);

            if (c < 256 && splits[c]) {
                onSplit();
            }
        }

//        System.out.println("Length of term list: " + finalTokens.size());
        return finalTokens.stream()
                .toArray(String[]::new);
    }

    public void onSplit() {
        if (position - lastSplit > 1) {
            int start = lastSplit + 1;
            String token = text.substring(start, position);
            processToken(token);
        }
        lastSplit = position;
    }

    public void processToken(String token) {
//        System.out.println("Token: " + token);
        StringStatus status = checkTokenStatus(token);

        switch (status) {
            case NeedsSimpleFix:
                token = normalizeSimple(token);
                break;

            case NeedsComplexFix:
                token = normalizeComplex(token);
                break;

            case NeedsAcronymProcessing:
                extractTermsFromAcronym(token);
                break;

            case Clean:
                // do nothing
                break;
        }

        if (status != StringStatus.NeedsAcronymProcessing) {
            if (token.length() > 0) {
//                System.out.println("Adding token: " + token);
                finalTokens.add(token);
            }
        }
    }

    public static String normalizeComplex(String token) {
        token = normalizeSimple(token);
        token = token.toLowerCase();

        return token;
    }

    /**
     * This method scans the token, looking for uppercase characters and
     * special characters.  If the token contains only numbers and lowercase
     * letters, it needs no further processing, and it returns Clean.
     * If it also contains uppercase letters or apostrophes, it returns
     * NeedsSimpleFix.  If it contains special characters (especially Unicode
     * characters), it returns NeedsComplexFix.  Finally, if any periods are
     * present, this returns NeedsAcronymProcessing.
     */
    public static StringStatus checkTokenStatus(final String token) {
        StringStatus status = StringStatus.Clean;
        char[] chars = token.toCharArray();

        for (char c : chars) {
            boolean isAsciiLowercase = (c >= 'a' && c <= 'z');
            boolean isAsciiNumber = (c >= '0' && c <= '9');

            if (isAsciiLowercase || isAsciiNumber) {
                continue;
            }
            boolean isAsciiUppercase = (c >= 'A' && c <= 'Z');
            boolean isPeriod = (c == '.');
            boolean isApostrophe = (c == '\'' || c == '“' || c == '”' || c == '’' || c == '‘' || c == '»' || c == '«');

            if ((isAsciiUppercase || isApostrophe) && status == StringStatus.Clean) {
                status = StringStatus.NeedsSimpleFix;
            } else if (!isPeriod) {
                status = StringStatus.NeedsComplexFix;
            } else {
                status = StringStatus.NeedsAcronymProcessing;
                break;
            }
        }

        return status;
    }


    /**
     * Scans through the token, removing apostrophes and converting
     * uppercase to lowercase letters.
     */
    public static String normalizeSimple(String token) {
        char[] chars = token.toCharArray();
        int j = 0;

        for (int i = 0; i < chars.length; i++) {
            char c = chars[i];
            boolean isAsciiUppercase = (c >= 'A' && c <= 'Z');
            boolean isApostrophe = (c == '\'');
            boolean isUnicodeQuote = (c == '“' || c == '”' || c == '’' || c == '‘');

            if (isAsciiUppercase) {
                chars[j] = (char) (chars[i] + 'a' - 'A');
            } else if (isApostrophe || isUnicodeQuote) {
                // it's an apostrophe or unicode quote, skip it
                j--;
            } else {
                chars[j] = chars[i];
            }

            j++;
        }

        token = new String(chars, 0, j);
        return token;
    }

    /**
     * This method does three kinds of processing:
     * <ul>
     *  <li>If the token contains periods at the beginning or the end,
     *      they are removed.</li>
     *  <li>If the token contains single letters followed by periods, such
     *      as I.B.M., C.I.A., or U.S.A., the periods are removed.</li>
     *  <li>If, instead, the token contains longer strings of state.text with
     *      periods in the middle, the token is split into
     *      smaller tokens ("umass.edu" becomes {"umass", "edu"}).  Notice
     *      that this means ("ph.d." becomes {"ph", "d"}).</li>
     * </ul>
     * @param token The term containing dots.
     */
    public void extractTermsFromAcronym(String token) {
        token = normalizeComplex(token);

        // remove start and ending periods
        while (token.startsWith(".")) {
            token = token.substring(1);
        }

        while (token.endsWith(".")) {
            token = token.substring(0, token.length() - 1);
        }

        // does the token have any periods left?
        if (token.indexOf('.') >= 0) {
            // is this an acronym?  then there will be periods
            // at odd state.positions:
            boolean isAcronym = token.length() > 0;
            for (int pos = 1; pos < token.length(); pos += 2) {
                if (token.charAt(pos) != '.') {
                    isAcronym = false;
                }
            }

            if (isAcronym) {
                token = token.replace(".", "");
                if (token.length() > 0) {
                    finalTokens.add(token);
                }
            } else {
                int s = 0;
                for (int e = 0; e < token.length(); e++) {
                    if (token.charAt(e) == '.') {
                        if (e - s > 1) {
                            String subtoken = token.substring(s, e);
                            if (subtoken.length() > 0) {
                                finalTokens.add(subtoken);
                            }
                        }
                        s = e + 1;
                    }
                }

                if (token.length() - s > 0) {
                    String subtoken = token.substring(s);
                    if (subtoken.length() > 0) {
                        finalTokens.add(subtoken);
                    }
                }
            }
        } else {
            if (token.length() > 0) {
                finalTokens.add(token);
            }
        }
    }
}

