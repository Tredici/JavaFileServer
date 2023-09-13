package it.sssupserver.app.handlers.simplecdnhandler;

import java.util.function.Predicate;
import java.util.regex.Pattern;

// for import static
public class FilenameCheckers {
    // initialize parameters used to check validity of file path(s)
    private static Predicate<String> directoryNameTest;
    private static Predicate<String> regularFileNameTest;
    static {
        directoryNameTest = Pattern.compile(
            "^(_|-|\\+|\\w)(\\s*(_|-|\\+|\\w|\\(|\\))+)*$",
            Pattern.CASE_INSENSITIVE
        ).asMatchPredicate();
        regularFileNameTest = Pattern.compile(
            "^(_|-|\\+|\\w)(\\s*(_|-|\\+|\\w|\\(|\\))+)*(\\.(_|-|\\+|\\w|\\(|\\))+)+$",
            Pattern.CASE_INSENSITIVE
        ).asMatchPredicate();
    }

    // directories should match "^(_|-|\+|\w)+$"
    // Easy: they must NOT contains any dot
    public static boolean isValidDirectoryName(String dirname) {
        return dirname.length() > 0 && directoryNameTest.test(dirname);
    }

    // directories should match "^(_|-|\+|\w)+(\.(_|-|\+|\w)+)+$"
    public static boolean isValidRegularFileName(String filename) {
        return filename.length() > 0 && regularFileNameTest.test(filename);
    }

    public static boolean isValidPathName(it.sssupserver.app.base.Path path) {
        if (path.isEmpty()) {
            return false;
        }
        var isDir = path.isDir();
        var components = path.getPath();
        var length = components.length;
        var lastName = components[length-1];
        for (int i=0; i<length-1; ++i) {
            if (!isValidDirectoryName(components[i])) {
                return false;
            }
        }
        return isDir ? isValidDirectoryName(lastName)
            : isValidRegularFileName(lastName);
    }

    public static boolean isValidPathName(String path) {
        return isValidPathName(new it.sssupserver.app.base.Path(path));
    }    
}
