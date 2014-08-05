package spout.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

/**
 * Created by lakshmisha.bhat on 8/4/14.
 */
public class PeekableScanner {
    private Scanner fastScanner;
    private Scanner slowScanner;
    private String next;

    public PeekableScanner(File source) throws FileNotFoundException {
        fastScanner = new Scanner(source);
        slowScanner = new Scanner(source);
        if (slowScanner.hasNextLine()) {
            next = slowScanner.nextLine();
        } else {
            throw new IllegalStateException("Empty peekable peekableScanner. Consider using an ordinary peekableScanner with safety checks.");
        }
    }

    public boolean hasNext() {
        return fastScanner.hasNextLine();
    }

    public String next() {
        next = (slowScanner.hasNext() ? slowScanner.next() : null);
        return fastScanner.nextLine();
    }

    public String peek() {
        return next;
    }

    public void close() {
        fastScanner.close();
        slowScanner.close();
    }
}
