import java.text.MessageFormat;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ThreeHeadsProbabilityCalc {
    public static void main(String[] args) {
        long noOfExperiments = 0;
        int headCount;
        int totalTests = 500000;
        while (totalTests-- > 0) {
            while (true) {
                noOfExperiments++;
                headCount = 0;

                for (int i = 0; i < 3; i++) {
                    if (toss() == 1) {
                        headCount++;
                    } else {
                        break;
                    }
                }
                if (headCount == 3) {
                    break;
                }
            }
        }
        Logger.getAnonymousLogger().log(Level.INFO, MessageFormat.format("average no of flips to get 3 heads in  a row is {0}", noOfExperiments * 3.0 / 500000));
    }

    private static int toss() {
        Random r = new Random();
        return r.nextInt(2);
    }
}
