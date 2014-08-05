package bolt.general;

import backtype.storm.tuple.Values;
import storm.trident.operation.Filter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

/* license text */
public class Printer implements Filter {
    @Override
    public boolean isKeep(TridentTuple tuple) {
        System.err.println(new Values(tuple));
        return true;
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {

    }

    @Override
    public void cleanup() {

    }
}
