package bolt.ml.state.weka;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import storm.trident.state.State;

import java.util.concurrent.TimeUnit;

/**
 * User: lbhat <laksh85@gmail.com>
 * Date: 12/16/13
 * Time: 8:00 PM
 */
public abstract class BaseState implements State {
    public BaseState(){
        features = CacheBuilder.newBuilder().expireAfterAccess(24, TimeUnit.HOURS).maximumSize(1000).build();
    }
    public Cache<Integer, Double[]> getFeatures () {
        return features;
    }

    protected Cache<Integer, Double[]> features;
}
