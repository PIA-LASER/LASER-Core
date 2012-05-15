package LASER.Utils;

import LASER.mapreduce.similarity.measures.Similarity;
import com.google.common.primitives.Longs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.lang.reflect.InvocationTargetException;

public class LaserUtils {

    public static int idToIndex(long id) {
        return 0x7FFFFFFF & Longs.hashCode(id);
    }

    public static Similarity getSimilarity(Reducer.Context context){
        return getSimilarityFromConfiguration(context.getConfiguration());
    }

    public static Similarity getSimilarity(Mapper.Context context){
        return getSimilarityFromConfiguration(context.getConfiguration());
    }

    private static Similarity getSimilarityFromConfiguration(Configuration conf){
        String similarityClassPath = "LASER.mapreduce.similarity.measures";
        String similarityName = conf.get("similarity");

        try {
            return Class.forName(similarityClassPath+"."+similarityName).asSubclass(Similarity.class).getConstructor().newInstance();
        } catch (ClassNotFoundException e){
            throw new IllegalStateException(e);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(e);
        } catch (InstantiationException e) {
            throw new IllegalStateException(e);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException(e);
        }
    }
}
