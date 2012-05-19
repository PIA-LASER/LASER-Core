package LASER.mapreduce.preparation;

import LASER.mapreduce.similarity.measures.Similarity;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.*;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;


public class ToItemVectorMapper extends Mapper<VarIntWritable, VectorWritable, VarIntWritable, VectorWritable> {

    Vector norms;
    Similarity similarity;

    @Override
    protected void setup(Context context){
        norms = new RandomAccessSparseVector(Integer.MAX_VALUE, 100);
        String similarityClassName = "LASER.mapreduce.similarity.measures.";
        similarityClassName = similarityClassName.concat(context.getConfiguration().get("similarity"));

        try {
            similarity = Class.forName(similarityClassName).asSubclass(Similarity.class).getConstructor().newInstance();
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException();
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException();
        } catch (InstantiationException e){
            throw new IllegalStateException();
        } catch (IllegalAccessException e){
            throw new IllegalStateException();
        } catch (InvocationTargetException e){
            throw new IllegalStateException();
        }
    }

    @Override
    protected void map(VarIntWritable key, VectorWritable prefs, org.apache.hadoop.mapreduce.Mapper.Context context)
            throws IOException, InterruptedException{
        //map data to item vectors
        int userId = key.get();

        VectorWritable preferencesVector = new VectorWritable(new RandomAccessSparseVector(Integer.MAX_VALUE, 1));
        //preferencesVector.setWritesLaxPrecision(true);

        Iterator<Vector.Element> iterator = prefs.get().iterateNonZero();

        while(iterator.hasNext()) {
            Vector.Element elem = iterator.next();
            preferencesVector.get().setQuick(userId, elem.get());
            context.write(new VarIntWritable(elem.index()), preferencesVector);
        }

        //calculate vector norms


    }
}
