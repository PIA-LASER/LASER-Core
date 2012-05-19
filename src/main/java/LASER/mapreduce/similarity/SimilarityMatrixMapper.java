package LASER.mapreduce.similarity;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.util.Iterator;

public class SimilarityMatrixMapper extends Mapper<VarIntWritable, VectorWritable, VarIntWritable, VectorWritable> {

    @Override
    public void map(VarIntWritable itemKey, VectorWritable similarities, Context context)
            throws IOException, InterruptedException{

        Iterator<Vector.Element> similarityIterator = similarities.get().iterateNonZero();

        //map old similarity
        context.write(itemKey, similarities);

        while (similarityIterator.hasNext()) {
            Vector.Element similarity = similarityIterator.next();

            //map similarity with inverted index to create sym. matrix
            Vector invertedIndexSimilarities = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);
            invertedIndexSimilarities.set(itemKey.get(), similarity.get());

            context.write(new VarIntWritable(similarity.index()), new VectorWritable(invertedIndexSimilarities));
        }
    }
}
