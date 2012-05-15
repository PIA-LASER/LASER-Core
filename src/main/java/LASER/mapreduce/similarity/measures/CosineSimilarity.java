package LASER.mapreduce.similarity.measures;


import org.apache.mahout.math.Vector;

public class CosineSimilarity implements Similarity{

    @Override
    public double similarity(double dots) {
        return dots;
    }

    @Override
    public double dot(double a, double b) {
        return a*b;
    }

    @Override
    public double norm(Vector vector) {
        return 0;
    }

    @Override
    public Vector normalize(Vector vector) {
        return vector.normalize();
    }
}
