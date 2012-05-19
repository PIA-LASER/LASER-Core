package LASER.mapreduce.similarity.measures;


import org.apache.mahout.math.Vector;

public class CosineSimilarity implements Similarity{

    public double similarity(double dots, double normA, double normB, int numberOfColumns) {
        return dots;
    }

    public double dot(double a, double b) {
        return a*b;
    }

    public double norm(Vector vector) {
        return 0;
    }

    public Vector normalize(Vector vector) {
        return vector.normalize();
    }
}
