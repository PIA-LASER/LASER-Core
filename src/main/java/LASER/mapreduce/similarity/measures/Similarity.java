package LASER.mapreduce.similarity.measures;


import org.apache.mahout.math.Vector;

public interface Similarity {
    public double similarity(double dots, double normA, double normB, int numberOfColumns);
    public double dot(double a, double b);
    public double norm(Vector vector);
    public Vector normalize(Vector vector);
}
