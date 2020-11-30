package FusionDs;

public class fusionMatrix {
    public Matrix matrix;

    public fusionMatrix(int n, int f){
        matrix = buildMatrix(n, (n+f));
    }

    private static Matrix buildMatrix(int dataShards, int totalShards) {
        // Start with a Vandermonde matrix.  This matrix would work,
        // in theory, but doesn't have the property that the data
        // shards are unchanged after encoding.
        Matrix vandermonde = vandermonde(totalShards, dataShards);

        // Multiple by the inverse of the top square of the matrix.
        // This will make the top square be the identity matrix, but
        // preserve the property that any square subset of rows is
        // invertible.
        Matrix top = vandermonde.submatrix(0, 0, dataShards, dataShards);
        return vandermonde.times(top.invert());
    }

    private static Matrix vandermonde(int rows, int cols) {
        Matrix result = new Matrix(rows, cols);
        for (int r = 0; r < rows; r++) {
            for (int c = 0; c < cols; c++) {
                result.set(r, c, Galois.exp((byte) r, c));
            }
        }
        return result;
    }
}
