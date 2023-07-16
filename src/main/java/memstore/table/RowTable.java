package memstore.table;

import memstore.data.ByteFormat;
import memstore.data.DataLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * RowTable, which stores data in row-major format.
 * That is, data is laid out like
 *   row 1 | row 2 | ... | row n.
 */
public class RowTable implements Table {
    protected int numCols;
    protected int numRows;
    protected ByteBuffer rows;

    public RowTable() { }

    /**
     * Loads data into the table through passed-in data loader. Is not timed.
     *
     * @param loader Loader to load data from.
     * @throws IOException
     */
    @Override
    public void load(DataLoader loader) throws IOException {
        this.numCols = loader.getNumCols();
        List<ByteBuffer> rows = loader.getRows();
        numRows = rows.size();
        this.rows = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * numCols);

        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            for (int colId = 0; colId < numCols; colId++) {
                int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
                this.rows.putInt(offset, curRow.getInt(ByteFormat.FIELD_LEN * colId));
            }
        }
    }

    /**
     * Returns the int field at row `rowId` and column `colId`.
     */
    @Override
    public int getIntField(int rowId, int colId) {
        // TODO: Implement this!
        int offset = ByteFormat.FIELD_LEN * numCols;
        int index = (offset * rowId) + (colId * ByteFormat.FIELD_LEN);
        return rows.getInt(index);
    }

    /**
     * Inserts the passed-in int field at row `rowId` and column `colId`.
     */
    @Override
    public void putIntField(int rowId, int colId, int field) {
        // TODO: Implement this!
        int offset = ByteFormat.FIELD_LEN * numCols;
        int index = (offset * rowId) + (colId * ByteFormat.FIELD_LEN);
        rows.putInt(index, field);
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) FROM table;
     *
     *  Returns the sum of all elements in the first column of the table.
     */
    @Override
    public long columnSum() {
        // TODO: Implement this!
        int offset = ByteFormat.FIELD_LEN * numCols;
        long sum = 0;
        for(int row = 0; row < numRows; row++){
            int index = offset * row;
            int value = rows.getInt(index);
            sum += value;
        }
        return sum;
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) FROM table WHERE col1 > threshold1 AND col2 < threshold2;
     *
     *  Returns the sum of all elements in the first column of the table,
     *  subject to the passed-in predicates.
     */
    @Override
    public long predicatedColumnSum(int threshold1, int threshold2) {
        // TODO: Implement this!
        int offset = ByteFormat.FIELD_LEN * numCols;
        long sum = 0;
        for(int row = 0; row < numRows; row++){
            int index0 = offset * row;

            int col1 = rows.getInt(index0 + ByteFormat.FIELD_LEN);
            int col2 = rows.getInt(index0 + (2 * ByteFormat.FIELD_LEN));
//            int col2 = rows.getInt(index0 + (ByteFormat.FIELD_LEN << 1));

            if(col1 > threshold1 && col2 < threshold2){
                int col0 = rows.getInt(index0);
                sum += col0;
            }
        }
        return sum;
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) + SUM(col1) + ... + SUM(coln) FROM table WHERE col0 > threshold;
     *
     *  Returns the sum of all elements in the rows which pass the predicate.
     */
    @Override
    public long predicatedAllColumnsSum(int threshold) {
        // TODO: Implement this!
        int offset = ByteFormat.FIELD_LEN * numCols;
        long totalSum = 0;

        for(int row = 0; row < numRows; row++){
            int col0 = rows.getInt(row * offset);
            if(col0 <= threshold) continue;

            for(int col = 0; col < numCols; col++){
                int val = rows.getInt((col * ByteFormat.FIELD_LEN) + (row * offset));
                totalSum += val;
            }
        }
        return totalSum;
    }

    /**
     * Implements the query
     *   UPDATE(col3 = col3 + col2) WHERE col0 < threshold;
     *   Returns the number of rows updated.
     */
    @Override
    public int predicatedUpdate(int threshold) {
        // TODO: Implement this!
        int updated = 0;
        int offset = ByteFormat.FIELD_LEN * numCols;
        for(int row = 0; row < numRows; row++){
            int index0 = row * offset;
            int col0 = rows.getInt(index0);
            if(col0 < threshold){
                int col2 = rows.getInt(index0 + (2 * ByteFormat.FIELD_LEN));
                int index3 = index0 + (3 * ByteFormat.FIELD_LEN);
                int col3 = rows.getInt(index3) + col2;
                rows = rows.putInt(index3, col3);
                updated += 1;
            }
        }
        return updated;
    }
}
