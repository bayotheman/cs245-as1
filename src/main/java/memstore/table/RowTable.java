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
        return rows.getInt((ByteFormat.FIELD_LEN * numCols * rowId) + colId);
    }

    /**
     * Inserts the passed-in int field at row `rowId` and column `colId`.
     */
    @Override
    public void putIntField(int rowId, int colId, int field) {
        int index = (ByteFormat.FIELD_LEN * rowId * numCols) + colId;
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
        return columnSum(0);
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
        long sum = 0;
        for(int rowId = 0; rowId < numRows; rowId++){
            int offset = ByteFormat.FIELD_LEN * numCols * rowId;
            int col1 = rows.getInt(offset + ByteFormat.FIELD_LEN);
            int col2 = rows.getInt(offset + ((ByteFormat.FIELD_LEN >> 1) << 2)); // same as ByteFormat.FIELD_LEN  * 2
            if(col1 > threshold1 && col2 < threshold2){
                sum += rows.getInt(offset);
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
        long sum = 0;
        for(int rowId = 0; rowId < numRows; rowId++){
            int offset = ByteFormat.FIELD_LEN * numCols * rowId ;
            int col0 = rows.getInt(offset);
            if(col0 <= threshold) {
                continue;
            }
            for(int colId = 0; colId < numCols; colId++){
                offset = (ByteFormat.FIELD_LEN * numCols * rowId) + (colId * ByteFormat.FIELD_LEN) ;
                int value = rows.getInt(offset);
                sum+= value;
            }
        }
        return sum;
    }

    private long columnSum(int colId){
        long sum = 0;
        for(int rowId = 0; rowId < numRows; rowId++) {
            int offset = (ByteFormat.FIELD_LEN * numCols * rowId) + (colId * ByteFormat.FIELD_LEN);
            sum += rows.getInt(offset);
        }
        return sum;
    }
    /**
     * Implements the query
     *   UPDATE(col3 = col3 + col2) WHERE col0 < threshold;
     *
     *   Returns the number of rows updated.
     */
    @Override
    public int predicatedUpdate(int threshold) {
        int numOfUpdatedRows = 0;
        for(int rowId = 0; rowId < numRows; rowId++){
            int offset = (ByteFormat.FIELD_LEN * numCols * rowId);
            int col0 = rows.getInt(offset);
            if(col0 < threshold){
                int col2Offset = offset + ((ByteFormat.FIELD_LEN >> 1) << 2);
                int col2 = rows.getInt(col2Offset);
                int col3Offset = offset + (((ByteFormat.FIELD_LEN >> 1) << 2 ) | 0x04);
                int col3 = rows.getInt(col3Offset);
                putIntField(rowId, col3, col2 + col3);
                numOfUpdatedRows++;
            }
        }
        return numOfUpdatedRows;
    }
}
