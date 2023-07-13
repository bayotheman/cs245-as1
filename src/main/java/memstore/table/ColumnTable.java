package memstore.table;

import memstore.data.ByteFormat;
import memstore.data.DataLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * ColumnTable, which stores data in column-major format.
 * That is, data is laid out like
 *   col 1 | col 2 | ... | col m.
 */
public class ColumnTable implements Table {
    int numCols;
    int numRows;
    ByteBuffer columns;

    public ColumnTable() { }

    /**
     * Loads data into the table through passed-in data loader. Is not timed.
     *
     * @param loader Loader to load data from.
     * @throws IOException
     */
    public void load(DataLoader loader) throws IOException {
        this.numCols = loader.getNumCols();
        List<ByteBuffer> rows = loader.getRows();
        numRows = rows.size();
        this.columns = ByteBuffer.allocate(ByteFormat.FIELD_LEN*numRows*numCols);

        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            for (int colId = 0; colId < numCols; colId++) {
                int offset = ByteFormat.FIELD_LEN * ((colId * numRows) + rowId);
                this.columns.putInt(offset, curRow.getInt(ByteFormat.FIELD_LEN*colId));
            }
        }
    }

    /**
     * Returns the int field at row `rowId` and column `colId`.
     */
    @Override
    public int getIntField(int rowId, int colId) {
        int offset = ByteFormat.FIELD_LEN * ((colId * numRows) + rowId);
        return columns.getInt(offset);
    }

    /**
     * Inserts the passed-in int field at row `rowId` and column `colId`.
     */
    @Override
    public void putIntField(int rowId, int colId, int field) {
        int offset = ByteFormat.FIELD_LEN * ((colId * numRows) + rowId);
        columns.putInt(offset, field);
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
        int len = ByteFormat.FIELD_LEN * numRows;
        long sum = 0;
        for(int row = 0; row < numRows; row ++){
            sum += columns.getInt(row * ByteFormat.FIELD_LEN);
        }
//        for(int row = 0; row < len; row += ByteFormat.FIELD_LEN){
//            sum += columns.getInt(row);
//        }
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
        int offset = ByteFormat.FIELD_LEN * numRows;
        long sum = 0;
        for(int row = 0; row < numRows; row ++){
            int col1 = columns.getInt((row * ByteFormat.FIELD_LEN) + offset);
            int col2 = columns.getInt((row * ByteFormat.FIELD_LEN) + (offset * 2));

            if(col1 > threshold1 && col2 < threshold2)
                sum += columns.getInt(row * ByteFormat.FIELD_LEN);
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
        long totalSum = 0;
        int offset = ByteFormat.FIELD_LEN * numRows;

       for(int col = 0; col < numCols; col++){
           int sum = 0;
           for(int row = 0; row < numRows; row++){
               int col0 = columns.getInt(row * ByteFormat.FIELD_LEN);
               if(col0 > threshold){
                   sum += columns.getInt((ByteFormat.FIELD_LEN * row) + (col * offset));
               }
           }
           System.out.printf("sum of col%s: %s \n",col, sum);
           totalSum += sum;
       }

        System.out.printf("total sum: %s\n",totalSum);
        return totalSum;
    }

    /**
     * Implements the query
     *   UPDATE(col3 = col3 + col2) WHERE col0 < threshold;
     *
     *   Returns the number of rows updated.
     */
    @Override
    public int predicatedUpdate(int threshold) {
        // TODO: Implement this!
        int offset = ByteFormat.FIELD_LEN * numRows;
        int rowUpdated = 0;
        for(int row = 0; row < numRows; row++){
            int col0 = columns.getInt(row * ByteFormat.FIELD_LEN);


            if(col0 < threshold ){
                int index0 = (row * ByteFormat.FIELD_LEN);
                int index2 = index0 + (offset * 2);
                int index3= index0 +(offset * 3);

                int col2 = columns.getInt(index2);
                int col3 = columns.getInt(index3);
                int sum = col3 + col2;

                columns.putInt(index3, sum);
                rowUpdated+= 1;
            }

        }
        return rowUpdated;
    }
}
