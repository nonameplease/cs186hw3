package edu.berkeley.cs186.database.query; //hw4

import java.nio.ByteBuffer;
import java.util.*;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

public class BNLJOperator extends JoinOperator {

    private int numBuffers;

    public BNLJOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        Database.Transaction transaction) throws QueryPlanException, DatabaseException {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.BNLJ);

        this.numBuffers = transaction.getNumMemoryPages();
    }

    public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
        return new BNLJIterator();
    }

    public int estimateIOCost() throws QueryPlanException {
        //This method implements the the IO cost estimation of the Block Nested Loop Join

        int usableBuffers = numBuffers - 2; //Common mistake have to first calculate the number of usable buffers

        int numLeftPages = getLeftSource().getStats().getNumPages();

        int numRightPages = getRightSource().getStats().getNumPages();

        return ((int) Math.ceil((double) numLeftPages / (double) usableBuffers)) * numRightPages + numLeftPages;

    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     */

    private class BNLJIterator extends JoinIterator {
        /**
         * Some member variables are provided for guidance, but there are many possible solutions.
         * You should implement the solution that's best for you, using any member variables you need.
         * You're free to use these member variables, but you're not obligated to.
         */

        private Iterator<Page> leftIterator = null;
        private Iterator<Page> rightIterator = null;
        private BacktrackingIterator<Record> leftRecordIterator = null;
        private BacktrackingIterator<Record> rightRecordIterator = null;
        private Record leftRecord = null;
        private Record nextRecord = null;

        private Page[] leftPages;
        private Page rightPage;

        public BNLJIterator() throws QueryPlanException, DatabaseException {
            super();
            //throw new UnsupportedOperationException("TODO(hw3): implement");
            this.leftIterator = BNLJOperator.this.getPageIterator(this.getLeftTableName());
            this.rightIterator = BNLJOperator.this.getPageIterator(this.getRightTableName());
            this.leftIterator.next();
            this.rightIterator.next();

            this.leftPages = new Page[numBuffers];
            for (int i = 0; i < numBuffers; i++) {
                this.leftPages[i] = this.leftIterator.hasNext() ? this.leftIterator.next() : null;
            }

            this.leftPages = Arrays.stream(this.leftPages).filter(x -> x != null).toArray(Page[]::new);

            this.rightPage = this.rightIterator.hasNext() ? this.rightIterator.next() : null;

            this.leftRecordIterator = BNLJOperator.this.getBlockIterator(this.getLeftTableName(), this.leftPages);

            /*this.leftRecordIterator = BNLJOperator.this.getBlockIterator(this.getLeftTableName(), new Page[]{this.leftIterator.next()});
            this.rightRecordIterator = BNLJOperator.this.getBlockIterator(this.getRightTableName(), new Page[]{this.rightIterator.next()});

            this.leftRecord = this.leftRecordIterator.hasNext() ? this.leftRecordIterator.next() : null;
            this.nextRecord = null;


            if (this.rightRecordIterator.hasNext()) {
                this.rightRecordIterator.next();
                this.rightRecordIterator.mark();
                this.rightRecordIterator.reset();
            } else {
                return;
            }*/
        }

        private void resetRightRecord() {
            this.rightRecordIterator.reset();
            assert(this.rightRecordIterator.hasNext());
            rightRecordIterator.mark();
        }

        private void nextLeftRecord() {

        }


        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        public boolean hasNext() {
            //throw new UnsupportedOperationException("TODO(hw3): implement");
            if (this.nextRecord != null) {
                return true;
            }

            try{
                while (true) {
                    if (this.leftRecord == null) {
                        if (this.leftRecordIterator.hasNext()) {
                            this.leftRecord = this.leftRecordIterator.next();
                            this.rightRecordIterator = BNLJOperator.this.getBlockIterator(this.getRightTableName(), new Page[]{this.rightPage});
                        } else {
                            if (!this.rightRecordIterator.hasNext()) {
                                this.leftPages = new Page[numBuffers];
                                for (int i = 0; i < numBuffers; i++) {
                                    this.leftPages[i] = this.leftIterator.hasNext() ? this.leftIterator.next() : null;
                                }

                                this.leftPages = Arrays.stream(this.leftPages).filter(x -> x != null).toArray(Page[]::new);

                                this.leftRecordIterator = BNLJOperator.this.getBlockIterator(this.getLeftTableName(), this.leftPages);

                                if (!this.leftRecordIterator.hasNext()) {
                                    return false;
                                }

                                this.leftRecord = this.leftRecordIterator.next();
                                this.rightIterator = BNLJOperator.this.getPageIterator(this.getRightTableName());
                                this.rightIterator.next();
                            } else {
                                this.leftRecordIterator = BNLJOperator.this.getBlockIterator(this.getLeftTableName(), leftPages);
                                assert(leftRecordIterator.hasNext());
                                this.leftRecord = this.leftRecordIterator.next();
                            }

                            this.rightPage = this.rightIterator.next();
                            this.rightRecordIterator = BNLJOperator.this.getBlockIterator(this.getRightTableName(), new Page[]{rightPage});
                        }
                    }
                    while (this.rightRecordIterator.hasNext()) {
                        Record rightRecord = this.rightRecordIterator.next();
                        DataBox leftJoinValue = this.leftRecord.getValues().get(BNLJOperator.this.getLeftColumnIndex());
                        DataBox rightJoinValue = rightRecord.getValues().get(BNLJOperator.this.getRightColumnIndex());
                        if (leftJoinValue.equals(rightJoinValue)) {
                            List<DataBox> leftValues = new ArrayList<>(this.leftRecord.getValues());
                            List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
                            leftValues.addAll(rightValues);
                            this.nextRecord = new Record(leftValues);
                            return true;
                        }
                    }
                    this.leftRecord = null;
                }
            } catch (DatabaseException e) {
                System.err.println(e.getMessage());
                return false;
            }
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        public Record next() {
            //throw new UnsupportedOperationException("TODO(hw3): implement");
            if (this.nextRecord != null) {
                Record nextRecrod = this.nextRecord;
                this.nextRecord = null;
                return nextRecrod;
            }
            throw new  NoSuchElementException("next() on empty");
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

    }
}
