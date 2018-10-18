package edu.berkeley.cs186.database.query; //hw4

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Arrays;

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
        private Page[] currentLeftPages;
        private Page currentRightPage;


        public BNLJIterator() throws QueryPlanException, DatabaseException {
            super();
            //throw new UnsupportedOperationException("TODO(hw3): implement");

            leftIterator = getPageIterator(getLeftTableName());
            rightIterator = getPageIterator(getRightTableName());

            rightIterator.next();
            leftIterator.next();

            currentLeftPages = new Page[numBuffers];
            for (int i = 0; i < numBuffers; i++) {
                currentLeftPages[i] = leftIterator.hasNext() ? leftIterator.next() : null;
            }

            currentLeftPages = Arrays.stream(currentLeftPages).filter(x -> x != null).toArray(Page[]::new);

            currentRightPage = rightIterator.next();
            leftRecordIterator = getBlockIterator(getLeftTableName(), currentLeftPages);

        }


        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        public boolean hasNext() {
            //throw new UnsupportedOperationException("TODO(hw3): implement");

            if (nextRecord != null) {
                return true;
            }

            try {
                while (true) {
                    if (leftRecord == null) {
                        if (leftRecordIterator.hasNext()) {
                            leftRecord = leftRecordIterator.next();
                            rightRecordIterator = getBlockIterator(getRightTableName(), new Page[]{currentRightPage});
                        } else {
                            if (!rightIterator.hasNext()) {
                                currentLeftPages = new Page[numBuffers];
                                for (int i = 0; i < numBuffers; i++) {
                                    currentLeftPages[i] = leftIterator.hasNext() ? leftIterator.next() : null;
                                }

                                currentLeftPages = Arrays.stream(currentLeftPages).filter(x -> x != null).toArray(Page[]::new);

                                leftRecordIterator = getBlockIterator(getLeftTableName(), currentLeftPages);
                                if (!leftRecordIterator.hasNext()) {
                                    return false;
                                }

                                leftRecord = leftRecordIterator.next();
                                rightIterator = getPageIterator(getRightTableName());
                                rightIterator.next();
                            } else {
                                leftRecordIterator = getBlockIterator(getLeftTableName(), currentLeftPages);
                                assert leftRecordIterator.hasNext() : "leftRecordIterator degenerate";
                                leftRecord = leftRecordIterator.next();
                            }

                            currentRightPage = rightIterator.next();
                            rightRecordIterator = getBlockIterator(getRightTableName(), new Page[]{currentRightPage});
                        }
                    }
                    while (rightRecordIterator.hasNext()) {
                        Record rightRecord = rightRecordIterator.next();
                        DataBox leftJoinValue = leftRecord.getValues().get(getLeftColumnIndex());
                        DataBox rightJoinValue = rightRecord.getValues().get(getRightColumnIndex());

                        if (leftJoinValue.equals(rightJoinValue)) {
                            List<DataBox> leftValues = new ArrayList<>(leftRecord.getValues());
                            List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
                            leftValues.addAll(rightValues);
                            nextRecord = new Record(leftValues);
                            return true;
                        }
                    }
                    leftRecord = null;
                }
            } catch (DatabaseException e) {
                System.err.println("Caught database error " + e.getMessage());
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
            if (nextRecord != null) {
                Record out = nextRecord;
                nextRecord = null;
                return out;
            }
            throw new NoSuchElementException("next() on empty iterator");
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

    }
}
