package edu.berkeley.cs186.database.query;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

import javax.xml.crypto.Data;

public class PNLJOperator extends JoinOperator {

  public PNLJOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource,
          rightSource,
          leftColumnName,
          rightColumnName,
          transaction,
          JoinType.PNLJ);

  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new PNLJIterator();
  }


  public int estimateIOCost() throws QueryPlanException {
	    //does nothing
	    return 0;
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class PNLJIterator extends JoinIterator {
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

    public PNLJIterator() throws QueryPlanException, DatabaseException {
      super();
      //throw new UnsupportedOperationException("TODO(hw3): implement");
      this.leftIterator = PNLJOperator.this.getPageIterator(this.getLeftTableName());
      this.rightIterator = PNLJOperator.this.getPageIterator(this.getRightTableName());

      this.nextRecord = null;

      this.leftIterator.next();
      this.rightIterator.next();

      this.leftRecordIterator = PNLJOperator.this.getRecordIterator(this.getLeftTableName());
      this.rightRecordIterator = PNLJOperator.this.getRecordIterator(this.getRightTableName());

      this.leftRecord = leftIterator.hasNext() ? leftRecordIterator.next() : null;

      //mark the first record so we can reset to it when we advance the left record.
      if (rightRecordIterator != null) {
        rightRecordIterator.mark();
      } else {
        return;
      }


      //??? maybe more stuff?
    }



    private void resetRightRecord() {
      this.rightRecordIterator.reset();
      assert(rightRecordIterator.hasNext());
      nextRecord = rightRecordIterator.next();
      rightRecordIterator.mark();
    }


    private void nextLeftRecord() throws DatabaseException {
      if (!leftIterator.hasNext()) {
        throw new DatabaseException("All Done!");
      }

      if(leftRecordIterator.hasNext()) {
        leftRecord = leftRecordIterator.next();
      } else {
        leftIterator.next();
        this.leftRecordIterator = PNLJOperator.this.getRecordIterator(this.getLeftTableName());
        leftRecord = leftRecordIterator.next();
      }
    }


    private void fetchNextRecord() throws DatabaseException {
      if (!this.leftIterator.hasNext()) {
        throw new DatabaseException("No new page to fetch");
      }
      this.nextRecord = null;
      do {
        if (this.rightRecordIterator.hasNext()) {
          Record rightRecord = rightRecordIterator.next();
          DataBox leftJoinValue = this.leftRecord.getValues().get(PNLJOperator.this.getLeftColumnIndex());
          DataBox rightJoinValue = rightRecord.getValues().get(PNLJOperator.this.getRightColumnIndex());
          if (leftJoinValue.equals(rightJoinValue)) {
            List<DataBox> leftValues = new ArrayList<>(this.leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
            leftValues.addAll(rightValues);
            this.nextRecord = new Record(leftValues);
          }
        } else {
          nextLeftRecord();
          resetRightRecord();
        }
      } while (!hasNext());
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

      if (this.leftRecord != null) {

      }
      return false;
    }

    /**
     * Yields the next record of this iterator.
     *
     * @return the next Record
     * @throws NoSuchElementException if there are no more Records to yield
     */
    public Record next() {
        //throw new UnsupportedOperationException("TODO(hw3): implement");
      if (!this.hasNext()) {
        throw new NoSuchElementException();
      }

      Record nextRecord = this.nextRecord;
      return nextRecord;
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}

