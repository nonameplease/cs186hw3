package edu.berkeley.cs186.database.query;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordIterator;
import edu.berkeley.cs186.database.table.Schema;
import sun.jvm.hotspot.debugger.win32.coff.COMDATSelectionTypes;

public class SortMergeOperator extends JoinOperator {

  public SortMergeOperator(QueryOperator leftSource,
                           QueryOperator rightSource,
                           String leftColumnName,
                           String rightColumnName,
                           Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new SortMergeOperator.SortMergeIterator();
  }

  public int estimateIOCost() throws QueryPlanException {
    //does nothing
    return 0;
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   *
   * Before proceeding, you should read and understand SNLJOperator.java
   *    You can find it in the same directory as this file.
   *
   * Word of advice: try to decompose the problem into distinguishable sub-problems.
   *    This means you'll probably want to add more methods than those given (Once again,
   *    SNLJOperator.java might be a useful reference).
   *
   */
  private class SortMergeIterator extends JoinIterator {
    /**
     * Some member variables are provided for guidance, but there are many possible solutions.
     * You should implement the solution that's best for you, using any member variables you need.
     * You're free to use these member variables, but you're not obligated to.
     */

    //private String leftTableName;
    //private String rightTableName;
    private RecordIterator leftIterator;
    private RecordIterator rightIterator;
    private Record leftRecord;
    private Record nextRecord;
    //private Record rightRecord;
    //private boolean marked;
    private int rightRecord;
    private Comparator<Record> cmp = (x, y) -> x.getValues().get(getLeftColumnIndex()).compareTo(y.getValues().get(getRightColumnIndex()));

    public SortMergeIterator() throws QueryPlanException, DatabaseException {
      super();
      //throw new UnsupportedOperationException("TODO(hw3): implement");

      Comparator<Record> lcmp = Comparator.comparing(x -> x.getValues().get(getLeftColumnIndex()));
      Comparator<Record> rcmp = Comparator.comparing(y -> y.getValues().get(getRightColumnIndex()));

      SortOperator left = new SortOperator(getTransaction(), getLeftTableName(), lcmp);
      SortOperator right = new SortOperator(getTransaction(), getRightTableName(), rcmp);

      this.leftIterator = SortMergeOperator.this.getRecordIterator(left.sort());
      this.rightIterator = SortMergeOperator.this.getRecordIterator(right.sort());
      this.rightRecord = -1;
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

      if (!((this.leftIterator.hasNext() || this.leftRecord != null))) {
        return false;
      }

      if (this.rightRecord == 0) {
        this.rightIterator.mark();
        this.rightRecord = 1;
      }

      Record left = this.leftRecord != null ? this.leftRecord : this.leftIterator.next();
      Record right;

      if (!this.rightIterator.hasNext()) {
        if (this.leftIterator.hasNext()) {
          this.leftRecord = this.leftIterator.next();
        } else {
          return false;
        }

        this.rightIterator.reset();
        return hasNext();
      } else {
        right = this.rightIterator.next();
      }

      if (this.rightRecord == -1) {
        this.rightRecord = 1;
        this.rightIterator.mark();
      }

      if (this.leftRecord == null) {
        this.leftRecord = left;
      }

      while (this.cmp.compare(left, right) != 0) {
        if (this.cmp.compare(left, right) < 0) {
          if (!this.leftIterator.hasNext()) {
            return false;
          }

          left = this.leftIterator.next();
          this.leftRecord = left;
          this.rightIterator.reset();
          right = this.rightIterator.next();
        } else {
          if (!this.rightIterator.hasNext()) {
            return false;
          }

          right = this.rightIterator.next();
          this.rightRecord = 0;
        }
      }

      List<DataBox> leftValues = new ArrayList<>(left.getValues());
      List<DataBox> rightValues = new ArrayList<>(right.getValues());
      leftValues.addAll(rightValues);
      this.nextRecord = new Record(leftValues);
      return true;
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
        Record nextRecord = this.nextRecord;
        this.nextRecord = null;
        return nextRecord;
      }

      throw new NoSuchElementException("next() on empty iterator");
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }


    private class LeftRecordComparator implements Comparator<Record> {
      public int compare(Record o1, Record o2) {
        return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
      }
    }

    private class RightRecordComparator implements Comparator<Record> {
      public int compare(Record o1, Record o2) {
        return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
      }
    }

    /**
     * Left-Right Record comparator
     * o1 : leftRecord
     * o2: rightRecord
     */
    private class LR_RecordComparator implements Comparator<Record> {
      public int compare(Record o1, Record o2) {
        return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
      }
    }
  }
}