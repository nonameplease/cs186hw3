package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Table;

import java.util.*;


public class SortOperator  {
  private Database.Transaction transaction;
  private String tableName;
  private Comparator<Record> comparator;
  private Schema operatorSchema;
  private int numBuffers;

  public SortOperator(Database.Transaction transaction, String tableName, Comparator<Record> comparator) throws DatabaseException, QueryPlanException {
    this.transaction = transaction;
    this.tableName = tableName;
    this.comparator = comparator;
    this.operatorSchema = this.computeSchema();
    this.numBuffers = this.transaction.getNumMemoryPages();
  }

  public Schema computeSchema() throws QueryPlanException {
    try {
      return this.transaction.getFullyQualifiedSchema(this.tableName);
    } catch (DatabaseException de) {
      throw new QueryPlanException(de);
    }
  }


  public class Run {
    String tempTableName;

    public Run() throws DatabaseException {
      this.tempTableName = SortOperator.this.transaction.createTempTable(SortOperator.this.operatorSchema);
    }

    public void addRecord(List<DataBox> values) throws DatabaseException {
      SortOperator.this.transaction.addRecord(this.tempTableName, values);
    }

    public void addRecords(List<Record> records) throws DatabaseException {
      for (Record r: records) {
        this.addRecord(r.getValues());
      }
    }

    public Iterator<Record> iterator() throws DatabaseException {
      return SortOperator.this.transaction.getRecordIterator(this.tempTableName);
    }

    public String tableName() {
      return this.tempTableName;
    }
  }


  /**
   * Returns a NEW run that is the sorted version of the input run.
   * Can do an in memory sort over all the records in this run
   * using one of Java's built-in sorting methods.
   * Note: Don't worry about modifying the original run.
   * Returning a new run would bring one extra page in memory beyond the
   * size of the buffer, but it is done this way for ease.
   */
  public Run sortRun(Run run) throws DatabaseException {
    //throw new UnsupportedOperationException("TODO(hw3): implement");
    Run r = new Run();
    List<Record> records = new ArrayList<>();
    run.iterator().forEachRemaining(records::add);

    records.sort(comparator);
    r.addRecords(records);

    return r;
  }



  /**
   * Given a list of sorted runs, returns a new run that is the result
   * of merging the input runs. You should use a Priority Queue (java.util.PriorityQueue)
   * to determine which record should be should be added to the output run next.
   * It is recommended that your Priority Queue hold Pair<Record, Integer> objects
   * where a Pair (r, i) is the Record r with the smallest value you are
   * sorting on currently unmerged from run i.
   */
  public Run mergeSortedRuns(List<Run> runs) throws DatabaseException {
    //throw new UnsupportedOperationException("TODO(hw3): implement");
    Queue<Pair<Record, Integer>> pq = new PriorityQueue<>((x, y) -> comparator.compare(x.getFirst(), y.getFirst()));

    List<Iterator<Record>> li = new ArrayList<>();

    int i = 0;

    for (Run r : runs) {
      li.add(i, r.iterator());

      if (li.get(i).hasNext()) {
        pq.add(new Pair<>(li.get(i).next(), i));
        i++;
      }
    }

    Run r= new Run();
    while(pq.size() > 0) {
      Pair<Record, Integer> nextRecord = pq.remove();
      r.addRecord(nextRecord.getFirst().getValues());

      if (li.get(nextRecord.getSecond()).hasNext()) {
        pq.add(new Pair<>(li.get(nextRecord.getSecond()).next(), nextRecord.getSecond()));
      }
    }
    return r;
  }

  /**
   * Given a list of N sorted runs, returns a list of
   * sorted runs that is the result of merging (numBuffers - 1)
   * of the input runs at a time.
   */
  public List<Run> mergePass(List<Run> runs) throws DatabaseException {
    //throw new UnsupportedOperationException("TODO(hw3): implement");
    List<Run> lr = new ArrayList<>();
    int step = numBuffers - 1;

    for (int i = 0; i < runs.size(); i += step) {
      lr.add(mergeSortedRuns(runs.subList(i, i + step)));
    }
    return lr;
  }


  /**
   * Does an external merge sort on the table with name tableName
   * using numBuffers.
   * Returns the name of the table that backs the final run.
   */
  public String sort() throws DatabaseException {
    //throw new UnsupportedOperationException("TODO(hw3): implement");
    BacktrackingIterator<Page> pageIterator = this.transaction.getPageIterator(this.tableName);
    pageIterator.next();
    List<Run> sortedRuns = new ArrayList<>();

    while (pageIterator.hasNext()) {
      Iterator<Record> recordIterator = this.transaction.getBlockIterator(this.tableName, pageIterator, this.numBuffers - 1);
      List<Record> records = new ArrayList<>();
      recordIterator.forEachRemaining(records::add);

      Run unsortedRun = new Run() {{
        addRecords(records);
      }};

      sortedRuns.add(sortRun(unsortedRun));
    }

    while (sortedRuns.size() > 1) {
      sortedRuns = this.mergePass(sortedRuns);
    }

    return sortedRuns.get(0).tableName();
  }

  private class RecordPairComparator implements Comparator<Pair<Record, Integer>> {
    public int compare(Pair<Record, Integer> o1, Pair<Record, Integer> o2) {
      return SortOperator.this.comparator.compare(o1.getFirst(), o2.getFirst());

    }
  }
  public Run createRun() throws DatabaseException {
    return new Run();
  }
}

