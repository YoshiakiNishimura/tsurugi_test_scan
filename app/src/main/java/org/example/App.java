package org.example;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.SessionBuilder;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.Transaction;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.sql.Placeholders;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.sql.ResultSet;
import com.tsurugidb.tsubakuro.sql.Parameters;
import com.tsurugidb.tsubakuro.kvs.KvsClient;
import com.tsurugidb.tsubakuro.kvs.RecordBuffer;
import com.tsurugidb.tsubakuro.kvs.TransactionHandle;

class Table {
    private String name;
    private int start;
    private int end;

    public Table(String n, int s, int e) {
        this.name = n;
        this.start = s;
        this.end = e;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getStart() {
        return start;
    }

    public void setStart(int start) {
        this.start = start;
    }

    public int getEnd() {
        return end;
    }

    public void setEnd(int end) {
        this.end = end;
    }
}

public class App {
    static int start = 0;
    static int end = 10;
    static int end2 = 20;

    public static void sql(String sql_statement, SqlClient sql) {
        try (PreparedStatement preparedStatement = sql.prepare(sql_statement, Placeholders.of("id", int.class),
                Placeholders.of("name", int.class), Placeholders.of("note", int.class)).get();) {
            try (Transaction transaction = sql.createTransaction().get()) {
                try (FutureResponse<ResultSet> resultSet = transaction.executeQuery(preparedStatement,
                        Parameters.of("id", (int) 999), Parameters.of("name", (int) 999),
                        Parameters.of("note", (int) 999))) {
                    System.out.println(sql_statement);
                    ResultSet r = resultSet.await();
                    while (r.nextRow()) {
                        while (r.nextColumn()) {
                            if (!r.isNull()) {
                                System.out.print(r.fetchInt4Value() + " ");
                            }
                        }
                        System.out.println("");
                    }
                }
                transaction.close();
            }
            preparedStatement.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void sql2(String sql_statement, SqlClient sql) {
        System.out.println(sql_statement);
        try (Transaction transaction = sql.createTransaction().get()) {
            transaction.executeStatement(sql_statement).await();
            transaction.commit().await();
            transaction.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public static void insert(KvsClient kvs, Table t) {
        try (TransactionHandle tx = kvs.beginTransaction().await()) {
            IntStream.range(t.getStart(), t.getEnd()).forEach(i -> {
                RecordBuffer record = new RecordBuffer();
                record.add("id", i);
                record.add("name", i + 1);
                record.add("note", i + 2);
                try {
                    kvs.put(tx, t.getName(), record).await();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            kvs.commit(tx).await();
            tx.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void dropAndCreate(SqlClient sql, Table t) {
        try (Transaction transaction = sql.createTransaction().get()) {
            transaction.executeStatement(String.format("DROP TABLE %s", t.getName())).await();
            transaction.commit().await();
            transaction.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        try (Transaction transaction = sql.createTransaction().await()) {
            transaction
                    .executeStatement(
                            String.format("create table %s (id int primary key , name int , note int)", t.getName()))
                    .await();
            transaction.commit().await();
            transaction.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        List<Table> tables = new ArrayList<Table>();
        String tableName = "test_table";
        String tableName2 = "test_table2";
        String tableName3 = "test_table3";
        tables.add(new Table(tableName, 0, 10));
        tables.add(new Table(tableName2, 11, 20));
        tables.add(new Table("insert_table", 21, 30));
        List<String> sqls = new ArrayList<String>();

        sqls.add(String.format("select count(*) from %s ", tableName));
        sqls.add(String.format("select max(id) from %s ", tableName));
        sqls.add(String.format("select min(id) from %s ", tableName));
        sqls.add(String.format("select avg(id) from %s ", tableName));
        sqls.add(String.format("select sum(id) from %s ", tableName));
        sqls.add(String.format("SELECT * FROM %s cross join %s", tableName, tableName2));
        sqls.add(String.format("SELECT * FROM %s a RIGHT OUTER JOIN %s b on a.id = b.id;", tableName, tableName2));
        sqls.add(String.format("SELECT count(*) FROM %s a RIGHT OUTER JOIN %s b on a.id = b.id;", tableName,
                tableName2));
        sqls.add(String.format("select sum(name) from %s where id > 3", tableName));
        sqls.add(String.format("select max(name) from %s where name is not null", tableName));
        sqls.add(String.format("select * from %s where name between 1 and 10", tableName));
        sqls.add(String.format("select * from %s where name in ( 1 , 10)", tableName));
        sqls.add(String.format("select * from %s ORDER BY name DESC", tableName));
        sqls.add(String.format("select * from %s ORDER BY name DESC LIMIT 1", tableName));
        sqls.add(String.format(
                "SELECT t1.id, t1.name, t1.note FROM %s t1 JOIN ( SELECT id FROM %s WHERE note >= 5 ) t2 ON t1.id = t2.id;",
                tableName, tableName));
        sqls.add(String.format(
                "SELECT t1.id, t1.name, t1.note FROM %s t1 JOIN ( SELECT id, SUM(note) AS total_note FROM %s GROUP BY id HAVING SUM(note) > 4 ) t2 ON t1.id = t2.id ORDER BY t2.total_note DESC",
                tableName, tableName));
        sqls.add(String.format(
                "SELECT t1.id, t1.name, t1.note FROM %s t1 JOIN ( SELECT name, MAX(id) as max_id FROM %s GROUP BY name ) t2 ON t1.name = t2.name AND t1.id = t2.max_id ORDER BY t1.name",
                tableName, tableName));
        sqls.add(String.format(
                "SELECT t1.id, t1.name, t1.note FROM %s t1 JOIN ( SELECT id, SUM(note) AS total_note FROM %s GROUP BY id HAVING SUM(note) > 5 ) t2 ON t1.id = t2.id WHERE t1.name < 100 ORDER BY t2.total_note DESC",
                tableName, tableName));
        sqls.add(String.format("select count(*) from %s ", tableName3));
        try (Session session = SessionBuilder.connect("ipc://tsurugi").create();
                SqlClient sql = SqlClient.attach(session);
                KvsClient kvs = KvsClient.attach(session)) {
            tables.stream().forEach(table -> {
                dropAndCreate(sql, table);
                insert(kvs, table);
            });
            dropAndCreate(sql, new Table(tableName3, 0, 0));

            sql2(String.format(
                    "INSERT INTO %s (id, name) SELECT t1.id, t1.name FROM %s t1 JOIN ( SELECT id, SUM(note) AS total_note FROM test_table GROUP BY id HAVING SUM(note) > 5 ) t2 ON t1.id = t2.id WHERE t1.name < 100",
                    tableName3, tableName), sql);
            sqls.stream().forEach(s -> {
                sql(s, sql);

            });
            sql2(String.format(
                    "update %s set name = 3",
                    tableName), sql);
            sql(String.format("select * from %s", tableName), sql);
            tables.stream().forEach(table -> {
                dropAndCreate(sql, table);
                insert(kvs, table);
            });
            dropAndCreate(sql, new Table(tableName3, 0, 0));
            sql2(String.format(
                    "delete from %s where name = 3",
                    tableName), sql);
            sql(String.format("select * from %s", tableName), sql);
            sql2(String.format(
                    "INSERT INTO %s SELECT * FROM %s",
                    tableName, tableName2), sql);
            sql(String.format("select * from %s", tableName), sql);
            sql2(String.format(
                    "delete from %s",
                    tableName), sql);
            sql(String.format("select count(*) from %s", tableName), sql);
            sql.close();
            kvs.close();
            session.close();
        }
    }
}
