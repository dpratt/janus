package janus

import bitronix.tm.resource.jdbc.PoolingDataSource
import org.scalatest.FlatSpec
import bitronix.tm.TransactionManagerServices
import org.springframework.transaction.jta.JtaTransactionManager
import org.springframework.jdbc.datasource.DataSourceUtils
import java.sql.ResultSet
import concurrent.Await
import java.util.concurrent.TimeUnit
import concurrent.duration.Duration

class JTASpec extends FlatSpec {

  "A database" should "properly participate in an XA tranasaction with a single datasource" in {
    val ds = emptyDataSource("first-ds")
    val txManager = transactionManager
    val db = new SpringDatabase(ds, new JtaTransactionManager(txManager, txManager))
    try {
      db.withSession { session =>
        val rows = session.executeQuery("select count(*) from test")
        assert(1 === rows.head(0).as[Long])

        intercept[RuntimeException] {
          session.withTransaction { trans =>
            session.executeSql("insert into test (id, name) VALUES (3, 'Test Value')")
            throw new RuntimeException("Sample exception")
          }
        }
      }

      //create a new session to ensure isolation from the above operations
      db.withSession { session =>
        val rows = session.executeQuery("select count(*) from test")
        assert(1 === rows.head(0).as[Long])
        //should only be one row - the rows inserted above should have been rolled back
      }
    } finally {
      ds.close()
      txManager.shutdown()
    }

  }

  "A database" should "roll back transactions in another context" in {
    val unmanagedDs = emptyDataSource("first-ds")
    val managedDs = emptyDataSource("second-ds")
    val txManager = transactionManager
    val managedDatabase = new SpringDatabase(unmanagedDs, new JtaTransactionManager(txManager, txManager))
    try {

      def validateUnmanaged(expectedCount: Int) {
        //validate the unmanaged ds has the proper number of rows
        val unmanagedConn = DataSourceUtils.getConnection(unmanagedDs)
        val unmanagedStmt = unmanagedConn.createStatement()
        val rs: ResultSet = unmanagedStmt.executeQuery("select count(*) from test")
        assertResult(expectedCount) {
          rs.next()
          rs.getInt(1)
        }
        rs.close()
        unmanagedStmt.close()
        DataSourceUtils.releaseConnection(unmanagedConn, unmanagedDs)
      }

      def validateManaged(expectedCount: Int) {
        managedDatabase.withSession { session =>
          val rows = session.executeQuery("select count(*) from test")
          assert(expectedCount === rows.head(0).as[Long])
        }
      }

      def validateUnsynchronized(expectedCount: Int) {
        //query in a different thread (and thus not participate in an active transaction)
        val validF = managedDatabase.withDetachedSession { session =>
          val rows = session.executeQuery("select count(*) from test")
          assert(expectedCount === rows.head(0).as[Long])
        }
        Await.result(validF, Duration(30, TimeUnit.SECONDS))

      }
      managedDatabase.withSession { session =>
        //ensure that there's only one row in the managed DB
        validateManaged(1)
        validateUnmanaged(1)

        intercept[RuntimeException] {
          //start a transaction
          session.withTransaction { transaction =>
            //okay, now insert a row in both databases
            val newUnmanagedConn = DataSourceUtils.getConnection(unmanagedDs)
            val newUnmanagedStmt = newUnmanagedConn.createStatement()
            newUnmanagedStmt.execute("insert into test values(2, 'Hello1', 24)")
            newUnmanagedStmt.close()
            DataSourceUtils.releaseConnection(newUnmanagedConn, unmanagedDs)

            session.executeSql("insert into test values(3, 'Hello3', 25)")

            validateManaged(2)
            validateUnmanaged(2)
            //other threads should only see one row
            validateUnsynchronized(1)

            //okay, now throw to rollback
            throw new RuntimeException("Rolling back.")
          }
        }
      }
      validateManaged(1)
      validateUnmanaged(1)

    } finally {
      unmanagedDs.close()
      managedDs.close()
      txManager.shutdown()
    }
  }

  private def transactionManager = {
    val config = TransactionManagerServices.getConfiguration
    config.setServerId("test-id")
    config.setLogPart1Filename("/tmp/btm1.tlog")
    config.setLogPart2Filename("/tmp/btm2.tlog")

    TransactionManagerServices.getTransactionManager
  }

  private def emptyDataSource(dsName: String): PoolingDataSource = {

    val ds = new PoolingDataSource
    val url = "jdbc:h2:mem:" + dsName
    ds.setClassName("org.h2.jdbcx.JdbcDataSource")
    ds.getDriverProperties.put("URL", url)
    ds.setUniqueName(dsName)
    ds.setMinPoolSize(1)
    ds.setMaxPoolSize(20)
    ds.setAcquireIncrement(1)
    ds.setAllowLocalTransactions(true)
    ds.setPreparedStatementCacheSize(20)

    ds.init()

    val c = ds.getConnection
    try {
      val stat = c.createStatement()
      stat.execute("DROP ALL OBJECTS")
      stat.execute("create table test(id int primary key, name varchar(255) NOT NULL, score BIGINT)")
      stat.execute("insert into test values(1, 'Hello', 23)")

      stat.execute("create table orgs(id int primary key, name varchar(255) NOT NULL)")
      stat.execute("create table users(id int primary key, name varchar(255) NOT NULL, org_id int NOT NULL)")

    } finally {
      c.close()
    }
    ds
  }


}
