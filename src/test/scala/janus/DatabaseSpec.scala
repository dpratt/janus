package janus

import org.scalatest.FlatSpec

import scala.language.postfixOps
import com.jolbox.bonecp.BoneCPDataSource
import javax.sql.DataSource
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import java.util.UUID

class DatabaseSpec extends FlatSpec {

  def basicDatabase = {
    val ds = emptyDataSource()
    new JdbcDatabase(ds)
  }

  def springDatabase = {
    val ds = emptyDataSource()
    new SpringDatabase(ds, new DataSourceTransactionManager(ds))
  }

  "A Database based on a simple DataSource" should behave like basicDatabaseBehavior(basicDatabase)
  "A Database synchronized with Spring transactions" should behave like basicDatabaseBehavior(springDatabase)

  def basicDatabaseBehavior(newDatabase: => Database) {

    it should "support basic queries" in {
      val db = newDatabase
      db.withSession { session =>
        session.withPreparedStatement("select * from test") { ps =>
          val rows = ps.executeQuery()
          assert(rows.size == 1)
          val row = rows.head
          assert(1 === row.value("id").as[Int])
          assert("Hello" === row.value(1).as[String])
          assert(23 == row.value("score").as[Long])
        }
      }
    }

    it should "support inserting inside a transaction" in {
      val db = newDatabase

      db.withSession { session =>
        session.withTransaction { transaction =>
          //insert a value
          session.withPreparedStatement("insert into test (id, name) VALUES (?,?)") { ps =>
            ps.setParam(0, DbValue.fromAny(2))
            ps.setParam(1, DbValue.fromAny("Test 2"))
            ps.executeUpdate()
          }
        }
      }

      db.withSession { session =>
        //query for this value
        val rows = session.executeQuery("select * from test where id = 2")
        assert(1 === rows.size)
        assert("Test 2" === rows.head(1).as[String])
      }
    }

    it should "properly rollback on exception" in {
      val db = newDatabase

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
        //should only be one row - the rows inserted above should have been rolled back
        assert(1 === rows.head(0).as[Long])
      }
    }

    it should "support nested transactions" in {
      val db = newDatabase

      db.withSession { session =>
        intercept[RuntimeException] {
          session.withTransaction { transaction =>
            session.executeSql("insert into test (id, name) VALUES (3, 'Test 3')")
            intercept[RuntimeException] {
              session.withTransaction { nested =>
                val rows = session.executeQuery("select count(*) from test")
                assert(2 === rows.head(0).as[Int])
                //throw an exception - this *shouldn't* roll back the transaction since we're nested
                throw new RuntimeException("This is a sample")
              }
            }
            //should still be there
            val rows = session.executeQuery("select count(*) from test")
            assert(2 == rows.head(0).as[Int])
            //throw another exception - this *should* roll it back
            throw new RuntimeException("This is a second exception")
          }
        }
        //row should be gone
        val rows = session.executeQuery("select count(*) from test")
        assert(1 === rows.head(0).as[Long])
      }

      //more checks - ensure that it doesn't show up in another session either
      db.withSession { session =>
      //row should be gone
        val rows = session.executeQuery("select count(*) from test")
        assert(1 === rows.head(0).as[Long])
      }
    }

    it should "support manual rollback" in {
      val db = newDatabase

      db.withSession { session =>
        session.withTransaction { transaction =>
          session.executeSql("insert into test (id, name) VALUES (3, 'Test 3')")
          //should still be there
          val rows = session.executeQuery("select count(*) from test")
          assert(2 === rows.head(0).as[Long])
          transaction.setRollback()
        }
        //row should be gone, due to manual rollback
        val rows = session.executeQuery("select count(*) from test")
        assert(1 === rows.head(0).as[Long])
      }
    }

    case class ColumnByName(id: Long, name: String, score: Option[Long])

    it should "get values from columns by name" in {
      val db = newDatabase

      db.withSession { session =>
        val row = session.executeQuery("select * from test where id = 1").head
        assertResult(ColumnByName(1, "Hello", Some(23))) {
          ColumnByName(row("id").as[Int], row("name").as[String], row("score").as[Option[Long]])
        }
      }
    }

    it should "properly address columns with from multiple tables" in {
      val db = newDatabase

      db.withSession { session =>
        session.executeSql("insert into users values (1, 'Test User', 1)")
        session.executeSql("insert into orgs values (1, 'Test Org')")

        val rows = session.executeQuery("select users.name as userLabel, users.*, orgs.* from users, orgs where orgs.id = users.org_id")
        assert(1 === rows.size)
        val row = rows.head
        assert(1 === row("users.id").as[Int])
        assert("Test User" === row("users.name").as[String])
        assert("Test User" === row("userLabel").as[String])
        assert(1 === row("users.org_id").as[Int])

        assert(1 === row("orgs.id").as[Int])
        assert("Test Org" === row("orgs.name").as[String])
      }
    }
  }

  private def emptyDataSource(): DataSource = {

    val dbName = UUID.randomUUID().toString

    val dataSource = new BoneCPDataSource
    dataSource.setDriverClass("org.h2.Driver")
    dataSource.setJdbcUrl("jdbc:h2:mem:" + dbName + ";MVCC=true")
    dataSource.setUsername("sa")
    dataSource.setPassword("sa")
    dataSource.setIdleConnectionTestPeriodInMinutes(60)
    dataSource.setIdleMaxAgeInMinutes(240)
    dataSource.setMaxConnectionsPerPartition(30)
    dataSource.setMinConnectionsPerPartition(10)
    dataSource.setPartitionCount(3)
    dataSource.setAcquireIncrement(5)
    dataSource.setStatementsCacheSize(100)
    dataSource.setReleaseHelperThreads(3)
    //watch for unclosed connections
    dataSource.setCloseConnectionWatch(true)
    dataSource.setCloseConnectionWatchTimeoutInMs(1000)

    dataSource.setDefaultAutoCommit(true)

    //uncomment to turn on statement logging
    //testDB = new Log4jdbcProxyDataSource(JdbcConnectionPool.create("jdbc:h2:mem:test;MVCC=true", "sa", "sa"))

    val c = dataSource.getConnection
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
    dataSource
  }

}
