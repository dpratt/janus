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
    Database(ds)
  }

  def springDatabase = {
    val ds = emptyDataSource()
    Database(ds, new DataSourceTransactionManager(ds))
  }

  "A Database based on a simple DataSource" should behave like basicDatabaseBehavior(basicDatabase)
  "A Database synchronized with Spring transactions" should behave like basicDatabaseBehavior(springDatabase)

  def basicDatabaseBehavior(newDatabase: => Database) {

    it should "support basic queries" in {
      val db = newDatabase
      db.withSession { session =>
        session.withPreparedStatement("select * from test") { ps =>
          ps.executeQuery { rs =>
            expectResult(1) {
              rs.head.value[Int](0)
            }
          }
        }
      }
    }

    it should "support inserting inside a transaction" in {
      val db = newDatabase

      db.withSession { session =>
        session.withTransaction { transaction =>
          //insert a value
          session.withPreparedStatement("insert into test (id, name) VALUES (?,?)") { ps =>
            ps.setParam(0, 2)
            ps.setParam(1, "Test 2")
            ps.executeUpdate()
          }
        }
      }

      db.withSession { session =>
        //query for this value
        session.executeQuery("select * from test where id = 2") { rs =>
          expectResult(1) {
            rs.size
          }
          expectResult("Test 2") {
            rs.head.value[String](1)
          }
        }
      }
    }

    it should "properly rollback on exception" in {
      val db = newDatabase

      db.withSession { session =>
        session.executeQuery("select count(*) from test") { rs =>
          expectResult(Some(1)) {
            rs.head.value[Option[Long]](0)
          }
        }

        intercept[RuntimeException] {
          session.withTransaction { trans =>
            session.executeSql("insert into test (id, name) VALUES (3, 'Test Value')")
            throw new RuntimeException("Sample exception")
          }
        }
      }

      //create a new session to ensure isolation from the above operations
      db.withSession { session =>
        session.executeQuery("select count(*) from test") { rs =>
        //should only be one row - the rows inserted above should have been rolled back
          expectResult(Some(1)) {
            rs.head.value[Option[Long]](0)
          }
        }
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
                session.executeQuery("select count(*) from test") { rs =>
                  expectResult(2) {
                    rs.head[Int](0)
                  }
                }
                //throw an exception - this *shouldn't* roll back the transaction since we're nested
                throw new RuntimeException("This is a sample")
              }
            }
            //should still be there
            session.executeQuery("select count(*) from test") { rs =>
              expectResult(Some(2)) {
                rs.head[Option[Long]](0)
              }
            }
            //throw another exception - this *should* roll it back
            throw new RuntimeException("This is a second exception")
          }
        }
        //row should be gone
        session.executeQuery("select count(*) from test") { rs =>
          expectResult(Some(1)) {
            rs.head[Option[Long]](0)
          }
        }
      }

      //more checks - ensure that it doesn't show up in another session either
      db.withSession { session =>
      //row should be gone
        session.executeQuery("select count(*) from test") { rs =>
          expectResult(Some(1)) {
            rs.head[Option[Long]](0)
          }
        }
      }
    }

    it should "support manual rollback" in {
      val db = newDatabase

      db.withSession { session =>
        session.withTransaction { transaction =>
          session.executeSql("insert into test (id, name) VALUES (3, 'Test 3')")
          //should still be there
          session.executeQuery("select count(*) from test") { rs =>
            expectResult(Some(2)) {
              rs.head[Option[Long]](0)
            }
          }
          transaction.setRollback()
        }
        //row should be gone, due to manual rollback
        session.executeQuery("select count(*) from test") { rs =>
          expectResult(Some(1)) {
            rs.head[Option[Long]](0)
          }
        }
      }
    }

    it should "properly detect nullable columns" in {
      val db = newDatabase

      db.withSession { session =>
        session.executeSql("insert into test (id, name) values (3, 'test name')")
        intercept[NullableColumnException] {
          session.executeQuery("select * from test where id = 3") { rs =>
            rs.head[Long]("score")
          }
        }
      }
    }

    case class ColumnByName(id: Long, name: String, score: Option[Long])

    it should "get values from columns by name" in {
      val db = newDatabase

      db.withSession { session =>
        val result = session.executeQuery("select * from test where id = 1") { rs =>
          val row = rs.head
          ColumnByName(row[Long]("id"), row[String]("name"), row[Option[Long]]("score"))
        }
        expectResult(ColumnByName(1, "Hello", Some(23))) {
          result
        }
      }
    }

    it should "properly address columns with from multiple tables" in {
      val db = newDatabase

      db.withSession { session =>
        session.executeSql("insert into users values (1, 'Test User', 1)")
        session.executeSql("insert into orgs values (1, 'Test Org')")

        session.executeQuery("select users.name as userLabel, users.*, orgs.* from users, orgs where orgs.id = users.org_id") { rs =>
          expectResult(1) {
            rs.size
          }
          val row = rs.head

          expectResult(1)(row[Long]("users.id"))
          expectResult("Test User")(row[String]("users.name"))
          expectResult("Test User")(row[String]("userLabel"))
          expectResult(1)(row[Long]("users.org_id"))

          expectResult(1)(row[Long]("orgs.id"))
          expectResult("Test Org")(row[String]("orgs.name"))
        }
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
