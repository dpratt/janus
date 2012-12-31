package janus

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import concurrent.{Await, Future}
import concurrent.duration._

import scala.concurrent.ExecutionContext.Implicits.global

@RunWith(classOf[JUnitRunner])
class BasicTest extends FunSuite with TestDBSupport {

  test("Basic query test") {
    val db = Database(testDB)
    val session = db.createSession
    session.withPreparedStatement("select * from test") { ps =>
      val results = ps.executeQuery.map { row =>
        row[Int](0)
      }
      expect(1) {
        results.head
      }
    }
    session.close()
  }

  test("Basic transaction handling.") {

    val db = Database(testDB)

    db.withSession { session =>
      session.withTransaction { transactionalSession =>
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
      val results = session.executeQuery("select * from test where id = 2").map { row =>
        row[String](1)
      }
      expect(1) {
        results.size
      }
      expect("Test 2") {
        results.head
      }
      results.force
    }
  }

  test("Transaction rollback") {

    val db = Database(testDB)

    db.withSession { session =>
      val results = session.executeQuery("select count(*) from test").map { row =>
        row[Option[Long]](0)
      }.head
      expect(Some(1)) {
        results
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
      val newRowCount = session.executeQuery("select count(*) from test").map { row =>
        row[Option[Long]](0)
      }.head
      expect(Some(1)) {
        //should only be one row - the rows inserted above should have been rolled back
        newRowCount
      }
    }
  }

  test("Async transactions") {

    val db = Database(testDB)

    val resultFuture: Future[Boolean] = db.withSession { session =>
      session.withTransaction { transactionalSession =>
        session.withPreparedStatement("insert into test (id, name) VALUES (?,?)") { ps =>
          ps.setParam(0, 2)
          ps.setParam(1, "Test 2")
          ps.executeUpdate()
        }
        //this future completes with an exception, so the transaction
        //should be rolled back
        failedExternalCall()
      }
    }

    //wait for the future to complete
    intercept[RuntimeException] {
      Await.result(resultFuture, 10 seconds)
    }

    //now make sure it's been rolled back
    db.withSession { session =>
      if(!session.executeQuery("select * from test where id = 2").map(row =>"row" ).isEmpty) {
        fail("Row present!")
      }
    }
  }

  test("Nested transactions") {

    val db = Database(testDB)

    db.withSession { session =>
      intercept[RuntimeException] {
        session.withTransaction { transaction =>
          session.executeSql("insert into test (id, name) VALUES (3, 'Test 3')")
          intercept[RuntimeException] {
            session.withTransaction { nested =>
              val rowCount = session.executeQuery("select count(*) from test").map { row =>
                row[Int](0)
              }.head
              expect(2) {
                rowCount
              }
              //throw an exception - this *shouldn't* roll back the transaction since we're nested
              throw new RuntimeException("This is a sample")
            }
          }
          //should still be there
          val rowCount = session.executeQuery("select count(*) from test").map { row =>
            row[Option[Long]](0)
          }
          expect(Some(2)) {
            rowCount.head
          }
          //throw another exception - this *should* roll it back
          throw new RuntimeException("This is a second exception")
        }
      }
      //row should be gone
      val rowCount = session.executeQuery("select count(*) from test").map { row =>
        row[Option[Long]](0)
      }
      expect(Some(1)) {
        rowCount.head
      }
    }

    //more checks - ensure that it doesn't show up in another connection either
    db.withSession { session =>
      //row should be gone
      val rowCount = session.executeQuery("select count(*) from test").map { row =>
        row[Option[Long]](0)
      }
      expect(Some(1)) {
        rowCount.head
      }
    }
  }

  test("Manual rollback") {

    val db = Database(testDB)

    db.withSession { session =>
      session.withTransaction { transaction =>
        session.executeSql("insert into test (id, name) VALUES (3, 'Test 3')")
        //should still be there
        val results = session.executeQuery("select count(*) from test").map { row =>
          row[Option[Long]](0)
        }
        expect(Some(2)) {
          results.head
        }
        transaction.rollback()
      }
      //row should be gone, due to manual rollback
      val rowCount = session.executeQuery("select count(*) from test").map { row =>
        row[Option[Long]](0)
      }
      expect(Some(1)) {
        rowCount.head
      }
    }
  }

  test("Null Column") {
    val db = Database(testDB)

    db.withSession { session =>
      session.executeSql("insert into test (id, name) values (3, 'test name')")
      intercept[NullableColumnException] {
        session.executeQuery("select * from test where id = 3").map { row =>
          val value = row[Long]("score")
        }
      }
    }
  }

  case class ColumnByName(id: Long, name: String, score: Option[Long])

  test("Columns by name") {
    val db = Database(testDB)

    db.withSession { session =>
      val results = session.executeQuery("select * from test where id = 1").map { row =>
        ColumnByName(row[Long]("id"), row[String]("name"), row[Option[Long]]("score"))
      }
      expect(1) {
        results.size
      }
      expect(ColumnByName(1, "Hello", Some(23))) {
        results.head
      }
    }
  }

  test("Complex columns by name") {
    val db = Database(testDB)

    db.withSession { session =>
      session.executeSql("insert into users values (1, 'Test User', 1)")
      session.executeSql("insert into orgs values (1, 'Test Org')")

      val results = session.executeQuery("select users.name as userLabel, users.*, orgs.* from users, orgs where orgs.id = users.org_id")
      expect(1) {
        results.size
      }
      val row = results.head

      expect(1)(row[Long]("users.id"))
      expect("Test User")(row[String]("users.name"))
      expect("Test User")(row[String]("userLabel"))
      expect(1)(row[Long]("users.org_id"))

      expect(1)(row[Long]("orgs.id"))
      expect("Test Org")(row[String]("orgs.name"))
    }
  }

  def successfulExternalCall(): Future[Boolean] = {
    Future {
      Thread.sleep(500)
      true
    }
  }

  def failedExternalCall(): Future[Boolean] = {
    Future {
      Thread.sleep(500)
      throw new RuntimeException("External call failed")
    }
  }

}
