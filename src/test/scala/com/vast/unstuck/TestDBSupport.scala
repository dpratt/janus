package com.vast.unstuck

import org.scalatest._
import javax.sql.DataSource
import org.h2.jdbcx.JdbcConnectionPool

trait TestDBSupport extends BeforeAndAfterEach {
  this: Suite =>

  var testDB: DataSource = _

  override protected def beforeEach() {
    testDB = JdbcConnectionPool.create("jdbc:h2:mem:test;MVCC=true", "sa", "sa")
    val c = testDB.getConnection
    try {
      val stat = c.createStatement()
      stat.execute("DROP ALL OBJECTS")
      stat.execute("create table test(id int primary key, name varchar(255))")
      stat.execute("insert into test values(1, 'Hello')")
    } finally {
      c.close()
    }

    super.beforeEach()
  }

}
