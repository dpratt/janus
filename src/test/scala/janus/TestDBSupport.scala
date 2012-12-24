package janus

import org.scalatest._
import javax.sql.DataSource
import org.h2.jdbcx.JdbcConnectionPool
import net.sf.log4jdbc.Log4jdbcProxyDataSource
import com.jolbox.bonecp.BoneCPDataSource

trait TestDBSupport extends BeforeAndAfterEach {
  this: Suite =>

  var testDB: DataSource = _

  override protected def beforeEach() {

    val dataSource = new BoneCPDataSource
    dataSource.setDriverClass("org.h2.Driver")
    dataSource.setJdbcUrl("jdbc:h2:mem:test;MVCC=true")
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

    testDB = dataSource

    //uncomment to turn on statement logging
    //testDB = new Log4jdbcProxyDataSource(JdbcConnectionPool.create("jdbc:h2:mem:test;MVCC=true", "sa", "sa"))

    val c = testDB.getConnection
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

    super.beforeEach()
  }

}
