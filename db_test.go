package dbagent

import (
	"database/sql"
	"github.com/jmoiron/sqlx"
	"github.com/mengxiaozhu/go-sqlmock"
	"github.com/newrelic/go-agent"
	"os"
	"testing"
	"time"
)

type TestObject struct {
	Name string `db:"name"`
	Age  int    `db:"age"`
}

func TestNew(t *testing.T) {
	app, err := newrelic.NewApplication(newrelic.NewConfig(os.Getenv("app"), os.Getenv("license")))
	if err != nil {
		t.Fatal(err)
	}
	driver := New(sqlmock.Pool, app, "fortest_")
	_, sm, err := sqlmock.New()
	if err != nil {
		panic(err)
	}
	sql.Register("proxy", driver)
	db, err := sql.Open("proxy", "sqlmock_db_0")
	if err != nil {
		panic(err)
	}
	sm.ExpectBegin()
	sm.ExpectQuery(`select.*`).
		WillDelayFor(time.Second).
		WillReturnRows(sqlmock.NewRows([]string{"name", "age"}).
			AddRow("a", 1))
	DBX := sqlx.NewDb(db, "mysql")
	o := &TestObject{}
	tx, err := DBX.Beginx()
	if err != nil {
		panic(err)
	}
	err = tx.Get(o, "select * from table")
	if err != nil {
		panic(err)
	}
	tx.Commit()
	if o.Name != "a" {
		t.Fatal(o.Name)
	}
}
