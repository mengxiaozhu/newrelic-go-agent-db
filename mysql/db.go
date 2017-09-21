package dbagent

import (
	"database/sql/driver"
	"github.com/go-sql-driver/mysql"
	"github.com/mengxiaozhu/newrelic-go-agent-db"
	"github.com/newrelic/go-agent"
)

func New(driver driver.Driver, application newrelic.Application) *WrapperDriver {
	return &WrapperDriver{
		driver, application,
	}
}

type WrapperDriver struct {
	driver.Driver
	Application newrelic.Application
}

func (d *WrapperDriver) Open(name string) (driver.Conn, error) {
	config, err := mysql.ParseDSN(name)
	if err != nil {
		return nil, err
	}
	prefix := config.DBName + ":"
	conn, err := d.Driver.Open(name)
	if err != nil {
		return nil, err
	}
	if qc, ok := conn.(driver.QueryerContext); ok {
		if q, ok := conn.(driver.Queryer); ok {
			if ec, ok := conn.(driver.ExecerContext); ok {
				if e, ok := conn.(driver.Execer); ok {
					return &dbagent.WrapperConnQueryerAndExecer{
						WrapperConn:    dbagent.WrapperConn{prefix, conn, d.Application},
						QueryerContext: qc,
						ExecerContext:  ec,
						Queryer:        q,
						Execer:         e,
					}, nil
				}
			}
		}
	}
	return &dbagent.WrapperConn{prefix, conn, d.Application}, nil
}
