package dbagent

import "database/sql/driver"
import (
	"context"
	"github.com/newrelic/go-agent"
)

var _ driver.Driver = &WrapperDriver{}

func New(driver driver.Driver, application newrelic.Application, prefix string) *WrapperDriver {
	return &WrapperDriver{
		driver, prefix, application,
	}
}

type WrapperDriver struct {
	driver.Driver
	prefix      string
	application newrelic.Application
}

func (d *WrapperDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.Driver.Open(name)
	if err != nil {
		return nil, err
	}
	if qc, ok := conn.(driver.QueryerContext); ok {
		if q, ok := conn.(driver.Queryer); ok {
			if ec, ok := conn.(driver.ExecerContext); ok {
				if e, ok := conn.(driver.Execer); ok {
					return &WrapperConnQueryerAndExecer{
						WrapperConn:    WrapperConn{d.prefix, conn, d.application},
						QueryerContext: qc,
						ExecerContext:  ec,
						Queryer:        q,
						Execer:         e,
					}, nil
				}
			}
		}
	}
	return &WrapperConn{d.prefix, conn, d.application}, nil
}

type WrapperConnQueryerAndExecer struct {
	WrapperConn
	driver.Queryer
	driver.QueryerContext
	driver.Execer
	driver.ExecerContext
}

func (w *WrapperConnQueryerAndExecer) Query(query string, args []driver.Value) (driver.Rows, error) {
	txn := w.Application.StartTransaction(w.Prefix+query, nil, nil)
	defer txn.End()
	rs, err := w.Queryer.Query(query, args)
	if err != nil {
		if err == driver.ErrSkip {
			txn.Ignore()
		} else {
			txn.NoticeError(err)
		}
	}
	return rs, err
}

func (w *WrapperConnQueryerAndExecer) Exec(query string, args []driver.Value) (driver.Result, error) {
	txn := w.Application.StartTransaction(w.Prefix+query, nil, nil)
	defer txn.End()
	rs, err := w.Execer.Exec(query, args)
	if err != nil {
		if err == driver.ErrSkip {
			txn.Ignore()
		} else {
			txn.NoticeError(err)
		}
	}
	return rs, err
}

func (w *WrapperConnQueryerAndExecer) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	txn := w.Application.StartTransaction(w.Prefix+query, nil, nil)
	defer txn.End()
	rs, err := w.QueryerContext.QueryContext(ctx, query, args)
	if err != nil {
		if err == driver.ErrSkip {
			txn.Ignore()
		} else {
			txn.NoticeError(err)
		}
	}
	return rs, err
}

func (w *WrapperConnQueryerAndExecer) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	txn := w.Application.StartTransaction(w.Prefix+query, nil, nil)
	defer txn.End()
	rs, err := w.ExecerContext.ExecContext(ctx, query, args)
	txn.End()
	if err != nil {
		if err == driver.ErrSkip {
			txn.Ignore()
		} else {
			txn.NoticeError(err)
		}
	}
	return rs, err
}

type WrapperConn struct {
	Prefix string
	driver.Conn
	newrelic.Application
}

func (w *WrapperConn) Prepare(query string) (driver.Stmt, error) {
	stmt, err := w.Conn.Prepare(query)
	if err != nil {
		return nil, err
	}
	if qs, ok := stmt.(driver.StmtQueryContext); ok {
		if es, ok := stmt.(driver.StmtExecContext); ok {
			return &ContextWrapperStmt{
				WrapperStmt:      WrapperStmt{w.Prefix, stmt, w.Application, query},
				StmtQueryContext: qs,
				StmtExecContext:  es,
			}, nil
		}
	}
	return &WrapperStmt{w.Prefix, stmt, w.Application, query}, nil
}

type WrapperStmt struct {
	prefix string
	driver.Stmt
	newrelic.Application
	query string
}

func (s *WrapperStmt) Exec(args []driver.Value) (driver.Result, error) {
	txn := s.Application.StartTransaction(s.prefix+s.query, nil, nil)
	defer txn.End()
	r, e := s.Stmt.Exec(args)

	if e != nil {
		if e == driver.ErrSkip {
			txn.Ignore()
		} else {
			txn.NoticeError(e)
		}
	}
	return r, e
}

func (s *WrapperStmt) Query(args []driver.Value) (driver.Rows, error) {
	txn := s.Application.StartTransaction(s.prefix+s.query, nil, nil)
	defer txn.End()
	r, e := s.Stmt.Query(args)
	if e != nil {
		if e == driver.ErrSkip {
			txn.Ignore()
		} else {
			txn.NoticeError(e)
		}
	}
	return r, e
}

type ContextWrapperStmt struct {
	WrapperStmt
	driver.StmtExecContext
	driver.StmtQueryContext
}

func (s *ContextWrapperStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	txn := s.Application.StartTransaction(s.prefix+s.query, nil, nil)
	defer txn.End()
	r, e := s.StmtExecContext.ExecContext(ctx, args)
	if e != nil {
		if e == driver.ErrSkip {
			txn.Ignore()
		} else {
			txn.NoticeError(e)
		}
	}
	return r, e
}

func (s *ContextWrapperStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	txn := s.Application.StartTransaction(s.prefix+s.query, nil, nil)
	defer txn.End()
	r, e := s.StmtQueryContext.QueryContext(ctx, args)
	if e != nil {
		if e == driver.ErrSkip {
			txn.Ignore()
		} else {
			txn.NoticeError(e)
		}
	}
	return r, e
}
