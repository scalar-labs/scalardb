package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
)

func step(name string, f func() (any, error)) {
	r, err := f()
	if err != nil {
		fmt.Println("FAIL", name, err)
	} else {
		fmt.Println("ok  ", name, r)
	}
}

func main() {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://postgres@localhost:15444/orm?sslmode=disable")
	if err != nil {
		fmt.Println("FAIL connect", err)
		os.Exit(1)
	}
	defer conn.Close(ctx)
	step("version", func() (any, error) { var v string; err := conn.QueryRow(ctx, "SELECT version()").Scan(&v); return v, err })
	step("create", func() (any, error) {
		conn.Exec(ctx, "DROP TABLE IF EXISTS go_items")
		_, err := conn.Exec(ctx, "CREATE TABLE go_items (id int PRIMARY KEY, name text, qty bigint, price double precision, created timestamptz, active boolean)")
		return nil, err
	})
	step("insert binary params", func() (any, error) {
		_, err := conn.Exec(ctx, "INSERT INTO go_items (id, name, qty, price, created, active) VALUES ($1, $2, $3, $4, $5, $6)", 1, "a", int64(10), 1.5, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), true)
		if err != nil {
			return nil, err
		}
		_, err = conn.Exec(ctx, "INSERT INTO go_items (id, name, qty, price, created, active) VALUES ($1, $2, $3, $4, $5, $6)", 2, "b", int64(20), 2.5, time.Date(2024, 6, 1, 12, 30, 0, 0, time.UTC), false)
		return nil, err
	})
	step("any int4 array", func() (any, error) {
		rows, err := conn.Query(ctx, "SELECT id, name, qty, price, created, active FROM go_items WHERE id = ANY($1) ORDER BY id", []int32{1, 2})
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var id int32
			var name string
			var qty int64
			var price float64
			var created time.Time
			var active bool
			if err := rows.Scan(&id, &name, &qty, &price, &created, &active); err != nil {
				return nil, err
			}
			out = append(out, fmt.Sprintf("%d:%s:%d:%g:%s:%v", id, name, qty, price, created.UTC().Format(time.RFC3339), active))
		}
		return out, rows.Err()
	})
	step("any text array", func() (any, error) {
		rows, err := conn.Query(ctx, "SELECT id FROM go_items WHERE name = ANY($1)", []string{"a", "zz"})
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var ids []int32
		for rows.Next() {
			var id int32
			if err := rows.Scan(&id); err != nil {
				return nil, err
			}
			ids = append(ids, id)
		}
		return ids, rows.Err()
	})
	step("tx", func() (any, error) {
		tx, err := conn.Begin(ctx)
		if err != nil {
			return nil, err
		}
		if _, err := tx.Exec(ctx, "UPDATE go_items SET qty = qty + 1 WHERE id = $1", 1); err != nil {
			tx.Rollback(ctx)
			return nil, err
		}
		return nil, tx.Commit(ctx)
	})
	step("batch pipeline", func() (any, error) {
		b := &pgx.Batch{}
		b.Queue("SELECT name FROM go_items WHERE id = $1", 1)
		b.Queue("SELECT qty FROM go_items WHERE id = $1", 2)
		br := conn.SendBatch(ctx, b)
		defer br.Close()
		var n string
		if err := br.QueryRow().Scan(&n); err != nil {
			return nil, err
		}
		var q int64
		if err := br.QueryRow().Scan(&q); err != nil {
			return nil, err
		}
		return fmt.Sprint(n, " ", q), nil
	})
	step("count + numeric", func() (any, error) {
		var n int64
		var avg string
		err := conn.QueryRow(ctx, "SELECT count(*), avg(qty)::text FROM go_items").Scan(&n, &avg)
		return fmt.Sprint(n, " ", avg), err
	})
	step("delete", func() (any, error) { ct, err := conn.Exec(ctx, "DELETE FROM go_items WHERE id = $1", 2); return ct.RowsAffected(), err })
}
