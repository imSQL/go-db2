package sqlutils

import (
	_ "bitbucket.org/phiggins/go-db2-cli"
	"database/sql"
	"fmt"
	"os"
)

type Result map[int]map[int]string

func (res *Result) GetMetric(db *sql.DB, query_text string) {

	rows, err := db.Query(query_text)
	if err != nil {
		return
	}

	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
	}

	values := make([][]byte, len(cols))
	scans := make([]interface{}, len(cols))

	for i := range values {
		scans[i] = &values[i]
	}

	result := make(Result)
	i := 0

	for rows.Next() {
		if err := rows.Scan(scans...); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		row := make(map[int]string)

		for k, v := range values {
			row[k] = string(v)
		}
		result[i] = row
		i++

	}
	*res = result

}
