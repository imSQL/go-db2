package sqlstat

import (
	"crypto/md5"
	"database/sql"
	"fmt"
	"os"
	"pdefcon-for-db2/utils/sqlutils"
	"strconv"
	"strings"
)

var (
	escaper = strings.NewReplacer(`=`, `\=`, `,`, `\,`, `"`, `'`)
)

type Sql struct {
	snapshot_timestamp       string
	num_executions           int64
	average_execution_time_s float64
	stmt_sorts               int64
	sorts_per_execution      float64
	sql_text                 string
	sqlmd5                   string
	dbpartitionnum           int64
}

type Cursor struct {
	sqlstat   []Sql
	cursor    sqlutils.Result
	DbHandler *sql.DB
}

const (
	SqlStat = `
SELECT 
        SNAPSHOT_TIMESTAMP, 
        NUM_EXECUTIONS, 
        AVERAGE_EXECUTION_TIME_S, 
        STMT_SORTS, 
        SORTS_PER_EXECUTION, 
        DBMS_LOB.SUBSTR(STMT_TEXT,500) AS SQL_TEXT, 
        DBPARTITIONNUM 
FROM SYSIBMADM.TOP_DYNAMIC_SQL
`
)

func (cs *Cursor) GetMetrics() {
	cs.cursor.GetMetric(cs.DbHandler, SqlStat)
	for _, val := range cs.cursor {
		tmp := new(Sql)
		for k, v := range val {
			switch k {
			case 0:
				tmp.snapshot_timestamp = v
			case 1:
				tmp.num_executions, _ = strconv.ParseInt(v, 10, 64)
			case 2:
				tmp.average_execution_time_s, _ = strconv.ParseFloat(v, 64)
			case 3:
				tmp.stmt_sorts, _ = strconv.ParseInt(v, 10, 64)
			case 4:
				tmp.sorts_per_execution, _ = strconv.ParseFloat(v, 64)
			case 5:
				tmp.sql_text = escaper.Replace(v)
			case 6:
				tmp.dbpartitionnum, _ = strconv.ParseInt(v, 10, 64)
			default:
				fmt.Println("Nothing")
			}
		}
		cs.sqlstat = append(cs.sqlstat, *tmp)
	}
}

func (cs *Cursor) PrintMetrics() {
	current_hostname, _ := os.Hostname()
	for _, av := range cs.sqlstat {
		data := []byte(av.sql_text)
		has := md5.Sum(data)
		av.sqlmd5 = fmt.Sprintf("%x", has)

		fmt.Fprintf(os.Stdout, "DB2SqlStat,host=%s,region=%s,sqlmd5=%q snapshot_timestamp=%q,num_executions=%d,average_execution_time_s=%.2f,stmt_sorts=%d,sorts_per_execution=%.2f,sql_text=%q,sqlmd5=%q,dbpartitionnum=%d\n",
			current_hostname,
			"SqlStat",
			av.sqlmd5,
			av.snapshot_timestamp,
			av.num_executions,
			av.average_execution_time_s,
			av.stmt_sorts,
			av.sorts_per_execution,
			av.sql_text,
			av.sqlmd5,
			av.dbpartitionnum,
		)
	}
}
