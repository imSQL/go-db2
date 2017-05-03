package main

import (
	_ "bitbucket.org/phiggins/go-db2-cli"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"pdefcon-for-db2/defcon/applications"
	//"pdefcon-for-db2/utils/sqlutils"
	//	"time"
)

var (
	connStr string
	repeat  = flag.Uint("repeat", 1, "number of times to repeat query")
)

func usage() {
	fmt.Fprintf(os.Stderr, `usage: %s [options]

%s connects to DB2 and executes a simple SQL statement a configurable
number of times.

Here is a sample connection string:

DATABASE=MYDBNAME; HOSTNAME=localhost; PORT=60000; PROTOCOL=TCPIP; UID=username; PWD=password;
`, os.Args[0], os.Args[0])
	flag.PrintDefaults()
	os.Exit(1)
}

func getDSN() string {
	var dsn string
	if len(os.Args) > 1 {
		return dsn
	}
	dsn = os.Getenv("DB2DBI")
	if dsn != "" {
		return dsn
	}
	fmt.Fprintln(os.Stderr, "Please specifiy connection parameter DB2DBI environment variable")
	return "Nothing"
}

func main() {

	connStr = getDSN()
	db, err := sql.Open("db2-cli", connStr)
	if err != nil {
		return
	}
	defer db.Close()

	res1 := new(applications.Cursor)
	res1.DbHandler = db
	res1.GetMetrics()
	res1.PrintMetrics()

}
