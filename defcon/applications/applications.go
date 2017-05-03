package applications

import (
	"database/sql"
	"fmt"
	"pdefcon-for-db2/utils/sqlutils"
)

type Cursor struct {
	cursor    sqlutils.Result
	DbHandler *sql.DB
}

const (
	Appls = `SELECT SNAPSHOT_TIMESTAMP, CLIENT_DB_ALIAS, DB_NAME, AGENT_ID, APPL_NAME, AUTHID, APPL_ID, APPL_STATUS, STATUS_CHANGE_TIME, SEQUENCE_NO, CLIENT_PRDID, CLIENT_PID, CLIENT_PLATFORM, CLIENT_PROTOCOL, CLIENT_NNAME, COORD_NODE_NUM, COORD_AGENT_PID, NUM_ASSOC_AGENTS, TPMON_CLIENT_USERID, TPMON_CLIENT_WKSTN, TPMON_CLIENT_APP, TPMON_ACC_STR, DBPARTITIONNUM FROM SYSIBMADM.APPLICATIONS
    `
)

func (cs *Cursor) GetMetrics() {
	cs.cursor.GetMetric(cs.DbHandler, Appls)
}

func (cs *Cursor) PrintMetrics() {
	fmt.Println(cs.cursor)
}
