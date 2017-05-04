package applications

import (
	"database/sql"
	"fmt"
	"os"
	"pdefcon-for-db2/utils/sqlutils"
	"strconv"
	"strings"
)

type DB2Appls struct {
	snapshot_timestamp  string
	client_db_alias     string
	db_name             string
	agent_id            int64
	appl_name           string
	authid              string
	appl_id             string
	appl_status         string
	status_change_time  string
	sequence_no         string
	client_prdid        string
	client_pid          int64
	client_platform     string
	client_protocol     string
	client_nname        string
	coord_node_num      int64
	coord_agent_pid     int64
	num_assoc_agents    int64
	tpmon_client_userid string
	tpmon_client_wkstn  string
	tpmon_client_app    string
	tpmon_acc_str       string
	dbpartitionnum      string
}

type Cursor struct {
	db2appls  []DB2Appls
	cursor    sqlutils.Result
	DbHandler *sql.DB
}

const (
	Appls = `
SELECT 
    SNAPSHOT_TIMESTAMP, 
    CLIENT_DB_ALIAS, 
    DB_NAME, 
    AGENT_ID, 
    APPL_NAME, 
    AUTHID, 
    APPL_ID, 
    APPL_STATUS, 
    STATUS_CHANGE_TIME, 
    SEQUENCE_NO, 
    CLIENT_PRDID, 
    CLIENT_PID, 
    CLIENT_PLATFORM, 
    CLIENT_PROTOCOL, 
    CLIENT_NNAME, 
    COORD_NODE_NUM, 
    COORD_AGENT_PID, 
    NUM_ASSOC_AGENTS, 
    TPMON_CLIENT_USERID, 
    TPMON_CLIENT_WKSTN, 
    TPMON_CLIENT_APP, 
    TPMON_ACC_STR, 
    DBPARTITIONNUM 
FROM 
    SYSIBMADM.APPLICATIONS
    `
)

func (cs *Cursor) GetMetrics() {
	cs.cursor.GetMetric(cs.DbHandler, Appls)
	for _, val := range cs.cursor {
		tmp := new(DB2Appls)
		for k, v := range val {
			switch k {
			case 0:
				tmp.snapshot_timestamp = v
			case 1:
				tmp.client_db_alias = v
			case 2:
				tmp.db_name = v
			case 3:
				tmp.agent_id, _ = strconv.ParseInt(v, 10, 64)
			case 4:
				tmp.appl_name = v
			case 5:
				tmp.authid = v
			case 6:
				tmp.appl_id = v
			case 7:
				tmp.appl_status = v
			case 8:
				tmp.status_change_time = v
			case 9:
				tmp.sequence_no = v
			case 10:
				tmp.client_prdid = v
			case 11:
				tmp.client_pid, _ = strconv.ParseInt(v, 10, 64)
			case 12:
				tmp.client_platform = v
			case 13:
				tmp.client_protocol = v
			case 14:
				tmp.client_nname = v
			case 15:
				tmp.coord_node_num, _ = strconv.ParseInt(v, 10, 64)
			case 16:
				tmp.coord_agent_pid, _ = strconv.ParseInt(v, 10, 64)
			case 17:
				tmp.num_assoc_agents, _ = strconv.ParseInt(v, 10, 64)
			case 18:
				tmp.tpmon_client_userid = v
			case 19:
				tmp.tpmon_client_wkstn = v
			case 20:
				tmp.tpmon_client_app = v
			case 21:
				tmp.tpmon_acc_str = v
			case 22:
				tmp.dbpartitionnum = v
			default:
				fmt.Println("Nothing")
			}
		}
		cs.db2appls = append(cs.db2appls, *tmp)
	}
}

func (cs *Cursor) GetTotalCounter() int {
	return (len(cs.db2appls))
}

func (cs *Cursor) GetTotalCounterByUsername() map[string]int {
	result := make(map[string]int)
	for _, v := range cs.db2appls {
		if _, ok := result[v.authid]; ok {
			result[v.authid]++
		} else {
			result[v.authid] = 1
		}
	}
	return (result)
}

func (cs *Cursor) GetTotalCounterByApplStatus() map[string]int {
	result := make(map[string]int)
	for _, v := range cs.db2appls {
		if _, ok := result[v.appl_status]; ok {
			result[v.appl_status]++
		} else {
			result[v.appl_status] = 1

		}
	}
	return (result)
}

func (cs *Cursor) GetTotalCounterByApplName() map[string]int {
	result := make(map[string]int)
	for _, v := range cs.db2appls {
		if _, ok := result[v.appl_name]; ok {
			result[v.appl_name]++
		} else {
			result[v.appl_name] = 1

		}
	}
	return (result)
}

func (cs *Cursor) GetTotalCounterByDbName() map[string]int {
	result := make(map[string]int)
	for _, v := range cs.db2appls {
		if _, ok := result[v.db_name]; ok {
			result[v.db_name]++
		} else {
			result[v.db_name] = 1

		}
	}
	return (result)
}

func (cs *Cursor) GetTotalCounterByClientPlatform() map[string]int {
	result := make(map[string]int)
	for _, v := range cs.db2appls {
		if _, ok := result[v.client_platform]; ok {
			result[v.client_platform]++
		} else {
			result[v.client_platform] = 1

		}
	}
	return (result)
}

func (cs *Cursor) GetTotalCounterByClientProtocol() map[string]int {
	result := make(map[string]int)
	for _, v := range cs.db2appls {
		if _, ok := result[v.client_protocol]; ok {
			result[v.client_protocol]++
		} else {
			result[v.client_protocol] = 1

		}
	}
	return (result)
}

func (cs *Cursor) GetTotalCounterByClientNname() map[string]int {
	result := make(map[string]int)
	for _, v := range cs.db2appls {
		if _, ok := result[v.client_nname]; ok {
			result[v.client_nname]++
		} else {
			result[v.client_nname] = 1

		}
	}
	return (result)
}

func (cs *Cursor) PrintMetrics() {
	current_hostname, _ := os.Hostname()
	fmt.Fprintf(os.Stdout, "DB2Applications,host=%s,region=TotalConnections,connections=all Connections=%d\n", current_hostname, cs.GetTotalCounter())
	for k, v := range cs.GetTotalCounterByUsername() {
		fmt.Fprintf(os.Stdout, "DB2Applications,host=%s,region=ConnectionsByUser,username=%s Connections=%d\n", current_hostname, k, v)
	}
	for k, v := range cs.GetTotalCounterByApplStatus() {
		fmt.Fprintf(os.Stdout, "DB2Applications,host=%s,region=ConnectionsByStatus,status=%s Connections=%d\n", current_hostname, k, v)
	}
	for k, v := range cs.GetTotalCounterByApplName() {
		fmt.Fprintf(os.Stdout, "DB2Applications,host=%s,region=ConnectionsByApplName,applname=%s Connections=%d\n", current_hostname, k, v)
	}
	for k, v := range cs.GetTotalCounterByDbName() {
		fmt.Fprintf(os.Stdout, "DB2Applications,host=%s,region=ConnectionsByDbName,dbname=%s Connections=%d\n", current_hostname, k, v)
	}
	for k, v := range cs.GetTotalCounterByClientPlatform() {
		fmt.Fprintf(os.Stdout, "DB2Applications,host=%s,region=ConnectionsByClientPlatform,platform=%q Connections=%d\n", current_hostname, strings.Replace(k, " ", "_", -1), v)
	}
	for k, v := range cs.GetTotalCounterByClientProtocol() {

		fmt.Fprintf(os.Stdout, "DB2Applications,host=%s,region=ConnectionsByClientProtocol,protocol=%s Connections=%d\n", current_hostname, k, v)
	}
	for k, v := range cs.GetTotalCounterByClientNname() {
		fmt.Fprintf(os.Stdout, "DB2Applications,host=%s,region=ConnectionsByClientNname,clientnname=%q Connections=%d\n", current_hostname, k, v)
	}
}
