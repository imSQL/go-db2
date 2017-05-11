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
	snapshot_timestamp             string
	client_db_alias                string
	db_name                        string
	agent_id                       int64
	appl_name                      string
	authid                         string
	appl_id                        string
	appl_status                    string
	status_change_time             string
	sequence_no                    string
	client_prdid                   string
	client_pid                     int64
	client_platform                string
	client_protocol                string
	client_nname                   string
	coord_node_num                 int64
	coord_agent_pid                int64
	num_assoc_agents               int64
	tpmon_client_userid            string
	tpmon_client_wkstn             string
	tpmon_client_app               string
	tpmon_acc_str                  string
	dbpartitionnum                 string
	session_auth_id                string
	total_app_commits              int64
	total_app_rollbacks            int64
	act_completed_total            int64
	app_rqsts_completed_total      int64
	avg_rqst_cpu_time              int64
	routine_time_rqst_percent      float64
	rqst_wait_time_percent         float64
	act_wait_time_percent          float64
	io_wait_time_percent           float64
	lock_wait_time_percent         float64
	agent_wait_time_percent        float64
	network_wait_time_percent      float64
	section_proc_time_percent      float64
	section_sort_proc_time_percent float64
	compile_proc_time_percent      float64
	transact_end_proc_time_percent float64
	utils_proc_time_percent        float64
	avg_lock_waits_per_act         float64
	avg_lock_timeouts_per_act      float64
	avg_deadlocks_per_act          float64
	avg_lock_escals_per_act        float64
	rows_read_per_rows_returned    float64
	total_bp_hit_ratio_percent     float64
}

type Cursor struct {
	db2appls  []DB2Appls
	cursor    sqlutils.Result
	DbHandler *sql.DB
}

const (
	Conns = `
 SELECT 
        SA.CLIENT_DB_ALIAS, 
        SA.DB_NAME, 
        SA.AGENT_ID, 
        SA.APPL_NAME, 
        SA.AUTHID, 
        SA.APPL_ID, 
        SA.APPL_STATUS, 
        SA.STATUS_CHANGE_TIME, 
        SA.SEQUENCE_NO, 
        SA.CLIENT_PRDID, 
        SA.CLIENT_PID, 
        SA.CLIENT_PLATFORM, 
        SA.CLIENT_PROTOCOL, 
        SA.CLIENT_NNAME, 
        SA.COORD_NODE_NUM, 
        SA.COORD_AGENT_PID, 
        SA.NUM_ASSOC_AGENTS, 
        SA.TPMON_CLIENT_USERID, 
        SA.TPMON_CLIENT_WKSTN, 
        SA.TPMON_CLIENT_APP, 
        SA.TPMON_ACC_STR, 
        SA.DBPARTITIONNUM,
        SM.SESSION_AUTH_ID,
        SM.TOTAL_APP_COMMITS,
        SM.TOTAL_APP_ROLLBACKS,
        SM.ACT_COMPLETED_TOTAL,
        SM.APP_RQSTS_COMPLETED_TOTAL,
        SM.AVG_RQST_CPU_TIME,
        SM.ROUTINE_TIME_RQST_PERCENT,
        SM.RQST_WAIT_TIME_PERCENT,
        SM.ACT_WAIT_TIME_PERCENT,
        SM.IO_WAIT_TIME_PERCENT,
        SM.LOCK_WAIT_TIME_PERCENT,
        SM.AGENT_WAIT_TIME_PERCENT,
        SM.NETWORK_WAIT_TIME_PERCENT,
        SM.SECTION_PROC_TIME_PERCENT,
        SM.SECTION_SORT_PROC_TIME_PERCENT,
        SM.COMPILE_PROC_TIME_PERCENT,
        SM.TRANSACT_END_PROC_TIME_PERCENT,
        SM.UTILS_PROC_TIME_PERCENT,
        SM.AVG_LOCK_WAITS_PER_ACT,
        SM.AVG_LOCK_TIMEOUTS_PER_ACT,
        SM.AVG_DEADLOCKS_PER_ACT,
        SM.AVG_LOCK_ESCALS_PER_ACT,
        SM.ROWS_READ_PER_ROWS_RETURNED,
        SM.TOTAL_BP_HIT_RATIO_PERCENT
FROM SYSIBMADM.APPLICATIONS AS SA,SYSIBMADM.MON_CONNECTION_SUMMARY AS SM
WHERE SA.APPL_ID = SM.APPLICATION_ID   
`
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
	cs.cursor.GetMetric(cs.DbHandler, Conns)
	for _, val := range cs.cursor {
		tmp := new(DB2Appls)
		for k, v := range val {
			switch k {
			case 0:
				tmp.client_db_alias = v
			case 1:
				tmp.db_name = v
			case 2:
				tmp.agent_id, _ = strconv.ParseInt(v, 10, 64)
			case 3:
				tmp.appl_name = v
			case 4:
				tmp.authid = v
			case 5:
				tmp.appl_id = v
			case 6:
				tmp.appl_status = v
			case 7:
				tmp.status_change_time = v
			case 8:
				tmp.sequence_no = v
			case 9:
				tmp.client_prdid = v
			case 10:
				tmp.client_pid, _ = strconv.ParseInt(v, 10, 64)
			case 11:
				tmp.client_platform = v
			case 12:
				tmp.client_protocol = v
			case 13:
				tmp.client_nname = v
			case 14:
				tmp.coord_node_num, _ = strconv.ParseInt(v, 10, 64)
			case 15:
				tmp.coord_agent_pid, _ = strconv.ParseInt(v, 10, 64)
			case 16:
				tmp.num_assoc_agents, _ = strconv.ParseInt(v, 10, 64)
			case 17:
				tmp.tpmon_client_userid = v
			case 18:
				tmp.tpmon_client_wkstn = v
			case 19:
				tmp.tpmon_client_app = v
			case 20:
				tmp.tpmon_acc_str = v
			case 21:
				tmp.dbpartitionnum = v
			case 22:
				tmp.session_auth_id = v
			case 23:
				tmp.total_app_commits, _ = strconv.ParseInt(v, 10, 64)
			case 24:
				tmp.total_app_rollbacks, _ = strconv.ParseInt(v, 10, 64)
			case 25:
				tmp.act_completed_total, _ = strconv.ParseInt(v, 10, 64)
			case 26:
				tmp.app_rqsts_completed_total, _ = strconv.ParseInt(v, 10, 64)
			case 27:
				tmp.avg_rqst_cpu_time, _ = strconv.ParseInt(v, 10, 64)
			case 28:
				tmp.routine_time_rqst_percent, _ = strconv.ParseFloat(v, 64)
			case 29:
				tmp.rqst_wait_time_percent, _ = strconv.ParseFloat(v, 64)
			case 30:
				tmp.act_wait_time_percent, _ = strconv.ParseFloat(v, 64)
			case 31:
				tmp.io_wait_time_percent, _ = strconv.ParseFloat(v, 64)
			case 32:
				tmp.lock_wait_time_percent, _ = strconv.ParseFloat(v, 64)
			case 33:
				tmp.agent_wait_time_percent, _ = strconv.ParseFloat(v, 64)
			case 34:
				tmp.network_wait_time_percent, _ = strconv.ParseFloat(v, 64)
			case 35:
				tmp.section_proc_time_percent, _ = strconv.ParseFloat(v, 64)
			case 36:
				tmp.section_sort_proc_time_percent, _ = strconv.ParseFloat(v, 64)
			case 37:
				tmp.compile_proc_time_percent, _ = strconv.ParseFloat(v, 64)
			case 38:
				tmp.transact_end_proc_time_percent, _ = strconv.ParseFloat(v, 64)
			case 39:
				tmp.utils_proc_time_percent, _ = strconv.ParseFloat(v, 64)
			case 40:
				tmp.avg_lock_waits_per_act, _ = strconv.ParseFloat(v, 64)
			case 41:
				tmp.avg_lock_timeouts_per_act, _ = strconv.ParseFloat(v, 64)
			case 42:
				tmp.avg_deadlocks_per_act, _ = strconv.ParseFloat(v, 64)
			case 43:
				tmp.avg_lock_escals_per_act, _ = strconv.ParseFloat(v, 64)
			case 44:
				tmp.rows_read_per_rows_returned, _ = strconv.ParseFloat(v, 64)
			case 45:
				tmp.total_bp_hit_ratio_percent, _ = strconv.ParseFloat(v, 64)
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
	for _, v := range cs.db2appls {
		fmt.Fprintf(os.Stdout, "DB2Applications,host=%s,region=EachApplicationsStats,applid=\"%s\" session_auth_id=\"%s\",total_app_commits=%d,total_app_rollbacks=%d,act_completed_total=%d,app_rqsts_completed_total=%d,avg_rqst_cpu_time=%d,routine_time_rqst_percent=%.2f,rqst_wait_time_percent=%.2f,act_wait_time_percent=%.2f,io_wait_time_percent=%.2f,lock_wait_time_percent=%.2f,agent_wait_time_percent=%.2f,network_wait_time_percent=%.2f,section_proc_time_percent=%.2f,section_sort_proc_time_percent=%.2f,compile_proc_time_percent=%.2f,transact_end_proc_time_percent=%.2f,utils_proc_time_percent=%.2f,avg_lock_waits_per_act=%.2f,avg_lock_timeouts_per_act=%.2f,avg_deadlocks_per_act=%.2f,avg_lock_escals_per_act=%.2f,rows_read_per_rows_returned=%.2f,total_bp_hit_ratio_percent=%.2f\n",
			current_hostname,
			v.appl_id,
			v.session_auth_id,
			v.total_app_commits,
			v.total_app_rollbacks,
			v.act_completed_total,
			v.app_rqsts_completed_total,
			v.avg_rqst_cpu_time,
			v.routine_time_rqst_percent,
			v.rqst_wait_time_percent,
			v.act_wait_time_percent,
			v.io_wait_time_percent,
			v.lock_wait_time_percent,
			v.agent_wait_time_percent,
			v.network_wait_time_percent,
			v.section_proc_time_percent,
			v.section_sort_proc_time_percent,
			v.compile_proc_time_percent,
			v.transact_end_proc_time_percent,
			v.utils_proc_time_percent,
			v.avg_lock_waits_per_act,
			v.avg_lock_timeouts_per_act,
			v.avg_deadlocks_per_act,
			v.avg_lock_escals_per_act,
			v.rows_read_per_rows_returned,
			v.total_bp_hit_ratio_percent,
		)
	}
}
