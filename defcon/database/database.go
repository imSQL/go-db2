package database

import (
	"database/sql"
	"fmt"
	"os"
	"pdefcon-for-db2/utils/sqlutils"
	"strconv"
)

type Database struct {
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
	database  Database
	cursor    sqlutils.Result
	DbHandler *sql.DB
}

const (
	DatabaseStat = `
 SELECT 
        TOTAL_APP_COMMITS, 
        TOTAL_APP_ROLLBACKS, 
        ACT_COMPLETED_TOTAL, 
        APP_RQSTS_COMPLETED_TOTAL, 
        AVG_RQST_CPU_TIME, 
        ROUTINE_TIME_RQST_PERCENT, 
        RQST_WAIT_TIME_PERCENT, 
        ACT_WAIT_TIME_PERCENT, 
        IO_WAIT_TIME_PERCENT, 
        LOCK_WAIT_TIME_PERCENT, 
        AGENT_WAIT_TIME_PERCENT, 
        NETWORK_WAIT_TIME_PERCENT, 
        SECTION_PROC_TIME_PERCENT, 
        SECTION_SORT_PROC_TIME_PERCENT, 
        COMPILE_PROC_TIME_PERCENT, 
        TRANSACT_END_PROC_TIME_PERCENT, 
        UTILS_PROC_TIME_PERCENT, 
        AVG_LOCK_WAITS_PER_ACT, 
        AVG_LOCK_TIMEOUTS_PER_ACT,
        AVG_DEADLOCKS_PER_ACT, 
        AVG_LOCK_ESCALS_PER_ACT, 
        ROWS_READ_PER_ROWS_RETURNED, 
        TOTAL_BP_HIT_RATIO_PERCENT 
FROM SYSIBMADM.MON_DB_SUMMARY   
`
)

func (cs *Cursor) GetMetrics() {
	cs.cursor.GetMetric(cs.DbHandler, DatabaseStat)
	for _, val := range cs.cursor {
		for k, v := range val {
			switch k {
			case 0:
				cs.database.total_app_commits, _ = strconv.ParseInt(v, 10, 64)
			case 1:
				cs.database.total_app_rollbacks, _ = strconv.ParseInt(v, 10, 64)
			case 2:
				cs.database.act_completed_total, _ = strconv.ParseInt(v, 10, 64)
			case 3:
				cs.database.app_rqsts_completed_total, _ = strconv.ParseInt(v, 10, 64)
			case 4:
				cs.database.avg_rqst_cpu_time, _ = strconv.ParseInt(v, 10, 64)
			case 5:
				cs.database.routine_time_rqst_percent, _ = strconv.ParseFloat(v, 64)
			case 6:
				cs.database.rqst_wait_time_percent, _ = strconv.ParseFloat(v, 64)
			case 7:
				cs.database.act_wait_time_percent, _ = strconv.ParseFloat(v, 64)
			case 8:
				cs.database.io_wait_time_percent, _ = strconv.ParseFloat(v, 64)
			case 9:
				cs.database.lock_wait_time_percent, _ = strconv.ParseFloat(v, 64)
			case 10:
				cs.database.agent_wait_time_percent, _ = strconv.ParseFloat(v, 64)
			case 11:
				cs.database.network_wait_time_percent, _ = strconv.ParseFloat(v, 64)
			case 12:
				cs.database.section_proc_time_percent, _ = strconv.ParseFloat(v, 64)
			case 13:
				cs.database.section_sort_proc_time_percent, _ = strconv.ParseFloat(v, 64)
			case 14:
				cs.database.compile_proc_time_percent, _ = strconv.ParseFloat(v, 64)
			case 15:
				cs.database.transact_end_proc_time_percent, _ = strconv.ParseFloat(v, 64)
			case 16:
				cs.database.utils_proc_time_percent, _ = strconv.ParseFloat(v, 64)
			case 17:
				cs.database.avg_lock_waits_per_act, _ = strconv.ParseFloat(v, 64)
			case 18:
				cs.database.avg_lock_timeouts_per_act, _ = strconv.ParseFloat(v, 64)
			case 19:
				cs.database.avg_deadlocks_per_act, _ = strconv.ParseFloat(v, 64)
			case 20:
				cs.database.avg_lock_escals_per_act, _ = strconv.ParseFloat(v, 64)
			case 21:
				cs.database.rows_read_per_rows_returned, _ = strconv.ParseFloat(v, 64)
			case 22:
				cs.database.total_bp_hit_ratio_percent, _ = strconv.ParseFloat(v, 64)

			default:

				fmt.Println("Nothing")
			}
		}
	}
}

func (cs *Cursor) PrintMetrics() {
	current_hostname, _ := os.Hostname()
	fmt.Fprintf(os.Stdout, "Db2DatabaseStat,host=%s,region=%s total_app_commits=%d,total_app_rollbacks=%d,act_completed_total=%d,app_rqsts_completed_total=%d,avg_rqst_cpu_time=%d,routine_time_rqst_percent=%.2f,rqst_wait_time_percent=%.2f,act_wait_time_percent=%.2f,io_wait_time_percent=%.2f,lock_wait_time_percent=%.2f,agent_wait_time_percent=%.2f,network_wait_time_percent=%.2f,section_proc_time_percent=%.2f,section_sort_proc_time_percent=%.2f,compile_proc_time_percent=%.2f,transact_end_proc_time_percent=%.2f,utils_proc_time_percent=%.2f,avg_lock_waits_per_act=%.2f,avg_lock_timeouts_per_act=%.2f,avg_deadlocks_per_act=%.2f,avg_lock_escals_per_act=%.2f,rows_read_per_rows_returned=%.2f,total_bp_hit_ratio_percent=%.2f\n",
		current_hostname,
		"DatabaseStat",
		cs.database.total_app_commits,
		cs.database.total_app_rollbacks,
		cs.database.act_completed_total,
		cs.database.app_rqsts_completed_total,
		cs.database.avg_rqst_cpu_time,
		cs.database.routine_time_rqst_percent,
		cs.database.rqst_wait_time_percent,
		cs.database.act_wait_time_percent,
		cs.database.io_wait_time_percent,
		cs.database.lock_wait_time_percent,
		cs.database.agent_wait_time_percent,
		cs.database.network_wait_time_percent,
		cs.database.section_proc_time_percent,
		cs.database.section_sort_proc_time_percent,
		cs.database.compile_proc_time_percent,
		cs.database.transact_end_proc_time_percent,
		cs.database.utils_proc_time_percent,
		cs.database.avg_lock_waits_per_act,
		cs.database.avg_lock_timeouts_per_act,
		cs.database.avg_deadlocks_per_act,
		cs.database.avg_lock_escals_per_act,
		cs.database.rows_read_per_rows_returned,
		cs.database.total_bp_hit_ratio_percent,
	)
}
