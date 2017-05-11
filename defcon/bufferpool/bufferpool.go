package bufferpool

import (
	"database/sql"
	"fmt"
	"os"
	"pdefcon-for-db2/utils/sqlutils"
	"strconv"
	"strings"
)

type BpHitRatio struct {
	snapshot_timestamp          string
	db_name                     string
	bp_name                     string
	total_logical_reads         int64
	total_physical_reads        int64
	total_hit_ratio_percent     float64
	data_logical_reads          int64
	data_physical_reads         int64
	data_hit_ratio_percent      float64
	index_logical_reads         int64
	index_physical_reads        int64
	index_hit_ratio_percent     float64
	xda_logical_reads           int64
	xda_physical_reads          int64
	xda_hit_ratio_percent       float64
	average_read_time_ms        int64
	total_async_reads           int64
	average_async_read_time_ms  int64
	total_sync_reads            int64
	average_sync_read_time_ms   int64
	percent_sync_reads          float64
	async_not_read_percent      float64
	total_writes                int64
	average_write_time_ms       int64
	total_async_writes          int64
	percent_writes_async        float64
	average_async_write_time_ms int64
	total_sync_writes           int64
	average_sync_write_time_ms  int64
	dbpartitionnum              int64
}

type Cursor struct {
	bphitratio []BpHitRatio
	cursor     sqlutils.Result
	DbHandler  *sql.DB
}

const (
	BP_STATS = `
SELECT 
        DB_NAME, 
        BH.BP_NAME, 
        TOTAL_LOGICAL_READS, 
        BH.TOTAL_PHYSICAL_READS, 
        TOTAL_HIT_RATIO_PERCENT, 
        DATA_LOGICAL_READS, 
        DATA_PHYSICAL_READS, 
        DATA_HIT_RATIO_PERCENT, 
        INDEX_LOGICAL_READS, 
        INDEX_PHYSICAL_READS, 
        INDEX_HIT_RATIO_PERCENT, 
        XDA_LOGICAL_READS, 
        XDA_PHYSICAL_READS, 
        XDA_HIT_RATIO_PERCENT,
        AVERAGE_READ_TIME_MS, 
        TOTAL_ASYNC_READS, 
        AVERAGE_ASYNC_READ_TIME_MS, 
        TOTAL_SYNC_READS, 
        AVERAGE_SYNC_READ_TIME_MS, 
        PERCENT_SYNC_READS, 
        ASYNC_NOT_READ_PERCENT,
        TOTAL_WRITES, 
        AVERAGE_WRITE_TIME_MS, 
        TOTAL_ASYNC_WRITES, 
        PERCENT_WRITES_ASYNC, 
        AVERAGE_ASYNC_WRITE_TIME_MS, 
        TOTAL_SYNC_WRITES, 
        AVERAGE_SYNC_WRITE_TIME_MS, 
        BH.DBPARTITIONNUM 
FROM SYSIBMADM.BP_HITRATIO AS BH,SYSIBMADM.BP_READ_IO AS BRI,SYSIBMADM.BP_WRITE_IO AS BWI 
WHERE BH.BP_NAME = BRI.BP_NAME AND BH.BP_NAME = BWI.BP_NAME   
`
)

func (cs *Cursor) GetMetrics() {
	cs.cursor.GetMetric(cs.DbHandler, BP_STATS)
	for _, val := range cs.cursor {
		tmp := new(BpHitRatio)
		for k, v := range val {
			switch k {
			case 0:
				tmp.snapshot_timestamp = v
			case 1:
				tmp.db_name = strings.Replace(v, " ", "", -1)
			case 2:
				tmp.bp_name = v
			case 3:
				tmp.total_logical_reads, _ = strconv.ParseInt(v, 10, 64)
			case 4:
				tmp.total_physical_reads, _ = strconv.ParseInt(v, 10, 64)
			case 5:
				tmp.total_hit_ratio_percent, _ = strconv.ParseFloat(v, 64)
			case 6:
				tmp.data_logical_reads, _ = strconv.ParseInt(v, 10, 64)
			case 7:
				tmp.data_physical_reads, _ = strconv.ParseInt(v, 10, 64)
			case 8:
				tmp.data_hit_ratio_percent, _ = strconv.ParseFloat(v, 64)
			case 9:
				tmp.index_logical_reads, _ = strconv.ParseInt(v, 10, 64)
			case 10:
				tmp.index_physical_reads, _ = strconv.ParseInt(v, 10, 64)
			case 11:
				tmp.index_hit_ratio_percent, _ = strconv.ParseFloat(v, 64)
			case 12:
				tmp.xda_logical_reads, _ = strconv.ParseInt(v, 10, 64)
			case 13:
				tmp.xda_physical_reads, _ = strconv.ParseInt(v, 10, 64)
			case 14:
				tmp.xda_hit_ratio_percent, _ = strconv.ParseFloat(v, 64)
			case 15:
				tmp.average_read_time_ms, _ = strconv.ParseInt(v, 10, 64)
			case 16:
				tmp.total_async_reads, _ = strconv.ParseInt(v, 10, 64)
			case 17:
				tmp.average_async_read_time_ms, _ = strconv.ParseInt(v, 10, 64)
			case 18:
				tmp.total_sync_reads, _ = strconv.ParseInt(v, 10, 64)
			case 19:
				tmp.average_sync_read_time_ms, _ = strconv.ParseInt(v, 10, 64)
			case 20:
				tmp.percent_sync_reads, _ = strconv.ParseFloat(v, 64)
			case 21:
				tmp.async_not_read_percent, _ = strconv.ParseFloat(v, 64)
			case 22:
				tmp.total_writes, _ = strconv.ParseInt(v, 10, 64)
			case 23:
				tmp.average_write_time_ms, _ = strconv.ParseInt(v, 10, 64)
			case 24:
				tmp.total_async_writes, _ = strconv.ParseInt(v, 10, 64)
			case 25:
				tmp.percent_writes_async, _ = strconv.ParseFloat(v, 64)
			case 26:
				tmp.average_async_write_time_ms, _ = strconv.ParseInt(v, 10, 64)
			case 27:
				tmp.total_sync_writes, _ = strconv.ParseInt(v, 10, 64)
			case 28:
				tmp.average_sync_write_time_ms, _ = strconv.ParseInt(v, 10, 64)
			case 29:
				tmp.dbpartitionnum, _ = strconv.ParseInt(v, 10, 64)
			default:
				fmt.Println("Nothing")
			}
		}
		cs.bphitratio = append(cs.bphitratio, *tmp)
	}

}

func (cs *Cursor) PrintMetrics() {
	current_hostname, _ := os.Hostname()
	for _, v := range cs.bphitratio {
		fmt.Fprintf(os.Stdout, "DB2BufferPool,host=%s,region=hitratio,dbname=%s,dbpartitionnum=%d,bpname=%s db_name=%q,bp_name=%q,total_logical_reads=%d,total_physical_reads=%d,total_hit_ratio_percent=%.2f,data_logical_reads=%d,data_physical_reads=%d,data_hit_ratio_percent=%.2f,index_logical_reads=%d,index_physical_reads=%d,index_hit_ratio_percent=%.2f,xda_logical_reads=%d,xda_physical_reads=%d,xds_hit_ratio_percent=%.2f,average_read_time_ms=%d,total_async_reads=%d,average_async_read_time_ms=%d,total_sync_reads=%d,average_sync_read_time_ms=%d,percent_sync_reads=%.2f,async_not_read_percent=%.2f,total_writes=%d,average_write_time_ms=%d,total_async_writes=%d,percent_writes_async=%.2f,average_async_write_time_ms=%d,total_sync_writes=%d,average_sync_write_time_ms=%d,dbpartitionnum=%d\n",
			current_hostname,
			v.db_name,
			v.dbpartitionnum,
			v.bp_name,
			v.db_name,
			v.bp_name,
			v.total_logical_reads,
			v.total_physical_reads,
			v.total_hit_ratio_percent,
			v.data_logical_reads,
			v.data_physical_reads,
			v.data_hit_ratio_percent,
			v.index_logical_reads,
			v.index_physical_reads,
			v.index_hit_ratio_percent,
			v.xda_logical_reads,
			v.xda_physical_reads,
			v.xda_hit_ratio_percent,
			v.average_read_time_ms,
			v.total_async_reads,
			v.average_async_read_time_ms,
			v.total_sync_reads,
			v.average_sync_read_time_ms,
			v.percent_sync_reads,
			v.async_not_read_percent,
			v.total_writes,
			v.average_write_time_ms,
			v.total_async_writes,
			v.percent_writes_async,
			v.average_async_write_time_ms,
			v.total_sync_writes,
			v.average_sync_write_time_ms,
			v.dbpartitionnum,
		)
	}
}
