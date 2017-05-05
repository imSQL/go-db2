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
	snapshot_timestamp      string
	db_name                 string
	bp_name                 string
	total_logical_reads     int64
	total_physical_reads    int64
	total_hit_ratio_percent float64
	data_logical_reads      int64
	data_physical_reads     int64
	data_hit_ratio_percent  float64
	index_logical_reads     int64
	index_physical_reads    int64
	index_hit_ratio_percent float64
	xda_logical_reads       int64
	xda_physical_reads      int64
	xda_hit_ratio_percent   float64
	dbpartitionnum          int64
}

type Cursor struct {
	bphitratio []BpHitRatio
	cursor     sqlutils.Result
	DbHandler  *sql.DB
}

const (
	BP_HITRATIO = `
SELECT 
    SNAPSHOT_TIMESTAMP, 
    DB_NAME, 
    BP_NAME, 
    TOTAL_LOGICAL_READS, 
    TOTAL_PHYSICAL_READS, 
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
    DBPARTITIONNUM 
FROM 
    SYSIBMADM.BP_HITRATIO
	`
	BP_READ_IO = `
SELECT 
    SNAPSHOT_TIMESTAMP, 
    BP_NAME, 
    TOTAL_PHYSICAL_READS, 
    AVERAGE_READ_TIME_MS, 
    TOTAL_ASYNC_READS, 
    AVERAGE_ASYNC_READ_TIME_MS, 
    TOTAL_SYNC_READS, 
    AVERAGE_SYNC_READ_TIME_MS, 
    PERCENT_SYNC_READS, 
    ASYNC_NOT_READ_PERCENT, 
    DBPARTITIONNUM 
FROM 
    SYSIBMADM.BP_READ_IO
	`
	BP_WRITE_IO = `
SELECT 
    SNAPSHOT_TIMESTAMP, 
    BP_NAME, 
    TOTAL_WRITES, 
    AVERAGE_WRITE_TIME_MS, 
    TOTAL_ASYNC_WRITES, 
    PERCENT_WRITES_ASYNC, 
    AVERAGE_ASYNC_WRITE_TIME_MS, 
    TOTAL_SYNC_WRITES, 
    AVERAGE_SYNC_WRITE_TIME_MS, 
    DBPARTITIONNUM 
FROM 
    SYSIBMADM.BP_WRITE_IO
	`
)

func (cs *Cursor) GetMetrics() {
	cs.cursor.GetMetric(cs.DbHandler, BP_HITRATIO)
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
		fmt.Fprintf(os.Stdout, "DB2BufferPool,host=%s,region=hitratio,dbname=%s,dbpartitionnum=%d,bpname=%s db_name=%q,bp_name=%q,total_logical_reads=%d,total_physical_reads=%d,total_hit_ratio_percent=%.2f,data_logical_reads=%d,data_physical_reads=%d,data_hit_ratio_percent=%.2f,index_logical_reads=%d,index_physical_reads=%d,index_hit_ratio_percent=%.2f,xda_logical_reads=%d,xda_physical_reads=%d,xds_hit_ratio_percent=%.2f,dbpartitionnum=%d\n",
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
			v.dbpartitionnum,
		)
	}
}
