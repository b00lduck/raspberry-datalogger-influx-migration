package main

import (
	"log"
	"time"
	"github.com/influxdata/influxdb/client/v2"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"fmt"
)

const (
	influxDbName = "energy"
	influxDbUsername = "root"
	influxDbPassword = "root"
	influxDbUrl = "http://localhost:8086"
)

func main() {
	// Make influx client
	influxClient, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: influxDbUrl,
		Username: influxDbUsername,
		Password: influxDbPassword,
	})
	if err != nil {
		log.Fatalln("Error: ", err)
	}

	// Make mySQL driver
	mysqlClient, err := sql.Open("mysql", "root:root@/rem-dataservice")
	if err != nil {
		log.Fatalln("Error: ", err)
	}

	_, err = queryDB(influxClient, "CREATE DATABASE energy")
	if err != nil {
		log.Fatalln("Error: ", err)
	}

//	migrate(influxClient, mysqlClient, 1000, "select timestamp, reading from counter_event where counter_id=1", "gas_usage")
//	migrate(influxClient, mysqlClient, 1000, "select timestamp, reading from thermometer_reading where thermometer_id=1", "temperature_air")
	migrate(influxClient, mysqlClient, 1000, "select timestamp, reading from thermometer_reading where thermometer_id=2", "temperature_boiler")
	migrate(influxClient, mysqlClient, 1000, "select timestamp, reading from thermometer_reading where thermometer_id=3", "temperature_hot_water_tank")
	migrate(influxClient, mysqlClient, 1000, "select timestamp, reading from thermometer_reading where thermometer_id=4", "temperature_boiler_room")
	migrate(influxClient, mysqlClient, 1000, "select timestamp, reading from thermometer_reading where thermometer_id=5", "temperature_hallway_1")
	migrate(influxClient, mysqlClient, 1000, "select timestamp, reading from thermometer_reading where thermometer_id=6", "temperature_hallway_2")
	migrate(influxClient, mysqlClient, 1, "select timestamp, state from flag_state where flag_id=1", "flag_hot_water_circulation_pump_active")
	migrate(influxClient, mysqlClient, 1, "select timestamp, state from flag_state where flag_id=2", "flag_heating_winter")
	migrate(influxClient, mysqlClient, 1, "select timestamp, state from flag_state where flag_id=3", "flag_heating_circulation_pump_active")
	migrate(influxClient, mysqlClient, 1, "select timestamp, state from flag_state where flag_id=5", "flag_heating_day")
	migrate(influxClient, mysqlClient, 1, "select timestamp, state from flag_state where flag_id=6", "flag_hot_water_loading_pump_active")

}

func migrate(influxClient client.Client, mysqlClient *sql.DB, divisor int, query string, seriesName string) error {

	fmt.Println("Remove old data " + seriesName)
	queryDB(influxClient, "DROP MEASUREMENT " + seriesName)

	rows, err := mysqlClient.Query(query)
	if err != nil {
		log.Fatalln("Error: ", err)
	}
	defer rows.Close()

	for rows.Next() {

        	// Create a new point batch
	        bp, err := client.NewBatchPoints(client.BatchPointsConfig{
                	Database:  influxDbName,
        	        Precision: "ms",
	        })
	        if err != nil {
        	        return err
	        }

		var sqlTimestamp int64
		var reading float64
		err = rows.Scan(&sqlTimestamp, &reading)
		if err != nil {
			log.Fatalln(err, "Error scanning rows")
			return err
		}

		value := reading / float64(divisor)

	       //fmt.Printf("Value %f\n", value)	

		// Create a point and add to batch
		tags := map[string]string{}
		fields := map[string]interface{}{
			"value": value,
		}
		timestamp := time.Unix(0, int64(sqlTimestamp) * 1000000)

		pt, err := client.NewPoint(seriesName, tags, fields, timestamp)
		if err != nil {
			return err
		}

		bp.AddPoint(pt)

        	fmt.Println("Writing batch")

	        // Write the batch
        	err = influxClient.Write(bp)
	        if err != nil {
                	log.Fatalln(err, "Error writing batch ")
        	        return err
	        }


	}

	return nil

}


// queryDB convenience function to query the database
func queryDB(clnt client.Client, cmd string) (res []client.Result, err error) {
	q := client.Query{
		Command:  cmd,
		Database: influxDbName,
	}
	if response, err := clnt.Query(q); err == nil {
		if response.Error() != nil {
			return res, response.Error()
		}
		res = response.Results
	} else {
		return res, err
	}
	return res, nil
}
