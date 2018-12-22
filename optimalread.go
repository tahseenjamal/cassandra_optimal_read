package main

import (
	"bufio"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/antigloss/go/logger"
	"github.com/gocql/gocql"
)

func readFromCassandra(msisdn string, subid string, id string, wg *sync.WaitGroup) {

	defer wg.Done()

	start := time.Now()

	session.Query(``).Consistency(gocql.One).Scan()

	elapsed := time.Since(start).Nanoseconds() / 1000000

	processedLogQueue <- fmt.Sprintf()

}

func writeToFile() {

	for {

		message := <-processedLogQueue

		elapsed, _ := strconv.Atoi(strings.Split(message, ",")[0])

		length, _ := strconv.Atoi(strings.Split(message, ",")[3])

		if elapsed > upperlimit || length <= 0 {

			cpus -= 1

			if cpus < 1 {

				cpus = 1
			}
		}

		if elapsed < lowerlimit && length > 0 && cpus <= 256 {

			cpus += 1
		}

		logger.Info(message)
	}
}

var processedLogQueue chan string

var cluster *gocql.ClusterConfig

var session *gocql.Session

var cpus int

var lowerlimit int

var upperlimit int

func main() {

	var queuesize int = 1000

	// Please read logger parameters.

	logger.Init("readlogs", 10, 1, 500, false)

	var wg sync.WaitGroup

	// Cassandra cluster initialization, please your Cluster IPs

	cluster = gocql.NewCluster()

	// Decide on the expected consistency and change as per your requirement

	cluster.Consistency = gocql.Any

	session, _ = cluster.CreateSession()

	defer session.Close()

	processedLogQueue = make(chan string, queuesize)

	// Read and write the Process ID of the program in a file

	// You can even create a bash file with kill -9 PID so that you just have to run it to kill the program

	pidfile, _ := os.Create("read_program.pid")

	pidfile.Write([]byte(strconv.Itoa(os.Getpid())))

	pidfile.Close()

	channelValue := make(chan os.Signal, 1)

	// Signal construct in case you want to stop the program in a certain way

	signal.Notify(channelValue, syscall.SIGHUP, syscall.SIGINT)

	// Reading number of threads that you want to spawn

	cpus, _ = strconv.Atoi(os.Args[2])

	// Low and upper bound in milli seconds

	lowerlimit, _ = strconv.Atoi(os.Args[3])

	upperlimit, _ = strconv.Atoi(os.Args[4])

	waitGroupCount := 0

	go writeToFile()

	// Endless loop to continuously read. Make sure file is large enough to avoid Cassandra from caching data in memory

	for {

		// Pass the csv file name on the command line to be read

		file, _ := os.Open(os.Args[1])

		scanner := bufio.NewScanner(file)

		for scanner.Scan() {

			// Each line read and now you have

			line := scanner.Text()

			// You have to split the line basis separator

			// This is used increase active thread counter

			wg.Add(1)

			waitGroupCount++

			// Here you pass the parameters with last one as wg as that is also read by the function if you check

			go readFromCassandra()

			// Wait group count compared with cpus or say max threads passed via commandline

			if waitGroupCount+1 >= cpus {

				wg.Wait()

				waitGroupCount = 0

			}

		}

		file.Close()
	}
}
