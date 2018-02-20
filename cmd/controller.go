
package cmd

import (
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"log"
	"github.com/problame/ba-doltp/rpc"
	"context"
	"text/template"
	"io"
	"bytes"
	"io/ioutil"
	"path/filepath"
	"gopkg.in/yaml.v2"
	"os"
	"fmt"
	"github.com/kr/pretty"
	"encoding/json"
	"database/sql"
	"time"
	_ "github.com/go-sql-driver/mysql"
	"os/signal"
	"syscall"
	"github.com/pkg/errors"
	"path"
)

type MySQLAuth struct {
	Host string
	Port int
	User string
	Password string
	DB string
}

func (a *MySQLAuth) String() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", a.User, a.Password, a.Host, a.Port, a.DB)
}

// ControllerConfig is passed to controllerCmd as a yaml-marshaled file
type ControllerConfig struct {
	Worker        string
	MySQLCommand  []string
	PerfCommand   []string
	MySQL MySQLAuth
	TPCC struct {
		Seconds int
		Terminals []int
	}
}

// TPCCConfig abstracts the OLTP TPCC configuration file
type TPCCConfig struct {
	MySQL MySQLAuth
	Terminals int
	Seconds int
}

// Run represents a single TPCC run. Stored aside runs.json
type Run struct {
	Dir string
	TPCCConfig TPCCConfig
	MySQLCommand []string
	MySQLAuth MySQLAuth
	PerfCommand []string
	Result []byte
}


var contrArgs struct {
	configFile string
	results    string
}

var controllerCmd = &cobra.Command{
	Use:   "controller",
	Short: "The controller to run on the server with MySQL",
	RunE: doController,
}

func init() {
	RootCmd.AddCommand(controllerCmd)
	controllerCmd.PersistentFlags().StringVar(&contrArgs.configFile, "config", "", "config YAML file in empty results dir")
	controllerCmd.PersistentFlags().StringVar(&contrArgs.results, "results", "", "results directory")
}

func copyFile(dest, src string) (err error) {
	_, err = os.Stat(dest)
	if err == nil && !os.IsNotExist(err) {
		return fmt.Errorf("%s already exists", dest)
	}
	conf, err := os.Open(src)
	if err != nil {
		return err
	}
	defer conf.Close()
	newconf, err := os.OpenFile(dest, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	defer newconf.Close()

	_, err = io.Copy(newconf, conf)
	if err != nil {
		return err
	}

	return nil
}

func doController(cmd *cobra.Command, args []string) error {

	if contrArgs.results == "" {
		return errors.New("must specify results argument")
	}

	workdir := contrArgs.results

	if err := os.Mkdir(workdir, 0777); err != nil {
		return err
	}

	// keep copy for reference
	configCopy := path.Join(workdir, "config.yml")
	if err := copyFile(configCopy, contrArgs.configFile); err != nil {
		return err
	}

	// parse config
	b, err := ioutil.ReadFile(configCopy)
	if err != nil {
		return err
	}
	var conf ControllerConfig
	err = yaml.Unmarshal(b, &conf)
	if err != nil {
		return err
	}

	log := log.New(os.Stdout, "", log.LstdFlags)
	log.Printf("workdir=%s", workdir)
	log.Printf("conf:\n%s\n\n", pretty.Sprint(conf))

	ctx, cancel := context.WithCancel(context.Background())
	sigchan := make(chan os.Signal)
	go func() {
		select {
		case sig := <- sigchan:
			if sig != nil {
				log.Print("received SIGINT")
				cancel()
				break
			}
		}
	}()
	signal.Notify(sigchan, syscall.SIGINT)

	log.Print("connecting to worker")
	conn, err := grpc.Dial(conf.Worker, grpc.WithInsecure())
	if err != nil {
		log.Panic(err)
	}
	defer conn.Close()
	w := rpc.NewWorkerClient(conn)
	log.Print("connected")

	runs := []Run{}
	for _, t := range conf.TPCC.Terminals {
		tpcc := TPCCConfig{
			MySQL: conf.MySQL,
			Terminals: t,
			Seconds: conf.TPCC.Seconds,
		}
		dirname := fmt.Sprintf("terminals_%d", t)
		run := Run {
			TPCCConfig: tpcc,
			Dir: dirname,
			MySQLCommand: conf.MySQLCommand,
			MySQLAuth: conf.MySQL,
			PerfCommand: conf.PerfCommand,
		}
		log.Printf("Doing Run:\n%s", pretty.Sprint(run))

		od := filepath.Join(workdir, dirname)
		if err := os.Mkdir(od, 0777); err != nil {
			log.Panic(err)
		}

		res, hadErr := run.Run(ctx, w, log, od)
		if hadErr {
			log.Print("error running command, not storing it in runs")
			continue
		}

		run.Result = res

		runs = append(runs, run)
		// save runs after successful run
		f, err := os.OpenFile(filepath.Join(workdir, "runs.json"), os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			log.Panic(err)
		}
		err = json.NewEncoder(f).Encode(runs)
		f.Close()
		if err != nil {
			log.Panic(err)
		}
	}

	return nil
}

func (r *Run) Run(ctx context.Context, worker rpc.WorkerClient, log *log.Logger, outdir string) (oltpResults []byte, runHadError bool) {

	outdirAbs, err := filepath.Abs(outdir)
	if err != nil {
		log.Panic(err)
	}
	env := append(os.Environ(), fmt.Sprintf("OUTDIR=%s", outdirAbs))

	// own ctx to try cancelling it gracefully
	mysql := NewAsyncCommand(ctx, r.MySQLCommand, env)
	mysql.Start()
	defer func() {
		log.Print("waiting for mysql to exit")
		if err := mysql.SignalAndWaitTimeout(syscall.SIGINT, 10*time.Second); err != nil {
			log.Panic(err)
		}
	}()

	log.Printf("wait for MySQL server to come up: %s", r.MySQLAuth.String())
	mysqlReachable := make(chan struct{})
	go func() {
		defer close(mysqlReachable)
		for  {
			log.Print("dialing attempt")
			db, err := sql.Open("mysql", r.MySQLAuth.String()+"?timeout=1s&readTimeout=1s&writeTimeout=1s")
			if err != nil {
				log.Printf("dial error: %s", err)
			} else {
				err = db.Ping()
				db.Close()
				if err == nil {
					return
				} else {
					log.Printf("dial error: %s", err)
				}
			}
			select {
			case <-time.After(1 * time.Second):
				// retry
				case <- ctx.Done():
					return
			}
		}
	}()
	select {
	case err := <-mysql.Wait():
		log.Panic("mysql exited unexpectedly: %s", err)
	case <- mysqlReachable:
	}

	// Write OLTP config
	var oltpconf bytes.Buffer
	if err := r.TPCCConfig.Render(&oltpconf); err != nil {
		log.Panic(err)
	}
	oltp_config_file := filepath.Join(outdir, "oltp_tpcc.xml")
	if err := ioutil.WriteFile(oltp_config_file, oltpconf.Bytes(), 0666); err != nil {
		log.Panic(err)
	}

	log.Print("starting perf")
	perf := NewAsyncCommand(ctx, r.PerfCommand, env)
	perf.Start()
	defer func() {
		log.Print("waiting for perf to exit")
		if err := perf.SignalAndWaitTimeout(syscall.SIGINT, 10*time.Second); err != nil {
			log.Panic(err)
		}
	}()

	log.Print("starting tpcc")
	rchan := make(chan *rpc.OLTPTPCCResponse)
	go func() {
		req := rpc.OLTPTPCCRequest{oltpconf.Bytes()}
		res, err := worker.OLTPTPCC(ctx, &req)
		if err != nil {
			log.Panicf("non-recoverable grpc error: %s", err)
		}
		rchan <- res
	}()

	log.Print("waiting for tpcc to finish")
	select {
	case err := <- mysql.Wait():
		log.Panicf("mysql exited unexpectedly: %s", err)
	case err :=  <- perf.Wait():
		log.Panicf("perf exited unexpectedly: %s", err)
	case res := <- rchan:
		if res.Error != "" {
			var msg bytes.Buffer
			fmt.Fprintf(&msg, "worker responded with error message:\n")
			fmt.Fprintf(&msg, "Error message:\n%s", res.Error)
			fmt.Fprintf(&msg, "OLTP output:\n%s", res.OLTPOutput)
			log.Print(msg.String())
		}
		resultfile := filepath.Join(outdir, "result.csv")
		if err := ioutil.WriteFile(resultfile, res.Results, 0666); err != nil {
			log.Panic("error writing results file: %s", err)
		}
		return res.Results, false
	}

	log.Panic("implementation error")
	return nil, true
}

func (c *TPCCConfig) Render(w io.Writer) (error) {

	const oltp_tpcc_template string = `<?xml version="1.0"?>
<parameters>
    <!-- Connection details -->
    <dbtype>mysql</dbtype>
    <driver>com.mysql.jdbc.Driver</driver>
    <DBUrl>jdbc:mysql://{{ .MySQL.Host }}:{{ .MySQL.Port }}/{{ .MySQL.DB }}</DBUrl>
    <username>{{ .MySQL.User }}</username>
    <password>{{ .MySQL.Password }}</password>
    <isolation>TRANSACTION_SERIALIZABLE</isolation>
    <!-- Scale factor is the number of warehouses in TPCC -->
    <scalefactor>2</scalefactor>
    <!-- The workload -->
    <terminals>{{ .Terminals }}</terminals>
    <works>
        <work>
          <time>{{ .Seconds }}</time>
          <rate>10000</rate>
          <weights>45,43,4,4,4</weights>
        </work>
    </works>
    
    <!-- TPCC specific -->    
    <transactiontypes>
        <transactiontype>
            <name>NewOrder</name>
        </transactiontype>
        <transactiontype>
            <name>Payment</name>
        </transactiontype>
        <transactiontype>
            <name>OrderStatus</name>
        </transactiontype>
        <transactiontype>
            <name>Delivery</name>
        </transactiontype>
        <transactiontype>
            <name>StockLevel</name>
        </transactiontype>
    </transactiontypes> 
</parameters>
`

	tmpl, err := template.New("test").Parse(oltp_tpcc_template)
	if err != nil {
		log.Panic(err)
	}
	return tmpl.Execute(w, c)
}
