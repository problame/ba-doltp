package cmd

import (
	"github.com/spf13/cobra"
	"net"
	"log"
	"google.golang.org/grpc"
	"github.com/problame/ba-doltp/rpc"
	"golang.org/x/net/context"
	"os/exec"
	"os"
	"io/ioutil"
	"path"
	"path/filepath"
	"strings"
	"bytes"
	"fmt"
)

var workerArgs struct {
	listen string
	oltpBenchmarkDir string
	limitCores []uint
	workdir string
}

// workerCmd represents the worker command
var workerCmd = &cobra.Command{
	Use:   "worker",
	Short: "The command to run on the worker computers",
	Run: func(cmd *cobra.Command, args []string) {

		l, err := net.Listen("tcp", workerArgs.listen)
		if err != nil {
			log.Panic(err)
		}

		grpcServer := grpc.NewServer()
		w := Worker{}
		rpc.RegisterWorkerServer(grpcServer, &w)
		if err := grpcServer.Serve(l); err != nil {
			log.Panic(err)
		}

	},
}

func init() {
	RootCmd.AddCommand(workerCmd)
	workerCmd.Flags().StringVar(&workerArgs.listen, "listen", WORKER_LISTEN, "")
	workerCmd.Flags().StringVar(&workerArgs.oltpBenchmarkDir, "oltpdir", "", "")
	workerCmd.Flags().UintSliceVar(&workerArgs.limitCores, "cores", []uint{}, "")
	workerCmd.Flags().StringVar(&workerArgs.workdir, "workdir", "", "")
}

type Worker struct {}

func (*Worker) OLTPTPCC(ctx context.Context, req *rpc.OLTPTPCCRequest) (*rpc.OLTPTPCCResponse, error) {

	if err := os.RemoveAll(workerArgs.workdir); err != nil {
		log.Panic(err)
	}

	if err := os.Mkdir(workerArgs.workdir, 0770); err != nil {
		log.Panic(err)
	}

	configpath := path.Join(workerArgs.workdir, "oltp_tpcc_config.xml")
	if err := ioutil.WriteFile(configpath, req.Config, 0660); err != nil {
		log.Panic(err)
	}
	abspath, err := filepath.Abs(configpath)
	if err != nil {
		log.Panic(err)
	}

	const resultfileBaseNoExt = "oltp_tpcc_result"
	oltp := []string{
		"./oltpbenchmark", "--config", abspath , "--bench=tpcc", "--execute=true",
		"-s", "2",
		"--directory", workerArgs.workdir, "-o", resultfileBaseNoExt,
	}

	run := func(cmdline []string) ([]byte, error) {
		if len(cmdline) < 2 {
			log.Panic("command line too short")
		}
		log.Printf("Running: %s", strings.Join(cmdline, " "))
		var buf bytes.Buffer
		cmd := exec.CommandContext(ctx, cmdline[0], cmdline[1:]...)
		cmd.Dir = workerArgs.oltpBenchmarkDir
		cmd.Stdout = &buf
		cmd.Stderr = &buf
		err := cmd.Run()
		return buf.Bytes(), err
	}

	var runErr error
	var oltpout []byte
	if len(workerArgs.limitCores) > 0 {
		coreStrs := []string{}
		for _, c := range workerArgs.limitCores {
			coreStrs = append(coreStrs, fmt.Sprintf("%d", c))
		}
		full := []string{
			"taskset", "-c", strings.Join(coreStrs, ","),
		}
		full = append(full, oltp...)
		oltpout, runErr = run(full)
	} else {
		oltpout, runErr = run(oltp)
	}

	log.Print(string(oltpout))

	if runErr != nil {
		return &rpc.OLTPTPCCResponse{Error: runErr.Error(), OLTPOutput: oltpout}, nil
	}

	resultfile := filepath.Join(workerArgs.workdir, resultfileBaseNoExt + ".res")
	contents, err := ioutil.ReadFile(resultfile)
	if err != nil {
		log.Panic("cannot read result file: " + err.Error())
	}
	return &rpc.OLTPTPCCResponse{Results: contents, OLTPOutput: oltpout}, nil
}
