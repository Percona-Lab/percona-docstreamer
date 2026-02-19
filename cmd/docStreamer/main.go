package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"golang.org/x/term"

	"github.com/Percona-Lab/percona-docstreamer/internal/api"
	"github.com/Percona-Lab/percona-docstreamer/internal/cdc"
	"github.com/Percona-Lab/percona-docstreamer/internal/checkpoint"
	"github.com/Percona-Lab/percona-docstreamer/internal/cloner"
	"github.com/Percona-Lab/percona-docstreamer/internal/config"
	"github.com/Percona-Lab/percona-docstreamer/internal/dbops"
	"github.com/Percona-Lab/percona-docstreamer/internal/discover"
	"github.com/Percona-Lab/percona-docstreamer/internal/flow"
	"github.com/Percona-Lab/percona-docstreamer/internal/indexer"
	"github.com/Percona-Lab/percona-docstreamer/internal/logging"
	"github.com/Percona-Lab/percona-docstreamer/internal/pid"
	"github.com/Percona-Lab/percona-docstreamer/internal/status"
	"github.com/Percona-Lab/percona-docstreamer/internal/topo"
	"github.com/Percona-Lab/percona-docstreamer/internal/validator"
)

var version = "1"

func getPassword(prompt string) (string, error) {
	fmt.Print(prompt)
	bytePassword, err := term.ReadPassword(int(syscall.Stdin))
	if err != nil {
		return "", err
	}
	fmt.Println()
	return strings.TrimSpace(string(bytePassword)), nil
}

func getConfirmation(prompt string) (bool, error) {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print(prompt)
	text, err := reader.ReadString('\n')
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(text) == "yes", nil
}

var rootCmd = &cobra.Command{
	Use:     "docStreamer",
	Version: version,
	Short:   "DocumentDB to MongoDB Migration and Sync Tool",
	Long: fmt.Sprintf(`docStreamer is a tool for performing a full load and continuous data
capture (CDC) migration from AWS DocumentDB to MongoDB.

docStreamer %s `, version),
	CompletionOptions: cobra.CompletionOptions{
		DisableDefaultCmd: true,
	},
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if cmd.Name() == "help" {
			return
		}
		config.LoadConfig()

		debug, _ := cmd.Flags().GetBool("debug")
		if debug {
			config.Cfg.Logging.Level = "debug"
		}

		if cmd.Name() != "status" {
			logging.Init()
			logging.InitOpLogger()
			logging.InitFullLoadLogger()
		}
		if cmd.Name() != "run" && cmd.Name() != "status" {
			logging.PrintHeader("DocMongo Stream")
		}
	},
}

func startAction(cmd *cobra.Command, args []string) {
	logging.PrintPhase("1", "VALIDATION")
	docdbUser := viper.GetString("docdb.user")
	mongoUser := viper.GetString("mongo.user")
	docdbPass := viper.GetString("DOCDB_PASS")
	mongoPass := viper.GetString("MONGO_PASS")
	if docdbUser == "" {
		logging.PrintError("Missing source DocumentDB username.", 0)
		os.Exit(1)
	}
	if mongoUser == "" {
		logging.PrintError("Missing target MongoDB username.", 0)
		os.Exit(1)
	}
	var err error
	if docdbPass == "" {
		docdbPass, err = getPassword(fmt.Sprintf("Enter DocumentDB password for user '%s': ", docdbUser))
		if err != nil {
			os.Exit(1)
		}
	}
	if mongoPass == "" {
		mongoPass, err = getPassword(fmt.Sprintf("Enter MongoDB password for user '%s': ", mongoUser))
		if err != nil {
			os.Exit(1)
		}
	}
	docdbURI := config.Cfg.BuildDocDBURI(docdbUser, docdbPass)
	mongoURI := config.Cfg.BuildMongoURI(mongoUser, mongoPass)

	logging.PrintStep("Connecting to source DocumentDB...", 0)
	clientOpts := options.Client().ApplyURI(docdbURI)

	if config.Cfg.DocDB.TLS && config.Cfg.DocDB.TlsAllowInvalidHostnames {
		clientOpts.SetTLSConfig(&tls.Config{InsecureSkipVerify: true})
	}
	sourceClient, err := mongo.Connect(clientOpts)
	if err != nil {
		logging.PrintError(fmt.Sprintf("Failed to create source client: %v", err), 0)
		os.Exit(1)
	}
	defer sourceClient.Disconnect(context.TODO())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err = sourceClient.Ping(ctx, readpref.Primary()); err != nil {
		logging.PrintError(fmt.Sprintf("Failed to connect to source: %v", err), 0)
		os.Exit(1)
	}
	logging.PrintSuccess("Connection to source successful.", 0)

	logging.PrintStep("Connecting to target MongoDB...", 0)
	mongoClientOpts := options.Client().ApplyURI(mongoURI)

	if config.Cfg.Mongo.TLS && config.Cfg.Mongo.TlsAllowInvalidHostnames {
		mongoClientOpts.SetTLSConfig(&tls.Config{InsecureSkipVerify: true})
	}

	targetClient, err := mongo.Connect(mongoClientOpts)
	if err != nil {
		logging.PrintError(fmt.Sprintf("Failed to create target client: %v", err), 0)
		os.Exit(1)
	}
	defer targetClient.Disconnect(context.TODO())
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err = targetClient.Ping(ctx, readpref.Primary()); err != nil {
		logging.PrintError(fmt.Sprintf("Failed to connect to target: %v", err), 0)
		os.Exit(1)
	}
	logging.PrintSuccess("Connection to target successful.", 0)

	logging.PrintStep("Validating DocumentDB Change Stream configuration...", 0)
	ctxValidate, cancelValidate := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelValidate()

	isStreamEnabled, err := dbops.ValidateDocDBStreamConfig(ctxValidate, sourceClient)
	if err != nil {
		logging.PrintError(fmt.Sprintf("Failed to validate change stream configuration: %v", err), 0)
		os.Exit(1)
	}
	if !isStreamEnabled {
		logging.PrintError("CRITICAL: DocumentDB cluster-wide change stream is NOT enabled.", 0)
		os.Exit(1)
	}
	logging.PrintSuccess("DocumentDB Change Stream configuration is valid.", 0)

	if config.Cfg.Migration.DryRun {
		logging.PrintSuccess("Dry Run mode enabled via configuration. Exiting.", 0)
		os.Exit(0)
	}

	if pidVal, err := pid.Read(); err == nil && pid.IsRunning(pidVal) {
		logging.PrintError(fmt.Sprintf("Application is already running with PID %d.", pidVal), 0)
		os.Exit(1)
	}

	if config.Cfg.Migration.Destroy {
		checkpointManager := checkpoint.NewManager(targetClient)
		_, found := checkpointManager.GetResumeTimestamp(ctx, config.Cfg.Migration.CheckpointDocID)
		if found {
			logging.PrintWarning("--- DESTROY DATA CONFIRMATION ---", 0)
			confirmed, err := getConfirmation("Type 'yes' to confirm and destroy all target data: ")
			if err != nil || !confirmed {
				os.Exit(1)
			}
			logging.PrintSuccess("Destruction confirmed. Proceeding.", 0)
		}
	}

	logging.PrintPhase("2", "LAUNCHING BACKGROUND PROCESS")
	executable, err := os.Executable()
	if err != nil {
		os.Exit(1)
	}

	var hiddenargs = []string{"run"}

	if config.Cfg.Migration.Destroy {
		hiddenargs = append(hiddenargs, "--destroy")
	}

	debug, _ := cmd.Flags().GetBool("debug")
	if debug {
		hiddenargs = append(hiddenargs, "--debug")
	}

	runCmd := exec.Command(executable, hiddenargs...)
	runCmd.SysProcAttr = &syscall.SysProcAttr{
		Setsid: true,
	}

	runCmd.Env = os.Environ()
	memLimitBytes := config.ResolveGoMemLimit()
	runCmd.Env = append(runCmd.Env, fmt.Sprintf("GOMEMLIMIT=%s", memLimitBytes))

	gogc := config.Cfg.Migration.GoGC
	if gogc == 0 {
		gogc = 50
	}
	runCmd.Env = append(runCmd.Env, fmt.Sprintf("GOGC=%d", gogc))

	runCmd.Env = append(runCmd.Env, fmt.Sprintf("DOCDB_USER=%s", docdbUser))
	runCmd.Env = append(runCmd.Env, fmt.Sprintf("MONGO_USER=%s", mongoUser))
	runCmd.Env = append(runCmd.Env, fmt.Sprintf("DOCDB_PASS=%s", docdbPass))
	runCmd.Env = append(runCmd.Env, fmt.Sprintf("MONGO_PASS=%s", mongoPass))

	if err := runCmd.Start(); err != nil {
		logging.PrintError(fmt.Sprintf("Failed to launch background process: %v", err), 0)
		os.Exit(1)
	}
	logging.PrintSuccess(fmt.Sprintf("Application started in background with PID: %d", runCmd.Process.Pid), 0)

	processExitChan := make(chan struct{})
	go func() {
		runCmd.Wait()
		close(processExitChan)
	}()

	logFile, err := os.Open(config.Cfg.Logging.FilePath)
	var initialOffset int64 = 0
	if err == nil {
		initialOffset, _ = logFile.Seek(0, 2)
		logFile.Close()
	}

	monitorStartup(config.Cfg.Logging.FilePath, initialOffset, runCmd.Process.Pid, config.Cfg.Migration.StatusHTTPPort, processExitChan)
}

func stopAction(phaseTitle string) error {
	logging.PrintPhase("6", phaseTitle)

	pidVal, err := pid.Read()
	if err != nil {
		return fmt.Errorf("could not read PID file. Is the application running")
	}

	logFile, err := os.Open(config.Cfg.Logging.FilePath)
	if err == nil {
		logFile.Seek(0, 2)
	}
	defer func() {
		if logFile != nil {
			logFile.Close()
		}
	}()

	if err := pid.Stop(); err != nil {
		return err
	}
	logging.PrintSuccess("Stop signal sent.", 0)

	if logFile != nil {
		reader := bufio.NewReader(logFile)
		for {
			line, err := reader.ReadString('\n')
			if err == nil {
				fmt.Print(line)
			} else if err != io.EOF {
				break
			} else {
				time.Sleep(100 * time.Millisecond)
			}

			if !pid.IsRunning(pidVal) {
				for {
					line, err := reader.ReadString('\n')
					if err == nil {
						fmt.Print(line)
					} else {
						break
					}
				}
				break
			}
		}
	}
	return nil
}

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Starts the full load and CDC migration",
	Long: `Starts the migration process in the background.
This command will tail the logs until the application API is healthy,
then it will detach and return you to the command prompt.`,
	Run: startAction,
}

var stopCmd = &cobra.Command{
	Use:   "stop",
	Short: "Finds the running application and stops it",
	Run: func(cmd *cobra.Command, args []string) {
		if err := stopAction("STOPPING APPLICATION"); err != nil {
			logging.PrintError(err.Error(), 0)
			os.Exit(1)
		}
		logging.PrintSuccess("Application stopped.", 0)
	},
}

var restartCmd = &cobra.Command{
	Use:   "restart",
	Short: "Restarts the application",
	Long:  `Stops the running application (if any) and starts it again.`,
	Run: func(cmd *cobra.Command, args []string) {
		pidVal, err := pid.Read()
		if err == nil && pid.IsRunning(pidVal) {
			if err := stopAction("STOPPING FOR RESTART"); err != nil {
				logging.PrintError(fmt.Sprintf("Failed to stop application: %v", err), 0)
				os.Exit(1)
			}
			logging.PrintSuccess("Application stopped.", 0)
			time.Sleep(1 * time.Second)
		} else {
			logging.PrintInfo("Application is not running. Proceeding to start.", 0)
		}
		startAction(cmd, args)
	},
}

func monitorStartup(logPath string, offset int64, pidVal int, port string, exitChan chan struct{}) {
	if port == "" {
		port = "8080"
	}
	statusURL := fmt.Sprintf("http://localhost:%s/status", port)

	time.Sleep(500 * time.Millisecond)

	file, err := os.Open(logPath)
	if err != nil {
		fmt.Printf("Warning: Could not read log file: %v\n", err)
		return
	}
	defer file.Close()
	file.Seek(offset, 0)
	reader := bufio.NewReader(file)

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-exitChan:
			for {
				line, err := reader.ReadString('\n')
				if err == nil {
					fmt.Print(line)
				} else {
					break
				}
			}
			logging.PrintInfo("Background process exited.", 0)
			return
		default:
		}

		for {
			line, err := reader.ReadString('\n')
			if err == nil {
				fmt.Print(line)
			} else {
				break
			}
		}

		if !pid.IsRunning(pidVal) {
			logging.PrintError(("Process exited unexpectedly. Check logs for details."), 0)
			return
		}

		select {
		case <-ticker.C:
			resp, err := http.Get(statusURL)
			if err == nil {
				defer resp.Body.Close()
				var s status.StatusOutput
				if json.NewDecoder(resp.Body).Decode(&s) == nil {
					if s.OK && s.State == "running" {
						logging.PrintSuccess(fmt.Sprintf("Application is healthy (State: %s).", s.State), 0)
						return
					}
					if s.State == "error" {
						logging.PrintError(fmt.Sprintf("Application reported an error: %s", s.Info), 0)
						os.Exit(1)
					}
				}
			}
		default:
			time.Sleep(200 * time.Millisecond)
		}
	}
}

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Checks and prints the current status of the migration",
	Run: func(cmd *cobra.Command, args []string) {
		pidVal, err := pid.Read()
		if err != nil || !pid.IsRunning(pidVal) {
			fmt.Println("Application is not running.")
			os.Exit(1)
		}
		port := config.Cfg.Migration.StatusHTTPPort
		if port == "" {
			port = "8080"
		}
		url := fmt.Sprintf("http://localhost:%s/status", port)
		resp, err := http.Get(url)
		if err != nil {
			fmt.Printf("Application is running (PID %d), but failed to get status from %s: %v\n", pidVal, url, err)
			os.Exit(1)
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			fmt.Printf("Failed to read status response: %v\n", err)
			os.Exit(1)
		}

		var data status.StatusOutput
		if err := json.Unmarshal(body, &data); err != nil {
			// Fallback if not valid JSON
			fmt.Println(string(body))
		} else {
			pretty, _ := json.MarshalIndent(data, "", "    ")
			fmt.Println("--- docStreamer Status (Live) ---")
			fmt.Printf("PID: %d (Querying %s)\n", pidVal, url)
			fmt.Println(string(pretty))
		}
	},
}

var runCmd = &cobra.Command{
	Use:    "run",
	Hidden: true,
	Run: func(cmd *cobra.Command, args []string) {
		runMigrationProcess(cmd, args)
	},
}

func runMigrationProcess(cmd *cobra.Command, args []string) {
	if err := pid.Write(); err != nil {
		logging.PrintError(fmt.Sprintf("Failed to write PID file: %v", err), 0)
		os.Exit(1)
	}
	defer pid.Clear()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var statusManager *status.Manager
	var apiServer *api.Server

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		logging.PrintWarning(fmt.Sprintf("Received signal: %v. Initiating graceful shutdown...", sig), 0)
		logging.PrintInfo("!!! DO NOT FORCE QUIT (Ctrl+C). Waiting for workers to finish and flush data...", 0)

		if statusManager != nil {
			statusManager.SetState("stopping", "Initiating shutdown. Waiting for workers...")
			statusManager.Persist(context.Background())
		}

		if apiServer != nil {
			stopCtx, stopCancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer stopCancel()
			apiServer.Stop(stopCtx)
		}

		cancel()

		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		startShutdown := time.Now()
		const maxWait = 300 * time.Second

		for range ticker.C {
			elapsed := time.Since(startShutdown).Round(time.Second)

			applied := "N/A"
			if statusManager != nil {
				applied = fmt.Sprintf("%d", statusManager.GetEventsApplied())
			}

			msg := fmt.Sprintf("Still shutting down... (%s elapsed). Events Applied: %s", elapsed, applied)
			logging.PrintInfo(fmt.Sprintf(">>> %s", msg), 0)

			if statusManager != nil {
				statusManager.SetState("stopping", msg)
				statusManager.Persist(context.Background())
			}

			if elapsed > maxWait {
				logging.PrintError("Shutdown timeout exceeded. Force exiting to prevent hang.", 0)
				pid.Clear()
				os.Exit(1)
			}
		}
	}()

	docdbUser, _ := cmd.Flags().GetString("docdb-user")
	if docdbUser == "" {
		docdbUser = os.Getenv("DOCDB_USER")
	}

	mongoUser, _ := cmd.Flags().GetString("mongo-user")
	if mongoUser == "" {
		mongoUser = os.Getenv("MONGO_USER")
	}

	docdbPass, _ := cmd.Flags().GetString("docdb-pass")
	if docdbPass == "" {
		docdbPass = os.Getenv("DOCDB_PASS")
	}

	mongoPass, _ := cmd.Flags().GetString("mongo-pass")
	if mongoPass == "" {
		mongoPass = os.Getenv("MONGO_PASS")
	}

	destroy, _ := cmd.Flags().GetBool("destroy")

	docdbURI := config.Cfg.BuildDocDBURI(docdbUser, docdbPass)
	mongoURI := config.Cfg.BuildMongoURI(mongoUser, mongoPass)

	metaOpts := options.Client().ApplyURI(mongoURI).SetAppName("docStreamer-Metadata")
	if config.Cfg.Mongo.TLS && config.Cfg.Mongo.TlsAllowInvalidHostnames {
		metaOpts.SetTLSConfig(&tls.Config{InsecureSkipVerify: true})
	}
	targetClient, err := mongo.Connect(metaOpts)
	if err != nil {
		logging.PrintError(fmt.Sprintf("Metadata client error: %v", err), 0)
		return
	}
	defer targetClient.Disconnect(context.Background())

	checkpointManager := checkpoint.NewManager(targetClient)
	resumeAt, found := checkpointManager.GetResumeTimestamp(ctx, config.Cfg.Migration.CheckpointDocID)

	var collectionsToMigrate []discover.CollectionInfo

	if destroy {
		logging.PrintPhase("3", "DISCOVERY (for Destroy)")
		discOpts := options.Client().ApplyURI(docdbURI).SetAppName("docStreamer-Discovery")
		if config.Cfg.DocDB.TLS && config.Cfg.DocDB.TlsAllowInvalidHostnames {
			discOpts.SetTLSConfig(&tls.Config{InsecureSkipVerify: true})
		}
		discClient, _ := mongo.Connect(discOpts)
		collectionsToMigrate, err = discover.DiscoverCollections(ctx, discClient)
		discClient.Disconnect(context.Background())

		if err != nil {
			logging.PrintError(err.Error(), 0)
			return
		}
		dbsFromSource := extractDBNames(collectionsToMigrate)
		logging.PrintPhase("DESTROY", "Dropping target databases...")
		dbops.DropAllDatabases(ctx, targetClient, dbsFromSource)

		logging.PrintPhase("DESTROY", "Dropping metadata database...")
		if err := targetClient.Database(config.Cfg.Migration.MetadataDB).Drop(ctx); err != nil {
			logging.PrintWarning(fmt.Sprintf("Failed to drop metadata DB: %v", err), 0)
		}
		found = false
	}

	var startAt bson.Timestamp
	var anchorFound bool

	if !found {
		startAt, anchorFound = checkpointManager.GetAnchorTimestamp(ctx)
		if anchorFound {
			logging.PrintInfo(fmt.Sprintf("Found partial migration state (Anchor T0: %v). Resuming Full Load...", startAt), 0)
		} else {
			logging.PrintInfo("No valid global checkpoint or anchor found. Starting fresh.", 0)
		}
	}

	tracker := validator.NewInFlightTracker()
	statusManager = status.NewManager(targetClient, false)

	if err := statusManager.LoadAndMerge(ctx); err != nil {
		logging.PrintInfo(fmt.Sprintf("Status load skipped: %v", err), 0)
	}

	flowManager := flow.NewManager(targetClient, statusManager, mongoUser, mongoPass)
	flowManager.Start()
	defer flowManager.Stop()

	valStore := validator.NewStore(targetClient, flowManager, statusManager)

	apiServer = api.NewServer(config.Cfg.Migration.StatusHTTPPort)

	valSrcOpts := options.Client().ApplyURI(docdbURI).SetAppName("docStreamer-Validator-Source")
	if config.Cfg.DocDB.TLS && config.Cfg.DocDB.TlsAllowInvalidHostnames {
		valSrcOpts.SetTLSConfig(&tls.Config{InsecureSkipVerify: true})
	}
	valDstOpts := options.Client().ApplyURI(mongoURI).SetAppName("docStreamer-Validator-Target")
	if config.Cfg.Mongo.TLS && config.Cfg.Mongo.TlsAllowInvalidHostnames {
		valDstOpts.SetTLSConfig(&tls.Config{InsecureSkipVerify: true})
	}
	valSourceClient, _ := mongo.Connect(valSrcOpts)
	valTargetClient, _ := mongo.Connect(valDstOpts)
	defer valSourceClient.Disconnect(context.Background())
	defer valTargetClient.Disconnect(context.Background())

	validationManager := validator.NewManager(valSourceClient, valTargetClient, tracker, valStore, statusManager, flowManager)
	defer validationManager.Close()

	apiServer.RegisterRoute("/status", statusManager.StatusHandler)
	apiServer.RegisterRoute("/validate", validationManager.HandleValidateRequest)
	apiServer.RegisterRoute("/validate/adhoc", validationManager.HandleAdHocValidation)
	apiServer.RegisterRoute("/validate/stats", validationManager.HandleGetStats)
	apiServer.RegisterRoute("/validate/retry", validationManager.HandleRetryFailures)
	apiServer.RegisterRoute("/validate/reset", validationManager.HandleReset)
	apiServer.RegisterRoute("/validate/failures", validationManager.HandleGetFailures)
	apiServer.RegisterRoute("/validate/queue", validationManager.HandleGetQueueStatus)
	apiServer.RegisterRoute("/scan", validationManager.HandleScan)

	apiServer.Start()

	if !found {
		statusManager.SetInitialSyncStart(time.Now().UTC())

		flSrcOpts := options.Client().ApplyURI(docdbURI).SetAppName("docStreamer-FullLoad-Source")
		if config.Cfg.DocDB.TLS && config.Cfg.DocDB.TlsAllowInvalidHostnames {
			flSrcOpts.SetTLSConfig(&tls.Config{InsecureSkipVerify: true})
		}
		flDstOpts := options.Client().ApplyURI(mongoURI).SetAppName("docStreamer-FullLoad-Target")
		if config.Cfg.Mongo.TLS && config.Cfg.Mongo.TlsAllowInvalidHostnames {
			flDstOpts.SetTLSConfig(&tls.Config{InsecureSkipVerify: true})
		}
		sourceClient, _ := mongo.Connect(flSrcOpts)
		targetWriterClient, _ := mongo.Connect(flDstOpts)
		defer sourceClient.Disconnect(context.Background())
		defer targetWriterClient.Disconnect(context.Background())

		if !anchorFound {
			t0, _ := topo.ClusterTime(ctx, sourceClient)
			startAt = t0
			checkpointManager.SaveAnchorTimestamp(ctx, startAt)
		}

		logging.PrintPhase("3", "DISCOVERY")
		statusManager.SetState("discovering", "Discovering collections...")
		collectionsToMigrate, _ = discover.DiscoverCollections(ctx, sourceClient)

		if len(collectionsToMigrate) > 0 {
			completedColls, _ := checkpointManager.GetCompletedCollections(ctx)
			statusManager.ResetProgress()

			var toRun []discover.CollectionInfo
			for _, coll := range collectionsToMigrate {
				if completedColls[coll.Namespace] {
					statusManager.AddEstimatedBytes(coll.Size)
					statusManager.AddEstimatedDocs(coll.Count)
					statusManager.AddClonedBytes(coll.Size)
					statusManager.AddClonedDocs(coll.Count)
				} else {
					toRun = append(toRun, coll)
					statusManager.AddEstimatedBytes(coll.Size)
					statusManager.AddEstimatedDocs(coll.Count)
				}
			}

			if len(toRun) > 0 {
				logging.PrintPhase("4", "FULL DATA LOAD")
				statusManager.SetState("running", "Initial Sync")
				launchFullLoadWorkers(ctx, sourceClient, targetWriterClient, toRun, statusManager, checkpointManager, flowManager)
			}
		}

		statusManager.SetInitialSyncEnd(time.Now().UTC())
		checkpointManager.SaveResumeTimestamp(ctx, config.Cfg.Migration.CheckpointDocID, startAt)
		checkpointManager.DeleteAnchorTimestamp(ctx)
		statusManager.SetInitialSyncCompleted(0)
		statusManager.Persist(ctx)

	} else {
		logging.PrintPhase("3", "DISCOVERY (SKIPPED)")
		logging.PrintPhase("4", "FULL DATA LOAD (SKIPPED)")
		startAt = resumeAt
		statusManager.SetInitialSyncCompleted(0)
	}

	logging.PrintPhase("5", "CONTINUOUS SYNC (CDC)")
	statusManager.SetState("running", "Change Data Capture")

	cdcSrcOpts := options.Client().ApplyURI(docdbURI).SetAppName("docStreamer-CDC-Watcher")
	if config.Cfg.DocDB.TLS && config.Cfg.DocDB.TlsAllowInvalidHostnames {
		cdcSrcOpts.SetTLSConfig(&tls.Config{InsecureSkipVerify: true})
	}
	cdcDstOpts := options.Client().ApplyURI(mongoURI).SetAppName("docStreamer-CDC-Writer")
	if config.Cfg.Mongo.TLS && config.Cfg.Mongo.TlsAllowInvalidHostnames {
		cdcDstOpts.SetTLSConfig(&tls.Config{InsecureSkipVerify: true})
	}

	cdcSourceClient, _ := mongo.Connect(cdcSrcOpts)
	cdcTargetClient, _ := mongo.Connect(cdcDstOpts)
	defer cdcSourceClient.Disconnect(context.Background())
	defer cdcTargetClient.Disconnect(context.Background())

	cdcManager := cdc.NewManager(
		cdcSourceClient,
		cdcTargetClient,
		config.Cfg.Migration.CheckpointDocID,
		startAt,
		checkpointManager,
		statusManager,
		tracker,
		valStore,
		validationManager,
		flowManager,
	)

	cdcManager.Start(ctx)
	logging.PrintInfo("CDC process stopped. Exiting.", 0)
}

func extractDBNames(collections []discover.CollectionInfo) []string {
	dbMap := make(map[string]bool)
	for _, coll := range collections {
		dbMap[coll.DB] = true
	}
	dbNames := make([]string, 0, len(dbMap))
	for dbName := range dbMap {
		dbNames = append(dbNames, dbName)
	}
	return dbNames
}

func launchFullLoadWorkers(ctx context.Context, source, target *mongo.Client, collections []discover.CollectionInfo, statusMgr *status.Manager, checkpointMgr *checkpoint.Manager, flowMgr *flow.Manager) (bson.Timestamp, error) {
	copiers := make([]*cloner.CopyManager, len(collections))
	for i, coll := range collections {
		copiers[i] = cloner.NewCopyManager(source, target, coll, statusMgr, checkpointMgr, config.Cfg.Migration.CheckpointDocID, flowMgr)
	}

	workerCount := config.Cfg.Migration.MaxConcurrentWorkers
	logging.PrintPhase("4a", "FULL LOAD: PREPARATION")
	indexer.StopBalancer(ctx, target)

	for _, cm := range copiers {
		if ctx.Err() != nil {
			return bson.Timestamp{}, ctx.Err()
		}
		cm.Prepare(ctx)
	}

	logging.PrintStep("Preparation complete. Restarting MongoDB Balancer...", 0)
	indexer.StartBalancer(ctx, target)
	logging.PrintPhase("4b", "FULL LOAD: DATA COPY")

	poolCtx, poolCancel := context.WithCancel(ctx)
	defer poolCancel()

	runQueue := make(chan *cloner.CopyManager, len(copiers))
	for _, cm := range copiers {
		runQueue <- cm
	}
	close(runQueue)

	resultsChan := make(chan error, len(copiers))
	var wg sync.WaitGroup
	wg.Add(workerCount)

	for i := 0; i < workerCount; i++ {
		go func(id int) {
			defer wg.Done()
			for cm := range runQueue {
				if poolCtx.Err() != nil {
					return
				}
				ns := cm.CollInfo.Namespace
				logging.PrintStep(fmt.Sprintf("[Worker %d] Starting Copy for %s", id, ns), 0)

				start := time.Now()
				count, _, err := cm.Run(poolCtx)
				logging.LogFullLoadOp(start, ns, count, err, id)
				resultsChan <- err

				if err != nil {
					logging.PrintError(fmt.Sprintf("[%s] Copy FAILED: %v", ns, err), 0)
					poolCancel()
				} else {
					logging.PrintSuccess(fmt.Sprintf("[%s] Copy COMPLETED: %d docs", ns, count), 0)
				}
			}
		}(i)
	}
	wg.Wait()
	close(resultsChan)

	for err := range resultsChan {
		if err != nil {
			return bson.Timestamp{}, err
		}
	}

	return bson.Timestamp{}, nil
}

func init() {
	rootCmd.PersistentFlags().String("docdb-user", "", "Source DocumentDB Username")
	rootCmd.PersistentFlags().String("mongo-user", "", "Target MongoDB Username")
	rootCmd.PersistentFlags().Bool("debug", false, "Enable debug logging")

	viper.BindPFlag("docdb.user", rootCmd.PersistentFlags().Lookup("docdb-user"))
	viper.BindPFlag("mongo.user", rootCmd.PersistentFlags().Lookup("mongo-user"))

	runCmd.Flags().String("docdb-user", "", "")
	runCmd.Flags().String("mongo-user", "", "")
	runCmd.Flags().String("docdb-pass", "", "")
	runCmd.Flags().String("mongo-pass", "", "")
	runCmd.Flags().Bool("destroy", false, "")
	runCmd.Flags().Bool("debug", false, "")
	runCmd.Flags().MarkHidden("docdb-user")
	runCmd.Flags().MarkHidden("mongo-user")
	runCmd.Flags().MarkHidden("docdb-pass")
	runCmd.Flags().MarkHidden("mongo-pass")
	runCmd.Flags().MarkHidden("destroy")
	runCmd.Flags().MarkHidden("debug")

	viper.BindEnv("DOCDB_PASS")
	viper.BindEnv("MONGO_PASS")

	rootCmd.AddCommand(startCmd)
	rootCmd.AddCommand(restartCmd)
	rootCmd.AddCommand(stopCmd)
	rootCmd.AddCommand(statusCmd)
	rootCmd.AddCommand(runCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
