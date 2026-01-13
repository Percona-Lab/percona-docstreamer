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
	"github.com/Percona-Lab/percona-docstreamer/internal/logging"
	"github.com/Percona-Lab/percona-docstreamer/internal/pid"
	"github.com/Percona-Lab/percona-docstreamer/internal/status"
	"github.com/Percona-Lab/percona-docstreamer/internal/topo"
	"github.com/Percona-Lab/percona-docstreamer/internal/validator"
)

// Declare the version variable.
// The Makefile will override this value during build.
var version = "1"

// --- Helper function for password prompt ---
func getPassword(prompt string) (string, error) {
	fmt.Print(prompt)
	bytePassword, err := term.ReadPassword(int(syscall.Stdin))
	if err != nil {
		return "", err
	}
	fmt.Println()
	return strings.TrimSpace(string(bytePassword)), nil
}

// --- Helper function for 'yes' confirmation ---
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
		// Do not run setup logic for the help command
		if cmd.Name() == "help" {
			return
		}
		config.LoadConfig()

		// Check for debug flag and override config if set
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

// startAction contains the core logic for starting the migration.
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
	// --- Conditionally apply InsecureSkipVerify ---
	// If TLS is ON and we need to skip hostname validation (e.g. tunneling),
	// we must force the driver to use a custom TLS config.
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
	// --- Conditionally apply InsecureSkipVerify ---
	// If TLS is ON and we need to skip hostname validation (e.g. tunneling),
	// we must force the driver to use a custom TLS config.
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

	var hiddenargs = []string{"run",
		"--docdb-user", docdbUser,
		"--mongo-user", mongoUser,
		"--docdb-pass", docdbPass,
		"--mongo-pass", mongoPass,
	}
	if config.Cfg.Migration.Destroy {
		hiddenargs = append(hiddenargs, "--destroy")
	}

	// Pass debug flag to the child process if it was set
	debug, _ := cmd.Flags().GetBool("debug")
	if debug {
		hiddenargs = append(hiddenargs, "--debug")
	}

	runCmd := exec.Command(executable, hiddenargs...)
	runCmd.SysProcAttr = &syscall.SysProcAttr{
		Setsid: true, // Detach
	}
	if err := runCmd.Start(); err != nil {
		logging.PrintError(fmt.Sprintf("Failed to launch background process: %v", err), 0)
		os.Exit(1)
	}
	logging.PrintSuccess(fmt.Sprintf("Application started in background with PID: %d", runCmd.Process.Pid), 0)

	// --- Zombie & Exit Detection ---
	// Create a channel that closes when the child process exits.
	processExitChan := make(chan struct{})
	go func() {
		// Wait ensures the zombie is removed if it exits while we are still parent.
		runCmd.Wait()
		close(processExitChan)
	}()

	logFile, err := os.Open(config.Cfg.Logging.FilePath)
	var initialOffset int64 = 0
	if err == nil {
		initialOffset, _ = logFile.Seek(0, 2) // Go to end of existing logs
		logFile.Close()
	}

	// Monitor via API and Logs
	monitorStartup(config.Cfg.Logging.FilePath, initialOffset, runCmd.Process.Pid, config.Cfg.Migration.StatusHTTPPort, processExitChan)
}

// stopAction encapsulates the logic to signal the app to stop and tail the logs until it exits.
func stopAction(phaseTitle string) error {
	logging.PrintPhase("6", phaseTitle)

	pidVal, err := pid.Read()
	if err != nil {
		return fmt.Errorf("could not read PID file. Is the application running")
	}

	// 1. Open log file before stopping to catch shutdown logs
	logFile, err := os.Open(config.Cfg.Logging.FilePath)
	if err == nil {
		logFile.Seek(0, 2)
	}
	defer func() {
		if logFile != nil {
			logFile.Close()
		}
	}()

	// 2. Send Stop Signal
	if err := pid.Stop(); err != nil {
		return err
	}
	logging.PrintSuccess("Stop signal sent.", 0)

	// 3. Tail logs until exit
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
				// Drain remainder
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
		// 1. Check if running and Stop
		pidVal, err := pid.Read()
		if err == nil && pid.IsRunning(pidVal) {
			if err := stopAction("STOPPING FOR RESTART"); err != nil {
				logging.PrintError(fmt.Sprintf("Failed to stop application: %v", err), 0)
				os.Exit(1)
			}
			logging.PrintSuccess("Application stopped.", 0)
			// Brief pause to ensure OS releases resources
			time.Sleep(1 * time.Second)
		} else {
			logging.PrintInfo("Application is not running. Proceeding to start.", 0)
		}

		// 2. Start
		startAction(cmd, args)
	},
}

// monitorStartup tails logs AND polls the status endpoint for readiness
func monitorStartup(logPath string, offset int64, pidVal int, port string, exitChan chan struct{}) {
	if port == "" {
		port = "8080"
	}
	statusURL := fmt.Sprintf("http://localhost:%s/status", port)

	// Give the app a moment to create/write to the file
	time.Sleep(500 * time.Millisecond)

	file, err := os.Open(logPath)
	if err != nil {
		fmt.Printf("Warning: Could not read log file: %v\n", err)
		return
	}
	defer file.Close()
	file.Seek(offset, 0)
	reader := bufio.NewReader(file)

	// Polling ticker for HTTP check
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		// --- Check if process has exited ---
		select {
		case <-exitChan:
			// Process has died or exited. Read final logs and quit.
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

		// 1. Read and print new logs
		for {
			line, err := reader.ReadString('\n')
			if err == nil {
				fmt.Print(line)
			} else {
				break // End of current logs
			}
		}

		// 2. Check if process crashed (PID check fallback)
		if !pid.IsRunning(pidVal) {
			logging.PrintError(("Process exited unexpectedly. Check logs for details."), 0)
			return
		}

		// 3. Poll HTTP Status
		select {
		case <-ticker.C:
			resp, err := http.Get(statusURL)
			if err == nil {
				defer resp.Body.Close()
				var s status.StatusOutput
				if json.NewDecoder(resp.Body).Decode(&s) == nil {
					// Check for healthy states
					if s.OK && s.State == "running" {
						logging.PrintSuccess(fmt.Sprintf("Application is healthy (State: %s).", s.State), 0)
						return
					}
					// Check for error state
					if s.State == "error" {
						logging.PrintError(fmt.Sprintf("Application reported an error: %s", s.Info), 0)
						os.Exit(1)
					}
				}
			}
		default:
			// Don't block waiting for ticker
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
		fmt.Println("--- docStreamer Status (Live) ---")
		fmt.Printf("PID: %d (Querying %s)\n", pidVal, url)
		fmt.Println(string(body))
	},
}

// Internal run command (hidden)
var runCmd = &cobra.Command{
	Use:    "run",
	Hidden: true,
	Run: func(cmd *cobra.Command, args []string) {
		runMigrationProcess(cmd, args)
	},
}

// Run the migration
func runMigrationProcess(cmd *cobra.Command, args []string) {
	if err := pid.Write(); err != nil {
		logging.PrintError(fmt.Sprintf("Failed to write PID file: %v", err), 0)
		os.Exit(1)
	}
	defer pid.Clear()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var statusManager *status.Manager
	// We need to declare apiServer here so the signal handler closure can access it
	var apiServer *api.Server

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		logging.PrintWarning(fmt.Sprintf("Received signal: %v. Initiating graceful shutdown...", sig), 0)
		logging.PrintInfo("!!! PLEASE WAIT: Flushing final CDC batches to destination...", 0)
		logging.PrintInfo("!!! DO NOT FORCE QUIT (Ctrl+C), or data may be lost.", 0)

		if statusManager != nil {
			statusManager.SetState("stopping", "Flushing pending events... Please wait.")
			statusManager.Persist(context.Background())
		}

		// --- SHUTDOWN WATCHDOG ---
		// If the app is still running after 15 seconds, force exit to avoid hanging forever.
		go func() {
			time.Sleep(15 * time.Second)
			logging.PrintError("Shutdown timed out. Forcing exit.", 0)
			pid.Clear()
			os.Exit(1)
		}()

		// Gracefully stop the new API server WITH TIMEOUT
		if apiServer != nil {
			stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer stopCancel()
			apiServer.Stop(stopCtx)
		}

		cancel()
	}()

	docdbUser, _ := cmd.Flags().GetString("docdb-user")
	mongoUser, _ := cmd.Flags().GetString("mongo-user")
	docdbPass, _ := cmd.Flags().GetString("docdb-pass")
	mongoPass, _ := cmd.Flags().GetString("mongo-pass")
	destroy, _ := cmd.Flags().GetBool("destroy")

	docdbURI := config.Cfg.BuildDocDBURI(docdbUser, docdbPass)
	mongoURI := config.Cfg.BuildMongoURI(mongoUser, mongoPass)

	// --- Use conditional TLS logic for Source (DocumentDB) ---
	clientOpts := options.Client().ApplyURI(docdbURI)
	if config.Cfg.DocDB.TLS && config.Cfg.DocDB.TlsAllowInvalidHostnames {
		clientOpts.SetTLSConfig(&tls.Config{InsecureSkipVerify: true})
	}
	sourceClient, err := mongo.Connect(clientOpts)

	if err != nil {
		logging.PrintError(err.Error(), 0)
		return
	}
	// --- Disconnect with timeout ---
	defer func() {
		dCtx, dCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer dCancel()
		if err := sourceClient.Disconnect(dCtx); err != nil {
			logging.PrintWarning(fmt.Sprintf("Source disconnect warning: %v", err), 0)
		}
	}()

	// --- Use conditional TLS logic for Target (MongoDB) ---
	mongoClientOpts := options.Client().ApplyURI(mongoURI)
	if config.Cfg.Mongo.TLS && config.Cfg.Mongo.TlsAllowInvalidHostnames {
		mongoClientOpts.SetTLSConfig(&tls.Config{InsecureSkipVerify: true})
	}
	targetClient, err := mongo.Connect(mongoClientOpts)

	if err != nil {
		logging.PrintError(err.Error(), 0)
		return
	}
	// --- Disconnect with timeout ---
	defer func() {
		dCtx, dCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer dCancel()
		if err := targetClient.Disconnect(dCtx); err != nil {
			logging.PrintWarning(fmt.Sprintf("Target disconnect warning: %v", err), 0)
		}
	}()

	// --- 1. CHECKPOINT MANAGER & CLEANUP ---
	checkpointManager := checkpoint.NewManager(targetClient)
	resumeAt, found := checkpointManager.GetResumeTimestamp(ctx, config.Cfg.Migration.CheckpointDocID)

	// Declared outside to prevent shadowing and persist discovery data if destroy is True
	var collectionsToMigrate []discover.CollectionInfo

	if destroy {
		logging.PrintPhase("3", "DISCOVERY (for Destroy)")
		collectionsToMigrate, err = discover.DiscoverCollections(ctx, sourceClient)
		if err != nil {
			logging.PrintError(err.Error(), 0)
			return
		}
		if ctx.Err() != nil {
			return
		}
		dbsFromSource := extractDBNames(collectionsToMigrate)
		logging.PrintPhase("DESTROY", "Dropping target databases...")
		dbops.DropAllDatabases(ctx, targetClient, dbsFromSource)

		// Explicitly drop metadata DB here to prevent partial resume detection later
		logging.PrintPhase("DESTROY", "Dropping metadata database...")
		if err := targetClient.Database(config.Cfg.Migration.MetadataDB).Drop(ctx); err != nil {
			logging.PrintWarning(fmt.Sprintf("Failed to drop metadata DB during destroy: %v", err), 0)
		}

		found = false
	}

	// --- Cleanup or Partial Resume ---
	var startAt bson.Timestamp
	var anchorFound bool

	if !found {
		// We don't have a global checkpoint (completed full load).
		// Check if we have an Anchor (partial full load).
		startAt, anchorFound = checkpointManager.GetAnchorTimestamp(ctx)

		if anchorFound {
			logging.PrintInfo(fmt.Sprintf("Found partial migration state (Anchor T0: %v). Resuming Full Load...", startAt), 0)
		} else {
			logging.PrintInfo("No valid global checkpoint or anchor found. Starting fresh.", 0)
			logging.PrintInfo("Cleaning up stale metadata (Status, Checkpoints, Validation)...", 0)

			// Drop the entire metadata database to ensure a clean slate
			err := targetClient.Database(config.Cfg.Migration.MetadataDB).Drop(ctx)
			if err != nil {
				logging.PrintWarning(fmt.Sprintf("Failed to drop metadata DB: %v", err), 0)
			} else {
				logging.PrintSuccess("Metadata cleanup complete.", 0)
			}
		}
	}

	// --- 2. Initialize Shared Components ---
	tracker := validator.NewInFlightTracker()
	valStore := validator.NewStore(targetClient)

	apiServer = api.NewServer(config.Cfg.Migration.StatusHTTPPort)
	statusManager = status.NewManager(targetClient, false)

	validationManager := validator.NewManager(sourceClient, targetClient, tracker, valStore, statusManager)
	defer validationManager.Close()

	// --- 3. Register API Routes ---
	apiServer.RegisterRoute("/status", statusManager.StatusHandler)
	apiServer.RegisterRoute("/validate", validationManager.HandleValidateRequest)
	apiServer.RegisterRoute("/validate/adhoc", validationManager.HandleAdHocValidation)
	apiServer.RegisterRoute("/validate/retry", validationManager.HandleRetryFailures)
	apiServer.RegisterRoute("/validate/stats", validationManager.HandleGetStats)
	apiServer.RegisterRoute("/validate/reset", validationManager.HandleReset)

	apiServer.Start()

	statusManager.SetState("connecting", "Connections established. Pinging...")

	if err = sourceClient.Ping(ctx, readpref.Primary()); err != nil {
		statusManager.SetError(err.Error())
		logging.PrintError(err.Error(), 0)
		return
	}
	if err = targetClient.Ping(ctx, readpref.Primary()); err != nil {
		statusManager.SetError(err.Error())
		logging.PrintError(err.Error(), 0)
		return
	}

	// --- 4. EXECUTION LOGIC ---

	if !found {
		// Phase 1: DISCOVERY
		if !anchorFound {
			t0, err := topo.ClusterTime(ctx, sourceClient)
			if err != nil {
				statusManager.SetError(err.Error())
				logging.PrintError(err.Error(), 0)
				return
			}
			startAt = t0
			logging.PrintInfo(fmt.Sprintf("Captured global T0 (Pre-Discovery): %v", startAt), 0)

			// Save Anchor immediately to enable resuming later
			checkpointManager.SaveAnchorTimestamp(ctx, startAt)
		}

		logging.PrintPhase("3", "DISCOVERY")
		statusManager.SetState("discovering", "Discovering collections to migrate...")
		if !destroy {
			collectionsToMigrate, err = discover.DiscoverCollections(ctx, sourceClient)
			if err != nil {
				statusManager.SetError(err.Error())
				logging.PrintError(err.Error(), 0)
				return
			}
		}

		if ctx.Err() != nil {
			logging.PrintWarning("Migration stopped during discovery.", 0)
			return
		}

		// Phase 2: FULL DATA LOAD
		if len(collectionsToMigrate) > 0 {
			completedColls, err := checkpointManager.GetCompletedCollections(ctx)
			if err != nil {
				logging.PrintWarning(fmt.Sprintf("Failed to fetch completed checkpoints: %v", err), 0)
			}

			var toRun []discover.CollectionInfo
			for _, coll := range collectionsToMigrate {
				if completedColls[coll.Namespace] {
					logging.PrintInfo(fmt.Sprintf("Skipping already copied collection: %s", coll.Namespace), 0)
				} else {
					toRun = append(toRun, coll)
					statusManager.AddEstimatedBytes(coll.Size)
				}
			}

			if len(toRun) > 0 {
				logging.PrintPhase("4", "FULL DATA LOAD")
				statusManager.SetState("running", "Initial Sync (Full Load)")

				_, err = launchFullLoadWorkers(ctx, sourceClient, targetClient, toRun, statusManager, checkpointManager)
				if err != nil {
					if err != context.Canceled {
						statusManager.SetError(err.Error())
						logging.PrintError(err.Error(), 0)
					}
					return
				}

				if ctx.Err() != nil {
					logging.PrintWarning("Migration stopped during Full Load.", 0)
					return
				}
				logging.PrintSuccess("All full load workers complete.", 0)
			} else {
				logging.PrintPhase("4", "FULL DATA LOAD (SKIPPED - ALL DONE)")
			}
		}

		// --- SAVE RESUME TIMESTAMP (T0) ---
		checkpointManager.SaveResumeTimestamp(ctx, config.Cfg.Migration.CheckpointDocID, startAt)
		logging.PrintInfo(fmt.Sprintf("Saved global resume timestamp: %v", startAt), 0)

		checkpointManager.DeleteAnchorTimestamp(ctx)

		statusManager.SetCloneCompleted()

		// Calculate lag for status
		now := bson.Timestamp{T: uint32(time.Now().Unix()), I: 0}
		lag := int64(now.T) - int64(startAt.T)
		if lag < 0 {
			lag = 0
		}
		statusManager.SetInitialSyncCompleted(lag)
		statusManager.Persist(ctx)

	} else {
		logging.PrintPhase("3", "DISCOVERY (SKIPPED)")
		logging.PrintPhase("4", "FULL DATA LOAD (SKIPPED)")

		// --- Load Status and Validate Integrity ---
		if err := statusManager.LoadAndMerge(ctx); err != nil {
			logging.PrintWarning(fmt.Sprintf("Failed to load previous status: %v", err), 0)
		}

		// If we have a global checkpoint (Full Load Done),
		// but status says otherwise, warn the user.
		if !statusManager.IsCloneCompleted() {
			logging.PrintWarning("Integrity Check: Global Checkpoint exists (Full Load Done), but Status metadata indicates incomplete.", 0)
			logging.PrintWarning("This implies a previous crash during the final commit phase.", 0)
			logging.PrintInfo("Proceeding with CDC based on Global Checkpoint (fail-open).", 0)
		}

		startAt = resumeAt
		statusManager.SetCloneCompleted()
	}

	if ctx.Err() != nil {
		logging.PrintWarning("Migration stopped before CDC start.", 0)
		return
	}

	logging.PrintPhase("5", "CONTINUOUS SYNC (CDC)")
	statusManager.SetState("running", "Change Data Capture")

	cdcManager := cdc.NewManager(
		sourceClient,
		targetClient,
		config.Cfg.Migration.CheckpointDocID,
		startAt,
		checkpointManager,
		statusManager,
		tracker,
		valStore,
		validationManager,
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

func launchFullLoadWorkers(ctx context.Context, source, target *mongo.Client, collections []discover.CollectionInfo, statusMgr *status.Manager, checkpointMgr *checkpoint.Manager) (bson.Timestamp, error) {
	jobs := make(chan discover.CollectionInfo, len(collections))
	for _, c := range collections {
		jobs <- c
	}
	close(jobs)
	resultsChan := make(chan error, len(collections))
	var wg sync.WaitGroup
	workerCount := config.Cfg.Migration.MaxConcurrentWorkers
	logging.PrintStep(fmt.Sprintf("Starting collection worker pool with %d concurrent workers...", workerCount), 0)
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for collInfo := range jobs {
				if ctx.Err() != nil {
					return
				}
				ns := collInfo.Namespace
				logging.PrintStep(fmt.Sprintf("[Worker %d] Starting full load for %s", workerID, ns), 0)
				copier := cloner.NewCopyManager(source, target, collInfo, statusMgr, checkpointMgr, config.Cfg.Migration.CheckpointDocID)
				docCount, _, err := copier.Do(ctx)
				start := time.Now()
				logging.LogFullLoadOp(start, ns, docCount, err)
				resultsChan <- err
				if err != nil {
					logging.PrintError(fmt.Sprintf("[%s] Full load FAILED: %v", ns, err), 0)
				} else {
					logging.PrintSuccess(fmt.Sprintf("[%s] Full load COMPLETED: %d docs", ns, docCount), 0)
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
