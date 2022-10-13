package backup

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/codeskyblue/go-sh"
	"github.com/dustin/go-humanize"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/stefanprodan/mgob/pkg/config"
)

var mongodbDatabaseListTimeout = 10 * time.Minute

type dumpConfig struct {
	plan        config.Plan
	conf        *config.AppConfig
	tmpPath     string
	storagePath string
	planDir     string
	ts          time.Time
	name        string
	database    string
}

func Run(plan config.Plan, conf *config.AppConfig, modules *config.ModuleConfig) (Result, error) {
	c := &dumpConfig{
		plan:        plan,
		database:    plan.Target.Database,
		conf:        conf,
		tmpPath:     conf.TmpPath,
		storagePath: conf.StoragePath,
		ts:          time.Now(),
		planDir:     fmt.Sprintf("%v/%v", conf.StoragePath, plan.Name),
		name:        plan.Name,
	}
	switch plan.Mode {
	case config.BackupModeDatabase:
		return runDumpPerDBAndUpload(c)
	case "", config.BackupModeSingle:
		if len(c.plan.Target.ExcludeDatabases) != 0 {
			return errRes(c), fmt.Errorf("cannot exclude databases with '%s' (default) backup mode", config.BackupModeSingle)
		}
		return runDumpAndUpload(c)
	default:
		return errRes(c), fmt.Errorf("unknown mode: '%s'", plan.Mode)
	}
}

func errRes(c *dumpConfig) Result {
	return Result{
		Plan:      c.plan.Name,
		Timestamp: c.ts.UTC(),
		Status:    500,
	}
}

func getDBNames(c *dumpConfig) ([]string, error) {
	mdbCtx, cancel := context.WithTimeout(context.Background(), mongodbDatabaseListTimeout)
	defer cancel()

	log.WithField("plan", c.plan.Name).Info("Listing MonogoDB databases: connecting")
	client, err := mongo.Connect(mdbCtx, options.Client().ApplyURI(c.plan.Target.Uri))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %s", err)
	}
	log.WithField("plan", c.plan.Name).Info("Listing MonogoDB databases: connected")
	defer client.Disconnect(context.Background())
	dbNames, err := client.ListDatabaseNames(mdbCtx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("failed to list databases: %s", err)
	}
	log.WithField("plan", c.plan.Name).Info("Listing MonogoDB databases: %d databases", len(dbNames))
	return dbNames, nil
}

func runDumpPerDBAndUpload(c *dumpConfig) (Result, error) {
	if c.plan.Target.Uri == "" {
		return errRes(c), fmt.Errorf("must use MongoDB URI with '%s' backup mode", c.plan.Mode)
	}

	dbNames, err := getDBNames(c)
	if err != nil {
		return errRes(c), err
	}

	attempts := 0
	totalSize := int64(0)
	failedDBs := make([]string, 0)
dbLoop:
	for _, dbName := range dbNames {
		for _, excluded := range c.plan.Target.ExcludeDatabases {
			if dbName == excluded {
				continue dbLoop
			}
			log.WithField("plan", c.name).Infof("Excluded backup of DB '%s'", dbName)
		}
		attempts++
		dc := *c
		dc.database = dbName
		dc.name = fmt.Sprintf("%s-%s", dc.plan.Name, dbName)
		res, err := runDumpAndUpload(c)
		if err != nil {
			log.WithField("plan", c.name).Errorf("Backup failed: %s", err)
			failedDBs = append(failedDBs, dbName)
		} else {
			totalSize += res.Size
		}
	}
	res := errRes(c)
	res.Duration = time.Since(c.ts)
	if len(failedDBs) > 0 {
		return res, fmt.Errorf("%d of %d database backups failed: %s", len(failedDBs), attempts, strings.Join(failedDBs, ","))
	}
	res.Status = 200
	res.Size = totalSize
	return res, nil
}

func runDumpAndUpload(c *dumpConfig) (Result, error) {
	archive, mlog, err := dump(c)
	log.WithFields(log.Fields{
		"archive": archive,
		"mlog":    mlog,
		"planDir": c.planDir,
		"err":     err,
	}).Info("new dump")

	res := errRes(c)
	_, res.Name = filepath.Split(archive)

	if err != nil {
		return res, err
	}

	err = sh.Command("mkdir", "-p", c.planDir).Run()
	if err != nil {
		return res, errors.Wrapf(err, "creating dir %v in %v failed", c.name, c.storagePath)
	}

	fi, err := os.Stat(archive)
	if err != nil {
		return res, errors.Wrapf(err, "stat file %v failed", archive)
	}
	res.Size = fi.Size()

	err = sh.Command("mv", archive, c.planDir).Run()
	if err != nil {
		return res, errors.Wrapf(err, "moving file from %v to %v failed", archive, c.planDir)
	}

	// check if log file exists, is not always created
	if _, err := os.Stat(mlog); os.IsNotExist(err) {
		log.Debug("appears no log file was generated")
	} else {
		err = sh.Command("mv", mlog, c.planDir).Run()
		if err != nil {
			return res, errors.Wrapf(err, "moving file from %v to %v failed", mlog, c.planDir)
		}
	}

	if c.plan.Scheduler.Retention > 0 {
		err = applyRetention(c.planDir, c.plan.Scheduler.Retention)
		if err != nil {
			return res, errors.Wrap(err, "retention job failed")
		}
	}

	file := filepath.Join(c.planDir, res.Name)

	if c.plan.Encryption != nil {
		encryptedFile := fmt.Sprintf("%v.encrypted", file)
		output, err := encrypt(file, encryptedFile, c.plan, c.conf)
		if err != nil {
			return res, err
		} else {
			removeUnencrypted(file, encryptedFile)
			file = encryptedFile
			log.WithField("plan", c.name).Infof("Encryption finished %v", output)
		}
	}

	if c.plan.SFTP != nil {
		sftpOutput, err := sftpUpload(file, c.plan)
		if err != nil {
			return res, err
		} else {
			log.WithField("plan", c.name).Info(sftpOutput)
		}
	}

	if c.plan.S3 != nil {
		s3Output, err := s3Upload(file, c.plan, c.ts, c.conf.UseAwsCli)
		if err != nil {
			return res, err
		} else {
			log.WithField("plan", c.name).Infof("S3 upload finished %v", s3Output)
		}
	}

	if c.plan.GCloud != nil {
		gCloudOutput, err := gCloudUpload(file, c.plan)
		if err != nil {
			return res, err
		} else {
			log.WithField("plan", c.name).Infof("GCloud upload finished %v", gCloudOutput)
		}
	}

	if c.plan.Azure != nil {
		azureOutput, err := azureUpload(file, c.plan)
		if err != nil {
			return res, err
		} else {
			log.WithField("plan", c.name).Infof("Azure upload finished %v", azureOutput)
		}
	}

	if c.plan.Rclone != nil {
		rcloneOutput, err := rcloneUpload(file, c.plan)
		if err != nil {
			return res, err
		} else {
			log.WithField("plan", c.name).Infof("Rclone upload finished %v", rcloneOutput)
		}
	}

	t2 := time.Now()
	res.Status = 200
	res.Duration = t2.Sub(c.ts)
	log.WithFields(log.Fields{
		"plan":     c.name,
		"size":     humanize.Bytes(uint64(res.Size)),
		"archive":  archive,
		"duration": res.Duration.String(),
	}).Infof("dump succeeded")
	return res, nil
}
