package backup

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/codeskyblue/go-sh"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

func dump(c *dumpConfig) (string, string, error) {

	archive := fmt.Sprintf("%v/%v-%v.gz", c.tmpPath, c.name, c.ts.Unix())
	mlog := fmt.Sprintf("%v/%v-%v.log", c.tmpPath, c.name, c.ts.Unix())
	dump := fmt.Sprintf("mongodump --archive=%v --gzip ", archive)

	if c.plan.Target.Uri != "" {
		// using uri (New in version 3.4.6)
		// host/port/username/password are incompatible with uri
		// https://docs.mongodb.com/manual/reference/program/mongodump/#cmdoption-mongodump-uri
		dump += fmt.Sprintf(`--uri "%v" `, c.plan.Target.Uri)
	} else {
		// use older host/port
		dump += fmt.Sprintf("--host %v --port %v ", c.plan.Target.Host, c.plan.Target.Port)

		if c.plan.Target.Username != "" && c.plan.Target.Password != "" {
			dump += fmt.Sprintf(`-u "%v" -p "%v" `, c.plan.Target.Username, c.plan.Target.Password)
		}
	}

	if c.database != "" {
		dump += fmt.Sprintf("--db %v ", c.database)
	}
	if c.plan.Target.Collection != "" {
		dump += fmt.Sprintf("--collection %v ", c.plan.Target.Collection)
	}

	for _, excludeCollection := range c.plan.Target.ExcludeCollections {
		if excludeCollection != "" {
			dump += fmt.Sprintf("--excludeCollection %v ", excludeCollection)
		}
	}

	if c.plan.Target.Params != "" {
		dump += fmt.Sprintf("%v", c.plan.Target.Params)
	}

	// TODO: mask password
	log.Debugf("dump cmd: %v", dump)
	output, err := sh.Command("/bin/sh", "-c", dump).SetTimeout(time.Duration(c.plan.Scheduler.Timeout) * time.Minute).CombinedOutput()
	if err != nil {
		ex := ""
		if len(output) > 0 {
			ex = strings.Replace(string(output), "\n", " ", -1)
		}
		// Try and clean up tmp file after an error
		os.Remove(archive)
		return "", "", errors.Wrapf(err, "mongodump log %v", ex)
	}
	logToFile(mlog, output)

	return archive, mlog, nil
}

func logToFile(file string, data []byte) error {
	if len(data) > 0 {
		err := ioutil.WriteFile(file, data, 0644)
		if err != nil {
			return errors.Wrapf(err, "writing log %v failed", file)
		}
	}

	return nil
}

func applyRetention(path string, retention int) error {
	gz := fmt.Sprintf("cd %v && rm -f $(ls -1t *.gz *.gz.encrypted | tail -n +%v)", path, retention+1)
	err := sh.Command("/bin/sh", "-c", gz).Run()
	if err != nil {
		return errors.Wrapf(err, "removing old gz files from %v failed", path)
	}

	log.Debug("apply retention")
	log := fmt.Sprintf("cd %v && rm -f $(ls -1t *.log | tail -n +%v)", path, retention+1)
	err = sh.Command("/bin/sh", "-c", log).Run()
	if err != nil {
		return errors.Wrapf(err, "removing old log files from %v failed", path)
	}

	return nil
}

// TmpCleanup remove files older than one day
func TmpCleanup(path string) error {
	rm := fmt.Sprintf("find %v -not -name \"mgob.db\" -mtime +%v -type f -delete", path, 1)
	err := sh.Command("/bin/sh", "-c", rm).Run()
	if err != nil {
		return errors.Wrapf(err, "%v cleanup failed", path)
	}

	return nil
}
