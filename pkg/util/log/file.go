// Copyright 2013 Google Inc. All Rights Reserved.
//
// Go support for leveled logs, analogous to https://code.google.com/p/google-clog/
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// File I/O for logs.

// Author: Bram Gruneir (bram@cockroachlabs.com)

package log

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/pkg/errors"
)

// MaxSize is the maximum size of a log file in bytes.
var MaxSize uint64 = 1024 * 1024 * 10

// If non-empty, overrides the choice of directory in which to write logs. See
// createLogDirs for the full list of possible destinations. Note that the
// default is to log to stderr independent of this setting. See --logtostderr.

type logDirName struct {
	syncutil.Mutex
	name string
}

var _ flag.Value = &logDirName{}

var logDir logDirName

// Set implements the flag.Value interface.
func (l *logDirName) Set(dir string) error {
	l.Lock()
	defer l.Unlock()
	l.name = dir
	return nil
}

// Type implements the flag.Value interface.
func (l *logDirName) Type() string {
	return "string"
}

// String implements the flag.Value interface.
func (l *logDirName) String() string {
	l.Lock()
	defer l.Unlock()
	return l.name
}

func (l *logDirName) clear() {
	// For testing only.
	l.Lock()
	defer l.Unlock()
	l.name = ""
}

func (l *logDirName) get() (string, error) {
	l.Lock()
	defer l.Unlock()
	if len(l.name) == 0 {
		return "", errDirectoryNotSet
	}
	return l.name, nil
}

func (l *logDirName) isSet() bool {
	l.Lock()
	res := l.name != ""
	l.Unlock()
	return res
}

// DirSet returns true of the log directory has been changed from its default.
func DirSet() bool { return logDir.isSet() }

// logFileRE matches log files to avoid exposing non-log files accidentally
// and it splits the details of the filename into groups for easy parsing.
// The log file format is {process}.{host}.{username}.{timestamp}.{pid}.{severity}.log
// cockroach.Brams-MacBook-Pro.bram.2015-06-09T16-10-48Z.30209.WARNING.log
// All underscore in process, host and username are escaped to double
// underscores and all periods are escaped to an underscore.
// For compatibility with Windows filenames, all colons from the timestamp
// (RFC3339) are converted from underscores.
var logFileRE = regexp.MustCompile(`^(?:.*/)?([^/.]+)\.([^/\.]+)\.([^/\.]+)\.([^/\.]+)\.(\d+)\.(ERROR|WARNING|INFO)\.log$`)

var (
	pid      = os.Getpid()
	program  = filepath.Base(os.Args[0])
	host     = "unknownhost"
	userName = "unknownuser"
)

func init() {
	h, err := os.Hostname()
	if err == nil {
		host = shortHostname(h)
	}

	current, err := user.Current()
	if err == nil {
		userName = current.Username
	}

	// Sanitize userName since it may contain filepath separators on Windows.
	userName = strings.Replace(userName, `\`, "_", -1)
}

// shortHostname returns its argument, truncating at the first period.
// For instance, given "www.google.com" it returns "www".
func shortHostname(hostname string) string {
	if i := strings.Index(hostname, "."); i >= 0 {
		return hostname[:i]
	}
	return hostname
}

// removePeriods removes all extraneous periods. This is required to ensure that
// the only periods in the filename are the ones added by logName so it can
// be easily parsed.
func removePeriods(s string) string {
	return strings.Replace(s, ".", "", -1)
}

// logName returns a new log file name containing the severity, with start time
// t, and the name for the symlink for the severity.
func logName(severity Severity, t time.Time) (name, link string) {
	// Replace the ':'s in the time format with '_'s to allow for log files in
	// Windows.
	tFormatted := strings.Replace(t.Format(time.RFC3339), ":", "_", -1)

	name = fmt.Sprintf("%s.%s.%s.%s.%06d.%s.log",
		removePeriods(program),
		removePeriods(host),
		removePeriods(userName),
		tFormatted,
		pid,
		severity.Name())
	return name, removePeriods(program) + "." + severity.Name()
}

var errMalformedName = errors.New("malformed log filename")
var errMalformedSev = errors.New("malformed severity")

func parseLogFilename(filename string) (FileDetails, error) {
	matches := logFileRE.FindStringSubmatch(filename)
	if matches == nil || len(matches) != 7 {
		return FileDetails{}, errMalformedName
	}

	// Replace the '_'s with ':'s to restore the correct time format.
	fixTime := strings.Replace(matches[4], "_", ":", -1)
	time, err := time.Parse(time.RFC3339, fixTime)
	if err != nil {
		return FileDetails{}, err
	}

	pid, err := strconv.ParseInt(matches[5], 10, 0)
	if err != nil {
		return FileDetails{}, err
	}

	sev, sevFound := SeverityByName(matches[6])
	if !sevFound {
		return FileDetails{}, errMalformedSev
	}

	return FileDetails{
		Program:  matches[1],
		Host:     matches[2],
		UserName: matches[3],
		Severity: sev,
		Time:     time.UnixNano(),
		PID:      pid,
	}, nil
}

var errDirectoryNotSet = errors.New("log: log directory not set")

// create creates a new log file and returns the file and its filename, which
// contains severity ("INFO", "FATAL", etc.) and t. If the file is created
// successfully, create also attempts to update the symlink for that tag, ignoring
// errors.
func create(
	severity Severity, t time.Time, lastRotation int64,
) (f *os.File, updatedRotation int64, filename string, err error) {
	dir, err := logDir.get()
	if err != nil {
		return nil, lastRotation, "", err
	}

	// Ensure that the timestamp of the new file name is greater than
	// the timestamp of the previous generated file name.
	unix := t.Unix()
	if unix <= lastRotation {
		unix = lastRotation + 1
	}
	updatedRotation = unix
	t = time.Unix(unix, 0)

	// Generate the file name.
	name, link := logName(severity, t)
	fname := filepath.Join(dir, name)
	// Open the file os.O_APPEND|os.O_CREATE rather than use os.Create.
	// Append is almost always more efficient than O_RDRW on most modern file systems.
	f, err = os.OpenFile(fname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0664)
	if err == nil {
		symlink := filepath.Join(dir, link)
		_ = os.Remove(symlink) // ignore err
		err = os.Symlink(fname, symlink)
	}
	return f, updatedRotation, fname, errors.Wrapf(err, "log: cannot create log")
}

var errNotAFile = errors.New("not a regular file")

// getFileDetails verifies that the file specified by filename is a
// regular file and filename matches the expected filename pattern.
// Returns the log file details success; otherwise error.
func getFileDetails(info os.FileInfo) (FileDetails, error) {
	if info.Mode()&os.ModeType != 0 {
		return FileDetails{}, errNotAFile
	}

	details, err := parseLogFilename(info.Name())
	if err != nil {
		return FileDetails{}, err
	}

	return details, nil
}

func verifyFile(filename string) error {
	info, err := os.Stat(filename)
	if err != nil {
		return errors.Errorf("stat: %s: %v", filename, err)
	}
	_, err = getFileDetails(info)
	return err
}

// ListLogFiles returns a slice of FileInfo structs for each log file
// on the local node, in any of the configured log directories.
func ListLogFiles() ([]FileInfo, error) {
	var results []FileInfo
	dir, err := logDir.get()
	if err != nil {
		// No log directory configured: simply indicate that there are no
		// log files.
		return nil, nil
	}
	infos, err := ioutil.ReadDir(dir)
	if err != nil {
		return results, err
	}
	for _, info := range infos {
		details, err := getFileDetails(info)
		if err == nil {
			results = append(results, FileInfo{
				Name:         info.Name(),
				SizeBytes:    info.Size(),
				ModTimeNanos: info.ModTime().UnixNano(),
				Details:      details,
			})
		}
	}
	return results, nil
}

// GetLogReader returns a reader for the specified filename. In
// restricted mode, the filename must be the base name of a file in
// this process's log directory (this is safe for cases when the
// filename comes from external sources, such as the admin UI via
// HTTP). In unrestricted mode any path is allowed, relative to the
// current directory, with the added feature that simple (base name)
// file names will be searched in this process's log directory if not
// found in the current directory.
func GetLogReader(filename string, restricted bool) (io.ReadCloser, error) {
	dir, err := logDir.get()
	if err != nil {
		return nil, err
	}

	switch restricted {
	case true:
		// Verify there are no path separators in a restricted-mode pathname.
		if filepath.Base(filename) != filename {
			return nil, errors.Errorf("pathnames must be basenames only: %s", filename)
		}
		filename = filepath.Join(dir, filename)

	case false:
		if !filepath.IsAbs(filename) {
			exists, err := fileExists(filename)
			if err != nil {
				return nil, err
			}
			if !exists && filepath.Base(filename) == filename {
				// If the file name is not absolute and is simple and not
				// found in the cwd, try to find the file first relative to
				// the log directory.
				fileNameAttempt := filepath.Join(dir, filename)
				exists, err = fileExists(fileNameAttempt)
				if err != nil {
					return nil, err
				}
				if !exists {
					return nil, errors.Errorf("no such file %s either in current directory or in %s", filename, dir)
				}
				filename = fileNameAttempt
			}
		}

		// Normalize the name to an absolute path without symlinks.
		filename, err = filepath.EvalSymlinks(filename)
		if err != nil {
			return nil, errors.Errorf("EvalSymLinks: %s: %v", filename, err)
		}
	}

	// Check the file name is valid.
	if err := verifyFile(filename); err != nil {
		return nil, err
	}

	return os.Open(filename)
}

func fileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

// sortableFileInfoSlice is required so we can sort FileInfos.
type sortableFileInfoSlice []FileInfo

func (a sortableFileInfoSlice) Len() int      { return len(a) }
func (a sortableFileInfoSlice) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a sortableFileInfoSlice) Less(i, j int) bool {
	return a[i].Details.Time < a[j].Details.Time
}

// selectFiles selects all log files that have an timestamp before the endTime and
// the correct severity. It then sorts them in decreasing order, with the most
// recent as the first one.
func selectFiles(logFiles []FileInfo, severity Severity, endTimestamp int64) []FileInfo {
	files := sortableFileInfoSlice{}
	for _, logFile := range logFiles {
		if logFile.Details.Severity == severity && logFile.Details.Time <= endTimestamp {
			files = append(files, logFile)
		}
	}

	// Sort the files in reverse order so we will fetch the newest first.
	sort.Sort(sort.Reverse(files))
	return files
}

// FetchEntriesFromFiles fetches all available log entries on disk that match
// the log 'severity' (or worse) and are between the 'startTimestamp' and
// 'endTimestamp'. It will stop reading new files if the number of entries
// exceeds 'maxEntries'. Log entries are further filtered by the regexp
// 'pattern' if provided. The logs entries are returned in reverse chronological
// order.
func FetchEntriesFromFiles(
	severity Severity, startTimestamp, endTimestamp int64, maxEntries int, pattern *regexp.Regexp,
) ([]Entry, error) {
	logFiles, err := ListLogFiles()
	if err != nil {
		return nil, err
	}

	selectedFiles := selectFiles(logFiles, severity, endTimestamp)

	entries := []Entry{}
	for _, file := range selectedFiles {
		newEntries, entryBeforeStart, err := readAllEntriesFromFile(
			file,
			startTimestamp,
			endTimestamp,
			maxEntries-len(entries),
			pattern)
		if err != nil {
			return nil, err
		}
		entries = append(entries, newEntries...)
		if len(entries) >= maxEntries {
			break
		}
		if entryBeforeStart {
			// Stop processing files that won't have any timestamps after
			// startTime.
			break
		}
	}
	return entries, nil
}

// readAllEntriesFromFile reads in all log entries from a given file that are
// between the 'startTimestamp' and 'endTimestamp' and match the 'pattern' if it
// exists. It returns the entries in the reverse chronological order. It also
// returns a flag that denotes if any timestamp occurred before the
// 'startTimestamp' to inform the caller that no more log files need to be
// processed. If the number of entries returned exceeds 'maxEntries' then
// processing of new entries is stopped immediately.
func readAllEntriesFromFile(
	file FileInfo, startTimestamp, endTimestamp int64, maxEntries int, pattern *regexp.Regexp,
) ([]Entry, bool, error) {
	reader, err := GetLogReader(file.Name, true /* restricted */)
	if reader == nil || err != nil {
		return nil, false, err
	}
	defer reader.Close()
	entries := []Entry{}
	decoder := NewEntryDecoder(reader)
	entryBeforeStart := false
	for {
		entry := Entry{}
		if err := decoder.Decode(&entry); err != nil {
			if err == io.EOF {
				break
			}
			return nil, false, err
		}
		var match bool
		if pattern == nil {
			match = true
		} else {
			match = pattern.MatchString(entry.Message) ||
				pattern.MatchString(entry.File)
		}
		if match && entry.Time >= startTimestamp && entry.Time <= endTimestamp {
			entries = append([]Entry{entry}, entries...)
			if len(entries) >= maxEntries {
				break
			}
		}
		if entry.Time < startTimestamp {
			entryBeforeStart = true
		}

	}
	return entries, entryBeforeStart, nil
}
