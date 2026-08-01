package syncevidence

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

const (
	storeLockName       = ".writer.lock"
	stagingPrefix       = ".stage-"
	quarantinePrefix    = ".quarantine-"
	journalPrefix       = ".journal-"
	dropPrefix          = ".drop-"
	journalReserveBytes = int64(4096)
)

type FileStoreConfig struct {
	Root       string
	QuotaBytes int64
	Retention  time.Duration
	Now        func() time.Time
	Entropy    io.Reader
	LockProof  []byte
}

type FileStore struct {
	mu                    sync.Mutex
	root                  string
	rootFile              *os.File
	lockFile              *os.File
	quota                 int64
	used                  int64
	reserved              int64
	retention             time.Duration
	now                   func() time.Time
	entropy               io.Reader
	lockProof             []byte
	reservations          map[uint64]int64
	nextReserve           uint64
	beforeDropRename      func()
	beforePublishedReopen func(string)
	closed                bool
}

type CaptureReservation struct {
	store *FileStore
	id    uint64
	max   int64
}

type journalRecord struct {
	Operation string `json:"operation"`
	Stage     string `json:"stage"`
	Final     string `json:"final"`
	Tomb      string `json:"tomb"`
}

func OpenFileStore(config FileStoreConfig) (*FileStore, error) {
	if !validFileStoreConfig(config) {
		return nil, ErrUnsafeStore
	}
	rootFile, err := openOrCreateStoreRoot(config.Root)
	if err != nil {
		return nil, err
	}
	return openFileStore(config, rootFile)
}

func openOneShotFileStore(parent *os.File, config FileStoreConfig) (*FileStore, error) {
	if parent == nil || !validFileStoreConfig(config) || verifyDirectoryFD(int(parent.Fd())) != nil {
		return nil, ErrUnsafeStore
	}
	rootFile, err := openOrCreateStoreDirectoryAt(parent, "store", config.Root)
	if err != nil {
		return nil, err
	}
	return openFileStore(config, rootFile)
}

func validFileStoreConfig(config FileStoreConfig) bool {
	return config.Root != "" && filepath.IsAbs(config.Root) && filepath.Clean(config.Root) == config.Root &&
		config.Root != string(filepath.Separator) && config.QuotaBytes > journalReserveBytes &&
		config.Retention >= 0 && (config.LockProof == nil || len(config.LockProof) == 32)
}

func openFileStore(config FileStoreConfig, rootFile *os.File) (*FileStore, error) {
	fail := func(result error) (*FileStore, error) {
		_ = rootFile.Close()
		return nil, result
	}
	lockFD, created, err := openStoreLock(int(rootFile.Fd()))
	if err != nil {
		return fail(err)
	}
	lockFile := os.NewFile(uintptr(lockFD), storeLockName)
	if created {
		err = unix.Fchmod(lockFD, 0o600)
	}
	if err != nil || verifyRegularFD(lockFD, 0o600) != nil {
		_ = lockFile.Close()
		return fail(ErrUnsafeStore)
	}
	if err := unix.Flock(lockFD, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = lockFile.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return fail(ErrStoreLocked)
		}
		return fail(ErrUnsafeStore)
	}
	entropy := config.Entropy
	if entropy == nil {
		entropy = rand.Reader
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	store := &FileStore{
		root: config.Root, rootFile: rootFile, lockFile: lockFile, quota: config.QuotaBytes,
		retention: config.Retention, now: now, entropy: entropy,
		lockProof: append([]byte(nil), config.LockProof...), reservations: make(map[uint64]int64),
	}
	if err := store.verifyLockOwnership(created); err != nil {
		_ = store.Close()
		return nil, err
	}
	if err := store.recoverAndMeasure(); err != nil {
		_ = store.Close()
		return nil, err
	}
	return store, nil
}

func openOrCreateStoreDirectoryAt(parent *os.File, name, label string) (*os.File, error) {
	if parent == nil || name != "store" || verifyDirectoryFD(int(parent.Fd())) != nil {
		return nil, ErrUnsafeStore
	}
	fd, err := unix.Openat(
		int(parent.Fd()),
		name,
		unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW,
		0,
	)
	created := false
	if errors.Is(err, unix.ENOENT) {
		if err := unix.Mkdirat(int(parent.Fd()), name, 0o700); err != nil {
			return nil, ErrUnsafeStore
		}
		created = true
		if err := syncDirectory(parent); err != nil {
			return nil, err
		}
		fd, err = unix.Openat(
			int(parent.Fd()),
			name,
			unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW,
			0,
		)
	}
	if err != nil {
		return nil, ErrUnsafeStore
	}
	if created {
		err = unix.Fchmod(fd, 0o700)
	}
	if err != nil || verifyDirectoryFD(fd) != nil {
		_ = unix.Close(fd)
		return nil, ErrUnsafeStore
	}
	return os.NewFile(uintptr(fd), label), nil
}

func openOrCreateStoreRoot(root string) (*os.File, error) {
	components := strings.Split(strings.TrimPrefix(root, string(filepath.Separator)), string(filepath.Separator))
	if len(components) == 0 {
		return nil, ErrUnsafeStore
	}
	currentFD, err := unix.Open(string(filepath.Separator), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, ErrUnsafeStore
	}
	for index, component := range components {
		if component == "" || component == "." || component == ".." {
			_ = unix.Close(currentFD)
			return nil, ErrUnsafeStore
		}
		last := index == len(components)-1
		nextFD, openErr := unix.Openat(currentFD, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if errors.Is(openErr, unix.ENOENT) && last {
			if err := unix.Mkdirat(currentFD, component, 0o700); err != nil {
				_ = unix.Close(currentFD)
				return nil, ErrUnsafeStore
			}
			parent := os.NewFile(uintptr(currentFD), "store-parent")
			if err := syncDirectory(parent); err != nil {
				_ = parent.Close()
				return nil, err
			}
			currentFD = int(parent.Fd())
			nextFD, openErr = unix.Openat(currentFD, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
			_ = parent.Close()
			currentFD = -1
		}
		if openErr != nil {
			if currentFD >= 0 {
				_ = unix.Close(currentFD)
			}
			return nil, ErrUnsafeStore
		}
		if currentFD >= 0 {
			_ = unix.Close(currentFD)
		}
		currentFD = nextFD
	}
	if err := unix.Fchmod(currentFD, 0o700); err != nil || verifyDirectoryFD(currentFD) != nil {
		_ = unix.Close(currentFD)
		return nil, ErrUnsafeStore
	}
	return os.NewFile(uintptr(currentFD), root), nil
}

func openStoreLock(rootFD int) (int, bool, error) {
	fd, err := unix.Openat(rootFD, storeLockName, unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err == nil {
		if verifyErr := verifyRegularFD(fd, 0o600); verifyErr != nil {
			_ = unix.Close(fd)
			return -1, false, verifyErr
		}
		return fd, false, nil
	}
	if !errors.Is(err, unix.ENOENT) {
		return -1, false, ErrUnsafeStore
	}
	fd, err = unix.Openat(rootFD, storeLockName, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		if errors.Is(err, unix.EEXIST) {
			return -1, false, ErrStoreLocked
		}
		return -1, false, ErrUnsafeStore
	}
	return fd, true, nil
}

func verifyDirectoryFD(fd int) error {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil || stat.Mode&unix.S_IFMT != unix.S_IFDIR ||
		uint32(stat.Mode)&0o777 != 0o700 || stat.Uid != uint32(os.Geteuid()) {
		return ErrUnsafeStore
	}
	return nil
}

func verifyRegularFD(fd int, wantedMode uint32) error {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil || stat.Mode&unix.S_IFMT != unix.S_IFREG ||
		uint64(stat.Nlink) != 1 || uint32(stat.Mode)&0o777 != wantedMode ||
		stat.Uid != uint32(os.Geteuid()) {
		return ErrUnsafeStore
	}
	return nil
}

func (store *FileStore) verifyLockOwnership(created bool) error {
	token := append([]byte(nil), store.lockProof...)
	if len(token) == 0 {
		token = make([]byte, 32)
		if _, err := io.ReadFull(store.entropy, token); err != nil {
			return ErrUnsafeStore
		}
	} else if !created {
		if _, err := store.lockFile.Seek(0, io.SeekStart); err != nil {
			return ErrUnsafeStore
		}
		readBack, err := io.ReadAll(io.LimitReader(store.lockFile, int64(len(token))+1))
		if err != nil || !bytes.Equal(readBack, token) {
			return ErrUnsafeStore
		}
		return nil
	}
	if err := store.lockFile.Truncate(0); err != nil {
		return ErrUnsafeStore
	}
	if _, err := store.lockFile.Seek(0, io.SeekStart); err != nil {
		return ErrUnsafeStore
	}
	if _, err := store.lockFile.Write(token); err != nil {
		return ErrUnsafeStore
	}
	if err := syncFile(store.lockFile); err != nil {
		return err
	}
	if _, err := store.lockFile.Seek(0, io.SeekStart); err != nil {
		return ErrUnsafeStore
	}
	readBack := make([]byte, len(token))
	if _, err := io.ReadFull(store.lockFile, readBack); err != nil || !bytes.Equal(readBack, token) {
		return ErrUnsafeStore
	}
	return nil
}

func (store *FileStore) recoverAndMeasure() error {
	if err := store.recoverJournals(); err != nil {
		return err
	}
	entries, err := store.listEntries()
	if err != nil {
		return err
	}
	changed := false
	for _, entry := range entries {
		name := entry.Name()
		switch {
		case name == storeLockName, strings.HasPrefix(name, quarantinePrefix):
			if err := store.verifyEntry(name); err != nil {
				return err
			}
		case strings.HasPrefix(name, stagingPrefix):
			if err := store.verifyEntry(name); err != nil {
				return err
			}
			if err := unix.Unlinkat(int(store.rootFile.Fd()), name, 0); err != nil {
				return ErrUnsafeStore
			}
			changed = true
		case strings.HasPrefix(name, journalPrefix), strings.HasPrefix(name, dropPrefix):
			return ErrUnsafeStore
		case strings.HasSuffix(name, ".json"):
			if err := store.verifyFinal(name); err != nil {
				if err := store.quarantine(name); err != nil {
					return err
				}
				changed = true
			}
		default:
			return ErrUnsafeStore
		}
	}
	if changed {
		if err := syncDirectory(store.rootFile); err != nil {
			return err
		}
	}
	if err := store.remeasure(); err != nil {
		return err
	}
	if store.retention > 0 {
		if _, err := store.enforceRetentionLocked(store.now().UTC()); err != nil {
			return err
		}
		return store.remeasure()
	}
	return nil
}

func (store *FileStore) recoverJournals() error {
	entries, err := store.listEntries()
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, journalPrefix) {
			continue
		}
		if err := store.verifyEntry(name); err != nil {
			return err
		}
		raw, err := store.readEntry(name, uint64(journalReserveBytes))
		if err != nil {
			return err
		}
		var journal journalRecord
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&journal); err != nil || !validJournal(journal) {
			return ErrUnsafeStore
		}
		switch journal.Operation {
		case "PUBLISH":
			if err := store.recoverPublish(journal); err != nil {
				return err
			}
		case "DROP":
			if err := store.recoverDrop(journal); err != nil {
				return err
			}
		default:
			return ErrUnsafeStore
		}
		if err := unix.Unlinkat(int(store.rootFile.Fd()), name, 0); err != nil {
			return ErrDurability
		}
		if err := syncDirectory(store.rootFile); err != nil {
			return err
		}
	}
	return nil
}

func validJournal(journal journalRecord) bool {
	if journal.Operation == "PUBLISH" {
		return validRandomEntryName(journal.Stage, stagingPrefix) && validFinalName(journal.Final) && journal.Tomb == ""
	}
	return journal.Operation == "DROP" && journal.Stage == "" && validFinalName(journal.Final) && validRandomEntryName(journal.Tomb, dropPrefix)
}

func validRandomEntryName(name, prefix string) bool {
	if !strings.HasPrefix(name, prefix) || len(name) != len(prefix)+32 || strings.ContainsAny(name, `/\\`) {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(name, prefix))
	return err == nil
}

func validFinalName(name string) bool {
	if !strings.HasSuffix(name, ".json") || strings.ContainsAny(name, `/\\`) {
		return false
	}
	return bundleIDPattern.MatchString(strings.TrimSuffix(name, ".json"))
}

func (store *FileStore) recoverPublish(journal journalRecord) error {
	finalExists, err := store.entryExists(journal.Final)
	if err != nil {
		return err
	}
	stageExists, err := store.entryExists(journal.Stage)
	if err != nil {
		return err
	}
	if finalExists {
		if err := store.verifyFinal(journal.Final); err != nil {
			return err
		}
		if stageExists {
			return ErrUnsafeStore
		}
		return nil
	}
	if !stageExists {
		return nil
	}
	raw, err := store.readEntry(journal.Stage, DefaultLimitsV1().MaxBundleBytes)
	if err != nil {
		return err
	}
	id, err := extractBundleID(raw)
	if err != nil || journal.Final != id+".json" {
		return ErrContractViolation
	}
	if err := renameNoReplace(int(store.rootFile.Fd()), journal.Stage, journal.Final); err != nil {
		return ErrDurability
	}
	return syncDirectory(store.rootFile)
}

func (store *FileStore) recoverDrop(journal journalRecord) error {
	tombExists, err := store.entryExists(journal.Tomb)
	if err != nil {
		return err
	}
	if !tombExists {
		finalExists, err := store.entryExists(journal.Final)
		if err != nil {
			return err
		}
		if !finalExists {
			return nil
		}
		if err := renameNoReplace(int(store.rootFile.Fd()), journal.Final, journal.Tomb); err != nil {
			return ErrDurability
		}
		if err := syncDirectory(store.rootFile); err != nil {
			return err
		}
	}
	if err := store.verifyEntry(journal.Tomb); err != nil {
		return err
	}
	if err := unix.Unlinkat(int(store.rootFile.Fd()), journal.Tomb, 0); err != nil {
		return ErrDurability
	}
	return syncDirectory(store.rootFile)
}

func (store *FileStore) remeasure() error {
	entries, err := store.listEntries()
	if err != nil {
		return err
	}
	used := int64(0)
	for _, entry := range entries {
		if err := store.verifyEntry(entry.Name()); err != nil {
			return err
		}
		size, err := store.entrySize(entry.Name())
		if err != nil || size < 0 || size > store.quota-used {
			return ErrQuotaExceeded
		}
		used += size
	}
	store.used = used
	if store.used+store.reserved > store.quota {
		return ErrQuotaExceeded
	}
	return nil
}

func (store *FileStore) entrySize(name string) (int64, error) {
	fd, err := unix.Openat(int(store.rootFile.Fd()), name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return 0, ErrUnsafeStore
	}
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil || verifyRegularFD(fd, 0o600) != nil || stat.Size < 0 {
		_ = unix.Close(fd)
		return 0, ErrUnsafeStore
	}
	if err := unix.Close(fd); err != nil {
		return 0, ErrUnsafeStore
	}
	return stat.Size, nil
}

func (store *FileStore) listEntries() ([]os.DirEntry, error) {
	fd, err := unix.Openat(int(store.rootFile.Fd()), ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, ErrUnsafeStore
	}
	directory := os.NewFile(uintptr(fd), "store-list")
	entries, err := directory.ReadDir(-1)
	closeErr := directory.Close()
	if err != nil || closeErr != nil {
		return nil, ErrUnsafeStore
	}
	return entries, nil
}

func (store *FileStore) verifyEntry(name string) error {
	fd, err := unix.Openat(int(store.rootFile.Fd()), name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return ErrUnsafeStore
	}
	verifyErr := verifyRegularFD(fd, 0o600)
	closeErr := unix.Close(fd)
	if verifyErr != nil {
		return verifyErr
	}
	if closeErr != nil {
		return ErrUnsafeStore
	}
	return nil
}

func (store *FileStore) verifyFinal(name string) error {
	if !strings.HasSuffix(name, ".json") {
		return ErrUnsafeStore
	}
	if err := store.verifyEntry(name); err != nil {
		return err
	}
	raw, err := store.readEntry(name, DefaultLimitsV1().MaxBundleBytes)
	if err != nil {
		return err
	}
	id, err := extractBundleID(raw)
	if err != nil || name != id+".json" {
		return ErrContractViolation
	}
	return nil
}

func (store *FileStore) readEntry(name string, maximum uint64) ([]byte, error) {
	fd, err := unix.Openat(int(store.rootFile.Fd()), name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, ErrUnsafeStore
	}
	file := os.NewFile(uintptr(fd), name)
	if err := verifyRegularFD(fd, 0o600); err != nil {
		_ = file.Close()
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(file, int64(maximum)+1))
	closeErr := file.Close()
	if err != nil || uint64(len(data)) > maximum {
		return nil, ErrLimitsExceeded
	}
	if closeErr != nil {
		return nil, ErrUnsafeStore
	}
	return data, nil
}

func (store *FileStore) quarantine(name string) error {
	target, err := store.randomName(quarantinePrefix)
	if err != nil {
		return err
	}
	if err := renameNoReplace(int(store.rootFile.Fd()), name, target); err != nil {
		return ErrUnsafeStore
	}
	return syncDirectory(store.rootFile)
}

func (store *FileStore) ReserveCapture(maxBundleBytes int64) (*CaptureReservation, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return nil, ErrStoreClosed
	}
	if maxBundleBytes <= 0 || maxBundleBytes > int64(DefaultLimitsV1().MaxBundleBytes) {
		return nil, ErrInvalidArgument
	}
	amount := maxBundleBytes + journalReserveBytes
	if amount > store.quota-store.used-store.reserved {
		return nil, ErrQuotaExceeded
	}
	store.nextReserve++
	id := store.nextReserve
	store.reservations[id] = amount
	store.reserved += amount
	return &CaptureReservation{store: store, id: id, max: maxBundleBytes}, nil
}

func (reservation *CaptureReservation) Publish(bundle []byte) (string, error) {
	return reservation.publish(bundle, nil)
}

func (reservation *CaptureReservation) PublishVerified(bundle, expectedReplay []byte) (string, error) {
	if len(expectedReplay) == 0 {
		return "", ErrInvalidArgument
	}
	return reservation.publish(bundle, expectedReplay)
}

func (reservation *CaptureReservation) publish(bundle, expectedReplay []byte) (string, error) {
	if reservation == nil || reservation.store == nil {
		return "", ErrInvalidArgument
	}
	store := reservation.store
	store.mu.Lock()
	defer store.mu.Unlock()
	amount, ok := store.reservations[reservation.id]
	if !ok || store.closed {
		if store.closed {
			return "", ErrStoreClosed
		}
		return "", ErrInvalidArgument
	}
	delete(store.reservations, reservation.id)
	store.reserved -= amount
	if int64(len(bundle)) > reservation.max {
		return "", ErrQuotaExceeded
	}
	id, err := store.publishLocked(bundle)
	if err != nil || expectedReplay == nil {
		return id, err
	}
	if err := store.verifyPublishedLocked(id, bundle, expectedReplay); err != nil {
		return "", err
	}
	return id, nil
}

func (reservation *CaptureReservation) Release() {
	if reservation == nil || reservation.store == nil {
		return
	}
	store := reservation.store
	store.mu.Lock()
	defer store.mu.Unlock()
	if amount, ok := store.reservations[reservation.id]; ok {
		delete(store.reservations, reservation.id)
		store.reserved -= amount
	}
}

func (store *FileStore) Publish(bundle []byte) (string, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return "", ErrStoreClosed
	}
	return store.publishLocked(bundle)
}

func (store *FileStore) publishLocked(bundle []byte) (string, error) {
	if len(bundle) == 0 || int64(len(bundle))+journalReserveBytes > store.quota-store.used-store.reserved {
		return "", ErrQuotaExceeded
	}
	id, err := extractBundleID(bundle)
	if err != nil {
		return "", err
	}
	finalName := id + ".json"
	if exists, err := store.entryExists(finalName); err != nil {
		return "", err
	} else if exists {
		return "", ErrBundleExists
	}
	stageName, stage, err := store.createExclusive(stagingPrefix)
	if err != nil {
		return "", err
	}
	stageSize := int64(0)
	journalName := ""
	journalSize := int64(0)
	published := false
	defer func() {
		_ = stage.Close()
		if !published {
			if journalName != "" {
				_ = unix.Unlinkat(int(store.rootFile.Fd()), journalName, 0)
				store.used -= journalSize
			}
			if exists, _ := store.entryExists(stageName); exists {
				_ = unix.Unlinkat(int(store.rootFile.Fd()), stageName, 0)
				store.used -= stageSize
			}
			_ = syncDirectory(store.rootFile)
		}
	}()
	if _, err := stage.Write(bundle); err != nil {
		return "", ErrDurability
	}
	stageSize = int64(len(bundle))
	store.used += stageSize
	if err := syncFile(stage); err != nil {
		return "", err
	}
	if _, err := stage.Seek(0, io.SeekStart); err != nil {
		return "", ErrDurability
	}
	readBack, err := io.ReadAll(io.LimitReader(stage, int64(len(bundle))+1))
	if err != nil || !bytes.Equal(readBack, bundle) {
		return "", ErrDurability
	}
	if verifiedID, err := extractBundleID(readBack); err != nil || verifiedID != id {
		return "", ErrContractViolation
	}
	journalName, journalSize, err = store.createJournal(journalRecord{Operation: "PUBLISH", Stage: stageName, Final: finalName})
	if err != nil {
		return "", err
	}
	if err := renameNoReplace(int(store.rootFile.Fd()), stageName, finalName); err != nil {
		if errors.Is(err, unix.EEXIST) {
			return "", ErrBundleExists
		}
		return "", ErrDurability
	}
	if err := syncDirectory(store.rootFile); err != nil {
		return "", err
	}
	if err := unix.Unlinkat(int(store.rootFile.Fd()), journalName, 0); err != nil {
		return "", ErrDurability
	}
	store.used -= journalSize
	journalName = ""
	if err := syncDirectory(store.rootFile); err != nil {
		return "", err
	}
	published = true
	return id, nil
}

func (store *FileStore) verifyPublishedLocked(id string, bundle, expectedReplay []byte) error {
	name := id + ".json"
	if store.beforePublishedReopen != nil {
		store.beforePublishedReopen(name)
	}
	reopened, err := store.readEntry(name, DefaultLimitsV1().MaxBundleBytes)
	if err != nil || !bytes.Equal(reopened, bundle) {
		return ErrDurability
	}
	reopenedID, err := extractBundleID(reopened)
	if err != nil || reopenedID != id {
		return ErrDurability
	}
	replayed, err := Replay(reopened)
	if err != nil || !bytes.Equal(replayed, expectedReplay) {
		return ErrDurability
	}
	return syncDirectory(store.rootFile)
}

func (store *FileStore) lookupOneShot(actionRef EvidenceRefV1, tuple SourceTupleV1) (OneShotLookupResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return OneShotLookupResult{}, ErrStoreClosed
	}
	if validateEvidenceRef(actionRef) != nil {
		return OneShotLookupResult{}, ErrInvalidArgument
	}
	authority, ok := boundSourceAuthority(tuple.SourceKind, tuple.Contract, tuple.Version)
	if !ok || authority.kind != tuple.SourceKind || authority.contract != tuple.Contract || authority.version != tuple.Version {
		return OneShotLookupResult{}, ErrInvalidArgument
	}
	entries, err := store.listEntries()
	if err != nil {
		return OneShotLookupResult{}, err
	}
	sort.Slice(entries, func(left, right int) bool {
		return entries[left].Name() < entries[right].Name()
	})
	result := OneShotLookupResult{Status: OneShotLookupNone}
	conflict := false
	actionKey := evidenceRefKey(actionRef)
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".json") {
			continue
		}
		if err := store.verifyFinal(name); err != nil {
			return OneShotLookupResult{}, err
		}
		raw, err := store.readEntry(name, DefaultLimitsV1().MaxBundleBytes)
		if err != nil {
			return OneShotLookupResult{}, err
		}
		bundle, err := verifyBundle(raw)
		if err != nil {
			return OneShotLookupResult{}, err
		}
		if evidenceRefKey(bundle.CaptureWindow.Action.EvidenceRef) != actionKey {
			continue
		}
		replay, err := Replay(raw)
		if err != nil {
			return OneShotLookupResult{}, err
		}
		if !retainedOneShotCloudContentMatches(bundle, actionRef) {
			conflict = true
			continue
		}
		matches := 0
		for _, source := range bundle.Sources {
			if source.SourceBinding.SourceKind == tuple.SourceKind &&
				source.SourceContract == tuple.Contract &&
				source.SourceSchemaVersion == tuple.Version {
				matches++
			}
		}
		if matches != 1 || result.Status == OneShotLookupExisting {
			conflict = true
			continue
		}
		result = OneShotLookupResult{
			Status: OneShotLookupExisting,
			Bundle: append([]byte(nil), raw...),
			Replay: append([]byte(nil), replay...),
		}
	}
	if conflict {
		return OneShotLookupResult{Status: OneShotLookupConflict}, nil
	}
	return result, nil
}

func retainedOneShotCloudContentMatches(
	bundle SynchronizedEvidenceBundleV1,
	actionRef EvidenceRefV1,
) bool {
	matches := 0
	actionKey := evidenceRefKey(actionRef)
	for _, artifact := range bundle.Artifacts {
		if artifact.SourceBinding.SourceKind != SourceCloudApp {
			continue
		}
		matches++
		if len(artifact.EvidenceRefs) != 1 || evidenceRefKey(artifact.EvidenceRefs[0]) != actionKey {
			return false
		}
	}
	return matches == 1
}

func (store *FileStore) createExclusive(prefix string) (string, *os.File, error) {
	name, err := store.randomName(prefix)
	if err != nil {
		return "", nil, err
	}
	fd, err := unix.Openat(int(store.rootFile.Fd()), name, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return "", nil, ErrUnsafeStore
	}
	file := os.NewFile(uintptr(fd), name)
	if err := unix.Fchmod(fd, 0o600); err != nil || verifyRegularFD(fd, 0o600) != nil {
		_ = file.Close()
		_ = unix.Unlinkat(int(store.rootFile.Fd()), name, 0)
		return "", nil, ErrUnsafeStore
	}
	return name, file, nil
}

func (store *FileStore) createJournal(journal journalRecord) (string, int64, error) {
	raw, err := canonicalMarshal(journal, DefaultLimitsV1(), false)
	if err != nil || int64(len(raw)) > journalReserveBytes || int64(len(raw)) > store.quota-store.used-store.reserved {
		return "", 0, ErrQuotaExceeded
	}
	name, file, err := store.createExclusive(journalPrefix)
	if err != nil {
		return "", 0, err
	}
	defer func() { _ = file.Close() }()
	if _, err := file.Write(raw); err != nil {
		_ = unix.Unlinkat(int(store.rootFile.Fd()), name, 0)
		return "", 0, ErrDurability
	}
	if err := syncFile(file); err != nil {
		_ = unix.Unlinkat(int(store.rootFile.Fd()), name, 0)
		return "", 0, err
	}
	if err := file.Close(); err != nil {
		_ = unix.Unlinkat(int(store.rootFile.Fd()), name, 0)
		return "", 0, ErrDurability
	}
	store.used += int64(len(raw))
	if err := syncDirectory(store.rootFile); err != nil {
		return name, int64(len(raw)), err
	}
	return name, int64(len(raw)), nil
}

func (store *FileStore) Drop(id string) (DropStatus, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return "", ErrStoreClosed
	}
	return store.dropLocked(id)
}

func (store *FileStore) dropLocked(id string) (DropStatus, error) {
	if !bundleIDPattern.MatchString(id) {
		return "", ErrInvalidArgument
	}
	name := id + ".json"
	fd, err := unix.Openat(int(store.rootFile.Fd()), name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if errors.Is(err, unix.ENOENT) {
		return DropStatusAlreadyGone, nil
	}
	if err != nil {
		return "", ErrUnsafeStore
	}
	var opened unix.Stat_t
	if err := unix.Fstat(fd, &opened); err != nil || verifyRegularFD(fd, 0o600) != nil {
		_ = unix.Close(fd)
		return "", ErrUnsafeStore
	}
	tomb, err := store.randomName(dropPrefix)
	if err != nil {
		_ = unix.Close(fd)
		return "", err
	}
	journalName, journalSize, err := store.createJournal(journalRecord{Operation: "DROP", Final: name, Tomb: tomb})
	if err != nil {
		_ = unix.Close(fd)
		return "", err
	}
	if store.beforeDropRename != nil {
		store.beforeDropRename()
	}
	if err := renameNoReplace(int(store.rootFile.Fd()), name, tomb); err != nil {
		_ = unix.Close(fd)
		return "", ErrDurability
	}
	tombFD, err := unix.Openat(int(store.rootFile.Fd()), tomb, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		_ = unix.Close(fd)
		return "", ErrUnsafeStore
	}
	var moved unix.Stat_t
	statErr := unix.Fstat(tombFD, &moved)
	_ = unix.Close(tombFD)
	_ = unix.Close(fd)
	if statErr != nil || opened.Dev != moved.Dev || opened.Ino != moved.Ino || opened.Size != moved.Size ||
		opened.Nlink != 1 || moved.Nlink != 1 || opened.Mode != moved.Mode {
		return "", ErrUnsafeStore
	}
	if err := syncDirectory(store.rootFile); err != nil {
		return "", err
	}
	if err := unix.Unlinkat(int(store.rootFile.Fd()), tomb, 0); err != nil {
		return "", ErrDurability
	}
	if err := syncDirectory(store.rootFile); err != nil {
		return "", err
	}
	if err := unix.Unlinkat(int(store.rootFile.Fd()), journalName, 0); err != nil {
		return "", ErrDurability
	}
	store.used -= opened.Size + journalSize
	if err := syncDirectory(store.rootFile); err != nil {
		return "", err
	}
	return DropStatusDropped, nil
}

func (store *FileStore) EnforceRetention(now time.Time) (int, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return 0, ErrStoreClosed
	}
	if store.retention <= 0 || !validateTimestamp(now.UTC()) {
		return 0, ErrInvalidArgument
	}
	return store.enforceRetentionLocked(now.UTC())
}

func (store *FileStore) enforceRetentionLocked(now time.Time) (int, error) {
	cutoff := now.Add(-store.retention)
	entries, err := store.listEntries()
	if err != nil {
		return 0, err
	}
	dropped := 0
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".json") || strings.HasPrefix(name, ".") {
			continue
		}
		raw, err := store.readEntry(name, DefaultLimitsV1().MaxBundleBytes)
		if err != nil {
			return dropped, err
		}
		bundle, err := verifyBundle(raw)
		if err != nil {
			return dropped, err
		}
		if bundle.CapturedAt.Before(cutoff) {
			status, err := store.dropLocked(bundle.BundleID)
			if err != nil {
				return dropped, err
			}
			if status == DropStatusDropped {
				dropped++
			}
		}
	}
	return dropped, nil
}

func (store *FileStore) Usage() (used, reserved int64, err error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return 0, 0, ErrStoreClosed
	}
	return store.used, store.reserved, nil
}

func (store *FileStore) Close() error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return nil
	}
	store.closed = true
	store.reservations = nil
	store.reserved = 0
	var result error
	if store.lockFile != nil {
		if err := unix.Flock(int(store.lockFile.Fd()), unix.LOCK_UN); err != nil {
			result = ErrUnsafeStore
		}
		if err := store.lockFile.Close(); err != nil && result == nil {
			result = ErrUnsafeStore
		}
	}
	if store.rootFile != nil {
		if err := store.rootFile.Close(); err != nil && result == nil {
			result = ErrUnsafeStore
		}
	}
	return result
}

func (store *FileStore) entryExists(name string) (bool, error) {
	fd, err := unix.Openat(int(store.rootFile.Fd()), name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if errors.Is(err, unix.ENOENT) {
		return false, nil
	}
	if err != nil {
		return false, ErrUnsafeStore
	}
	if err := verifyRegularFD(fd, 0o600); err != nil {
		_ = unix.Close(fd)
		return false, err
	}
	if err := unix.Close(fd); err != nil {
		return false, ErrUnsafeStore
	}
	return true, nil
}

func (store *FileStore) randomName(prefix string) (string, error) {
	raw := make([]byte, 16)
	if _, err := io.ReadFull(store.entropy, raw); err != nil {
		return "", ErrUnsafeStore
	}
	return prefix + hex.EncodeToString(raw), nil
}

func extractBundleID(bundle []byte) (string, error) {
	value, _, err := parseJSON(bundle, DefaultLimitsV1(), true)
	if err != nil {
		return "", err
	}
	root, ok := value.(map[string]any)
	if !ok {
		return "", ErrContractViolation
	}
	id, ok := root["bundle_id"].(string)
	if !ok || !bundleIDPattern.MatchString(id) {
		return "", ErrContractViolation
	}
	canonical, err := CanonicalizeJSON(bundle)
	if err != nil || !bytes.Equal(canonical, bundle) {
		return "", ErrContractViolation
	}
	if _, err := verifyBundle(bundle); err != nil {
		return "", err
	}
	return id, nil
}
