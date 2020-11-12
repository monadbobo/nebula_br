package storage

import (
	"fmt"

	"go.uber.org/zap"
)

type LocalBackedStore struct {
	dir string
	log *zap.Logger
}

func NewLocalBackedStore(dir string, log *zap.Logger) *LocalBackedStore {
	return &LocalBackedStore{dir: dir, log: log}
}

func (s LocalBackedStore) URI() string {
	return s.dir
}

func (s LocalBackedStore) copyCommand(src []string, dir string) string {
	cmdFormat := "mkdir -p " + dir + " && cp -rf %s " + dir
	//cmdFormat := "cp %s " + dir
	files := ""
	for _, f := range src {
		files += f + " "
	}

	return fmt.Sprintf(cmdFormat, files)
}

func (s LocalBackedStore) CopyMetaCommand(src []string) string {
	metaDir := s.dir + "/" + "meta"
	return s.copyCommand(src, metaDir)
}

func (s LocalBackedStore) CopyStorageCommand(src []string, prefix string) string {
	storageDir := s.dir + "/" + "storage/" + prefix
	return s.copyCommand(src, storageDir)
}

func (s LocalBackedStore) BackupMetaFileCommand(src string) string {
	f := "cp " + src + " " + s.dir + "/"
	return f
}
