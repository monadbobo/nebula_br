package restore

import (
	"context"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/facebook/fbthrift/thrift/lib/go/thrift"
	"github.com/monadbobo/br/pkg/config"
	"github.com/monadbobo/br/pkg/nebula"
	"github.com/monadbobo/br/pkg/nebula/meta"
	"github.com/monadbobo/br/pkg/ssh"
	"github.com/monadbobo/br/pkg/storage"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type Restore struct {
	config       config.RestoreConfig
	backend      storage.ExternalStorage
	log          *zap.Logger
	metaFileName string
}

type spaceInfo struct {
	spaceID nebula.GraphSpaceID
	cpDir   string
}

func NewRestore(config config.RestoreConfig, log *zap.Logger) *Restore {
	backend, err := storage.NewExternalStorage(config.BackendUrl, log)
	if err != nil {
		log.Error("new external storage failed", zap.Error(err))
		return nil
	}
	backend.SetBackupName(config.BackupName)
	return &Restore{config: config, log: log, backend: backend}
}

func (r *Restore) downloadMetaFile() error {
	r.metaFileName = r.config.BackupName + ".meta"
	cmdStr := r.backend.RestoreMetaFileCommand(r.metaFileName, "/tmp/")
	cmd := exec.Command(cmdStr[0], cmdStr[1:]...)
	err := cmd.Run()
	if err != nil {
		return err
	}
	cmd.Wait()

	return nil
}

func (r *Restore) restoreMetaFile() (*meta.BackupMeta, error) {
	file, err := os.OpenFile("/tmp/"+r.metaFileName, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	defer file.Close()

	trans := thrift.NewStreamTransport(file, file)

	binaryIn := thrift.NewBinaryProtocol(trans, false, true)
	defer trans.Close()
	m := meta.NewBackupMeta()
	err = m.Read(binaryIn)
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (r *Restore) downloadMeta(g *errgroup.Group, file []string) {
	cmd := r.backend.RestoreMetaCommand(file, r.config.MetaDataDir)
	for _, ip := range r.config.MetaAddrs {
		ipAddr := strings.Split(ip, ":")
		g.Go(func() error { return ssh.ExecCommandBySSH(ipAddr[0], r.config.MetaUser, cmd, r.log) })
	}
}

func hostaddrToString(host *nebula.HostAddr) string {
	return host.Host + ":" + string(host.Port)
}

func (r *Restore) downloadStorage(g *errgroup.Group, info map[nebula.GraphSpaceID]*meta.SpaceBackupInfo) {
	idMap := make(map[string][]string)
	for gid, bInfo := range info {
		for _, dir := range bInfo.CpDirs {
			idStr := strconv.FormatInt(int64(gid), 10)
			idMap[hostaddrToString(dir.Host)] = append(idMap[hostaddrToString(dir.Host)], idStr)
		}
	}

	i := 0
	for ip, ids := range idMap {
		r.log.Info("download", zap.String("ip", ip))
		ipAddr := strings.Split(ip, ":")
		cmd := r.backend.RestoreStorageCommand(ipAddr[0], ids, r.config.StorageDataDir)
		addr := strings.Split(r.config.StorageAddrs[i], ":")
		g.Go(func() error { return ssh.ExecCommandBySSH(addr[0], r.config.StorageUser, cmd, r.log) })
		i++
	}

}

func (r *Restore) RestoreCluster() error {
	err := r.downloadMetaFile()
	if err != nil {
		r.log.Error("download meta file failed", zap.Error(err))
		return err
	}

	m, err := r.restoreMetaFile()

	if err != nil {
		r.log.Error("restore meta file failed", zap.Error(err))
		return err
	}

	g, _ := errgroup.WithContext(context.Background())

	r.downloadMeta(g, m.MetaFiles)
	r.downloadStorage(g, m.BackupInfo)

	err = g.Wait()
	if err != nil {
		r.log.Error("restore error")
		return err
	}

	return nil

}
