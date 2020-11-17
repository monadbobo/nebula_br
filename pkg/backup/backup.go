package backup

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/facebook/fbthrift/thrift/lib/go/thrift"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"

	//	"github.com/vesoft-inc/nebula-clients/go/nebula/"
	//	"github.com/vesoft-inc/nebula-clients/go/nebula/meta"

	"github.com/monadbobo/br/pkg/config"
	"github.com/monadbobo/br/pkg/nebula"
	"github.com/monadbobo/br/pkg/nebula/meta"
	"github.com/monadbobo/br/pkg/ssh"
	"github.com/monadbobo/br/pkg/storage"
)

var defaultTimeout time.Duration = 120 * time.Second
var tmpDir = "/tmp/"

type BackupError struct {
	msg string
	Err error
}

type spaceInfo struct {
	spaceID       nebula.GraphSpaceID
	checkpointDir string
}

var LeaderNotFoundError = errors.New("not found leader")
var backupFailed = errors.New("backup failed")

func (e *BackupError) Error() string {
	return e.msg + e.Err.Error()
}

type Backup struct {
	client         *meta.MetaServiceClient
	config         config.BackupConfig
	metaAddr       string
	backendStorage storage.ExternalStorage
	log            *zap.Logger
	metaFileName   string
}

func NewBackupClient(cf config.BackupConfig, log *zap.Logger) *Backup {
	backend, err := storage.NewExternalStorage(cf.BackendUrl, log)
	if err != nil {
		log.Error("new external storage failed", zap.Error(err))
		return nil
	}
	return &Backup{config: cf, backendStorage: backend, log: log}
}

func hostaddrToString(host *nebula.HostAddr) string {
	return host.Host + ":" + string(host.Port)
}

func (b *Backup) CheckEnv() bool {
	return true
}

func (b *Backup) Open(addr string) error {

	if b.client != nil {
		if err := b.client.Transport.Close(); err != nil {
			b.log.Warn("close backup falied", zap.Error(err))
		}
	}

	timeoutOption := thrift.SocketTimeout(defaultTimeout)
	addressOption := thrift.SocketAddr(addr)
	sock, err := thrift.NewSocket(timeoutOption, addressOption)
	if err != nil {
		return err
	}

	transport := thrift.NewBufferedTransport(sock, 128<<10)

	pf := thrift.NewBinaryProtocolFactoryDefault()
	client := meta.NewMetaServiceClientFactory(transport, pf)
	if err := client.Transport.Open(); err != nil {
		return err
	}
	b.metaAddr = addr
	b.client = client
	return nil
}

func (b *Backup) Close() error {
	if b.client != nil {
		if err := b.client.Transport.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (b *Backup) CreateBackup(count int) (*meta.CreateBackupResp, error) {
	for {
		if count == 0 {
			return nil, LeaderNotFoundError
		}
		backupReq := meta.NewCreateBackupReq()
		defer b.client.Transport.Close()

		resp, err := b.client.CreateBackup(backupReq)
		if err != nil {
			return nil, err
		}

		if resp.GetCode() != meta.ErrorCode_E_LEADER_CHANGED && resp.GetCode() != meta.ErrorCode_SUCCEEDED {
			b.log.Error("backup failed", zap.String("error code", resp.GetCode().String()))
			return nil, backupFailed
		}

		if resp.GetCode() == meta.ErrorCode_SUCCEEDED {
			return resp, nil
		}

		leader := resp.GetLeader()
		if leader == meta.ExecResp_Leader_DEFAULT {
			return nil, LeaderNotFoundError
		}

		// we need reconnect the new leader
		err = b.Open(hostaddrToString(leader))
		if err != nil {
			return nil, err
		}
		count--
	}
}

func (b *Backup) writeMetadata(meta *meta.BackupMeta) error {
	b.metaFileName = tmpDir + meta.BackupName + ".meta"

	file, err := os.OpenFile(b.metaFileName, os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	defer file.Close()

	trans := thrift.NewStreamTransport(file, file)

	binaryOut := thrift.NewBinaryProtocol(trans, false, true)
	defer trans.Close()
	var absMetaFiles []string
	for _, files := range meta.MetaFiles {
		f := filepath.Base(files)
		absMetaFiles = append(absMetaFiles, f)
	}
	meta.MetaFiles = absMetaFiles
	err = meta.Write(binaryOut)
	if err != nil {
		return err
	}
	binaryOut.Flush()
	return nil
}

func (b *Backup) BackupCluster() error {

	resp, err := b.CreateBackup(3)
	if err != nil {
		b.log.Error("backup cluster failed", zap.Error(err))
		return err
	}

	meta := resp.GetMeta()
	err = b.UploadAll(meta)
	if err != nil {
		return err
	}

	return nil
}

func (b *Backup) uploadMeta(g *errgroup.Group, files []string) {

	b.log.Info("will upload meta", zap.Int("sst file count", len(files)))
	cmd := b.backendStorage.BackupMetaCommand(files)
	b.log.Info("start upload meta", zap.String("addr", b.metaAddr))
	ipAddr := strings.Split(b.metaAddr, ":")
	g.Go(func() error { return ssh.ExecCommandBySSH(ipAddr[0], b.config.MetaUser, cmd, b.log) })
}

func (b *Backup) uploadStorage(g *errgroup.Group, dirs map[string][]spaceInfo) {
	for k, v := range dirs {
		b.log.Info("start upload storage", zap.String("addr", k))
		idMap := make(map[string]string)
		for _, info := range v {
			idStr := strconv.FormatInt(int64(info.spaceID), 10)
			idMap[idStr] = info.checkpointDir
		}

		ipAddrs := strings.Split(k, ":")
		for id2, cp := range idMap {
			cmd := b.backendStorage.BackupStorageCommand(cp, ipAddrs[0], id2)

			g.Go(func() error { return ssh.ExecCommandBySSH(ipAddrs[0], b.config.StorageUser, cmd, b.log) })
		}
	}
}

func (b *Backup) uploadMetaFile() error {
	cmdStr := b.backendStorage.BackupMetaFileCommand(b.metaFileName)

	cmd := exec.Command(cmdStr[0], cmdStr[1:]...)
	err := cmd.Run()
	if err != nil {
		return err
	}
	cmd.Wait()

	return nil
}

func (b *Backup) execPreCommand(backupName string) error {
	b.backendStorage.SetBackupName(backupName)
	cmdStr := b.backendStorage.BackupPreCommand()

	cmd := exec.Command(cmdStr[0], cmdStr[1:]...)
	err := cmd.Run()
	if err != nil {
		return err
	}
	cmd.Wait()

	return nil
}

func (b *Backup) UploadAll(meta *meta.BackupMeta) error {
	//upload meta
	g, _ := errgroup.WithContext(context.Background())

	err := b.execPreCommand(meta.GetBackupName())
	if err != nil {
		return err
	}

	b.uploadMeta(g, meta.GetMetaFiles())
	//upload storage
	storageMap := make(map[string][]spaceInfo)
	for k, v := range meta.GetBackupInfo() {
		for _, f := range v.GetCpDirs() {
			cpDir := spaceInfo{k, string(f.CheckpointDir)}
			storageMap[hostaddrToString(f.Host)] = append(storageMap[hostaddrToString(f.Host)], cpDir)
		}
	}
	b.uploadStorage(g, storageMap)

	err = g.Wait()
	if err != nil {
		b.log.Error("upload error")
		return err
	}
	// write the meta for this backup to local

	err = b.writeMetadata(meta)
	if err != nil {
		b.log.Error("write the meta file failed", zap.Error(err))
		return err
	}
	b.log.Info("write meta data finished")
	// upload meta file
	err = b.uploadMetaFile()
	if err != nil {
		b.log.Error("upload meta file failed", zap.Error(err))
		return err
	}

	b.log.Info("upload done")

	return nil
}
