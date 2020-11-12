package backup

import (
	"errors"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/facebook/fbthrift/thrift/lib/go/thrift"
	"go.uber.org/zap"
	"golang.org/x/crypto/ssh"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"

	//	"github.com/vesoft-inc/nebula-clients/go/nebula/"
	//	"github.com/vesoft-inc/nebula-clients/go/nebula/meta"

	"github.com/monadbobo/br/pkg/config"
	"github.com/monadbobo/br/pkg/nebula"
	"github.com/monadbobo/br/pkg/nebula/meta"
	"github.com/monadbobo/br/pkg/storage"
)

var defaultTimeout time.Duration = 120 * time.Second
var metaFile = "/tmp/backup.meta"

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
	config         config.Config
	metaAddr       string
	backendStorage storage.ExternalStorage
	log            *zap.Logger
}

func NewBackupClient(cf config.Config, log *zap.Logger) *Backup {
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
	file, err := os.OpenFile(metaFile, os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	defer file.Close()

	trans := thrift.NewStreamTransport(file, file)

	binaryOut := thrift.NewBinaryProtocol(trans, false, true)
	defer trans.Close()
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

func (b *Backup) newSshSession(addr string, user string) (*ssh.Session, error) {
	key, err := ioutil.ReadFile(os.Getenv("HOME") + "/.ssh/id_rsa")
	if err != nil {
		b.log.Error("unable to read private key", zap.Error(err))
		return nil, err
	}

	// Create the Signer for this private key.
	signer, err := ssh.ParsePrivateKey(key)
	if err != nil {
		b.log.Error("unable to parse private key", zap.Error(err))
		return nil, err
	}
	config := &ssh.ClientConfig{
		User:            user,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
	}

	client, err := ssh.Dial("tcp", net.JoinHostPort(addr, "22"), config)
	if err != nil {
		b.log.Error("unable to connect host", zap.Error(err), zap.String("host", addr), zap.String("user", user))
		return nil, err
	}

	session, err := client.NewSession()
	if err != nil {
		b.log.Error("new session failed", zap.Error(err))
		return nil, err
	}

	return session, nil
}

func (b *Backup) uploadBySSH(addr string, user string, cmd string) error {
	session, err := b.newSshSession(addr, user)
	if err != nil {
		return err
	}
	defer session.Close()
	b.log.Info("ssh will exec", zap.String("cmd", cmd))

	err = session.Run(cmd)
	if err != nil {
		b.log.Error("ssh run failed", zap.Error(err))
		return err
	}
	return nil
}

func (b *Backup) uploadMeta(g *errgroup.Group, files []string) {

	b.log.Info("will upload meta", zap.Int("sst file count", len(files)))
	cmd := b.backendStorage.CopyMetaCommand(files)
	b.log.Info("start upload meta", zap.String("addr", b.metaAddr))
	ipAddr := strings.Split(b.metaAddr, ":")
	g.Go(func() error { return b.uploadBySSH(ipAddr[0], b.config.MetaUser, cmd) })
}

func (b *Backup) uploadStorage(g *errgroup.Group, dirs map[string][]spaceInfo) {
	for k, v := range dirs {
		b.log.Info("start upload storage", zap.String("addr", k))

		var cpDir []string
		for _, info := range v {
			cpDir = append(cpDir, info.checkpointDir)
		}

		ipAddrs := strings.Split(k, ":")
		cmd := b.backendStorage.CopyStorageCommand(cpDir, ipAddrs[0])

		g.Go(func() error { return b.uploadBySSH(ipAddrs[0], b.config.StorageUser, cmd) })
	}
}

func (b *Backup) uploadMetaFile() error {
	cmdStr := b.backendStorage.BackupMetaFileCommand(metaFile)

	cmd := exec.Command(cmdStr)
	err := cmd.Run()
	if err != nil {
		return err
	}

	return nil
}

func (b *Backup) UploadAll(meta *meta.BackupMeta) error {
	//upload meta
	g, _ := errgroup.WithContext(context.Background())

	b.uploadMeta(g, meta.GetMetaFiles())
	//upload storage
	storageMap := make(map[string][]spaceInfo)
	for k, v := range meta.GetBackupInfo() {
		for _, f := range v.GetBackupName() {
			cpDir := spaceInfo{k, string(f.CheckpointDir)}
			storageMap[hostaddrToString(f.Host)] = append(storageMap[hostaddrToString(f.Host)], cpDir)
		}
	}
	b.uploadStorage(g, storageMap)
	// write the meta for this backup to local
	g.Go(func() error {
		err := b.writeMetadata(meta)
		if err != nil {
			b.log.Error("write the meta file failed", zap.Error(err))
			return err
		}
		b.log.Info("write meta data finished")
		// upload meta file
		b.uploadMetaFile()
		return nil
	})

	err := g.Wait()
	if err != nil {
		b.log.Error("upload error")
		return err
	}

	b.log.Info("upload done")

	return nil
}
