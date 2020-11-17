package cmd

import (
	"github.com/monadbobo/br/pkg/restore"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func NewRestoreCMD() *cobra.Command {
	restoreCmd := &cobra.Command{
		Use:   "restore",
		Short: "restore Nebula Graph Database",
	}

	restoreCmd.AddCommand(newFullRestoreCmd())
	restoreCmd.PersistentFlags().StringArrayVar(&restoreConfig.MetaAddrs, "meta", nil, "meta server url")
	restoreCmd.MarkPersistentFlagRequired("meta")
	restoreCmd.PersistentFlags().StringArrayVar(&restoreConfig.StorageAddrs, "storage", nil, "storage server url")
	restoreCmd.MarkPersistentFlagRequired("storage")
	restoreCmd.PersistentFlags().StringVar(&restoreConfig.BackendUrl, "backend", "", "backend url")
	restoreCmd.MarkPersistentFlagRequired("backend")
	restoreCmd.PersistentFlags().StringVar(&restoreConfig.StorageUser, "storageuser", "", "storage server user")
	restoreCmd.MarkPersistentFlagRequired("storageuser")
	restoreCmd.PersistentFlags().StringVar(&restoreConfig.MetaUser, "metauser", "", "meta server user")
	restoreCmd.MarkPersistentFlagRequired("metauser")
	restoreCmd.PersistentFlags().StringVar(&restoreConfig.BackupName, "backupname", "", "backup name")
	restoreCmd.MarkPersistentFlagRequired("backupname")
	restoreCmd.PersistentFlags().StringVar(&restoreConfig.StorageDataDir, "sdir", "", "storage data dir")
	restoreCmd.MarkPersistentFlagRequired("sdir")
	restoreCmd.PersistentFlags().StringVar(&restoreConfig.MetaDataDir, "mdir", "", "meta data dir")
	restoreCmd.MarkPersistentFlagRequired("mdir")

	return restoreCmd
}

func newFullRestoreCmd() *cobra.Command {
	fullRestoreCmd := &cobra.Command{
		Use:   "full",
		Short: "full restore Nebula Graph Database",
		RunE: func(cmd *cobra.Command, args []string) error {
			// nil mean backup all space
			logger, _ := zap.NewProduction()

			defer logger.Sync() // flushes buffer, if any

			r := restore.NewRestore(restoreConfig, logger)
			err := r.RestoreCluster()
			if err != nil {
				return err
			}
			return nil
		},
	}

	return fullRestoreCmd
}
