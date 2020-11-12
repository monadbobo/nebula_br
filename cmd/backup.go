package cmd

import (
	"github.com/monadbobo/br/pkg/backup"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func NewBackupCmd() *cobra.Command {
	backupCmd := &cobra.Command{
		Use:   "backup",
		Short: "backup Nebula Graph Database",
	}

	backupCmd.AddCommand(newFullBackupCmd())
	backupCmd.PersistentFlags().StringArrayVar(&cf.MetaAddrs, "meta", nil, "meta server url")
	backupCmd.MarkPersistentFlagRequired("meta")
	backupCmd.PersistentFlags().StringArrayVar(&cf.StorageAddrs, "storage", nil, "storage server url")
	backupCmd.PersistentFlags().StringArrayVar(&cf.SpaceNames, "space", nil, "space name")
	backupCmd.PersistentFlags().StringVar(&cf.BackendUrl, "backend", "", "backend url")
	backupCmd.MarkPersistentFlagRequired("backend")
	backupCmd.PersistentFlags().StringVar(&cf.StorageUser, "storageuser", "", "storage server user")
	backupCmd.MarkPersistentFlagRequired("storageuser")
	backupCmd.PersistentFlags().StringVar(&cf.MetaUser, "metauser", "", "meta server user")
	backupCmd.MarkPersistentFlagRequired("metauser")

	return backupCmd
}

func newFullBackupCmd() *cobra.Command {
	fullBackupCmd := &cobra.Command{
		Use:   "full",
		Short: "full backup Nebula Graph Database",
		RunE: func(cmd *cobra.Command, args []string) error {
			// nil mean backup all space
			logger, _ := zap.NewProduction()

			defer logger.Sync() // flushes buffer, if any
			b := backup.NewBackupClient(cf, logger)
			err := b.Open(cf.MetaAddrs[0])
			if err != nil {
				return err
			}
			err = b.BackupCluster()
			if err != nil {
				return err
			}
			return nil
		},
	}

	return fullBackupCmd
}
