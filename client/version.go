package main

import (
	"github.com/gammadia/alfred/proto"
	"github.com/spf13/cobra"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Show the version number of Alfred",

	RunE: func(cmd *cobra.Command, args []string) error {
		cmd.Printf("alfred version %s (%s)\n", version, commit[:7])

		if response, err := client.Ping(cmd.Context(), &proto.PingRequest{}); err != nil {
			return err
		} else {
			cmd.Printf("server version %s (%s)\n", response.Version, response.Commit[:7])
			return nil
		}
	},
}
