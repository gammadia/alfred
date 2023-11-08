package openstack

import (
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/gammadia/alfred/namegen"
	"github.com/gammadia/alfred/scheduler"
	"github.com/gophercloud/gophercloud"
	"github.com/gophercloud/gophercloud/openstack"
	"github.com/gophercloud/gophercloud/openstack/compute/v2/extensions/keypairs"
	"github.com/gophercloud/gophercloud/openstack/compute/v2/servers"
	"golang.org/x/crypto/ssh"
)

type Provisioner struct {
	name   string
	config Config
	client *gophercloud.ServiceClient
	log    *slog.Logger
	fs     *fs

	keypairName string
	privateKey  ssh.Signer

	// WaitGroup tracking all provisioned resources
	// An item is added to track the first call to Shutdown()
	wg sync.WaitGroup

	// True if Shutdown() has been called
	shutdown bool
}

// Provisioner implements scheduler.Provisioner
var _ scheduler.Provisioner = (*Provisioner)(nil)

func New(config Config) (*Provisioner, error) {
	// OpenStack authentication
	opts, err := openstack.AuthOptionsFromEnv()
	if err != nil {
		return nil, fmt.Errorf("failed to get auth options from env: %w", err)
	}

	provider, err := openstack.AuthenticatedClient(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to get authenticated client: %w", err)
	}

	// OpenStack compute client
	client, err := openstack.NewComputeV2(provider, gophercloud.EndpointOpts{
		Region: os.Getenv("OS_REGION_NAME"),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get compute client: %w", err)
	}

	// Provisioner object
	name := namegen.Get()
	provisioner := &Provisioner{
		name:   name,
		config: config,
		client: client,
		log:    config.Logger.With(slog.Group("provisioner", "name", name)),

		keypairName: fmt.Sprintf("alfred-%s", name),
	}
	provisioner.wg.Add(1) // One item for the provisioner itself

	// Generate a temporary keypair
	provisioner.log.Debug("Creating SSH keypair", "keypair", provisioner.keypairName)
	keypair, err := keypairs.Create(client, keypairs.CreateOpts{Name: provisioner.keypairName}).Extract()
	if err != nil {
		return nil, fmt.Errorf("failed to create keypair: %w", err)
	} else {
		provisioner.wg.Add(1) // One item for the keypair
		provisioner.privateKey, err = ssh.ParsePrivateKey([]byte(keypair.PrivateKey))
		if err != nil {
			provisioner.deleteKeypair()
			return nil, fmt.Errorf("failed to parse private key: %w", err)
		}
	}

	return provisioner, nil
}

func (p *Provisioner) deleteKeypair() {
	p.log.Debug("Deleting SSH keypair", "keypair", p.keypairName)
	err := keypairs.Delete(p.client, p.keypairName, nil).ExtractErr()
	p.wg.Done()
	if err != nil {
		p.log.Warn("Failed to delete keypair", "error", err)
	}
}

func (p *Provisioner) GetName() string {
	return p.name
}

func (p *Provisioner) Provision(nodeName string) (scheduler.Node, error) {
	name := fmt.Sprintf("alfred-%s", nodeName)

	server, err := servers.Create(p.client, keypairs.CreateOptsExt{
		CreateOptsBuilder: servers.CreateOpts{
			Name:           name,
			ImageRef:       p.config.Image,
			FlavorRef:      p.config.Flavor,
			Networks:       p.config.Networks,
			SecurityGroups: p.config.SecurityGroups,
			Metadata: map[string]string{
				"alfred-provisioner":    p.name,
				"alfred-provisioned-at": time.Now().Format(time.RFC3339),
			},
		},
		KeyName: p.keypairName,
	}).Extract()
	if err != nil {
		return nil, fmt.Errorf("failed to create server '%s': %w", name, err)
	}

	node := &Node{
		name:        name,
		provisioner: p,

		log:    p.log.With(slog.Group("node", "name", name)),
		server: server,
	}

	node.log.Info("Created server, waiting for it to become ready")
	return node, node.connect(server)
}

func (p *Provisioner) Shutdown() {
	if p.shutdown {
		p.log.Debug("Ignoring duplicate call to Shutdown()")
		return
	}
	p.shutdown = true
	p.deleteKeypair()
	p.wg.Done() // Remove the provisioner itself from the WaitGroup
}

func (p *Provisioner) Wait() {
	p.wg.Wait()
}
