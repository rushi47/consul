package agent

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/consul/agent/agentpb"
	"github.com/hashicorp/consul/agent/config"
	"github.com/hashicorp/consul/lib"
)

const (
	// autoConfigFileName is the name of the file that the agent auto-config settings are
	// stored in within the data directory
	autoConfigFileName = "auto-config.json"
)

// loadConfig will build the agents configuration including the extraHead source injected
// after all other defaults but before any user supplied configuration.
func loadConfig(flags config.Flags, extraHead config.Source) (*config.RuntimeConfig, []string, error) {
	b, err := config.NewBuilder(flags)
	if err != nil {
		return nil, nil, err
	}

	if extraHead.Data != "" {
		b.Head = append(b.Head, extraHead)
	}

	cfg, err := b.BuildAndValidate()
	if err != nil {
		return nil, nil, err
	}

	return &cfg, b.Warnings, nil
}

// ReadConfig is used to reparse the configuration taking into account the agent
// auto-config settings
func (a *Agent) ReadConfig() (*config.RuntimeConfig, []string, error) {
	a.stateLock.Lock()
	defer a.stateLock.Unlock()
	return a.readConfigUnlocked()
}

// readConfigUnlocked is used to reparse the configuration taking into account the agent
// auto-config settings. This method assumes that the caller already holds a lock
// on the agent's stateLock.
func (a *Agent) readConfigUnlocked() (*config.RuntimeConfig, []string, error) {
	src := config.Source{
		Name:   autoConfigFileName,
		Format: "json",
		Data:   a.autoConfigJSON,
	}

	return loadConfig(a.configFlags, src)
}

// reloadConfigUnlocked will  reparse the configuration and simply replace the agents configuration.
// This method DOES NOT unload or reload any service definitions, checks etc. and is therefore only
// suitable for use during initial agent starting and in particular with the various places that
// auto-config might inject the configuration.
func (a *Agent) reloadConfigUnlocked() error {
	// reload the config
	config, warnings, err := a.readConfigUnlocked()
	if err != nil {
		return fmt.Errorf("Failed to parse configuration after apply auto-config configurations: %w", err)
	}

	for _, w := range warnings {
		a.logger.Warn(w)
	}

	a.config = config
	return nil
}

// restorePersistedAutoConfig will attempt to load the persisted auto-config
// settings from the data directory. It returns true either when there was an
// unrecoverable error or when the configuration was successfully loaded from
// disk. Recoverable errors, such as "file not found" are suppressed and this
// method will return false for the first boolean.
func (a *Agent) restorePersistedAutoConfig() (bool, error) {
	if a.config.DataDir == "" {
		// no data directory means we don't have anything to potentially load
		return false, nil
	}
	
	path := filepath.Join(a.config.DataDir, autoConfigFileName)
	a.logger.Debug("Attempting to restore auto-config settings", "path", path)

	content, err := ioutil.ReadFile(path)
	if err == nil {
		a.logger.Info("auto-config settings restored", "path", path)
		a.autoConfigJSON = string(content)
		return true, nil
	}

	if !os.IsNotExist(err) {
		return true, fmt.Errorf("Failed to load %s: %w", path, err)
	}

	// ignore non-existence errors as that is an indicator that we haven't
	// performed the auto configuration before
	return false, nil
}

// initializeAutoConfig is responsible for the first time auto-config
func (a *Agent) initializeAutoConfig() error {
	ac := a.config.AutoConfig

	// auto-config isn't enabled or not allowed to be on servers
	if a.config.ServerMode || !ac.Enabled {
		return nil
	}

	a.logger.Info("Initializing auto-config")

	// try to load the persisted auto-config.json
	if done, err := a.restorePersistedAutoConfig(); done {
		return err
	}

	// without an intro token or intro token file we cannot do anything
	if ac.IntroToken == "" && ac.IntroTokenFile == "" {
		return fmt.Errorf("Failed to initialize auto-config: intro_token and intro_token_file settings are not configured")
	}

	introToken := ac.IntroToken
	if introToken == "" {
		// load the intro token from the file
		content, err := ioutil.ReadFile(ac.IntroTokenFile)
		if err != nil {
			return fmt.Errorf("Failed to read intro token from file: %w", err)
		}

		introToken = string(content)
	}

	// setup the auto config request
	request := agentpb.AutoConfigRequest{
		Datacenter: a.config.Datacenter,
		Node:       a.config.NodeName,
		Segment:    a.config.SegmentName,
		JWT:        introToken,
	}

	var reply agentpb.AutoConfigResponse

	servers := ac.ServerAddresses
	disco, err := newDiscover()
	if err != nil && len(servers) == 0 {
		return err
	}

	servers = append(servers, retryJoinAddrs(disco, retryJoinSerfVariant, "Auto Config", servers, a.logger)...)

	for {
		// Check if the agent startup process should be stopped
		select {
		case <-a.InterruptStartCh:
			return fmt.Errorf("Aborting Auto Config - interrupted")
		default:
		}

		attempts := 0
		// loop over all the server join "addresses". These can be hostnames and may optionally have a port
		// to override the default
		for _, s := range servers {
			port := a.config.ServerPort
			host, portStr, err := net.SplitHostPort(s)
			if err != nil {
				if strings.Contains(err.Error(), "missing port in address") {
					host = s
				} else {
					a.logger.Warn("Error splitting auto-config server address", "address", s, "error", err)
					continue
				}
			} else {
				port, err = strconv.Atoi(portStr)
				if err != nil {
					a.logger.Warn("Parsed port is not an integer", "port", portStr, "error", err)
					continue
				}
			}

			// resolve the host to a list of IPs
			ips, err := net.LookupIP(host)
			if err != nil {
				a.logger.Warn("AutoConfig IP resolution failed", "host", host, "error", err)
				continue
			}

			// try each IP to see if we can successfully make the request
			for _, ip := range ips {
				addr := net.TCPAddr{IP: ip, Port: port}

				if err = a.connPool.RPC(a.config.Datacenter, a.config.NodeName, &addr, "Cluster.AutoConfig", &request, &reply); err == nil {
					return a.recordAutoConfigReply(&reply)
				} else {
					a.logger.Error("AutoConfig failed", "addr", addr.String(), "error", err)
				}
			}
		}
		attempts++

		delay := lib.RandomStagger(30 * time.Second)
		interval := (time.Duration(attempts) * delay) + delay
		a.logger.Warn("retrying AutoConfig", "retry_interval", interval)
		select {
		case <-time.After(interval):
			continue
		case <-a.InterruptStartCh:
			return fmt.Errorf("aborting AutoConfig because interrupted")
		case <-a.shutdownCh:
			return fmt.Errorf("aborting AutoConfig because shutting down")
		}
	}
	// this would be unreachable code. We hang out in the infinite loop until
	// the agent is shut down or until, some other unrecoverable error occurs
	// or until a successful Cluster.AutoConfig request is made
	// return nil
}

// recordAutoConfigReply takes an AutoConfig RPC reply records it with the agent
// This will persist the configuration to disk (unless in dev mode running without
// a data dir) and will reload the configuration.
//
// NOTE: It is expected that this be called while holding the state lock as we will
// be mutating some Agent state to store the rednered JSON configuration.
func (a *Agent) recordAutoConfigReply(reply *agentpb.AutoConfigResponse) error {
	conf, err := json.Marshal(reply.Config)
	if err != nil {
		return fmt.Errorf("Failed to encode auto-config configuration as JSON: %w", err)
	}

	// store it in the Agent so
	a.autoConfigJSON = string(conf)

	// The only way this wont be set is when running a dev mode agent.
	if a.config.DataDir == "" {
		a.logger.Debug("not persisting auto-config settings because there is no data directory")
		return nil	
	}
	
	path := filepath.Join(a.config.DataDir, autoConfigFileName)

	err = ioutil.WriteFile(path, conf, 0660)
	if err != nil {
		return fmt.Errorf("Failed to write auto-config configurations: %w", err)
	}

	a.logger.Debug("auto-config settings were persisted to disk")

	return nil
}
