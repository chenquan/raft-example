package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
)

type kvFsm struct {
	db *sync.Map
}

type setPayload struct {
	Key   string
	Value string
}

func (kf *kvFsm) Apply(log *raft.Log) any {
	switch log.Type {
	case raft.LogCommand:
		var sp setPayload
		err := json.Unmarshal(log.Data, &sp)
		if err != nil {
			return fmt.Errorf("could not parse payload: %s", err)
		}

		kf.db.Store(sp.Key, sp.Value)
	default:
		return fmt.Errorf("unknown raft log type: %#v", log.Type)
	}

	return nil
}

func (kf *kvFsm) Snapshot() (raft.FSMSnapshot, error) {
	return snapshot{kf.db}, nil
}

func (kf *kvFsm) Restore(rc io.ReadCloser) error {
	decoder := json.NewDecoder(rc)

	for decoder.More() {
		m := map[string]any{}
		err := decoder.Decode(&m)
		if err != nil {
			return fmt.Errorf("could not decode payload: %s", err)
		}

		for k, v := range m {
			kf.db.Store(k, v)
		}
	}

	return rc.Close()
}

type snapshot struct {
	m *sync.Map
}

func (sn snapshot) Persist(s raft.SnapshotSink) error {
	m := map[string]any{}
	sn.m.Range(func(key, value any) bool {
		m[key.(string)] = value
		return true
	})

	defer s.Close()
	if err := json.NewEncoder(s).Encode(m); err != nil {
		s.Cancel()
		return err
	}

	return nil
}
func (sn snapshot) Release() {}

func setupRaft(dir, nodeId, raftAddress string, kf *kvFsm) (*raft.Raft, error) {
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("could not create data directory: %s", err)
	}

	store, err := raftboltdb.NewBoltStore(path.Join(dir, "bolt"))
	if err != nil {
		return nil, fmt.Errorf("could not create bolt store: %s", err)
	}

	snapshots, err := raft.NewFileSnapshotStore(path.Join(dir, "snapshot"), 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("could not create snapshot store: %s", err)
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", raftAddress)
	if err != nil {
		return nil, fmt.Errorf("could not resolve address: %s", err)
	}

	transport, err := raft.NewTCPTransport(raftAddress, tcpAddr, 10, time.Second*10, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("could not create tcp transport: %s", err)
	}

	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(nodeId)
	raftCfg.SnapshotInterval = 10 * time.Second
	raftCfg.SnapshotThreshold = 2

	r, err := raft.NewRaft(raftCfg, kf, store, store, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("could not create raft instance: %s", err)
	}

	r.BootstrapCluster(raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID(nodeId),
				Address: transport.LocalAddr(),
			},
		},
	})

	return r, nil
}

type httpServer struct {
	r  *raft.Raft
	db *sync.Map
}

func (hs httpServer) setHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	bs, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("could not read key-value in http request: %s", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	future := hs.r.Apply(bs, 500*time.Millisecond)
	if err := future.Error(); err != nil {
		log.Printf("could not write key-value: %s", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	e := future.Response()
	if e != nil {
		log.Printf("could not write key-value, application: %s", e)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (hs httpServer) getHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	value, ok := hs.db.Load(key)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	rsp := struct {
		Data string `json:"data"`
	}{value.(string)}
	err := json.NewEncoder(w).Encode(rsp)
	if err != nil {
		log.Printf("could not encode key-value in http response: %s", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
	}
}

func (hs httpServer) joinHandler(w http.ResponseWriter, r *http.Request) {
	followerId := r.URL.Query().Get("followerId")
	followerAddr := r.URL.Query().Get("followerAddr")

	if hs.r.State() != raft.Leader {
		json.NewEncoder(w).Encode(struct {
			Error string `json:"error"`
		}{
			"Not the leader",
		})
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	err := hs.r.AddVoter(raft.ServerID(followerId), raft.ServerAddress(followerAddr), 0, 0).Error()
	if err != nil {
		log.Printf("failed to add follower: %s", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
	}
	w.WriteHeader(http.StatusOK)
}

type config struct {
	nodeId   string
	httpPort int
	raftPort int
}

func getConfig() config {
	var nodeId = flag.String("node-id", "node1", "Input node id")
	var httpPort = flag.Int("http-port", 2222, "Input http port")
	var raftPort = flag.Int("raft-port", 8222, "Input raft port")
	flag.Parse()

	return config{nodeId: *nodeId, httpPort: *httpPort, raftPort: *raftPort}
}

func main() {
	cfg := getConfig()

	db := &sync.Map{}
	kf := &kvFsm{db}

	dataDir := "data"
	r, err := setupRaft(path.Join(dataDir, "raft"+cfg.nodeId), cfg.nodeId, "localhost:"+strconv.Itoa(cfg.raftPort), kf)
	if err != nil {
		log.Fatal(err)
	}

	hs := httpServer{r, db}

	http.HandleFunc("/set", hs.setHandler)
	http.HandleFunc("/get", hs.getHandler)
	http.HandleFunc("/join", hs.joinHandler)
	http.ListenAndServe(":"+strconv.Itoa(cfg.httpPort), nil)
}
