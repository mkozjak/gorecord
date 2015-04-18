package main

import (
	"bytes"
	"code.google.com/p/go.net/ipv4"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"gopkg.in/ini.v1"
	"io"
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"os/exec"
	"time"
)

// Command-line interface flags definition
var (
	cfgFlag = flag.String(
		"c",
		"/etc/gorecord.ini",
		"full path to config file")
	srvFlag   = flag.Bool("l", false, "start server and listen for requests")
	portFlag  = flag.String("p", "", "network port the jsonrpc server will listen on")
	ifaceFlag = flag.String("i", "", "network interface used for capturing the stream media")
	mdirFlag  = flag.String("m", "", "recorded media filesystem location")
	dbFlag    = flag.String("d", "/var/lib/gorecord/db", "leveldb location")
)

func init() {
	flag.StringVar(
		cfgFlag,
		"config",
		"/etc/gorecord.ini",
		"full path to config file")
	flag.BoolVar(srvFlag, "listen", false, "start server and listen for requests")
	flag.StringVar(portFlag, "port", "", "network port the jsonrpc server will listen on")
	flag.StringVar(ifaceFlag, "interface", "", "network interface used for capturing the stream media")
	flag.StringVar(mdirFlag, "mediadir", "", "recorded media filesystem location")
	flag.StringVar(dbFlag, "database", "/var/lib/gorecord/db", "leveldb location")
}

// Config represents an application configuration instance.
type Config struct {
	opts map[string]string
	fh   *ini.File
}

// Database represents a database instance storage.
type Database struct {
	inst *leveldb.DB
	err  error
}

// GenericReply represents a generic jsonrpc reply content.
type GenericReply struct {
	Status      string `json:"status,omitempty"`
	Description string `json:"description,omitempty"`
	Error       error  `json:"error,omitempty"`
}

// AssetParams represents recordings parameters.
type AssetParams struct {
	Status        string `json:"status,omitempty"`
	Description   string `json:"description,omitempty"`
	Type          string `json:"type,omitempty"`
	Client        string `json:"client,omitempty"`
	AssetFilename string `json:"asset_filename,omitempty"`
	AssetUid      string `json:"asset_uid,omitempty"`
	ChannelUid    string `json:"channel_uid,omitempty"`
	Start         string `json:"start,omitempty"`
	End           string `json:"end,omitempty"`
	Check         string `json:"check,omitempty"`
	Id            int    `json:"id,omitempty"`
}

// ChParams represents channel parameters.
type ChParams struct {
	Status      string `json:"status,omitempty"`
	Description string `json:"description,omitempty"`
	Type        string `json:"type,omitempty"`
	Client      string `json:"client,omitempty"`
	ChannelUid  string `json:"channel_uid,omitempty"`
	Address     string `json:"address,omitempty"`
	Port        string `json:"port,omitempty"`
}

// TaskProps represents scheduled task properties.
type TaskProps struct {
	StartTimer *time.Timer
	EndTimer   *time.Timer
	Channel    chan string
	Start      int64
	End        int64
	ChannelUid string
}

// Methods represents a collection of methods used by jsonrpc.
type Methods struct {
	db    *Database
	cfg   *Config
	iface *net.Interface
	tasks map[string]*TaskProps
}

// Init initiates a persistent key/value store, sets a recording interface
// and declares a tasks map to store scheduled jobs.
// It returns an error if any init operation fails.
func (m *Methods) Init(cfg *Config) error {
	var err error

	// Connect to persistent key/value store
	m.db = &Database{}
	m.db.inst, err = leveldb.OpenFile(cfg.opts["database"], nil)
	if err != nil {
		return err
	}
	log.Println("Database opened")

	// Get recording interface
	m.iface, err = net.InterfaceByName(cfg.opts["interface"])
	if err != nil {
		return err
	}
	log.Println("Capture interface set to", cfg.opts["interface"])
	m.cfg = cfg

	// Initiate tasks store
	m.tasks = make(map[string]*TaskProps)

	// Reschedule unfinished tasks
	log.Println("Rescheduling pending tasks...")
	iter := m.db.inst.NewIterator(nil, nil)
	for iter.Next() {
		value := iter.Value()
		params := AssetParams{}

		if err := json.Unmarshal(value, &params); err != nil {
			log.Println("Init iter.Next json.Unmarshal error:", err)
			continue
		}
		if params.Type != "recording" || params.Status == "ready" {
			continue
		}
		if err := m.ScheduleRecording(&params, nil); err != nil {
			log.Println("Error (re)scheduling:", err)
		}
	}
	iter.Release()

	return nil
}

func (m *Methods) EditConfFile(section, name, value string) error {
	key, err := m.cfg.fh.Section(section).GetKey(name)
	if err != nil {
		log.Println("Error getting key:", err)
		return err
	}
	key.SetValue(value)

	if err := m.cfg.fh.SaveTo(*cfgFlag); err != nil {
		log.Println("Error storing key modifications to file:", err)
		return err
	}
	return nil
}

// GetInterface method returns the name of the recording interface.
// This method is rpc.Register compliant.
func (m *Methods) GetInterface(params, reply *GenericReply) error {
	*reply = GenericReply{Status: "OK", Description: m.iface.Name}
	return nil
}

// SetInterface method sets the recording interface to the provided value.
// This method is rpc.Register compliant.
func (m *Methods) SetInterface(iface string, reply *GenericReply) error {
	var err error
	m.iface, err = net.InterfaceByName(iface)
	if err != nil {
		*reply = GenericReply{Status: "error", Description: err.Error()}
		return err
	}

	// Set to running configuration
	m.cfg.opts["Interface"] = m.iface.Name
	// Set to permanent configuration (file)
	if err := m.EditConfFile("", "interface", m.iface.Name); err != nil {
		*reply = GenericReply{Status: "error", Description: err.Error()}
		return err
	}

	log.Println("Capture interface set to", m.iface.Name)
	*reply = GenericReply{Status: "OK"}
	return nil
}

// CheckAsset method returns current asset's state
// This method is rpc.Register compliant.
func (m *Methods) CheckAsset(params *AssetParams, reply *GenericReply) error {
	// TODO: check if a file physically exists ("nonexistent" status)
	// if _, err := os.Stat(filename); os.IsNotExist(err) {
	data, err := m.db.inst.Get([]byte(params.AssetUid), nil)
	if err != nil && err.Error() == "leveldb: not found" {
		*reply = GenericReply{Status: "OK", Description: "Asset not found"}
		return nil
	} else if err != nil {
		log.Println("GetRecording m.db.inst.Get error:", err)
		*reply = GenericReply{Status: "error", Description: err.Error()}
		return err
	}

	dec := json.NewDecoder(bytes.NewReader(data))
	for {
		if err := dec.Decode(&params); err == io.EOF {
			break
		} else if err != nil {
			log.Println("CheckAsset dec.Decode error:", err)
			*reply = GenericReply{Status: "error", Description: err.Error()}
			return err
		}
	}
	if params.Status == "ready" || params.Status == "modified" {
		md5, err := createAssetHash(m.cfg.opts["mediadir"] + "/" + params.AssetFilename)
		if err != nil {
			log.Println("CheckAsset: Failed to create md5 for asset", params.AssetUid, err)
			*reply = GenericReply{Status: "error", Description: err.Error()}
			return err
		}
		if params.Check == md5 && params.Status != "ready" {
			params.Status = "ready"
		} else if params.Check != md5 && params.Status != "modified" {
			params.Status = "modified"
		}

		// Set state to "modified" or "ready"
		uparams := AssetParams{
			Status:        params.Status,
			Type:          params.Type,
			Client:        params.Client,
			AssetFilename: params.AssetFilename,
			AssetUid:      params.AssetUid,
			ChannelUid:    params.ChannelUid,
			Start:         params.Start,
			End:           params.End,
			Check:         params.Check,
			Id:            params.Id,
		}
		j, err := json.Marshal(uparams)
		if err != nil {
			log.Println("CheckAsset json.Marshal error:", err)
			*reply = GenericReply{Status: "error", Description: err.Error()}
			return err
		}
		if err := m.db.inst.Put([]byte(params.AssetUid), j, nil); err != nil {
			log.Println("CheckAsset m.db.inst.Put error:", err)
			*reply = GenericReply{Status: "error", Description: err.Error()}
			return err
		}
	}

	*reply = GenericReply{Status: "OK", Description: params.Status}
	return nil
}

// AddChannel method is used to add new channels to persistent store.
// This method is rpc.Register compliant.
func (m *Methods) AddChannel(params *ChParams, reply *GenericReply) error {
	j, err := json.Marshal(params)
	if err != nil {
		log.Println("AddChannel json.Marshal error:", err)
		*reply = GenericReply{Status: "error", Description: err.Error()}
		return err
	}

	has, err := m.db.inst.Has([]byte(params.ChannelUid), nil)
	if err != nil {
		log.Println("AddChannel m.db.inst.Has error:", err)
		*reply = GenericReply{Status: "error", Description: err.Error()}
		return err
	}
	if has == false {
		log.Println("Adding new channel:", params.ChannelUid)
		if err := m.db.inst.Put([]byte(params.ChannelUid), j, nil); err != nil {
			log.Println("AddChannel json.Marshal error:", err)
			*reply = GenericReply{Status: "error", Description: err.Error()}
			return err
		}
	} else {
		*reply = GenericReply{Status: "OK", Description: "Channel already exists"}
		return nil
	}

	*reply = GenericReply{Status: "OK"}
	return nil
}

// ModifyChannel method is used to modify channel properties
// This method is rpc.Register compliant.
func (m *Methods) ModifyChannel(params *ChParams, reply *GenericReply) error {
	data, err := m.db.inst.Get([]byte(params.ChannelUid), nil)
	if err != nil && err.Error() == "leveldb: not found" {
		*reply = GenericReply{Status: "OK", Description: "Channel not found"}
		return nil
	} else if err != nil {
		log.Println("ModifyChannel m.db.inst.Get error:", err)
		*reply = GenericReply{Status: "error", Description: err.Error()}
		return err
	}

	var channel ChParams
	if err := json.Unmarshal(data, &channel); err != nil {
		log.Println("ModifyChannel json.Unmarshal error:", err)
		return err
	}

	// Copy old values if new ones are not provided
	// goleveldb doesn't support modifying
	// FIXME: is there a smarter way?
	if params.Type != "" {
		channel.Type = params.Type
	}
	if params.Address != "" {
		channel.Address = params.Address
	}
	if params.Port != "" {
		channel.Port = params.Port
	}
	if params.Client != "" {
		channel.Client = params.Client
	}

	j, err := json.Marshal(channel)
	if err != nil {
		log.Println("AddChannel json.Marshal error:", err)
		*reply = GenericReply{Status: "error", Description: err.Error()}
		return err
	}

	log.Println("Modifying channel:", params.ChannelUid)
	if err := m.db.inst.Put([]byte(params.ChannelUid), j, nil); err != nil {
		log.Println("AddChannel json.Marshal error:", err)
		*reply = GenericReply{Status: "error", Description: err.Error()}
		return err
	}

	*reply = GenericReply{Status: "OK"}
	return nil
}

// DeleteChannel method is used to delete a channel from persistent store.
// It also stops all running/scheduled tasks associated with that channel!
// This method is rpc.Register compliant.
func (m *Methods) DeleteChannel(params *ChParams, reply *GenericReply) error {
	// TODO: check if channel exists before deleting it
	/*
		data, err := m.db.inst.Get([]byte(params.ChannelUid), nil)
		if err != nil && err.Error() == "leveldb: not found" {
			*reply = GenericReply{Status: "OK", Description: "Channel not found"}
			return nil
		}
	*/

	// Find and stop tasks on that channel
	for recUid := range m.tasks {
		if m.tasks[recUid].ChannelUid != params.ChannelUid {
			continue
		}

		s := m.tasks[recUid].StartTimer.Stop()
		if s == true {
			log.Println("Task " + recUid + " stopped and removed")
		} else {
			log.Println("Task " + recUid + " already stopped or expired")
		}

		// Stop an associated recording
		if time.Now().Unix() > m.tasks[recUid].Start && time.Now().Unix() < m.tasks[recUid].End {
			m.tasks[recUid].Channel <- "stop"
		}

		// Delete an associated recording task (timer) from pool
		delete(m.tasks, recUid)

		// Delete an associated asset from db
		if err := m.db.inst.Delete([]byte(recUid), nil); err != nil {
			log.Println("DeleteChannel asset del m.db.inst.Delete error:", err)
			*reply = GenericReply{Status: "error", Description: err.Error()}
			return err
		}
	}

	// Finally, delete a channel
	log.Println("Deleting channel:", params.ChannelUid)
	if err := m.db.inst.Delete([]byte(params.ChannelUid), nil); err != nil {
		log.Println("DeleteChannel m.db.inst.Delete error:", err)
		*reply = GenericReply{Status: "error", Description: err.Error()}
		return err
	}

	*reply = GenericReply{Status: "OK"}
	return nil
}

// GetRecording method returns all recording parameters for a given id.
// This method is rpc.Register compliant.
func (m *Methods) GetRecording(params, reply *AssetParams) error {
	data, err := m.db.inst.Get([]byte(params.AssetUid), nil)
	if err != nil && err.Error() == "leveldb: not found" {
		*reply = AssetParams{Status: "OK", Description: "Not found"}
		return nil
	} else if err != nil {
		log.Println("GetRecording m.db.inst.Get error:", err)
		*reply = AssetParams{Status: "error", Description: err.Error()}
		return err
	}

	var results AssetParams
	if err := json.Unmarshal(data, &results); err != nil {
		log.Println("GetRecording json.Unmarshal error:", err)
		return err
	}

	*reply = results
	return nil
}

// ScheduleRecording method schedules a given recording according to provided parameters.
// This method is rpc.Register compliant.
func (m *Methods) ScheduleRecording(recData, reply *AssetParams) error {
	// Check if asset with specified uid already exists
	r, err := m.db.inst.Get([]byte(recData.AssetUid), nil)
	if err != nil && err.Error() != "leveldb: not found" {
		log.Println("ScheduleRecording m.db.inst.Get error:", err)
		if reply != nil {
			*reply = AssetParams{Status: "error", Description: err.Error()}
		}
		return err
	}
	if _, ok := m.tasks[recData.AssetUid]; ok {
		log.Println("Asset already scheduled:", recData.AssetUid)
		if reply != nil {
			*reply = AssetParams{Status: "OK", Description: "Already scheduled"}
		}
		return nil
	}
	var ri AssetParams
	err = json.Unmarshal(r, &ri)
	if err != nil && err.Error() != "unexpected end of JSON input" {
		log.Println("ScheduleRecording json.Unmarshal error:", err)
	}
	if ri.Status == "ready" {
		log.Println("Asset already done processing:", recData.AssetUid)
		if reply != nil {
			*reply = AssetParams{Status: "OK", Description: "Already processed"}
		}
		return nil
	}

	recData.Type = "recording"

	// Get start/end times in unix
	uTime, err := UnixTime(m.cfg.opts["timelayout"], []string{recData.Start, recData.End})
	if err != nil {
		log.Println("UnixTime error:", err)
		if reply != nil {
			*reply = AssetParams{Status: "error", Description: err.Error()}
		}
		return err
	}

	// Get current local time in unix and duration in seconds
	now := time.Now().Unix()
	if uTime[recData.End] < now {
		if reply != nil {
			*reply = AssetParams{Status: "OK", Description: "End is in the past"}
		}
		return nil
	}
	if uTime[recData.Start] > uTime[recData.End] {
		if reply != nil {
			*reply = AssetParams{Status: "OK", Description: "Start after end"}
		}
		return nil
	}
	if uTime[recData.Start] <= now {
		uTime[recData.Start] = now
	}
	dur := uTime[recData.End] - uTime[recData.Start]
	// HACK: only needed for Init to call this function
	recType := recData.Type
	recClient := recData.Client
	recCh := recData.ChannelUid
	recFname := recData.AssetFilename
	recUid := recData.AssetUid
	recStart := recData.Start
	recEnd := recData.End
	recId := recData.Id

	// Create a channel that will be used to talk to a goroutine
	ch := make(chan string)

	// Set state to "scheduled"
	recData.Status = "scheduled"
	j, err := json.Marshal(recData)
	if err != nil {
		log.Println("ScheduleRecording json.Marshal error:", err)
		if reply != nil {
			*reply = AssetParams{Status: "error", Description: err.Error()}
		}
		return err
	}

	if err := m.db.inst.Put([]byte(recData.AssetUid), j, nil); err != nil {
		log.Println("m.db.inst.Put error:", err)
		if reply != nil {
			*reply = AssetParams{Status: "error", Description: err.Error()}
		}
		return err
	}

	// Start timer which will trigger the recording goroutine
	log.Println("Scheduling asset:", recData.AssetUid, recData.AssetFilename, recData.Start, recData.End)
	stimer := time.AfterFunc(time.Duration(uTime[recData.Start]-now)*time.Second, func() {
		data, err := m.db.inst.Get([]byte(recCh), nil)
		if err != nil {
			log.Println("time.AfterFunc m.db.inst.Get error for " + recData.ChannelUid + ": " + err.Error())
			return
		}

		var chdata ChParams
		if err := json.Unmarshal(data, &chdata); err != nil {
			log.Println("time.AfterFunc json.Unmarshal error:", err)
			return
		}

		// Run Recorder and set a timer function to stop recording when End time is reached
		go Recorder(m.iface, m.cfg.opts["mediadir"], recFname, chdata.Address, chdata.Port, chdata.Type, ch)

		// Set state to "processing"
		uparams := AssetParams{
			Status:        "processing",
			Type:          recType,
			Client:        recClient,
			AssetFilename: recFname,
			AssetUid:      recUid,
			ChannelUid:    recCh,
			Start:         recStart,
			End:           recEnd,
			Id:            recId,
		}
		j, err := json.Marshal(uparams)
		if err != nil {
			log.Println("ScheduleRecording json.Marshal error:", err)
			return
		}
		if err := m.db.inst.Put([]byte(recUid), j, nil); err != nil {
			log.Println("m.db.inst.Put error:", err)
			return
		}

		// Start timer which will stop the recording goroutine
		m.tasks[recData.AssetUid].EndTimer = time.AfterFunc(time.Duration(dur)*time.Second, func() {
			ch <- "stop"

			// Create a simple md5 sum
			md5, err := createAssetHash(m.cfg.opts["mediadir"] + "/" + recFname)
			if err != nil {
				log.Println("ScheduleRecording: Failed to create md5 for asset", recFname, err)
				md5 = ""
			}

			// Set state to "ready"
			uparams := AssetParams{
				Status:        "ready",
				Type:          recType,
				Client:        recClient,
				AssetFilename: recFname,
				AssetUid:      recUid,
				ChannelUid:    recCh,
				Start:         recStart,
				End:           recEnd,
				Check:         md5,
				Id:            recId,
			}
			j, err := json.Marshal(uparams)
			if err != nil {
				log.Println("ScheduleRecording json.Marshal error:", err)
				return
			}
			if err := m.db.inst.Put([]byte(recUid), j, nil); err != nil {
				log.Println("m.db.inst.Put error:", err)
				return
			}

			delete(m.tasks, recUid)
		})
	})

	m.tasks[recData.AssetUid] = &TaskProps{
		StartTimer: stimer,
		Channel:    ch,
		Start:      uTime[recData.Start],
		End:        uTime[recData.End],
		ChannelUid: recData.ChannelUid,
	}

	if reply != nil {
		*reply = AssetParams{Status: "OK"}
	}
	return nil
}

// ModifyRecording method reschedules a recording according to provided parameters.
// This method is rpc.Register compliant.
func (m *Methods) ModifyRecording(recData, reply *AssetParams) error {
	// Get start/end times in unix
	uTime, err := UnixTime(m.cfg.opts["timelayout"], []string{recData.Start, recData.End})
	if err != nil {
		log.Println("UnixTime error:", err)
		if reply != nil {
			*reply = AssetParams{Status: "error", Description: err.Error()}
		}
		return err
	}

	data, err := m.db.inst.Get([]byte(recData.AssetUid), nil)
	if err != nil && err.Error() == "leveldb: not found" {
		*reply = AssetParams{Status: "OK", Description: "Not found"}
		return nil
	} else if err != nil {
		log.Println("GetRecording m.db.inst.Get error:", err)
		*reply = AssetParams{Status: "error", Description: err.Error()}
		return err
	}

	var currParams AssetParams
	if err := json.Unmarshal(data, &currParams); err != nil {
		log.Println("GetRecording json.Unmarshal error:", err)
		return err
	}

	// Check if job is processing and modify timer
	now := time.Now().Unix()

	if currParams.Status == "ready" {
		log.Println("Cannot modify asset which is already done processing")
		if reply != nil {
			*reply = AssetParams{Status: "error", Description: "Asset already done processing"}
		}
		return err
	}

	if uTime[recData.End] < now {
		log.Println("End is in the past! (placeholder)")
		if reply != nil {
			*reply = AssetParams{Status: "error", Description: "End is in the past"}
		}
		return err
	}

	if uTime[recData.Start] < now && uTime[recData.End] > now && currParams.Status == "processing" {
		if m.tasks[recData.AssetUid].EndTimer.Reset(time.Duration(uTime[recData.End]-now)*time.Second) == false {
			*reply = AssetParams{Status: "error", Description: "Already stopped or expired"}
			return err
		}
		log.Println("Task end time reset for asset uid " + recData.AssetUid + " done")

		m.tasks[recData.AssetUid].End = uTime[recData.End]

		// Update task properties in database
		currParams.End = recData.End
		j, err := json.Marshal(currParams)
		if err != nil {
			log.Println("ModifyRecording json.Marshal error:", err)
			if reply != nil {
				*reply = AssetParams{Status: "error", Description: err.Error()}
			}
			return err
		}

		if err := m.db.inst.Put([]byte(recData.AssetUid), j, nil); err != nil {
			log.Println("m.db.inst.Put error:", err)
			if reply != nil {
				*reply = AssetParams{Status: "error", Description: err.Error()}
			}
			return err
		}

		*reply = AssetParams{Status: "OK"}
		return nil
	}

	if err := m.DeleteRecording(recData, nil); err != nil {
		log.Println("ModifyRecording: Error deleting schedule:", err)
		if reply != nil {
			*reply = AssetParams{Status: "error", Description: err.Error()}
		}
		return err
	}
	if err := m.ScheduleRecording(recData, nil); err != nil {
		log.Println("ModifyRecording: Error (re)scheduling:", err)
		if reply != nil {
			*reply = AssetParams{Status: "error", Description: err.Error()}
		}
		return err
	}

	*reply = AssetParams{Status: "OK"}
	return nil
}

// DeleteRecording method deletes a recording according to provided parameters.
// This method is rpc.Register compliant.
func (m *Methods) DeleteRecording(params *AssetParams, reply *GenericReply) error {
	// Stop and delete a scheduled task (timer)
	if _, ok := m.tasks[params.AssetUid]; ok {
		s := m.tasks[params.AssetUid].StartTimer.Stop()
		if s == true {
			log.Println("Start timer for task " + params.AssetUid + " stopped and removed")
		} else {
			log.Println("Start timer for task " + params.AssetUid + " already expired")
		}

		// Stop recording
		// TODO: also check by asset Status
		if time.Now().Unix() > m.tasks[params.AssetUid].Start && time.Now().Unix() < m.tasks[params.AssetUid].End {
			m.tasks[params.AssetUid].Channel <- "stop"
		}

		// Delete a task (timer) from pool
		delete(m.tasks, params.AssetUid)
	}

	// Delete asset from db
	if err := m.db.inst.Delete([]byte(params.AssetUid), nil); err != nil {
		log.Println("DeleteRecording m.db.inst.Delete error:", err)
		return err
	}
	log.Println("Asset " + params.AssetUid + " removed")

	absf := m.cfg.opts["mediadir"] + "/" + params.AssetFilename

	// Delete file from fs (if it exists)
	if _, err := os.Stat(absf); err == nil {
		log.Println("Deleting asset:", absf)
		if err := os.Remove(absf); err != nil {
			log.Println("DeleteRecording os.Remove error:", err)
			return err
		}
	}

	if reply != nil {
		*reply = GenericReply{Status: "OK"}
	}
	return nil
}

// UnixTime returns a map of times converted to unix.
// Returns an error if parsing of provided strings fails.
func UnixTime(format string, atimes []string) (map[string]int64, error) {
	m := make(map[string]int64)

	for i, elem := range atimes {
		t, err := time.Parse(format, elem)
		if err != nil {
			log.Println("time.Parse error:", err)
			return nil, err
		}

		m[atimes[i]] = t.Unix()
	}

	return m, nil
}

// Recorder function uses the provided network interface and url to record content.
func Recorder(iface *net.Interface, recdir, filename, mcast, port, stype string, ch <-chan string) {
	ip := net.ParseIP(mcast)
	group := net.IPv4(ip[12], ip[13], ip[14], ip[15])

	localSock, err := net.ListenPacket("udp4", mcast+":"+port)
	if err != nil {
		log.Println("net.ListenPacket failed")
		return
	}

	defer localSock.Close()
	pktSock := ipv4.NewPacketConn(localSock)

	if err := pktSock.SetControlMessage(ipv4.FlagDst, true); err != nil {
		log.Println("pktSock.SetControlMessage failed for", mcast, port)
		return
	}

	if err := pktSock.JoinGroup(iface, &net.UDPAddr{IP: group}); err != nil {
		log.Println("Failed to join mcast", mcast, port)
		return
	}

	// TODO: check if file already exists and mv old (?)
	// FIXME: check if same recording and file already exists (before this func?)
	file, err := os.OpenFile(recdir+"/"+filename, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println("error opening file for appending", filename, err)
	}

	defer file.Close()

	pktSock.SetMulticastInterface(iface)

	log.Println("Recording asset:", filename)

REC:
	for {
		// Check if parent called 'stop'
		select {
		case msg := <-ch:
			if msg == "stop" {
				log.Println("Stop recording asset with filename:", filename)

				break REC
			}
		default:
			break
		}

		pkt := make([]byte, 1500)
		n, cmsg, _, err := pktSock.ReadFrom(pkt)
		if err != nil {
			continue
		}

		// Check if created packet buffer is too large and slice it if needed
		if len(pkt) > n {
			newPkt := make([]byte, n)
			copy(newPkt, pkt)
			pkt = newPkt
		}

		// If stream is via RTP, slice off rtp header
		if stype == "rtp" {
			pkt = pkt[12:]
		}

		// Store file
		if cmsg.Dst.IsMulticast() {
			if cmsg.Dst.Equal(group) {
				_, err := file.Write(pkt)
				if err != nil {
					log.Println("error writing to file")
					return
				}
			} else {
				continue
			}
		}
	}

	// Index file when recording ends
	log.Println("Indexing file", filename)
	cmd := exec.Command("/bvodindexer", recdir+"/"+filename, recdir+"/"+filename+".idx")
	err = cmd.Start()
	if err != nil {
		log.Println("indexing error for", filename, err)
	}
	err = cmd.Wait()
	if err != nil {
		log.Println("bvodindexer process error:", err)
	}
}

// Function createAssetHash uses a 1K start, end and length of file to create md5.
func createAssetHash(filename string) (string, error) {
	f, err := os.Open(filename)
	if err != nil {
		log.Println(err)
		return "", err
	}
	b := make([]byte, 1024)
	e := b[:]
	l := b[:]

	if _, err := f.Read(b); err != nil {
		log.Println("Read:", err)
		return "", err
	}
	fi, err := f.Stat()
	if err != nil {
		log.Println("Stat:", err)
		return "", err
	}
	si := fi.Size()
	f.ReadAt(e, si-int64(len(e)))
	binary.PutVarint(l, si)

	out := [][]byte{b, e, l}
	j := []byte{}
	j = bytes.Join(out, nil)
	sum := md5.Sum(j)

	return hex.EncodeToString(sum[:]), nil
}

func setConfig(cfg *Config) {
	if *portFlag != "" {
		cfg.opts["port"] = *portFlag
	} else if *portFlag == "" && cfg.opts["port"] == "" {
		cfg.opts["port"] = "50280"
	}
	if *ifaceFlag != "" {
		cfg.opts["interface"] = *ifaceFlag
	} else if *ifaceFlag == "" && cfg.opts["interface"] == "" {
		cfg.opts["interface"] = "eth0"
	}
	if *mdirFlag != "" {
		cfg.opts["mediadir"] = *mdirFlag
	} else if *mdirFlag == "" && cfg.opts["mediadir"] == "" {
		cfg.opts["mediadir"] = "/media"
	}
	if *dbFlag != "" {
		cfg.opts["database"] = *dbFlag
	} else if *dbFlag == "" && cfg.opts["database"] == "" {
		cfg.opts["database"] = "/var/lib/gorecord/db"
	}
}

func main() {
	flag.Parse()

	if flag.NFlag() < 1 {
		fmt.Println("gorecord 0.0.3\n\nUsage:")
		flag.PrintDefaults()
		os.Exit(0)
	}

	// TODO: Command-line interface client
	if *srvFlag == false {
		os.Exit(0)
	}

	// Server
	fh, err := ini.Load(*cfgFlag)
	if err != nil && *srvFlag == true {
		log.Fatalln("Error reading config file:", err)
	}
	cfg := Config{fh.Section("").KeysHash(), fh}

	// We cannot set defaults via `flag` because we have to check for ini first
	setConfig(&cfg)

	meth := Methods{}
	if err := meth.Init(&cfg); err != nil {
		log.Fatalln("meth.Init() error:", err)
	}
	defer meth.db.inst.Close()

	sock, err := net.Listen("tcp", ":"+cfg.opts["port"])
	if err != nil {
		log.Println(err)
		return
	}
	defer sock.Close()
	log.Println("JSONRPC Server listening on port " + cfg.opts["port"])

	rpc.Register(&meth)

	for {
		conn, err := sock.Accept()
		if err != nil {
			log.Println(err)
			return
		}
		log.Println("New client connection from", conn.RemoteAddr().String())

		go jsonrpc.ServeConn(conn)
	}
}
