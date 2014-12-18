package main

// TODO: detect if udp/rtp: mod 188 (pkt size) = udp; mod 12 or more = rtp

import (
	"code.google.com/p/gcfg"
	"code.google.com/p/go.net/ipv4"
	"encoding/json"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"time"
)

// Config represents main configuration parameters.
type Config struct {
	Main struct {
		Interface  string
		Port       string
		Location   string
		Timelayout string
		Mediadir   string
	}
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

// RecParams represents recordings parameters.
type RecParams struct {
	Status       string `json:"status,omitempty"`
	Description  string `json:"description,omitempty"`
	Type         string `json:"type,omitempty"`
	Client       string `json:"client,omitempty"`
	RecordingUid string `json:"recording_uid,omitempty"`
	ChannelUid   string `json:"channel_uid,omitempty"`
	Start        string `json:"start,omitempty"`
	End          string `json:"end,omitempty"`
	Id           int    `json:"id,omitempty"`
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
	Timer      *time.Timer
	Channel    chan string
	Start      int64
	End        int64
	ChannelUid string
}

// Methods represents a collection of methods used by jsonrpc.
type Methods struct {
	db    *Database
	loc   *time.Location
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
	m.db.inst, err = leveldb.OpenFile("db", nil)
	if err != nil {
		return err
	}
	fmt.Println("Database opened")

	// Get recording interface
	m.iface, err = net.InterfaceByName(cfg.Main.Interface)
	if err != nil {
		return err
	}
	fmt.Println("Capture interface configured")
	m.cfg = cfg

	// Initiate tasks store
	m.tasks = make(map[string]*TaskProps)

	// Reschedule unfinished tasks
	fmt.Println("Rescheduling pending tasks...")
	iter := m.db.inst.NewIterator(nil, nil)
	for iter.Next() {
		value := iter.Value()
		params := RecParams{}

		if err := json.Unmarshal(value, &params); err != nil {
			fmt.Println("Init iter.Next json.Unmarshal error:", err)
			continue
		}
		if params.Type != "recording" {
			continue
		}
		if err := m.ScheduleRecording(&params, &params); err != nil {
			fmt.Println("Error (re)scheduling:", err)
		}
	}
	iter.Release()

	return nil
}

// GetInterface method returns the name of the recording interface.
// This method is rpc.Register compliant.
func (m *Methods) GetInterface(params, reply *GenericReply) error {
	*reply = GenericReply{Status: "OK", Description: m.iface.Name}
	return nil
}

// AddChannel method is used to add new channels to persistent store.
// This method is rpc.Register compliant.
func (m *Methods) AddChannel(params *ChParams, reply *GenericReply) error {
	j, err := json.Marshal(params)
	if err != nil {
		fmt.Println("AddChannel json.Marshal error:", err)
		*reply = GenericReply{Status: "error", Description: err.Error()}
		return err
	}

	has, err := m.db.inst.Has([]byte(params.ChannelUid), nil)
	if err != nil {
		fmt.Println("AddChannel m.db.inst.Has error:", err)
		*reply = GenericReply{Status: "error", Description: err.Error()}
		return err
	}
	if has == false {
		fmt.Println("Adding new channel:", params.ChannelUid)
		if err := m.db.inst.Put([]byte(params.ChannelUid), j, nil); err != nil {
			fmt.Println("AddChannel json.Marshal error:", err)
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
		fmt.Println("GetRecording m.db.inst.Get error:", err)
		*reply = GenericReply{Status: "error", Description: err.Error()}
		return err
	}

	var channel ChParams
	if err := json.Unmarshal(data, &channel); err != nil {
		fmt.Println("GetRecording json.Unmarshal error:", err)
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
		fmt.Println("AddChannel json.Marshal error:", err)
		*reply = GenericReply{Status: "error", Description: err.Error()}
		return err
	}

	fmt.Println("Modifying channel:", params.ChannelUid)
	if err := m.db.inst.Put([]byte(params.ChannelUid), j, nil); err != nil {
		fmt.Println("AddChannel json.Marshal error:", err)
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

		s := m.tasks[recUid].Timer.Stop()
		if s == true {
			fmt.Println("Task " + recUid + " stopped and removed")
		} else {
			fmt.Println("Task " + recUid + " already stopped or expired")
		}

		// Stop an associated recording
		if time.Now().Unix() > m.tasks[recUid].Start && time.Now().Unix() < m.tasks[recUid].End {
			m.tasks[recUid].Channel <- "stop"
		}

		// Delete an associated recording task (timer) from pool
		delete(m.tasks, recUid)

		// Delete an associated asset from db
		if err := m.db.inst.Delete([]byte(recUid), nil); err != nil {
			fmt.Println("DeleteChannel asset del m.db.inst.Delete error:", err)
			*reply = GenericReply{Status: "error", Description: err.Error()}
			return err
		}
	}

	// Finally, delete a channel
	fmt.Println("Deleting channel:", params.ChannelUid)
	if err := m.db.inst.Delete([]byte(params.ChannelUid), nil); err != nil {
		fmt.Println("DeleteChannel m.db.inst.Delete error:", err)
		*reply = GenericReply{Status: "error", Description: err.Error()}
		return err
	}

	*reply = GenericReply{Status: "OK"}
	return nil
}

// GetRecording method returns all recording parameters for a given id.
// This method is rpc.Register compliant.
func (m *Methods) GetRecording(params, reply *RecParams) error {
	data, err := m.db.inst.Get([]byte(params.RecordingUid), nil)
	if err != nil && err.Error() == "leveldb: not found" {
		*reply = RecParams{Status: "OK", Description: "Not found"}
		return nil
	} else if err != nil {
		fmt.Println("GetRecording m.db.inst.Get error:", err)
		*reply = RecParams{Status: "error", Description: err.Error()}
		return err
	}

	var results RecParams
	if err := json.Unmarshal(data, &results); err != nil {
		fmt.Println("GetRecording json.Unmarshal error:", err)
		return err
	}

	*reply = results
	return nil
}

// ScheduleRecording method schedules a given recording according to provided parameters.
// This method is rpc.Register compliant.
func (m *Methods) ScheduleRecording(recData, reply *RecParams) error {
	// Check if asset with specified uid already exists
	r, err := m.db.inst.Get([]byte(recData.RecordingUid), nil)
	if err != nil && err.Error() != "leveldb: not found" {
		fmt.Println("ScheduleRecording m.db.inst.Get error:", err)
		*reply = RecParams{Status: "error", Description: err.Error()}
		return err
	}
	if _, ok := m.tasks[recData.RecordingUid]; ok {
		fmt.Println("Asset already scheduled:", recData.RecordingUid)
		*reply = RecParams{Status: "OK", Description: "Already scheduled"}
		return nil
	}
	var ri RecParams
	err = json.Unmarshal(r, &ri)
	if err != nil && err.Error() != "unexpected end of JSON input" {
		fmt.Println("ScheduleRecording json.Unmarshal error:", err)
	}
	if ri.Status == "ready" {
		fmt.Println("Asset already done processing:", recData.RecordingUid)
		*reply = RecParams{Status: "OK", Description: "Already processed"}
		return nil
	}

	recData.Type = "recording"
	recData.Status = "processing"
	j, err := json.Marshal(recData)
	if err != nil {
		fmt.Println("ScheduleRecording json.Marshal error:", err)
		*reply = RecParams{Status: "error", Description: err.Error()}
		return err
	}

	if err := m.db.inst.Put([]byte(recData.RecordingUid), j, nil); err != nil {
		fmt.Println("m.db.inst.Put error:", err)
		*reply = RecParams{Status: "error", Description: err.Error()}
		return err
	}

	// Get start/end times in unix
	uTime, err := UnixTime(m.cfg.Main.Timelayout, []string{recData.Start, recData.End})
	if err != nil {
		fmt.Println("UnixTime error:", err)
		*reply = RecParams{Status: "error", Description: err.Error()}
		return err
	}

	// Get current local time in unix and duration in seconds
	now := time.Now().Unix()
	if uTime[recData.End] < now {
		*reply = RecParams{Status: "OK", Description: "End is in past"}
		return nil
	}
	if uTime[recData.Start] > uTime[recData.End] {
		*reply = RecParams{Status: "OK", Description: "Start after end"}
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
	recUid := recData.RecordingUid
	recStart := recData.Start
	recEnd := recData.End
	recId := recData.Id

	// Create a channel that will be used to talk to a goroutine
	ch := make(chan string)

	// Start timer which will trigger the recording goroutine
	fmt.Println("Scheduling asset:", recData.RecordingUid, recData.Start)
	timer := time.AfterFunc(time.Duration(uTime[recData.Start]-now)*time.Second, func() {
		data, err := m.db.inst.Get([]byte(recCh), nil)
		if err != nil {
			fmt.Println("time.AfterFunc m.db.inst.Get error for " + recData.ChannelUid + ":" + err.Error())
			return
		}

		var chdata ChParams
		if err := json.Unmarshal(data, &chdata); err != nil {
			fmt.Println("time.AfterFunc json.Unmarshal error:", err)
			return
		}

		// Run Recorder and set a timer function to stop recording when End time is reached
		go Recorder(m.iface, m.cfg.Main.Mediadir, recUid, chdata.Address, chdata.Port, chdata.Type, ch)
		time.AfterFunc(time.Duration(dur)*time.Second, func() {
			ch <- "stop"

			uparams := RecParams{
				Status:       "ready",
				Type:         recType,
				Client:       recClient,
				RecordingUid: recUid,
				ChannelUid:   recCh,
				Start:        recStart,
				End:          recEnd,
				Id:           recId,
			}
			j, err := json.Marshal(uparams)
			if err != nil {
				fmt.Println("ScheduleRecording json.Marshal error:", err)
				return
			}
			if err := m.db.inst.Put([]byte(recUid), j, nil); err != nil {
				fmt.Println("m.db.inst.Put error:", err)
				return
			}

			delete(m.tasks, recUid)
		})
	})

	m.tasks[recData.RecordingUid] = &TaskProps{
		Timer:      timer,
		Channel:    ch,
		Start:      uTime[recData.Start],
		End:        uTime[recData.End],
		ChannelUid: recData.ChannelUid,
	}

	if reply != nil {
		*reply = RecParams{Status: "OK"}
	}
	return nil
}

// ModifyRecording method reschedules a recording according to provided parameters.
// This method is rpc.Register compliant.
func (m *Methods) ModifyRecording(recData, reply *RecParams) error {
	if err := m.DeleteRecording(recData, nil); err != nil {
		fmt.Println("ModifyRecording: Error deleting schedule:", err)
		*reply = RecParams{Status: "error", Description: err.Error()}
		return err
	}
	if err := m.ScheduleRecording(recData, nil); err != nil {
		fmt.Println("ModifyRecording: Error (re)scheduling:", err)
		*reply = RecParams{Status: "error", Description: err.Error()}
		return err
	}

	*reply = RecParams{Status: "OK"}
	return nil
}

// DeleteRecording method deletes a recording according to provided parameters.
// This method is rpc.Register compliant.
func (m *Methods) DeleteRecording(params *RecParams, reply *GenericReply) error {
	// Stop and delete a scheduled task (timer)
	if _, ok := m.tasks[params.RecordingUid]; ok {
		s := m.tasks[params.RecordingUid].Timer.Stop()
		if s == true {
			fmt.Println("Task " + params.RecordingUid + " stopped and removed")
		} else {
			fmt.Println("Task " + params.RecordingUid + " already stopped or expired")
		}

		// Stop recording
		// TODO: also check by asset Status
		if time.Now().Unix() > m.tasks[params.RecordingUid].Start && time.Now().Unix() < m.tasks[params.RecordingUid].End {
			m.tasks[params.RecordingUid].Channel <- "stop"
		}

		// Delete a task (timer) from pool
		delete(m.tasks, params.RecordingUid)
	}

	// Delete asset from db
	if err := m.db.inst.Delete([]byte(params.RecordingUid), nil); err != nil {
		fmt.Println("DeleteRecording m.db.inst.Delete error:", err)
		return err
	}
	fmt.Println("Asset " + params.RecordingUid + " removed")

	absf := m.cfg.Main.Mediadir + "/" + params.RecordingUid

	// Delete file from fs (if it exists)
	if _, err := os.Stat(absf); err == nil {
		fmt.Println("Deleting asset:", absf)
		if err := os.Remove(absf); err != nil {
			fmt.Println("DeleteRecording os.Remove error:", err)
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
			fmt.Println("time.Parse error:", err)
			return nil, err
		}

		m[atimes[i]] = t.Unix()
	}

	return m, nil
}

// Recorder function uses the provided network interface and url to record content.
func Recorder(iface *net.Interface, recdir, uid, mcast, port, stype string, ch <-chan string) {
	ip := net.ParseIP(mcast)
	group := net.IPv4(ip[12], ip[13], ip[14], ip[15])

	localSock, err := net.ListenPacket("udp4", mcast+":"+port)
	if err != nil {
		fmt.Println("net.ListenPacket failed")
		return
	}

	defer localSock.Close()
	pktSock := ipv4.NewPacketConn(localSock)

	if err := pktSock.SetControlMessage(ipv4.FlagDst, true); err != nil {
		fmt.Println("pktSock.SetControlMessage failed for", mcast, port)
		return
	}

	if err := pktSock.JoinGroup(iface, &net.UDPAddr{IP: group}); err != nil {
		fmt.Println("Failed to join mcast", mcast, port)
		return
	}

	// TODO: check if file already exists and mv old (?)
	// FIXME: check if same recording and file already exists (before this func?)
	file, err := os.OpenFile(recdir+"/"+uid, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("error opening file for appending", uid, err)
	}

	defer file.Close()

	pktSock.SetMulticastInterface(iface)

	fmt.Println("Recording asset:", uid)

REC:
	for {
		// Check if parent called 'stop'
		select {
		case msg := <-ch:
			if msg == "stop" {
				fmt.Println("Stop recording asset:", uid)
				break REC
			}
		default:
			break
		}

		pkt := make([]byte, 1500)
		n, cmsg, _, err := pktSock.ReadFrom(pkt)
		if err != nil {
			fmt.Println("pktSock.ReadFrom failed")
			return
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
					fmt.Println("error writing to file")
					return
				}
			} else {
				continue
			}
		}
	}
}

func main() {
	var cfg Config
	if err := gcfg.ReadFileInto(&cfg, "./conf.gcfg"); err != nil {
		fmt.Println("Error reading config file:", err)
		os.Exit(1)
	}

	meth := Methods{}
	if err := meth.Init(&cfg); err != nil {
		fmt.Println("meth.Init() error:", err)
		os.Exit(1)
	}
	defer meth.db.inst.Close()

	sock, err := net.Listen("tcp", ":"+cfg.Main.Port)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer sock.Close()
	fmt.Println("JSONRPC Server listening on port " + cfg.Main.Port)

	rpc.Register(&meth)

	for {
		conn, err := sock.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println("New client connection from", conn.RemoteAddr().String())

		go jsonrpc.ServeConn(conn)
	}
}
