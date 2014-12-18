Installation and requirements
-----------
    go get code.google.com/p/gcfg
    go get code.google.com/p/go.net/ipv4
    go get github.com/syndtr/goleveldb/leveldb

* Needs at least `go1.3.3` or newer.

Usage
-----------

Create/Edit configuration in the current git location (will be fixed with cli, eventually) as 'conf.gcfg':

    [main]
    interface = eth0
    port = 2000
    location = Europe/Zagreb
    timelayout = 2006-01-02T15:04:05-0700
    mediadir = /home/mkozjak/tmp

* interface: Recording interface used (linux/bsd: ifconfig)
* port: Listening port for JSONRPC server
* location: Geolocation of your server/desktop
* timelayout: Datetime layout provided in a request to the JSONRPC server
* mediadir: Directory where streaming media will be stored (recorded)

Build and run the application with:

    go install path/to/recorder.go
    go run path/to/recorder.go

Before scheduling content, a channel needs to be registered (JSON-RPC 2.0):

    Method: 'Methods.AddChannel'
    Params: [{ 'channel_uid': 'mychannel', 'type': 'udp', 'address': '224.2.2.100', 'port': '1234' }]

Schedule/record content (JSON-RPC 2.0):

    Method: 'Methods.ScheduleRecording'
    Params: [{ 'recording_uid': '861', 'channel_uid': 'mychannel', 'start': '2015-12-07T16:42:00+0100', 'end': '2015-12-07T16:42:20+0100' }]

Documentation
-----------

Use godoc to view the documentation.

TODO
-----------

* create cli interface (so users can schedule/start recordings via cli or jsonrpc)
* autodetect multicast stream type (udp/rtp)
* add unicast support
* autodetect location
* set defaults (port, timelayout, location, mediadir)
* complete documentation