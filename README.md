Installation and requirements
-----------
    go get code.google.com/p/gcfg
    go get code.google.com/p/go.net/ipv4
    go get github.com/syndtr/goleveldb/leveldb
    go get gopkg.in/ini.v1

* Needs at least `go1.3.3` or newer.

If you want to build it statically, use this command:
    CGO_ENABLED=0 go build -a -installsuffix gorecord.go

Usage
-----------

Create/Edit configuration in the current git location (will be fixed with cli, eventually) as 'conf.gcfg':

    [main]
    interface = eth0
    port = 2000
    timelayout = 2006-01-02T15:04:05-0700
    mediadir = /home/mkozjak/tmp

* interface: Recording interface used (linux/bsd: ifconfig)
* port: Listening port for JSONRPC server
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

Running with Docker:

    docker run --name gorecord --net=host -it -v /path/to/conf/gorecord.ini:/conf/gorecord.ini docker.registry.com/gorecord:latest /gorecord -l -c conf/gorecord.ini

Documentation
-----------

Use godoc to view the documentation.

TODO
-----------

* create cli interface (so users can schedule/start recordings via cli or jsonrpc)
* autodetect multicast stream type (udp/rtp) (mod 188 (pkt size) = udp; mod 12 or more = rtp)
* add unicast support
* complete documentation
