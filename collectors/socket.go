package collectors

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	measurement  = "ceph"
	typeMon      = "monitor"
	typeOsd      = "osd"
	typeClient   = "client"
	osdPrefix    = "ceph-osd"
	monPrefix    = "ceph-mon"
	clientPrefix = "ceph-client"
	sockSuffix   = "asok"
)

type Ceph struct {
	CephBinary             string
	OsdPrefix              string
	MonPrefix              string
	ClientPrefix           string
	SocketDir              string
	SocketSuffix           string
	CephUser               string
	CephConfig             string
	LibvirtPrefix          string
	GatherAdminSocketStats bool
	GatherClusterStats     bool
}

var findSockets = func(c *Ceph) ([]*socket, error) {
	listing, err := ioutil.ReadDir(c.SocketDir)
	if err != nil {
		return []*socket{}, fmt.Errorf("Failed to read socket directory '%s': %v", c.SocketDir, err)
	}
	sockets := make([]*socket, 0, len(listing))
	for _, info := range listing {
		f := info.Name()
		//fmt.Println(f)
		var sockType string
		var sockPrefix string
		if strings.HasPrefix(f, c.MonPrefix) {
			sockType = typeMon
			sockPrefix = monPrefix
		}
		if strings.HasPrefix(f, c.OsdPrefix) {
			sockType = typeOsd
			sockPrefix = osdPrefix
		}
		if strings.HasPrefix(f, c.ClientPrefix) {
			sockType = typeClient
			sockPrefix = clientPrefix
		}
		if sockType == typeOsd || sockType == typeMon || (sockType == typeClient && strings.Split(f, ".")[1] != "admin") {
			path := filepath.Join(c.SocketDir, f)
			sockets = append(sockets, &socket{parseSockId(f, sockPrefix, c.SocketSuffix), sockType, path})
		}
	}
	return sockets, nil
}
var perfDump = func(binary string, socket *socket) (string, error) {
	cmdArgs := []string{"--admin-daemon", socket.socket}
	if socket.sockType == typeOsd || socket.sockType == typeClient {
		cmdArgs = append(cmdArgs, "perf", "dump")
	} else if socket.sockType == typeMon {
		cmdArgs = append(cmdArgs, "perfcounters_dump")
	} else {
		return "", fmt.Errorf("ignoring unknown socket type: %s", socket.sockType)
	}

	cmd := exec.Command(binary, cmdArgs...)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return "", fmt.Errorf("error running ceph dump: %s", err)
	}

	return out.String(), nil
}

type metric struct {
	pathStack []string // lifo stack of name components
	value     float64
}

// Pops names of pathStack to build the flattened name for a metric
func (m *metric) name() string {
	buf := bytes.Buffer{}
	for i := len(m.pathStack) - 1; i >= 0; i-- {
		if buf.Len() > 0 {
			buf.WriteString(".")
		}
		buf.WriteString(m.pathStack[i])
	}
	return buf.String()
}

type metricMap map[string]interface{}

type taggedMetricMap map[string]metricMap

func parseDump(dump string) (taggedMetricMap, error) {
	data := make(map[string]interface{})
	err := json.Unmarshal([]byte(dump), &data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse json: '%s': %v", dump, err)
	}

	return newTaggedMetricMap(data), nil
}
func newTaggedMetricMap(data map[string]interface{}) taggedMetricMap {
	tmm := make(taggedMetricMap)
	for tag, datapoints := range data {
		mm := make(metricMap)
		for _, m := range flatten(datapoints) {
			mm[m.name()] = m.value
		}
		tmm[tag] = mm
	}
	return tmm
}

func flatten(data interface{}) []*metric {
	var metrics []*metric

	switch val := data.(type) {
	case float64:
		metrics = []*metric{&metric{make([]string, 0, 1), val}}
	case map[string]interface{}:
		metrics = make([]*metric, 0, len(val))
		for k, v := range val {
			for _, m := range flatten(v) {
				m.pathStack = append(m.pathStack, k)
				metrics = append(metrics, m)
			}
		}
	default:
		log.Printf("I! Ignoring unexpected type '%T' for value %v", val, val)
	}

	return metrics
}

func parseSockId(fname, prefix, suffix string) string {
	s := fname
	s = strings.TrimPrefix(s, prefix)
	s = strings.TrimSuffix(s, suffix)
	s = strings.Trim(s, ".-_")
	return s
}

type socket struct {
	sockId   string
	sockType string
	socket   string
}

func getInstanceID(s *socket) string {
	pid := strings.Split(s.sockId, ".")[1]

	cmdline := fmt.Sprintf("/proc/%s/cmdline", pid)

	content, err := ioutil.ReadFile(cmdline)
	if err != nil {
		//Do something
	}
	//Todo use trim
	rawid := strings.Split(string(content), "-")[3]
	id := strings.Split(rawid, "\x00")[0]
	return id
}

func (c *Ceph) getNameByInstanceID(instId string) (string, error) {
	xmlname := fmt.Sprintf("%sinstance-%s.xml", c.LibvirtPrefix, instId)

	xmlfile, err := os.Open(xmlname) // For read access.
	defer xmlfile.Close()
	if err != nil {
		fmt.Printf("open file error: %v", err)
		return "", err
	}
	data, err := ioutil.ReadAll(xmlfile)
	if err != nil {
		fmt.Printf("error: %v", err)
		return "", err
	}
	v := Domain{}
	err = xml.Unmarshal(data, &v)
	if err != nil {
		fmt.Printf("error: %v", err)
		return "", err
	}

	return v.NameInstanceMetadata, nil
}

type Domain struct {
	NameInstanceMetadata string `xml:"metadata>instance>name"`
}

// prometheus collect

const (
	cephClientNamespace = "ceph"
)

type ClientSocketUsageCollector struct {
	conn                Conn
	ReadBytes           *prometheus.GaugeVec
	WriteBytes          *prometheus.GaugeVec
	ReadWriteBytes      *prometheus.GaugeVec
	TotalReadBytes      *prometheus.GaugeVec
	TotalWriteBytes     *prometheus.GaugeVec
	TotalReadWriteBytes *prometheus.GaugeVec
}

// NewClientSocketUsageCollector creates a new instance of ClientSocketUsageCollector and returns
// its reference.
func NewClientSocketUsageCollector(conn Conn) *ClientSocketUsageCollector {
	var (
		subSystem = "qemu"
		//poolLabel = []string{"client"}
	)
	return &ClientSocketUsageCollector{
		conn: conn,

		ReadBytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: cephClientNamespace,
				Subsystem: subSystem,
				Name:      "read_bytes",
				Help:      "Capacity of the client that is currently read bytes",
			},
			[]string{"kvm"},
		),
		WriteBytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: cephClientNamespace,
				Subsystem: subSystem,
				Name:      "write_bytes",
				Help:      "Capacity of the client that is currently write bytes",
			},
			[]string{"kvm"},
		),
		ReadWriteBytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: cephClientNamespace,
				Subsystem: subSystem,
				Name:      "readwrite_bytes",
				Help:      "Capacity of the client that is currently total bytes",
			},
			[]string{"kvm"},
		),
		TotalReadBytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: cephClientNamespace,
				Subsystem: subSystem,
				Name:      "total_read_bytes",
				Help:      "TotalCapacity of the client that is currently read bytes",
			},
			[]string{"kvm"},
		),
		TotalWriteBytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: cephClientNamespace,
				Subsystem: subSystem,
				Name:      "total_write_bytes",
				Help:      "Capacity of the client that is currently write bytes",
			},
			[]string{"kvm"},
		),
		TotalReadWriteBytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: cephClientNamespace,
				Subsystem: subSystem,
				Name:      "total_readwrite_bytes",
				Help:      "Capacity of the client that is currently total bytes",
			},
			[]string{"kvm"},
		),
	}
}

func (p *ClientSocketUsageCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		p.ReadBytes,
		p.WriteBytes,
		p.ReadWriteBytes,
		p.TotalReadBytes,
		p.TotalWriteBytes,
		p.TotalReadWriteBytes,
	}
}

func (p *ClientSocketUsageCollector) collect() error {
	c := &Ceph{
		CephBinary:             "/usr/bin/ceph",
		OsdPrefix:              osdPrefix,
		MonPrefix:              monPrefix,
		ClientPrefix:           clientPrefix,
		SocketDir:              "/var/run/ceph/guests",
		SocketSuffix:           sockSuffix,
		CephUser:               "client.admin",
		CephConfig:             "/etc/ceph/ceph.conf",
		LibvirtPrefix:          "/etc/libvirt/qemu/",
		GatherAdminSocketStats: true,
		GatherClusterStats:     false,
	}

	sockets, err := findSockets(c)
	if err != nil {
		fmt.Println("socket err: ", sockets, err)
	}

	totalvals := make(map[string]float64)

	for _, s := range sockets {

		instId := getInstanceID(s)

		name, _ := c.getNameByInstanceID(instId)

		dump, err := perfDump(c.CephBinary, s)
		if err != nil {
			log.Printf("E! error reading from socket '%s': %v", s.socket, err)
			continue
		}
		data, _ := parseDump(dump)

		for tag, metric := range data {

			parseTag := strings.Split(tag, "-")
			if parseTag[0] == "librbd" {
				mLabel := fmt.Sprintf("%s-mount-%s-%s", name, parseTag[2], parseTag[7])

				p.ReadBytes.WithLabelValues(mLabel).Set(metric["rd_bytes"].(float64))
				totalvals[name+"-read"] += metric["rd_bytes"].(float64)

				p.WriteBytes.WithLabelValues(mLabel).Set(metric["wr_bytes"].(float64))
				totalvals[name+"-write"] += metric["wr_bytes"].(float64)
				p.ReadWriteBytes.WithLabelValues(mLabel).Set(metric["rd_bytes"].(float64) + metric["wr_bytes"].(float64))
				totalvals[name+"-readwrite"] += metric["rd_bytes"].(float64) + metric["wr_bytes"].(float64)

			}
		}
		mLabel := fmt.Sprintf("%s-all", name)
		p.TotalReadBytes.WithLabelValues(mLabel).Set(totalvals[name+"-read"])
		p.TotalWriteBytes.WithLabelValues(mLabel).Set(totalvals[name+"-write"])
		p.TotalReadWriteBytes.WithLabelValues(mLabel).Set(totalvals[name+"-readwrite"])
		if err != nil {
			log.Printf("E! error parsing dump from socket '%s': %v", s.socket, err)
			continue
		}
	}

	return nil
}

// Describe fulfills the prometheus.Collector's interface and sends the descriptors
// of client socket's metrics to the given channel.
func (p *ClientSocketUsageCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range p.collectorList() {
		metric.Describe(ch)
	}
}

// Collect extracts the current values of all the metrics and sends them to the
// prometheus channel.
func (p *ClientSocketUsageCollector) Collect(ch chan<- prometheus.Metric) {
	if err := p.collect(); err != nil {
		log.Println("[ERROR] failed collecting pool usage metrics:", err)
		return
	}

	for _, metric := range p.collectorList() {
		metric.Collect(ch)
	}
}
