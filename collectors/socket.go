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
			//fmt.Println(path)
			//fmt.Println(parseSockId(f, sockPrefix, c.SocketSuffix))
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
		//fmt.Println("####################################################\n\n")
		//fmt.Println("tag: ", tag, "datapoints: ", datapoints)
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
	//fmt.Println("PID:", pid)

	cmdline := fmt.Sprintf("/proc/%s/cmdline", pid)
	//fmt.Println(cmdline)

	content, err := ioutil.ReadFile(cmdline)
	if err != nil {
		//Do something
	}

	//      uuid = cmd.read().split(',')[45].split('=')[1].split('\x00')[0]
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

/*
func (c *Ceph) Test() {
	sockets, err := findSockets(c)
	if err != nil {
		fmt.Println("socket err: ", sockets, err)
	}
	//fmt.Println("####################################################\n\n")

	//fmt.Println(sockets)
	for _, s := range sockets {
		instId := getInstanceID(s)
		//fmt.Println(instId)
		name, _ := c.getNameByInstanceID(instId)
		//fmt.Println(name)
		dump, err := perfDump(c.CephBinary, s)
		if err != nil {
			log.Printf("E! error reading from socket '%s': %v", s.socket, err)
			continue
		}
		//

		//data, err := parseDump(dump)
		data, _ := parseDump(dump)
		d := make(map[string]taggedMetricMap)
		d[name] = data
		fmt.Println(d)
		if err != nil {
			log.Printf("E! error parsing dump from socket '%s': %v", s.socket, err)
			continue
		}

	}
}
*/

/*
func main() {
	c := Ceph{
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
	c.Test()
}
*/

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
	/*
		// UsedBytes tracks the amount of bytes currently allocated for the pool. This
		// does not factor in the overcommitment made for individual images.
		UsedBytes *prometheus.GaugeVec

		// RawUsedBytes tracks the amount of raw bytes currently used for the pool. This
		// factors in the replication factor (size) of the pool.
		RawUsedBytes *prometheus.GaugeVec

		// MaxAvail tracks the amount of bytes currently free for the pool,
		// which depends on the replication settings for the pool in question.
		MaxAvail *prometheus.GaugeVec

		// Objects shows the no. of RADOS objects created within the pool.
		Objects *prometheus.GaugeVec

		// DirtyObjects shows the no. of RADOS dirty objects in a cache-tier pool,
		// this doesn't make sense in a regular pool, see:
		// http://lists.ceph.com/pipermail/ceph-users-ceph.com/2015-April/000557.html
		DirtyObjects *prometheus.GaugeVec

		// ReadIO tracks the read IO calls made for the images within each pool.
		ReadIO *prometheus.GaugeVec

		// Readbytes tracks the read throughput made for the images within each pool.
		ReadBytes *prometheus.GaugeVec

		// WriteIO tracks the write IO calls made for the images within each pool.
		WriteIO *prometheus.GaugeVec

		// WriteBytes tracks the write throughput made for the images within each pool.
		WriteBytes *prometheus.GaugeVec
	*/
}

// NewPoolUsageCollector creates a new instance of PoolUsageCollector and returns
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
		/*
			RawUsedBytes: prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Namespace: cephNamespace,
					Subsystem: subSystem,
					Name:      "raw_used_bytes",
					Help:      "Raw capacity of the pool that is currently under use, this factors in the size",
				},
				poolLabel,
			),
			MaxAvail: prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Namespace: cephNamespace,
					Subsystem: subSystem,
					Name:      "available_bytes",
					Help:      "Free space for this ceph pool",
				},
				poolLabel,
			),
			Objects: prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Namespace: cephNamespace,
					Subsystem: subSystem,
					Name:      "objects_total",
					Help:      "Total no. of objects allocated within the pool",
				},
				poolLabel,
			),
			DirtyObjects: prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Namespace: cephNamespace,
					Subsystem: subSystem,
					Name:      "dirty_objects_total",
					Help:      "Total no. of dirty objects in a cache-tier pool",
				},
				poolLabel,
			),
			ReadIO: prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Namespace: cephNamespace,
					Subsystem: subSystem,
					Name:      "read_total",
					Help:      "Total read i/o calls for the pool",
				},
				poolLabel,
			),
			ReadBytes: prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Namespace: cephNamespace,
					Subsystem: subSystem,
					Name:      "read_bytes_total",
					Help:      "Total read throughput for the pool",
				},
				poolLabel,
			),
			WriteIO: prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Namespace: cephNamespace,
					Subsystem: subSystem,
					Name:      "write_total",
					Help:      "Total write i/o calls for the pool",
				},
				poolLabel,
			),
			WriteBytes: prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Namespace: cephNamespace,
					Subsystem: subSystem,
					Name:      "write_bytes_total",
					Help:      "Total write throughput for the pool",
				},
				poolLabel,
			),*/
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
		/*
			p.RawUsedBytes,
			p.MaxAvail,
			p.Objects,
			p.DirtyObjects,
			p.ReadIO,
			p.ReadBytes,
			p.WriteIO,
			p.WriteBytes,
		*/
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
			//	fmt.Println(tag)
			//prefix := strings.Split(tag, "_")[0]
			//fmt.Println(prefix)
			//vType := strings.TrimPrefix(tag, prefix)
			parseTag := strings.Split(tag, "-")
			//parseType := strings.Split(tag, "_")
			if parseTag[0] == "librbd" {
				mLabel := fmt.Sprintf("%s-mount-%s-%s", name, parseTag[2], parseTag[7])

				p.ReadBytes.WithLabelValues(mLabel).Set(metric["rd_bytes"].(float64))
				//read_total += metric["rd_bytes"].(float64)
				totalvals[name+"-read"] += metric["rd_bytes"].(float64)

				p.WriteBytes.WithLabelValues(mLabel).Set(metric["wr_bytes"].(float64))
				//write_total += metric["wr_bytes"].(float64)
				totalvals[name+"-write"] += metric["wr_bytes"].(float64)
				//		p.TotalBytes.WithLabelValues(mLabel).Set(metric["wr_bytes"].(float64) + metric["rd_bytes"].(float64))
				//		io_total += read_total + write_total
				p.ReadWriteBytes.WithLabelValues(mLabel).Set(metric["rd_bytes"].(float64) + metric["wr_bytes"].(float64))
				//readwrite_total += metric["rd_bytes"].(float64) + metric["wr_bytes"].(float64)
				totalvals[name+"-readwrite"] += metric["rd_bytes"].(float64) + metric["wr_bytes"].(float64)

			}
			//fmt.Println(metrics)
		}
		mLabel := fmt.Sprintf("%s-all", name)
		p.TotalReadBytes.WithLabelValues(mLabel).Set(totalvals[name+"-read"])
		p.TotalWriteBytes.WithLabelValues(mLabel).Set(totalvals[name+"-write"])
		p.TotalReadWriteBytes.WithLabelValues(mLabel).Set(totalvals[name+"-readwrite"])
		if err != nil {
			log.Printf("E! error parsing dump from socket '%s': %v", s.socket, err)
			continue
		}
	} /*
		cmd := p.cephUsageCommand()
		buf, _, err := p.conn.MonCommand(cmd)
		if err != nil {
			return err
		}

		stats := &cephPoolStats{}
		if err := json.Unmarshal(buf, stats); err != nil {
			return err
		}

		if len(stats.Pools) < 1 {
			return errors.New("no pools found in the cluster to report stats on")
		}
	*/

	/*
		for _, pool := range stats.Pools {
			p.UsedBytes.WithLabelValues(pool.Name).Set(pool.Stats.BytesUsed)
			p.RawUsedBytes.WithLabelValues(pool.Name).Set(pool.Stats.RawBytesUsed)
			p.MaxAvail.WithLabelValues(pool.Name).Set(pool.Stats.MaxAvail)
			p.Objects.WithLabelValues(pool.Name).Set(pool.Stats.Objects)
			p.DirtyObjects.WithLabelValues(pool.Name).Set(pool.Stats.DirtyObjects)
			p.ReadIO.WithLabelValues(pool.Name).Set(pool.Stats.ReadIO)
			p.ReadBytes.WithLabelValues(pool.Name).Set(pool.Stats.ReadBytes)
			p.WriteIO.WithLabelValues(pool.Name).Set(pool.Stats.WriteIO)
			p.WriteBytes.WithLabelValues(pool.Name).Set(pool.Stats.WriteBytes)
		}
	*/

	return nil
}

// Describe fulfills the prometheus.Collector's interface and sends the descriptors
// of pool's metrics to the given channel.
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
