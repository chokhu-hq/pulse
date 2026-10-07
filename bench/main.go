package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

const (
	appID     = "bench-app"
	appKey    = "bench-key"
	appSecret = "bench-secret"
	channel   = "bench"
)

type scenario struct {
	Name        string `json:"name"`
	Subscribers int    `json:"subscribers"`
	Events      int    `json:"events"`
	Publishers  int    `json:"publishers"`
	PayloadSize int    `json:"payload_bytes"`
}

type result struct {
	scenario
	PublishesPerSec   float64 `json:"publishes_per_sec"`
	DeliveriesPerSec  float64 `json:"deliveries_per_sec"`
	Delivered         int64   `json:"delivered"`
	Expected          int64   `json:"expected"`
	ServerCPUPerEvent float64 `json:"server_cpu_us_per_delivery"`
	RSSPerConnKB      float64 `json:"rss_per_conn_kb"`
}

var scenarios = []scenario{
	{"publish throughput", 1, 100000, 256, 200},
	{"fan-out 100 subscribers", 100, 10000, 32, 200},
	{"fan-out 1000 subscribers", 1000, 2000, 16, 200},
	{"fan-out 5000 subscribers", 5000, 500, 16, 200},
}

func main() {
	bin := flag.String("bin", "./pulse", "path to pulse binary")
	jsonOut := flag.String("json", "", "write results as JSON to this file")
	mdOut := flag.String("md", "", "write results as a markdown table to this file")
	readme := flag.String("readme", "", "replace the section between bench markers in this file")
	flag.Parse()

	var results []result
	for _, sc := range scenarios {
		fmt.Fprintf(os.Stderr, "running %q...\n", sc.Name)
		res, err := run(*bin, sc)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", sc.Name, err)
			os.Exit(1)
		}
		results = append(results, res)
	}

	table := markdown(results)
	md := "## Pulse benchmarks\n\n" + table
	fmt.Print(md)

	if *readme != "" {
		if err := updateReadme(*readme, table); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}

	if *mdOut != "" {
		if err := os.WriteFile(*mdOut, []byte(md), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	if *jsonOut != "" {
		data, _ := json.MarshalIndent(results, "", "  ")
		if err := os.WriteFile(*jsonOut, data, 0o644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}

func run(bin string, sc scenario) (result, error) {
	res := result{scenario: sc, Expected: int64(sc.Subscribers) * int64(sc.Events)}

	port, err := freePort()
	if err != nil {
		return res, err
	}
	addr := "127.0.0.1:" + port

	srv, err := startServer(bin, port)
	if err != nil {
		return res, err
	}
	defer func() {
		srv.Process.Signal(syscall.SIGTERM)
		srv.Wait()
	}()
	pid := srv.Process.Pid

	if err := waitHealthy(addr); err != nil {
		return res, err
	}

	rss0, _ := procStat(pid)
	var received atomic.Int64
	conns, err := subscribe(addr, sc.Subscribers, &received)
	if err != nil {
		return res, err
	}
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()

	time.Sleep(500 * time.Millisecond)
	rss1, cpu1 := procStat(pid)
	res.RSSPerConnKB = float64(rss1-rss0) / float64(sc.Subscribers)

	payload, _ := json.Marshal(strings.Repeat("x", sc.PayloadSize))
	body, _ := json.Marshal(map[string]string{"name": "ev", "channel": channel, "data": string(payload)})
	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: sc.Publishers, MaxConnsPerHost: sc.Publishers}}

	start := time.Now()
	if err := publishAll(client, addr, body, sc.Events, sc.Publishers); err != nil {
		return res, err
	}
	pubDur := time.Since(start)

	dur := waitDelivered(&received, res.Expected).Sub(start)
	_, cpu2 := procStat(pid)

	res.Delivered = received.Load()
	res.PublishesPerSec = float64(sc.Events) / pubDur.Seconds()
	res.DeliveriesPerSec = float64(res.Delivered) / dur.Seconds()
	if res.Delivered > 0 {
		res.ServerCPUPerEvent = (cpu2 - cpu1) * 1e6 / float64(res.Delivered)
	}
	return res, nil
}

func startServer(bin, port string) (*exec.Cmd, error) {
	dir, err := os.MkdirTemp("", "pulse-bench")
	if err != nil {
		return nil, err
	}
	config := fmt.Sprintf(`{"server":{"port":%q},"apps":[{"id":%q,"key":%q,"secret":%q,"name":"bench","enabled":true,
		"max_message_size":8192,"enable_client_events":false}]}`, port, appID, appKey, appSecret)
	configPath := filepath.Join(dir, "config.json")
	if err := os.WriteFile(configPath, []byte(config), 0o644); err != nil {
		return nil, err
	}

	cmd := exec.Command(bin, "-config="+configPath, "-max-connections=200000")
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	return cmd, cmd.Start()
}

func waitHealthy(addr string) error {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if resp, err := http.Get("http://" + addr + "/health"); err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("server did not become healthy on %s", addr)
}

func subscribe(addr string, n int, received *atomic.Int64) ([]*websocket.Conn, error) {
	var (
		mu       sync.Mutex
		conns    []*websocket.Conn
		firstErr error
		wg       sync.WaitGroup
	)
	sem := make(chan struct{}, 200)
	dialer := websocket.Dialer{HandshakeTimeout: 30 * time.Second}
	sub := []byte(`{"event":"pusher:subscribe","data":{"channel":"` + channel + `"}}`)

	for range n {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()

			ws, _, err := dialer.Dial("ws://"+addr+"/app/"+appKey+"?protocol=7", nil)
			if err == nil {
				_, _, err = ws.ReadMessage()
			}
			if err == nil {
				err = ws.WriteMessage(websocket.TextMessage, sub)
			}
			if err == nil {
				_, _, err = ws.ReadMessage()
			}

			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				return
			}
			conns = append(conns, ws)

			go func() {
				for {
					_, msg, err := ws.ReadMessage()
					if err != nil {
						return
					}
					if bytes.Contains(msg, []byte(`"event":"ev"`)) {
						received.Add(1)
					}
				}
			}()
		})
	}
	wg.Wait()
	return conns, firstErr
}

func publishAll(client *http.Client, addr string, body []byte, events, publishers int) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		next     atomic.Int64
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
	)
	for range publishers {
		wg.Go(func() {
			for ctx.Err() == nil && next.Add(1) <= int64(events) {
				if err := publish(client, addr, body); err != nil {
					errOnce.Do(func() { firstErr = err; cancel() })
				}
			}
		})
	}
	wg.Wait()
	return firstErr
}

func publish(client *http.Client, addr string, body []byte) error {
	path := "/apps/" + appID + "/events"
	query := fmt.Sprintf("auth_key=%s&auth_timestamp=%d&auth_version=1.0&body_md5=%x", appKey, time.Now().Unix(), md5.Sum(body))
	mac := hmac.New(sha256.New, []byte(appSecret))
	mac.Write([]byte("POST\n" + path + "\n" + query))
	url := "http://" + addr + path + "?" + query + "&auth_signature=" + hex.EncodeToString(mac.Sum(nil))

	resp, err := client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("publish returned %d", resp.StatusCode)
	}
	return nil
}

// returns when the last delivery was seen, so the stall wait for dropped messages isn't counted
func waitDelivered(received *atomic.Int64, expected int64) time.Time {
	deadline := time.Now().Add(30 * time.Second)
	last, stalled, lastProgress := received.Load(), 0, time.Now()
	for last < expected && time.Now().Before(deadline) && stalled < 1000 {
		time.Sleep(time.Millisecond)
		if r := received.Load(); r == last {
			stalled++
		} else {
			last, stalled, lastProgress = r, 0, time.Now()
		}
	}
	return lastProgress
}

func freePort() (string, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	defer l.Close()
	return strconv.Itoa(l.Addr().(*net.TCPAddr).Port), nil
}

// linux only; elsewhere the cpu and memory columns read zero
func procStat(pid int) (rssKB int64, cpuSeconds float64) {
	if status, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid)); err == nil {
		for line := range strings.SplitSeq(string(status), "\n") {
			if strings.HasPrefix(line, "VmRSS:") {
				rssKB, _ = strconv.ParseInt(strings.Fields(line)[1], 10, 64)
			}
		}
	}
	if stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid)); err == nil {
		s := string(stat)
		fields := strings.Fields(s[strings.LastIndex(s, ")")+2:])
		utime, _ := strconv.ParseFloat(fields[11], 64)
		stime, _ := strconv.ParseFloat(fields[12], 64)
		cpuSeconds = (utime + stime) / 100
	}
	return rssKB, cpuSeconds
}

func markdown(results []result) string {
	var b strings.Builder
	if os.Getenv("GITHUB_ACTIONS") == "true" {
		runURL := fmt.Sprintf("%s/%s/actions/runs/%s", os.Getenv("GITHUB_SERVER_URL"), os.Getenv("GITHUB_REPOSITORY"), os.Getenv("GITHUB_RUN_ID"))
		fmt.Fprintf(&b, "> Measured automatically on a shared GitHub Actions runner ([run](%s), commit `%.7s`). Expect lower numbers and some run-to-run variance compared to dedicated hardware.\n\n",
			runURL, os.Getenv("GITHUB_SHA"))
	}
	fmt.Fprintf(&b, "%s/%s, %d CPUs, payload %d bytes, server and load generator on the same host, measured %s\n\n",
		runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), scenarios[0].PayloadSize, time.Now().UTC().Format("2006-01-02"))
	fmt.Fprintf(&b, "| Scenario | Subscribers | Publishers | Publishes/s | Deliveries/s | Dropped | Server CPU / delivery | RSS / conn |\n")
	fmt.Fprintf(&b, "|---|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, r := range results {
		rss := "-"
		if r.Subscribers >= 1000 {
			rss = fmt.Sprintf("%.1f KB", r.RSSPerConnKB)
		}
		fmt.Fprintf(&b, "| %s | %d | %d | %s | %s | %d | %.2f µs | %s |\n",
			r.Name, r.Subscribers, r.Publishers, human(r.PublishesPerSec), human(r.DeliveriesPerSec),
			r.Expected-r.Delivered, r.ServerCPUPerEvent, rss)
	}
	return b.String()
}

func updateReadme(path, table string) error {
	const startMarker, endMarker = "<!-- bench:start -->", "<!-- bench:end -->"

	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	s := string(data)
	start := strings.Index(s, startMarker)
	end := strings.Index(s, endMarker)
	if start < 0 || end < start {
		return fmt.Errorf("%s: missing %s / %s markers", path, startMarker, endMarker)
	}

	updated := s[:start+len(startMarker)] + "\n\n" + table + "\n" + s[end:]
	return os.WriteFile(path, []byte(updated), 0o644)
}

func human(v float64) string {
	switch {
	case v >= 1e6:
		return fmt.Sprintf("%.2fM", v/1e6)
	case v >= 1e3:
		return fmt.Sprintf("%.1fk", v/1e3)
	default:
		return fmt.Sprintf("%.0f", v)
	}
}
