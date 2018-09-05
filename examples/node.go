package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strings"
	"time"

	"github.com/redsux/habolt"
)

var (
	name     string
	members  string
	serfPort int
	dbPath   string
)

func init() {
	flag.StringVar(&name, "name", "toto", "Cluster node name (default: toto)")
	flag.StringVar(&members, "members", "", "Cluster members (to join exisiting) split by comma, ex: 127.0.0.1:1111,127.0.0.1:2222")
	flag.IntVar(&serfPort, "serfPort", 0, "Serf Port (Raft Port = Serf Port +1), ex: 1111")
	flag.StringVar(&dbPath, "db", "./node.db", "DB Path, default : ./node.db")
}

type Toto struct {
	Name string `json:"name"`
	Value int   `json:"value"`
}

func NewToto(n string) *Toto {
	return &Toto{
		Name: n,
		Value: rand.Intn(100),
	}
}

func (t *Toto) Key() string {
	return fmt.Sprintf("%s_%v", t.Name, t.Value)
}

func main() {

	flag.Parse()

	var peers []string
	if members != "" {
		peers = strings.Split(members, ",")
	}

	ip := GetInternalIP()
	if ip == "" {
		log.Fatal("Ip error")
	}

	HAS, err := habolt.NewHaStore(ip, serfPort, habolt.Options{Path: dbPath})
	if err != nil {
		log.Fatal(err)
	}

	go HAS.Start(peers...)

	ticker  := time.NewTicker( time.Duration(4 + rand.Intn(6)) * time.Second) // between 4 and 10 sec
	ticker2 := time.NewTicker( 40 * time.Second) // between 30 and 60 sec
	ticker3 := time.NewTicker( 60 * time.Second )

	for {
		select {
		case <-ticker.C:
			t := NewToto(name)
			HAS.Set(t.Key(), t)
		case <- ticker2.C:
			var t []Toto
			if err := HAS.List(&t, name + "_*"); err != nil {
				for _, v := range t {
					fmt.Printf("\tName=%s Value=%v\n", v.Name, v.Value)
				}
			}
		case <- ticker3.C:
			srvs := make([]string, 0)
			if err := HAS.Members(&srvs); err == nil {
				for _, s := range srvs {
					fmt.Printf("\tMember : %s\n", s)
				}
			}
			HAS.LogLevel( rand.Intn(3) )
		}
	}
}

func GetInternalIP() string {
    itf, _ := net.InterfaceByName("eth0")
    item, _ := itf.Addrs()
    var ip net.IP
    for _, addr := range item {
        switch v := addr.(type) {
        case *net.IPNet:
            if !v.IP.IsLoopback() {
                if v.IP.To4() != nil {//Verify if IP is IPV4
                    ip = v.IP
                }
            }
        }
    }
    if ip != nil {
        return ip.String()
    } else {
        return ""
    }
}