package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/hashicorp/go-sockaddr"
	"github.com/redsux/habolt"
)

var (
	name     string
	members  string
	dbPath   string
	logLevel int
	listen   string
	bind     string
)

func init() {
	flag.StringVar(&name, "name", "toto", "Cluster node name (default: toto)")
	flag.StringVar(&members, "members", "", "Cluster members (to join exisiting) split by comma, ex: 127.0.0.1:1111,127.0.0.1:2222")
	flag.StringVar(&dbPath, "db", "./node.db", "DB Path, default : ./node.db")
	flag.IntVar(&logLevel, "level", 1, "Log level (0 = DEBUG, 1 = INFO, 2 = WARNING, 3 = ERROR)")
	flag.StringVar(&listen, "listen", ":10001", "Default Serf listening address 'host:port' (Raft Port = Serf + 1), default = ':10001'")
	flag.StringVar(&bind, "bind", "", "Used for NAT Traversal, advertised listening address 'host:port' (Raft Port = port + 1)")
}

type toto struct {
	Name  string `json:"name"`
	Value int    `json:"value"`
}

func newToto(n string) *toto {
	return &toto{
		Name:  n,
		Value: rand.Intn(100),
	}
}

func (t *toto) key() string {
	return fmt.Sprintf("%s_%v", t.Name, t.Value)
}

func main() {
	var (
		err   error
		lAddr *habolt.HaAddress
		bAddr *habolt.HaAddress
	)
	flag.Parse()

	var peers []string
	if members != "" {
		peers = strings.Split(members, ",")
	}

	lAddr, err = habolt.NewListen(listen)
	if err != nil {
		log.Fatal(err)
	}
	if lAddr.Address == "" {
		if lAddr.Address, err = sockaddr.GetInterfaceIP("eth0"); err != nil {
			log.Fatal(err)
		}
	}

	if bind != "" {
		bAddr, err = habolt.NewListen(bind)
		if err != nil {
			log.Fatal(err)
		}

	}

	HAS, err := habolt.NewHaStore(lAddr, bAddr, &habolt.Options{Path: dbPath})
	if err != nil {
		log.Fatal(err)
	}
	HAS.LogLevel(logLevel)

	go HAS.Start(peers...)

	ticker := time.NewTicker(time.Duration(4+rand.Intn(6)) * time.Second) // between 4 and 10 sec
	ticker2 := time.NewTicker(40 * time.Second)                           // between 30 and 60 sec
	ticker3 := time.NewTicker(60 * time.Second)

	for {
		select {
		case <-ticker.C:
			t := newToto(name)
			HAS.Set(t.key(), t)
		case <-ticker2.C:
			var t []toto
			if err := HAS.List(&t, name+"_*"); err != nil {
				fmt.Printf("[ERR] %v", err.Error())
			} else {
				for _, v := range t {
					fmt.Printf("\tName=%s Value=%v\n", v.Name, v.Value)
				}
			}
		case <-ticker3.C:
			srvs := make([]string, 0)
			if err := HAS.Members(&srvs); err == nil {
				for _, s := range srvs {
					fmt.Printf("\tMember : %s\n", s)
				}
			}
		}
	}
}
