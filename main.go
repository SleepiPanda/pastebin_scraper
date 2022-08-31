package main

import (
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"sync"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
)

// read config from config.json
var (
	config Config
	rules  = []*Rule{
		{
			Name: "Test Rule",
			Regexs: []*regexp.Regexp{
				regexp.MustCompile(`(?i)testrule 1234`),
			},
		},
	}
)

type Rule struct {
	Name   string           `json:"name"`
	Regexs []*regexp.Regexp `json:"regexs"`
}

type Paste struct {
	ScrapeURL string `json:"scrape_url"`
	FullURL   string `json:"full_url"`
	Date      string `json:"date"`
	Key       string `json:"key"`
	Size      string `json:"size"`
	Expire    string `json:"expire"`
	Title     string `json:"title"`
	Syntax    string `json:"syntax"`
	User      string `json:"user"`
	Hits      string `json:"hits"`
}

type Config struct {
	API_TOKEN       string
	CANCEL_INTERVAL int
	WORKERS         int
	WORKER_DELAY    int
}

type ScraperMeta struct {
	LastSlug string `json:"last_slug"`
}

type Queue struct {
	jobs chan Paste
	ctx  context.Context
}

func (q *Queue) AddPasteToQueue(paste Paste) error {
	select {
	case q.jobs <- paste:
		log.Debugf("Paste %s added to queue\n", paste.Key)
		return nil
	case <-time.After(time.Duration(config.CANCEL_INTERVAL) * time.Second):
		log.Warn("Queue Not Responding, cancelling")
		return errors.New("Queue Not Responding")
	}
}

type Worker struct {
	Queue *Queue
	wg    *sync.WaitGroup
}

func NewWorker(queue *Queue, wg *sync.WaitGroup) Worker {
	return Worker{Queue: queue, wg: wg}
}

func (w Worker) Work() {
	log.Info("Worker Started")
	for {
		select {
		case paste := <-w.Queue.jobs:
			log.Debugf("Processing paste: %s", paste.Key)
			w.ProcessPaste(paste)
		case <-w.Queue.ctx.Done():
			log.Info("Worker done")
			w.wg.Done()
			return
		}
		time.Sleep(time.Duration(config.WORKER_DELAY) * time.Second)
	}
}

func (w Worker) ProcessPaste(paste Paste) {
	resp, err := http.Get(paste.ScrapeURL)
	if err != nil {
		log.Error("Error fetching paste: ", err)
		return
	}
	if resp.StatusCode != 200 {
		log.Error("Status Code: ", resp.StatusCode)
		return
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Error("Error reading paste: ", err)
		return
	}
	var ruleMatched bool
	for _, rule := range rules {
		ruleMatched = true
		for _, re := range rule.Regexs {
			if !re.Match(body) {
				ruleMatched = false
			}
		}
		if ruleMatched {
			log.Infof("Paste %s matched rule %s", paste.Key, rule.Name)
			//write body to file with rule name as filename
			os.MkdirAll("./pastes/matched"+rule.Name, 0755)
			err = ioutil.WriteFile("./pastes/matched/"+rule.Name+"/"+paste.Key+".txt", body, 0644)
		}
		if err != nil {
			log.Error("Error writing paste: ", err)
		}
	}

}

func ReadConfig() {
	file, err := ioutil.ReadFile("config.json")
	if err != nil {
		log.Fatal(err)
	}
	json.Unmarshal(file, &config)
}

func main() {
	log.SetLevel(log.DebugLevel)
	var wg sync.WaitGroup
	ReadConfig()
	log.Info("Config Loaded")
	ctx, cancelFetcher := context.WithCancel(context.TODO())
	go Start(ctx, &wg)
	log.Info("Scraper Started")
	log.Debug("Config: ", config)
	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGINT, syscall.SIGTERM, os.Interrupt, os.Kill)
	<-sc
	cancelFetcher()
	log.Info("Scraper Stopped")
	wg.Wait()
	log.Info("GoRoutines Finished")
}

func ReadMetadata() *ScraperMeta {
	var scraperMeta ScraperMeta
	metadata, err := ioutil.ReadFile("scraper_meta.json")
	if err != nil {
		log.Error(err)
	}
	json.Unmarshal(metadata, &scraperMeta)
	return &scraperMeta
}

func SaveMetadata(scraperMeta *ScraperMeta) {
	metadata, err := json.Marshal(scraperMeta)
	if err != nil {
		log.Error(err)
	}
	err = ioutil.WriteFile("scraper_meta.json", metadata, 0644)
	if err != nil {
		log.Error(err)
	}
}

func GetNewPastes() (*[]Paste, error) {
	log.Info("Fetching new posts")
	var pastes []Paste
	resp, err := http.Get("https://scrape.pastebin.com/api_scraping.php?limit=150")
	if err != nil {
		log.Fatal(err)
	}
	if resp.StatusCode != 200 {
		log.Error("Status Code: ", resp.StatusCode)
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}
	err = json.Unmarshal(body, &pastes)
	if err != nil {
		log.Fatal(err)
	}
	return &pastes, nil
}

func CheckForLastPaste(pastes *[]Paste, lastSlug string) int {
	for index, paste := range *pastes {
		if paste.Key == lastSlug {
			return index
		}
	}
	return -1
}

func Start(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	var newPastes []Paste
	scraperMeta := ReadMetadata()

	var queue = Queue{
		jobs: make(chan Paste),
		ctx:  ctx,
	}
	for i := 0; i < config.WORKERS; i++ {
		wg.Add(1)
		w := NewWorker(&queue, wg)
		go w.Work()
	}

	for {
		select {
		case <-ctx.Done():
			close(queue.jobs)
			wg.Done()
			return
		default:
			pastes, err := GetNewPastes()
			if err != nil {
				break
			}
			lastIndex := CheckForLastPaste(pastes, scraperMeta.LastSlug)
			if lastIndex == -1 {
				newPastes = *pastes
				log.Info("Last Slug was not found in new pastes... They are all new")
			} else {
				newPastes = (*pastes)[:lastIndex]
				log.Infof("Last Slug was found in new pastes... %d new pastes", len(newPastes))
			}
			log.Infof("Processing %d new pastes", len(newPastes))
			if len(newPastes) > 0 {
				scraperMeta.LastSlug = newPastes[0].Key
				for _, paste := range newPastes {
					err = queue.AddPasteToQueue(paste)
					if err != nil {
						log.Error(err)
						break
					}
				}
			}
		}
		SaveMetadata(scraperMeta)
		for x := 0; x < 60/config.CANCEL_INTERVAL; x++ {
			select {
			case <-ctx.Done():
				close(queue.jobs)
				wg.Done()
				return
			default:
				log.Debug("Sleeping for ", config.CANCEL_INTERVAL, " seconds")
				time.Sleep(time.Second * time.Duration(config.CANCEL_INTERVAL))
			}
		}
	}
}
