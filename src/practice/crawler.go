package main

import (
	"fmt"
	"sync"
)

func main() {
	fmt.Println("SerialCrawl result:")
	SerialCrawl("http://golang.org/", fetcher, make(map[string]bool))
	fmt.Println("ConcurrentMutex result:")
	ConcurrentMutex("http://golang.org/", fetcher, &safeFetched{m: make(map[string]bool)})
	fmt.Println("ConcurrentChannel result:")
	ConcurrentChannel("http://golang.org/", fetcher)
}

func SerialCrawl(url string, fetcher Fetcher, fetched map[string]bool) {
	if fetched[url] {
		return
	}
	urls, err := fetcher.Fetch(url)
	fetched[url] = true
	if err != nil {
		return
	}
	for _, u := range urls {
		SerialCrawl(u, fetcher, fetched)
	}
}

type safeFetched struct {
	mu sync.Mutex
	m  map[string]bool
}

func ConcurrentMutex(url string, fetcher Fetcher, fetched *safeFetched) {
	if fetched.m[url] {
		return
	}
	fetched.mu.Lock()
	urls, err := fetcher.Fetch(url)
	fetched.m[url] = true
	fetched.mu.Unlock()
	if err != nil {
		return
	}
	wg := sync.WaitGroup{}
	wg.Add(len(urls))
	for _, u := range urls {
		go func(u string) { // 闭包传参啊啊啊啊啊啊啊啊啊嗷嗷
			defer wg.Done()
			ConcurrentMutex(u, fetcher, fetched)
		}(u)
	}
	wg.Wait()
}

func ConcurrentChannel(url string, fetcher Fetcher) {
	ch := make(chan []string)
	go func() {
		ch <- []string{url}
	}()
	master(ch, fetcher)
}

func master(ch chan []string, fetcher Fetcher) {
	fetched := make(map[string]bool)
	success := 1
	for urls := range ch {
		for _, u := range urls {
			if fetched[u] {
				continue
			}
			fetched[u] = true
			go worker(u, ch, fetcher)
			success++
		}
		success--
		if success == 0 {
			break
		}
	}
}

func worker(url string, ch chan []string, fetcher Fetcher) {
	urls, err := fetcher.Fetch(url)
	if err != nil {
		ch <- []string{}
		return
	}
	ch <- urls
}

//
// Fetcher
//

type Fetcher interface {
	// Fetch returns a slice of URLs found on the page.
	Fetch(url string) (urls []string, err error)
}

// fakeFetcher is Fetcher that returns canned results.
type fakeFetcher map[string]*fakeResult

type fakeResult struct {
	body string
	urls []string
}

func (f fakeFetcher) Fetch(url string) ([]string, error) {
	if res, ok := f[url]; ok {
		fmt.Printf("found:   %s\n", url)
		return res.urls, nil
	}
	fmt.Printf("missing: %s\n", url)
	return nil, fmt.Errorf("not found: %s", url)
}

// fetcher is a populated fakeFetcher.
var fetcher = fakeFetcher{
	"http://golang.org/": &fakeResult{
		"The Go Programming Language",
		[]string{
			"http://golang.org/pkg/",
			"http://golang.org/cmd/",
		},
	},
	"http://golang.org/pkg/": &fakeResult{
		"Packages",
		[]string{
			"http://golang.org/",
			"http://golang.org/cmd/",
			"http://golang.org/pkg/fmt/",
			"http://golang.org/pkg/os/",
		},
	},
	"http://golang.org/pkg/fmt/": &fakeResult{
		"Package fmt",
		[]string{
			"http://golang.org/",
			"http://golang.org/pkg/",
		},
	},
	"http://golang.org/pkg/os/": &fakeResult{
		"Package os",
		[]string{
			"http://golang.org/",
			"http://golang.org/pkg/",
		},
	},
}
