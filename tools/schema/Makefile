all: install build

install:
	go get github.com/alecthomas/kingpin
	go get github.com/alecthomas/participle

build: 
	go build -o generator generator.go
	go build -o loader loader.go

clean:
	rm -f generator
	rm -f loader
	rm -f generated.*
