
run: 
	go run main.go

perf: 
	go tool pprof -http 127.0.0.1:8080 cpu_profile.prof