package main

import "fmt"

func main() {
	fmt.Println("Go fraud pipeline")
	fmt.Println("  go run ./cmd/fraud-service")
	fmt.Println("  go run ./cmd/simulator --count 1000")
	fmt.Println("  go run ./cmd/fraudctl stats")
}
