go build -buildmode=plugin ../mrapps/wc.go
rm -rf mr-out*
rm -rf mr-inter*
go run mrmaster.go pg-*.txt
