go build -buildmode=plugin ../mrapps/wc.go
rm -rf mr-out*
go run mrsequential.go wc.so pg*.txt
cat mr-out-* | sort | more
