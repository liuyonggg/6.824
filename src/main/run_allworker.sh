go build -buildmode=plugin ../mrapps/wc.go
rm -rf mr-out*
rm -rf mr-inter*
for ((i = 0; i < 8; i++));
do
	go run mrworker.go wc.so &
	# go run mrworker.go wc.so
done


wait
sleep 10s

for ((i = 0; i < 10; i++));
do
	go run mrworker.go wc.so &
	#go run mrworker.go wc.so
done

wait
sleep 10s

