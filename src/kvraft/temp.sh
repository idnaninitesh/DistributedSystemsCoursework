for i in {1..10}
do
	echo "Test #$i"
	# time go test -race -run TestOnePartition3A
	# time go test -race -run TestPersistPartition3A
	time go test -race
	# time sleep 2
	echo ""
	echo ""
	echo ""
done
