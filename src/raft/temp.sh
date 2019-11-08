for i in {1..5}
do
	echo "Test #$i"
	# time go test -race
	# time go test -race -run TestPersist22C 
	# time go test -race -run TestFigure8
	time go test
	# time sleep 2
	echo ""
	echo ""
	echo ""
done
