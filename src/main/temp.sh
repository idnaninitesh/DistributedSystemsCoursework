for i in {1..25}
do
	echo "Test #$i"
	rm -f mrtmp.wcseq*
	go clean -cache 
	bash ./test-mr.sh
	echo ""
	echo ""
	echo ""
done
