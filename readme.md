VERBOSE=1 go test -run [testname] | python3 dslogs.py -c [#servers] -h out.html
python3 dstest.py 3C -n 100 -p 20