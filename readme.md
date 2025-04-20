VERBOSE=1 go test -run Install3D | python3 dslogs.py -c 3 -h out.html
VERBOSE=1 python3 dstest.py 3C -n 100 -p 20

go test -run LeaderFailure4A
python3 dstest.py [testname] -n 100 -p 20

python3 dslogs.py -c 3 -h out.html [logname]