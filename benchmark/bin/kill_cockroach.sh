for pid in $(ps -ef | awk '/cockroach/ {print $2}'); do kill -9 $pid; done
