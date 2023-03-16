
HOST=0.0.0.0
PORT=5004
DEBUG=true
WR_ONLY_MGR_URL="http://127.0.0.1:5001"
RD_ONLY_MGR1_URL="http://127.0.0.1:5002"
RD_ONLY_MGR2_URL="http://127.0.0.1:5003"

while getopts ":d:h:p:" arg; do
    case $arg in
        d) DEBUG=$OPTARG;;
        h) HOST=$OPTARG;;
        p) PORT=$OPTARG;;
    esac
done

export HOST
export PORT
export DEBUG
export WR_ONLY_MGR_URL
export RD_ONLY_MGR1_URL
export RD_ONLY_MGR2_URL

source env/bin/activate

if $DEBUG; then
    flask --app app run --host=$HOST --port=$PORT --debug
else
    flask --app app run --host=$HOST --port=$PORT
fi