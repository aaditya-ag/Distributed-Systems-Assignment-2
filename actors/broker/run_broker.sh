HOST=127.0.0.1
PORT=5000
DEBUG=true

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

source env/bin/activate

if $DEBUG; then
    flask --app app run --host=$HOST --port=$PORT --debug
else
    flask --app app run --host=$HOST --port=$PORT
fi