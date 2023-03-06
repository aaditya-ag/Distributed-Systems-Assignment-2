HOST=127.0.0.1
PORT=5001
DEBUG=true

while getopts ":d:h:p:" arg; do
    case $arg in
        d) DEBUG=$OPTARG;;
        h) HOST=$OPTARG;;
        p) PORT=$OPTARG;;
    esac
done

source env/bin/activate
cd actors/broker_manager

if $DEBUG; then
    flask --app app run --host=$HOST --port=$PORT --debug
else
    flask --app app run --host=$HOST --port=$PORT
fi