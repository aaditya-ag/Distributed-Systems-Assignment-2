sudo apt install python3-venv
cp ../../requirements.txt .
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
deactivate
rm requirements.txt

# Install PostgreSQL
# -----------------------------------------------------------------
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo apt-get update
sudo apt-get -y install postgresql
sudo apt install python3-venv
cp ../../requirements.txt .

printf "\nPostgres Installation: [SUCCESSFUL]\n\n"

DATABASE_NAME=$1
if [ -z "$DATABASE_NAME" ]
then
    printf "\n[FAILURE]: No database name supplied\n\n"
    echo "Exiting in 10 seconds..."
    sleep 10
    exit 0
fi
export DATABASE_NAME

echo "alter user postgres with password 'admin';
drop database $DATABASE_NAME;
create database $DATABASE_NAME;" > create_database.sql

cat create_database.sql | sudo -iu postgres psql

printf "\n[SUCCESSFUL]: Database::$DATABASE_NAME Creation\n\n"

rm create_database.sql

printf "\n[SUCCESSFUL]: Cleanup\n\n"