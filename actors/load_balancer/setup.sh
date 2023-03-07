# Env Setup
sudo apt install python3-venv
cp ../../requirements.txt .
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
deactivate
rm requirements.txt