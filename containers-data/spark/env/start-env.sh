echo "[DEBUG] Starting the environment configuration"

apt-get install -y python3-pip

sudo python3 -m pip install -r /env/requirements.txt
sudo pip3 install -r /env/requirements

echo "[DEBUG] Done with the environment configuration"