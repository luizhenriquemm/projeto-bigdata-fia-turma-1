apt-get update
apt-get install -y openssh-server sudo
sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config

useradd --uid 1010 --create-home --shell /bin/bash --user-group me && echo 'me:changeme' | passwd me

echo 'changeit' | passwd root

echo 'me ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/me

service ssh start && /opt/bitnami/scripts/spark/run.sh