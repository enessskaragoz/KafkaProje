komutlar:
git clone https://github.com/enessskaragoz/KafkaProje.git
sudo apt-get update
sudo apt-get -y install python3-pip
cd KafkaProje
pip install -r requirements.txt
docker compose up -d
python3 YoutubeAnalytics.py
PORT:9021
