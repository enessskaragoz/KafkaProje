komutlar:  
1-) git clone https://github.com/enessskaragoz/KafkaProje.git  
Bu komut, proje dosyalarnı github repomdan çeker.
2-) sudo apt-get update  
Bu komut, yerel paket dizinindeki paket listesini günceller. Yani sistemdeki paketlerin en son sürümlerinin listesini alır.  
3-) sudo apt-get -y install python3-pip  
 Bu komut, Python 3 için paket yöneticisi olan pipi yükler. pip, Python paketlerini yönetmek ve yüklemek için kullanılır.
4-) cd KafkaProje  
 Bu komut, klonladığınız projenin dizinine geçiş yapar.  
5-) pip install -r requirements.txt  
Bu komut, projenin gereksinimlerini (bağımlılıklarını) içeren requirements.txt dosyasını kullanarak gerekli Python paketlerini yükler.  
6-)docker compose up -d  
Bu komut, Docker Compose aracılığıyla projenin Docker yapılandırma dosyasını kullanarak, projeyi başlatır ve arka planda çalışmasını sağlar.   
7-) python3 YoutubeAnalytics.py  
YouTube verilerini toplayıp işleyeceğimiz komutları çalıştırır.
PORT:9021  
