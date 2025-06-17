docker run -d -p 9300:9300 -p 9200:9200 --name dev-elasticsearch-all -e bootstrap_memory_lock=true  -e ES_JAVA_OPTS="-Xms2g -Xmx2g" -e "discovery.type=single-node" -e "xpack.security.enabled=true" -v /home/xwxsee/es717_data/data:/usr/share/elasticsearch/data -v /home/xwxsee/es717_data/logs:/usr/share/elasticsearch/logs --ulimit memlock=-1:-1 --ulimit nofile=65536:65536 swr.cn-southwest-2.myhuaweicloud.com/wenxing/dev-elasticsearch-all:7.17.5

bin/elasticsearch-setup-passwords interactive
