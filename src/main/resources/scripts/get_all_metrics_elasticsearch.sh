
curl http://1.1.1.1:9200/_nodes/stats?pretty > node.stats &  

curl http://1.1.1.1:9200/_stats?pretty > stats & 
 
curl http://1.1.1.1:9200/_all?pretty > all &  

curl http://10.10.1.1.:9200/_cluster/state?pretty > cluster.state &  

curl http://1.1.1.1:9200/_segments?pretty > segments & 

curl http://1.1.1.1:9200/_cluster/health?pretty > health & 

wait

grep "\"size_in_bytes" node.stats | tr -s ' ' | cut -d ":" -f2 | cut -d "," -f1 | awk '{s+=$1} END {print s/5000000000}'

