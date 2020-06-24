nsq-routing is a fork or nsq 1.2.0

features:
1 message with the same routing key will route to the same client 
2 routing msg to channels, no "clone"
3 drain channel data to topic before channel deletion
4 clients will have auto-allocated channel with names "default-1" "default-2"...
5 messages don't route to channel without active client
6 one channel one client
7 comaptaible with original nsq message format and data file

