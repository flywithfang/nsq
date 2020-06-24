nsq-routing is a fork of nsq 1.2.0

features:
- message with the same routing key will route to the same client 
- routing msg to channels, no "clone"
- drain channel data to topic before channel deletion
	- /channel/drain?topic=xx&channel=yy
- clients will have auto-allocated channel with names "default-1" "default-2"...
- messages don't route to channel without active client
- one channel one client
- compatible with original nsq message format and data file

