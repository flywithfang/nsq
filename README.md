nsq-routing is a fork of nsq 1.2.0

features:
- message with the same routing key will route to the same client 
  ```golang
   func (w *Producer) Publish(topic string, body []byte, routingKey string) error {
	 return w.sendCommand(Publish(topic, body, routingKey))
    }
  ```
- routing msg to channels, no "clone", more like kafka partition
- drain channel data to topic before channel deletion
	- /channel/drain?topic=xx&channel=yy
- clients will have auto-allocated channel with names "default-1" "default-2"...
- messages don't route to channel without active client
- one channel one client
- compatible with original nsq message format and data file

