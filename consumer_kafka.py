from confluent_kafka import Consumer, OFFSET_BEGINNING
import logging
logger = logging.getLogger(__name__)
from tornado import gen
import tornado.ioloop


BROKER_URL = 'localhost:9092'

class KafkaConsumer:
    
        def __init__(
        self,
        topic_name_pattern,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,

        ):
            self.topic_name_pattern = topic_name_pattern
            self.sleep_secs = sleep_secs
            self.consume_timeout = consume_timeout
            self.offset_earliest = offset_earliest
            
            self.broker_properties = {
                "bootstrap.servers": BROKER_URL,
                'group.id': 'consumer',
                "auto.offset.reset": "earliest" if offset_earliest else "latest"
            }
            
            self.consumer = Consumer(self.broker_properties)


            self.consumer.subscribe(topics=[self.topic_name_pattern])

        

        def on_assign(self, consumer, partitions):
            """Callback for when topic assignment takes place"""
            # TODO: If the topic is configured to use `offset_earliest` set the partition offset to
            # the beginning or earliest
            logger.info("on_assign is incomplete - skipping")
            for partition in partitions:
                print(f"consume message {partition.key()}: {partition.value()}")
                if self.offset_earliest == True:
                    print('OFFSET_BEGINNING')
                    partition.offset=OFFSET_BEGINNING

                logger.info("partitions assigned for %s", self.topic_name_pattern)
                consumer.assign(partitions)

        async def consume(self):
            """Asynchronously consumes data from kafka topic"""
            while True:
                num_results = 1
                while num_results > 0:
                    num_results = self._consume()
                    
                await gen.sleep(self.sleep_secs)
                
        def _consume(self):
            """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        #
        #
            message = self.consumer.poll(1)
            if message is None:
                print('0')
                return 0 
            else :
                print('1')
                print(message.value())
                return 1 

            #
            #
            logger.info("_consume is incomplete - skipping")
            return 0
    
        def close(self):
            """Cleans up any open kafka consumers"""
            self.consumer.flush()

        
        
def main():
    cons = KafkaConsumer(
    topic_name_pattern = 'test',
    offset_earliest = True,
    )
    tornado.ioloop.IOLoop.current().spawn_callback(cons.consume)
    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    main()


