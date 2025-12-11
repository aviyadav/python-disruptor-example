from disruptor import Disruptor, Consumer
import time
import random
from benchmark import measure_performance

class MyConsumer(Consumer):
    def __init__(self, name: str):
        self.name = name
    
    def consume(self, elements):
        time.sleep(random.random() * 0.1)
        print(f"{self.name} consumed {elements}")

@measure_performance
def main():
    consumer_one = MyConsumer(name="Consumer One")
    consumer_two = MyConsumer(name="Consumer Two")
    # consumer_three = MyConsumer(name="Consumer Three")
    # consumer_four = MyConsumer(name="Consumer Four")
    # consumer_five = MyConsumer(name="Consumer Five")
    # consumer_six = MyConsumer(name="Consumer Six")
    # consumer_seven = MyConsumer(name="Consumer Seven")
    # consumer_eight = MyConsumer(name="Consumer Eight")
    # consumer_nine = MyConsumer(name="Consumer Nine")
    # consumer_ten = MyConsumer(name="Consumer Ten")

    disruptor = Disruptor(name='Example', size=1024*8)
    
    try:
        disruptor.register_consumer(consumer_one)
        disruptor.register_consumer(consumer_two)
        # disruptor.register_consumer(consumer_three)
        # disruptor.register_consumer(consumer_four)
        # disruptor.register_consumer(consumer_five)
        # disruptor.register_consumer(consumer_six)
        # disruptor.register_consumer(consumer_seven)
        # disruptor.register_consumer(consumer_eight)
        # disruptor.register_consumer(consumer_nine)
        # disruptor.register_consumer(consumer_ten)
        
        for i in range(10_000):
            print(f"Producing {i}")
            disruptor.produce([i])
        
        time.sleep(2)
    finally:
        disruptor.close()

if __name__ == "__main__":
    main()