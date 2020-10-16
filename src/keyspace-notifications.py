import redis
import threading
import random
import time

redisHost = 'localhost'
redisPort = 6379
keyPrefix = 'SESSIONS:'
random.seed(1)

class NoficationListener:
    def __init__(self, redis_client):
        self.redis_client= redis_client        

    def listen(self):
        # initialize keyspace notifications for hash key events and expiry 
        # K = keyspace events; h = hash commands; x = expired events
        # NOTE: this only needs to be enabled once 
        self.redis_client.config_set('notify-keyspace-events', 'Khx')
        
        # subscribe to keypace notifications with the SESSIONS: prefix 
        p = self.redis_client.pubsub()
        p.psubscribe('__key*__:{keyPrefix}:*')

        # infinitely listen for new messages and print output
        while True:
            ### Grab a message from the channel(s) subscribed to (note: Python implementation specfic)
            message = p.get_message(timeout=10.0)
            print(f'Notfication recieved: \"{message}\"')
        

# class to create and add sessions to the redis database 
class SessionCreator:
    def __init__(self, redis_client):
        self.redis_client= redis_client
    
    def addSessions(self):
        interval = 6

        # infinitely add sessions to Redis at interval seconds 
        while True:
            # create a pseudo random session and user id 
            sessionId = random.randint(1000, 2000)
            userId = random.randint(9000, 10000)
            currentTime = time.time()

            # create session key and value
            sessionKey = '{keyPrefix}:{sessionId}'
            sessionValue = {'user':userId, 'timestamp':str(currentTime)}

            print(sessionKey)

            # create a transaction to add session and expiration of interval * 1.5
            # NOTE: redis-py's pipline method executes a MULTI/EXEC transaction
            p = self.redis_client.pipeline()
            p.hset(sessionKey, mapping=sessionValue)
            p.expire(sessionKey, interval + int(interval/2))
            p.execute()

            #print(self.redis_client.hgetall(sessionKey))

            time.sleep(interval)


def main():
    print('starting \'er up, cap\'n!')
    redis_client = redis.Redis(redisHost, redisPort, decode_responses=True)
    redis_client.set('mykey', 'you there')
    print(redis_client.get('mykey'))

    sessionCreator = SessionCreator(redis_client)
    notificationListener = NoficationListener(redis_client)

    # create and execute threads to simultaneously create sessions and listen for events
    t1 = threading.Thread(target=notificationListener.listen)
    t2 = threading.Thread(target=sessionCreator.addSessions)
    t1.start()
    t2.start()



if __name__ == "__main__":
    main()