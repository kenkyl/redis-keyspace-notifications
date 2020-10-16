import redis
import threading
import random
import time

redisHost = 'localhost'
redisPort = 6379
random.seed(1)

# class to create and add sessions to the redis database 
class SessionCreator:
    def __init__(self, redis_client):
        self.r = redis_client
    
    def addSessions(self):
        interval = 3

        # infinitely add sessions to Redis at interval seconds 
        while True:
            # create a pseudo random session and user id 
            sessionId = random.randint(1000, 2000)
            userId = random.randint(9000, 10000)
            currentTime = time.time()

            sessionKey = 'SESSIONS:{sessionId}'
            sessionValue = {'user':userId, 'timestamp':str(currentTime)}

            # NOTE: redis-py's pipline method executes a MULTI/EXEC transaction
            p = self.r.pipeline()
            p.hset(sessionKey, mapping=sessionValue)
            p.expire(sessionKey, interval + int(interval/2))
            p.execute()

            print(self.r.hgetall(sessionKey))

            time.sleep(5.0)


def main():
    print('starting \'er up, cap\'n!')
    r = redis.Redis(redisHost, redisPort, decode_responses=True)
    r.set('mykey', 'you there')
    print(r.get('mykey'))

    sessionCreator = SessionCreator(r)

    t1 = threading.Thread(target=sessionCreator.addSessions)
    t1.start()



if __name__ == "__main__":
    main()