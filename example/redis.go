package main

type redisProcessorFactory struct{}

func (r *redisProcessorFactory) NewRedisClientProcessor() RedisClientProcessor {
}
