import tornado.options
import random
tornado.options.define("environment", default="dev", help="environment")

def randomize(values):
    """ this is a wrapper that returns a function which when called returns a random value"""
    def picker():
        return random.choice(values)
    return picker

options = {
    'dev' : {
        'mongo_database' : {'host' : '127.0.0.1', 'port' : 27017, 'dbname' : 'testdb', 'maxconnections':5}
    }
}

default = {}

def get(key):
    env = tornado.options.options.environment
    if env not in options:
        raise Exception("Invalid Environment (%s)" % env)
    v = options.get(env).get(key) or default.get(key)
    if callable(v):
        return v()
    return v
