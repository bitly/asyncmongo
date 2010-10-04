import os
from distutils.core import setup

version = '0.0.1'

setup(
    name="asyncmongo",
    version=version,
    keywords=["mongo", "mongodb", "pymongo", "asyncmongo"],
    long_description=open(os.path.join(os.path.dirname(__file__),"README.mk"), "r").read(),
    description="Asyncronus library for accessing mongodb built upon the tornado IOLoop.",
    author="Jehiah Czebotar",
    author_email="jehiah@bit.ly",
    url="http://github.com/bitly/asyncmongo",
    packages=['src'],
    # download_url="http://github.com/downloads/bitly/asyncmongo/asyncmongo-%s.tar.gz" % version,
)
