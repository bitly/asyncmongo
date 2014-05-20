import os
from distutils.core import setup

# also update version in __init__.py
version = '1.3'

setup(
    name="asyncmongo",
    version=version,
    keywords=["mongo", "mongodb", "pymongo", "asyncmongo", "tornado"],
    long_description=open(os.path.join(os.path.dirname(__file__),"README.md"), "r").read(),
    description="Asynchronous library for accessing mongodb built upon the tornado IOLoop.",
    author="Jehiah Czebotar",
    author_email="jehiah@gmail.com",
    url="http://github.com/bitly/asyncmongo",
    license="Apache Software License",
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
    ],
    packages=['asyncmongo', 'asyncmongo.backends'],
    requires=['pymongo (>=1.9)', 'tornado'],
    download_url="https://bitly-downloads.s3.amazonaws.com/asyncmongo/asyncmongo-%s.tar.gz" % version,
)
