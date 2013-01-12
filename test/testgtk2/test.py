#!/usr/bin/env python

import os
import base64
import pygtk
pygtk.require('2.0')
import gtk
import asyncmongo

database= {'host' : '127.0.0.1', 'port' : 27018, 'dbname' : 'testdb', 'maxconnections':5}

class TestApp(object):
    def __init__(self):
        self.__win = gtk.Window()
        self.__win.set_title("AsyncMongo test")
        box = gtk.VBox()
        self.__win.add(box)
        
        self.message = gtk.Label('')
        box.pack_start(self.message)

        btn = gtk.Button(label="Test Insert")
        box.pack_start(btn)
        btn.connect('clicked', self._on_insert_clicked)
        
        btn = gtk.Button(label="Test Query")
        box.pack_start(btn)
        btn.connect('clicked', self._on_query_clicked)
        
        self._db = asyncmongo.Client(pool_id='test_pool', backend="glib2", **database)
    
    def _on_query_clicked(self, obj):
        self._db.test.find({}, callback=self._on_query_response)

    def _on_query_response(self, data, error):
        if error:
            self.message.set_text(error)
        
        self.message.set_text('Query OK, %d objects found' % len(data))
            
    def _on_insert_clicked(self, obj):
        rand = base64.b64encode(os.urandom(32))
        try:
            self._db.test.insert({ 'blah': rand }, callback=self._on_insertion)
        except Exception, e:
            print e
            
    def _on_insertion(self, data, error):
        if error:
            self.message.set_text(error)
        
        self.message.set_text("Insert OK")
        
    def show(self):
        self.__win.show_all()

if __name__ == "__main__":
    app = TestApp()
    app.show()
    gtk.main()
