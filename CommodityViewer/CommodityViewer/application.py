"""
This script runs the CommodityViewer application using a development server.
"""

from os import environ
from CommodityViewer import application

if __name__ == '__main__':
    HOST = environ.get('SERVER_HOST', 'localhost')
    try:
        PORT = int(environ.get('SERVER_PORT', '5555'))
    except ValueError:
        PORT = 5555
    application.run(HOST, PORT)
