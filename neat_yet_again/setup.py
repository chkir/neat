try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

config = {
    'description': 'NEAT prototype',
    'author': 'Matt Amos',
    'url': '',
    'download_url': '.',
    'author_email': 'matt.amos@mapzen.com',
    'version': '0.1',
    'tests_require': ['nose'],
    'packages': ['neat'],
    'scripts': [],
    'name': 'neat',
    'test_suite': 'nose.collector'
}

setup(**config)
