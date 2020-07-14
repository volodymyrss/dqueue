from setuptools import setup

setup(name='dda-queue',
      version='1.1.15',
      description='a queue manager (yet another)',
      author='Volodymyr Savchenko',
      author_email='vladimir.savchenko@gmail.com',
      license='MIT',
      packages=['dqueue'],
      entry_points={
          'console_scripts':  [
              'dqueue=dqueue.cli:main',
                ]
          },
      install_requires=[
          'marshmallow',
          'apispec',
          'flasgger',
          'bravado',
          'termcolor',
          ],
      zip_safe=False,
     )
