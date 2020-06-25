from setuptools import setup

setup(name='dqueue',
      version='1.0',
      description='a queue manager (yet another)',
      author='Volodymyr Savchenko',
      author_email='vladimir.savchenko@gmail.com',
      license='MIT',
      py_modules=['dqueue'],
      entry_points={
          'console_scripts':  [
              'dqueue=dqueue:cli',
                ]
          },
      install_requires=[
          'marshmallow',
          'apispec',
          'flasgger',
          ],
      zip_safe=False,
     )
