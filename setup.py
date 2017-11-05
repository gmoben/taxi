from setuptools import setup, find_packages

setup(name='taxi',
      version='0.0',
      description='Multi-engine message bus wrapper',
      packages=find_packages(),
      zip_safe=True,
      entry_points={
          'console_scripts': [
              'taxi=taxi.cli:main'
              ]
      },
)
