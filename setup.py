from setuptools import setup

setup(name='filexfer',
      version='0.1',
      description='tansfer files in local network',
      author='sycoleo',
      packages=['filexfer'],
      install_requires=[
          'pyOpenSSL',
          'websockets',
          'aiortc',
      ],
      entry_points={
          'console_scripts': ['filexfer=filexfer.filexfer:main']
          },
      zip_safe=False)
