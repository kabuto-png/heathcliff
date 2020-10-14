from setuptools import setup, find_packages

setup(name='heathcliff',
      version='0.0.1',
      description='Xyla\'s Python Apple Search Ads API client.',
      url='https://github.com/xyla-io/heathcliff',
      author='Gregory Klein',
      author_email='gklei89@gmail.com',
      license='MIT',
      packages=find_packages(),
      install_requires=[
        'pandas',
        'requests',
      ],
      zip_safe=False)
