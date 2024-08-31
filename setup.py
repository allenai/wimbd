from setuptools import setup, find_packages

setup(
    name='wimbd',
    version='0.1.1',
    author='Yanai Elazar, Akshita Bhagia, Ian Magnusson, Abhilasha Ravichander, Dustin Schwenk, Alane Suhr, Pete Walsh, Dirk Groeneveld, Luca Soldaini, Sameer Singh, Hanna Hajishirzi, Noah A. Smith, Jesse Dodge',
    author_email='yanaiela@gmail.com',
    description='An elasticsearch wrapper that allows to query ES indices',
    long_description=open('wimbd/es/README.md').read(),
    long_description_content_type='text/markdown',
    url='https://wimbd.apps.allenai.org/',
    packages=find_packages(include=['wimbd.es']),  # Automatically find packages in the directory # include=['wimbd.es']
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.9',
    install_requires=[
        'elasticsearch>=8.6.2',
        'pyyaml'
    ],
)
