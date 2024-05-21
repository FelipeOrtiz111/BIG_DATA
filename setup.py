from setuptools import setup, find_packages

setup(
    name='dataflow_pipeline',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'apache-beam[gcp]',
        'requests',
        'beautifulsoup4'
    ]
)
