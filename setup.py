from setuptools import setup, find_packages

setup(
    name='pytaxi',
    version='0.1',
    author="gmoben",
    author_email="ben@warr.io",
    description='Multi-engine message bus wrapper',
    long_description="**Mutli-engine message bus wrapper**",
    long_descritpion_cotent_type='text/markdown',
    url="https://github.com/gmoben/taxi",
    packages=find_packages(),
    zip_safe=True,
    package_data={'taxi': ['config/*.yaml']},
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'taxi=taxi.cli:main'
            ]
    },
)
