from setuptools import setup, find_packages

setup(
    name="factory_design_pattern",
    version="1.0.0",
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'main_etl = main_etl:main',
        ],
    },
    install_requires=[
        "pyspark>=3.0.0",
    ],
)
