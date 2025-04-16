from setuptools import setup, find_packages

setup(
    name="gOS-api-sdk",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[],
    author="Team Implementaion",
    author_email="implementation@polyteia.com",
    description="Python SDK for the gOS reporting dashboard API",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/polyteia-de/gOS-api-toolkit.git",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: None",
    ],
    python_requires='>=3.7',
)
