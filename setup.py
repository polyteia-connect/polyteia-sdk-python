from setuptools import setup, find_packages

# Helper function to read requirements.txt
def load_requirements(filename):
    with open(filename) as f:
        return f.read().splitlines()

setup(
    name="gOS-api-sdk",
    version="0.1.12",
    packages=find_packages(),
    install_requires=load_requirements("requirements.txt"),
    extras_require={
        "spark": ["pyspark>=3.4.0"]  # Optional
    },
    include_package_data=True,
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
