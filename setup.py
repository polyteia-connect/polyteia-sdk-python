from setuptools import setup, find_packages

# Helper function to read requirements.txt
def load_requirements(filename):
    with open(filename) as f:
        return f.read().splitlines()

setup(
    name="polyteia-sdk-python",
    version="0.1.13",
    packages=find_packages(),
    install_requires=load_requirements("requirements.txt"),
    extras_require={
        "spark": ["pyspark>=3.4.0"]  # Optional
    },
    include_package_data=True,
    author="Team Implementaion",
    author_email="support@polyteia.com",
    description="Python SDK for interacting with the Polyteia API",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/polyteia-connect/polyteia-sdk-python.git",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: None",
    ],
    python_requires='>=3.7',
)
