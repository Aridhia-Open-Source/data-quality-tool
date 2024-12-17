from setuptools import setup, find_packages

# Function to read the requirements.txt file
def read_requirements():
    with open('requirements.txt') as f:
        return f.read().splitlines()

setup(
    name='dataqualitycheck',
    version='0.1',
    packages=find_packages(),
    install_requires=read_requirements(),
)