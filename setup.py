from setuptools import setup, find_packages

version = {}


def last_version():
    with open("./ffcclient/version.py") as fp:
        exec(fp.read(), version)
    return version['VERSION']


ffc_version = last_version()


def parse_requirements(filename):
    lineiter = (line.strip() for line in open(filename))
    return [line for line in lineiter if line and not line.startswith("#")]


base_reqs = parse_requirements('./requirements.txt')
dev_reqs = parse_requirements('./dev-requirements.txt')

with open('README.md') as f:
    long_description = f.read()

setup(
    name='ffc-server-python-sdk',
    version=ffc_version,
    author='Dian SUN',
    author_email='dian.sun0720@gmail.com',
    packages=find_packages(),
    url='https://github.com/feature-flags-co/ffc-server-python-sdk',
    project_urls={
        'Code': 'https://github.com/feature-flags-co/ffc-server-python-sdk',
        'Issue tracker': 'https://github.com/feature-flags-co/ffc-server-python-sdk/issues',
    },
    description='A Python Server SDK for featureflag.co project',
    long_description=long_description,
    long_description_content_type='text/markdown',
    install_requires=base_reqs,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Topic :: Software Development',
        'Topic :: Software Development :: Libraries',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
    extras_require={
        "dev": dev_reqs
    },
    python_requires='>=3.6, <=3.9'
)