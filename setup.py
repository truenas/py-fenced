from setuptools import find_packages, setup


setup(
    name='fenced',
    version='0.0.1',
    description='TrueNAS SCALE Fencing Daemon',
    packages=find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3',
    ],
    install_requires=[],
    entry_points={
        'console_scripts': [
            'fenced = fenced.main:main',
        ],
    },
)
